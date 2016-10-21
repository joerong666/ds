#include "redis.h"
#include "lzf.h"    /* LZF compression library */

#include "ds_log.h"
#include "zmalloc.h"
#include "ds_util.h"

#include <string.h>
#include <math.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <arpa/inet.h>
#include <sys/stat.h>

#define MOVE_BUF_PTR(PTR, OFFSET) \
{\
    if (PTR)\
    {\
        PTR += OFFSET;\
    }\
}

/* Convenience wrapper around fwrite, that returns the number of bytes written
 * to the file instead of the number of objects (see fwrite(3)) and -1 in the
 * case of an error. It also supports a NULL *fp to skip writing altogether
 * instead of writing to /dev/null. */
static int rdbWriteRaw(char *fp, const void *p, size_t len) 
{
    if (fp != NULL)
    {
        memcpy(fp, p, len);
    }
    return len;
}

static int rdbSaveType(char *fp, unsigned char type) 
{
    return rdbWriteRaw(fp, &type, sizeof(type));
}

static int rdbSaveVersion(char *fp, uint64_t version) 
{
    uint64_t v = htonll(version);
    return rdbWriteRaw(fp, &v, sizeof(version));
}

static int rdbSaveRsvd(char *fp, uint32_t reserved) 
{
    uint32_t v = htonl(reserved);
    return rdbWriteRaw(fp, &v, sizeof(reserved));
}

static int rdbSaveExpire(char *fp, uint32_t expire) 
{
    uint32_t v = htonl(expire);
    return rdbWriteRaw(fp, &v, sizeof(expire));
}

static int rdbSaveTimestamp(char *fp, uint64_t ts) 
{
    uint64_t v = htonll(ts);
    return rdbWriteRaw(fp, &v, sizeof(ts));
}

#if 0
static int rdbSaveTime(char *fp, time_t t) 
{
    int32_t t32 = (int32_t) t;
    return rdbWriteRaw(fp, &t32, 4);
}
#endif

static int rdbSaveLen(char *str, uint32_t len) 
{
    char *fp = str;
    unsigned char buf[2];
    int nwritten;

    if (len < (1<<6)) {
        /* Save a 6 bit len */
        buf[0] = (len&0xFF)|(REDIS_RDB_6BITLEN<<6);
        if (rdbWriteRaw(fp,buf,1) == -1) return -1;
        nwritten = 1;
    } else if (len < (1<<14)) {
        /* Save a 14 bit len */
        buf[0] = ((len>>8)&0xFF)|(REDIS_RDB_14BITLEN<<6);
        buf[1] = len&0xFF;
        if (rdbWriteRaw(fp,buf,2) == -1) return -1;
        nwritten = 2;
    } else {
        /* Save a 32 bit len */
        buf[0] = (REDIS_RDB_32BITLEN<<6);
        if (rdbWriteRaw(fp,buf,1) == -1) return -1;
        MOVE_BUF_PTR(fp, 1);
        len = htonl(len);
        if (rdbWriteRaw(fp,&len,4) == -1) return -1;
        nwritten = 1+4;
    }
    return nwritten;
}

/* Encode 'value' as an integer if possible (if integer will fit the
 * supported range). If the function sucessful encoded the integer
 * then the (up to 5 bytes) encoded representation is written in the
 * string pointed by 'enc' and the length is returned. Otherwise
 * 0 is returned. */
static int rdbEncodeInteger(long long value, unsigned char *enc)
{
    /* Finally check if it fits in our ranges */
    if (value >= -(1<<7) && value <= (1<<7)-1) {
        enc[0] = (REDIS_RDB_ENCVAL<<6)|REDIS_RDB_ENC_INT8;
        enc[1] = value&0xFF;
        return 2;
    } else if (value >= -(1<<15) && value <= (1<<15)-1) {
        enc[0] = (REDIS_RDB_ENCVAL<<6)|REDIS_RDB_ENC_INT16;
        enc[1] = value&0xFF;
        enc[2] = (value>>8)&0xFF;
        return 3;
    } else if (value >= -((long long)1<<31) && value <= ((long long)1<<31)-1) {
        enc[0] = (REDIS_RDB_ENCVAL<<6)|REDIS_RDB_ENC_INT32;
        enc[1] = value&0xFF;
        enc[2] = (value>>8)&0xFF;
        enc[3] = (value>>16)&0xFF;
        enc[4] = (value>>24)&0xFF;
        return 5;
    } else {
        return 0;
    }
}

/* String objects in the form "2391" "-100" without any space and with a
 * range of values that can fit in an 8, 16 or 32 bit signed value can be
 * encoded as integers to save space */
static int rdbTryIntegerEncoding(char *s, size_t len, unsigned char *enc)
{
    long long value;
    char *endptr, buf[32];

    /* Check if it's possible to encode this value as a number */
    value = strtoll(s, &endptr, 10);
    if (endptr[0] != '\0') return 0;
    ll2string(buf,32,value);

    /* If the number converted back into a string is not identical
     * then it's not possible to encode the string as integer */
    if (strlen(buf) != len || memcmp(buf,s,len)) return 0;

    return rdbEncodeInteger(value,enc);
}

static int rdbSaveLzfStringObject(char *str, const unsigned char *s, size_t len) 
{
    size_t comprlen, outlen;
    unsigned char byte;
    int n, nwritten = 0;
    void *out;
    char *fp = str;

    /* We require at least four bytes compression for this to be worth it */
    if (len <= 4) return 0;
    outlen = len-4;
    if ((out = zmalloc(outlen+1)) == NULL) return 0;
    comprlen = lzf_compress(s, len, out, outlen);
    if (comprlen == 0) {
        zfree(out);
        return 0;
    }
    /* Data compressed! Let's save it on disk */
    byte = (REDIS_RDB_ENCVAL<<6)|REDIS_RDB_ENC_LZF;
    if ((n = rdbWriteRaw(fp,&byte,1)) == -1) goto writeerr;
    nwritten += n;
    MOVE_BUF_PTR(fp, n);

    if ((n = rdbSaveLen(fp,comprlen)) == -1) goto writeerr;
    nwritten += n;
    MOVE_BUF_PTR(fp, n);

    if ((n = rdbSaveLen(fp,len)) == -1) goto writeerr;
    nwritten += n;
    MOVE_BUF_PTR(fp, n);

    if ((n = rdbWriteRaw(fp,out,comprlen)) == -1) goto writeerr;
    nwritten += n;
    MOVE_BUF_PTR(fp, n);

    zfree(out);
    return nwritten;

writeerr:
    zfree(out);
    return -1;
}

/* Save a string objet as [len][data] on disk. If the object is a string
 * representation of an integer value we try to save it in a special form */
static int rdbSaveRawString(char *str, const unsigned char *s, size_t len) 
{
    char *fp = str;
    int enclen;
    int n, nwritten = 0;

    /* Try integer encoding */
    if (len <= 11) {
        unsigned char buf[5];
        if ((enclen = rdbTryIntegerEncoding((char*)s,len,buf)) > 0) {
            if (rdbWriteRaw(fp,buf,enclen) == -1) return -1;
            return enclen;
        }
    }

    /* Try LZF compression - under 20 bytes it's unable to compress even
     * aaaaaaaaaaaaaaaaaa so skip it */
    if (server.rdbcompression && len > 20) {
        n = rdbSaveLzfStringObject(fp,s,len);
        if (n == -1) return -1;
        if (n > 0) return n;
        /* Return value of 0 means data can't be compressed, save the old way */
    }

    /* Store verbatim */
    if ((n = rdbSaveLen(fp,len)) == -1) return -1;
    nwritten += n;
    MOVE_BUF_PTR(fp, n);
    if (len > 0) {
        if (rdbWriteRaw(fp,s,len) == -1) return -1;
        nwritten += len;
    }
    return nwritten;
}

/* Save a long long value as either an encoded string or a string. */
static int rdbSaveLongLongAsStringObject(char *str, long long value)
{
    char *fp = str;
    unsigned char buf[32];
    int n, nwritten = 0;
    int enclen = rdbEncodeInteger(value,buf);
    if (enclen > 0) {
        return rdbWriteRaw(fp,buf,enclen);
    } else {
        /* Encode as string */
        enclen = ll2string((char*)buf,32,value);
        redisAssert(enclen < 32);
        if ((n = rdbSaveLen(fp,enclen)) == -1) return -1;
        nwritten += n;
        MOVE_BUF_PTR(fp, n);
        if ((n = rdbWriteRaw(fp,buf,enclen)) == -1) return -1;
        nwritten += n;
    }
    return nwritten;
}

/* Like rdbSaveStringObjectRaw() but handle encoded objects */
static int rdbSaveStringObject(char *fp, const robj *obj)
{
    /* Avoid to decode the object, then encode it again, if the
     * object is alrady integer encoded. */
    if (obj->encoding == REDIS_ENCODING_INT) {
        return rdbSaveLongLongAsStringObject(fp,(long)obj->ptr);
    } else {
        if (obj->encoding != REDIS_ENCODING_RAW)
        {
            log_error("obj->encoding=%d, type=%d", obj->encoding, obj->type);
        }
        redisAssert(obj->encoding == REDIS_ENCODING_RAW);
        return rdbSaveRawString(fp,obj->ptr,sdslen(obj->ptr));
    }
}

/* Save a double value. Doubles are saved as strings prefixed by an unsigned
 * 8 bit integer specifing the length of the representation.
 * This 8 bit integer has special values in order to specify the following
 * conditions:
 * 253: not a number
 * 254: + inf
 * 255: - inf
 */
static int rdbSaveDoubleValue(char *fp, double val) 
{
    unsigned char buf[128];
    int len;

    if (isnan(val)) {
        buf[0] = 253;
        len = 1;
    } else if (!isfinite(val)) {
        len = 1;
        buf[0] = (val < 0) ? 255 : 254;
    } else {
#if (DBL_MANT_DIG >= 52) && (LLONG_MAX == 0x7fffffffffffffffLL)
        /* Check if the float is in a safe range to be casted into a
         * long long. We are assuming that long long is 64 bit here.
         * Also we are assuming that there are no implementations around where
         * double has precision < 52 bit.
         *
         * Under this assumptions we test if a double is inside an interval
         * where casting to long long is safe. Then using two castings we
         * make sure the decimal part is zero. If all this is true we use
         * integer printing function that is much faster. */
        double min = -4503599627370495; /* (2^52)-1 */
        double max = 4503599627370496; /* -(2^52) */
        if (val > min && val < max && val == ((double)((long long)val)))
            ll2string((char*)buf+1,sizeof(buf),(long long)val);
        else
#endif
            snprintf((char*)buf+1,sizeof(buf)-1,"%.17g",val);
        buf[0] = strlen((char*)buf+1);
        len = buf[0]+1;
    }
    return rdbWriteRaw(fp,buf,len);
}

/* Save a Redis object. */
static int SaveObject(char *str, const robj *o)
{
    char *fp = str;
    int n, nwritten = 0;

    if (o->type == REDIS_STRING) {
        /* Save a string value */
        if ((n = rdbSaveStringObject(fp,o)) == -1) return -1;
        nwritten += n;
        MOVE_BUF_PTR(fp, n);
    } else if (o->type == REDIS_LIST) {
        /* Save a list value */
        if (o->encoding == REDIS_ENCODING_ZIPLIST) {
            size_t l = ziplistBlobLen((unsigned char*)o->ptr);

            if ((n = rdbSaveRawString(fp,o->ptr,l)) == -1) return -1;
            nwritten += n;
            MOVE_BUF_PTR(fp, n);
        } else if (o->encoding == REDIS_ENCODING_LINKEDLIST) {
            list *list = o->ptr;
            listIter li;
            listNode *ln;

            if ((n = rdbSaveLen(fp,listLength(list))) == -1) return -1;
            nwritten += n;
            MOVE_BUF_PTR(fp, n);

            listRewind(list,&li);
            while((ln = listNext(&li))) {
                robj *eleobj = listNodeValue(ln);
                if ((n = rdbSaveStringObject(fp,eleobj)) == -1) return -1;
                nwritten += n;
                MOVE_BUF_PTR(fp, n);
            }
        } else {
            log_fatal("Unknown list encoding");
            return -1;
        }
    } else if (o->type == REDIS_SET) {
        /* Save a set value */
        if (o->encoding == REDIS_ENCODING_HT) {
            dict *set = o->ptr;
            dictIterator *di = dictGetIterator(set);
            dictEntry *de;

            if ((n = rdbSaveLen(fp,dictSize(set))) == -1) return -1;
            nwritten += n;
            MOVE_BUF_PTR(fp, n);

            while((de = dictNext(di)) != NULL) {
                robj *eleobj = dictGetEntryKey(de);
                if ((n = rdbSaveStringObject(fp,eleobj)) == -1) return -1;
                nwritten += n;
                MOVE_BUF_PTR(fp, n);
            }
            dictReleaseIterator(di);
        } else if (o->encoding == REDIS_ENCODING_INTSET) {
            size_t l = intsetBlobLen((intset*)o->ptr);

            if ((n = rdbSaveRawString(fp,o->ptr,l)) == -1) return -1;
            nwritten += n;
            MOVE_BUF_PTR(fp, n);
        } else {
            log_fatal("Unknown set encoding");
            return -1;
        }
    } else if (o->type == REDIS_ZSET) {
        /* Save a sorted set value */
        if (o->encoding == REDIS_ENCODING_ZIPLIST) {
            size_t l = ziplistBlobLen((unsigned char*)o->ptr);

            if ((n = rdbSaveRawString(fp,o->ptr,l)) == -1) return -1;
            nwritten += n;
            MOVE_BUF_PTR(fp, n);
        } else if (o->encoding == REDIS_ENCODING_SKIPLIST) {
            zset *zs = o->ptr;
            dictIterator *di = dictGetIterator(zs->dict);
            dictEntry *de;

            if ((n = rdbSaveLen(fp,dictSize(zs->dict))) == -1) return -1;
            nwritten += n;
            MOVE_BUF_PTR(fp, n);

            while((de = dictNext(di)) != NULL) {
                robj *eleobj = dictGetEntryKey(de);
                double *score = dictGetEntryVal(de);

                if ((n = rdbSaveStringObject(fp,eleobj)) == -1) return -1;
                nwritten += n;
                MOVE_BUF_PTR(fp, n);
                if ((n = rdbSaveDoubleValue(fp,*score)) == -1) return -1;
                nwritten += n;
                MOVE_BUF_PTR(fp, n);
            }
            dictReleaseIterator(di);
        } else {
            log_fatal("Unknown sorted set encoding");
            return -1;
        }
    } else if (o->type == REDIS_HASH) {
        /* Save a hash value */
        if (o->encoding == REDIS_ENCODING_ZIPMAP) {
            size_t l = zipmapBlobLen((unsigned char*)o->ptr);

            if ((n = rdbSaveRawString(fp,o->ptr,l)) == -1) return -1;
            nwritten += n;
            MOVE_BUF_PTR(fp, n);
        } else {
            dictIterator *di = dictGetIterator(o->ptr);
            dictEntry *de;

            if ((n = rdbSaveLen(fp,dictSize((dict*)o->ptr))) == -1) return -1;
            nwritten += n;
            MOVE_BUF_PTR(fp, n);

            while((de = dictNext(di)) != NULL) {
                robj *key = dictGetEntryKey(de);
                robj *val = dictGetEntryVal(de);

                if ((n = rdbSaveStringObject(fp,key)) == -1) return -1;
                nwritten += n;
                MOVE_BUF_PTR(fp, n);
                if ((n = rdbSaveStringObject(fp,val)) == -1) return -1;
                nwritten += n;
                MOVE_BUF_PTR(fp, n);
            }
            dictReleaseIterator(di);
        }
    } else {
        log_fatal("Unknown object type");
        return -1;
    }
    return nwritten;
}

static int getObjSaveType(const robj *o)
{
    /* Fix the type id for specially encoded data types */
    if (o->type == REDIS_HASH && o->encoding == REDIS_ENCODING_ZIPMAP)
        return REDIS_HASH_ZIPMAP;
    else if (o->type == REDIS_LIST && o->encoding == REDIS_ENCODING_ZIPLIST)
        return REDIS_LIST_ZIPLIST;
    else if (o->type == REDIS_SET && o->encoding == REDIS_ENCODING_INTSET)
        return REDIS_SET_INTSET;
    else if (o->type == REDIS_ZSET && o->encoding == REDIS_ENCODING_ZIPLIST)
        return REDIS_ZSET_ZIPLIST;
    else
        return o->type;
}

int serializeObjLen(const robj *o)
{
    int len = -1;
    if (o)
    {
        /* format: type + version + [resered] + [expired] + value */
        len = 0;
        const int otype = getObjSaveType(o);
        const int t_len = rdbSaveType(0, otype);
        len += t_len;
        const int ver_len = rdbSaveVersion(0, o->version);
        len += ver_len;
        if (o->rsvd_bit)
        {
            const int rsvd_len = rdbSaveRsvd(0, 0);
            len += rsvd_len;
        }
        const int exp_len = rdbSaveExpire(0, 0);
        len += exp_len;
        const int val_len = SaveObject(0, o);
        len += val_len;
        if (server.has_dbe == 0 && o->ts_bit)
        {
            const int ts_len = rdbSaveTimestamp(0, 0);
            len += ts_len;
        }
    }

    return len;
}

int serializeObj2Buf(const robj *o, char *buf, int len, int expire)
{
    if (o && buf && len > 0)
    {
        char *ptr = buf;
        int ret = 0;
        const int otype = getObjSaveType(o)
            + (o->rsvd_bit << 4)
            + (server.has_dbe == 0 ? (o->ts_bit << 5) : 0);
        ret = rdbSaveType(ptr, otype);
        ptr += ret;

        ret = rdbSaveVersion(ptr, o->version);
        ptr += ret;

        if (o->rsvd_bit)
        {
            ret = rdbSaveRsvd(ptr, o->reserved);
            ptr += ret;
        }

        ret = rdbSaveExpire(ptr, expire);
        ptr += ret;

        ret = SaveObject(ptr, o);
        ptr += ret;

        if (server.has_dbe == 0 && o->ts_bit)
        {
            ret = rdbSaveTimestamp(ptr, o->timestamp);
            ptr += ret;
        }

        log_debug("serializeObj2Buf() succ for version=%lld, "
                  "expire=%d, len=%d, rsvd_bit=%d",
                  o->version, expire, len, o->rsvd_bit);
        log_buffer(buf, len);

        return 0;
    }
    else
    {
        return 1;
    }
}

char *serializeObj(const robj *o, int *pLen)
{
    char *buf = 0;
    if (o && pLen)
    {
        *pLen = serializeObjLen(o);
        if (*pLen > 0)
        {
            buf = (char *)zmalloc(*pLen);
            if (buf)
            {
                serializeObj2Buf(o, buf, *pLen, 0);
            }
        }
    }

    return buf;
}

char *serializeObjExp(const robj *o, int *pLen, int expire)
{
    char *buf = 0;
    if (o && pLen)
    {
        *pLen = serializeObjLen(o);
        if (*pLen > 0)
        {
            buf = (char *)zmalloc(*pLen);
            if (buf)
            {
                serializeObj2Buf(o, buf, *pLen, expire);
            }
        }
    }

    return buf;
}

static int sread(void *ptr, int size, int n, const char *stream)
{
    if (!ptr || size <= 0 || n <= 0 || !stream)
    {
        return 0;
    }

    memcpy(ptr, stream, size * n);
    /*
    int i;
    for (i = 0; i < n; i++)
    {
        int j;
        for (j = 0; j < size; j++)
        {
            *((char *)ptr + i * size + j) = *(stream + i * size + j);
        }
    }
    */
    return n * size;
}

static int rdbLoadType(const char *fp, unsigned char *type)
{
    if (!fp)
    {
        return -1;
    }
    *type = *(const unsigned char *)fp;
    return 1;
}

static int rdbLoadVersion(const char *fp, uint64_t *version)
{
    if (!fp)
    {
        return -1;
    }
    *version = ntohll(*(const uint64_t *)fp);
    //log_debug("version=%lld", *version);
    return sizeof(*version);
}

static int rdbLoadTimestamp(const char *fp, uint64_t *ts)
{
    if (!fp)
    {
        return -1;
    }
    *ts = ntohll(*(const uint64_t *)fp);
    //log_debug("ts=%lld", *ts);
    return sizeof(*ts);
}

static int rdbLoadRsvd(const char *fp, uint32_t *reserved)
{
    if (!fp)
    {
        return -1;
    }
    *reserved = ntohl(*(const uint32_t *)fp);
    //log_debug("reserved=%d", *reserved);
    return sizeof(*reserved);
}

static int rdbLoadExpire(const char *fp, uint32_t *expire)
{
    if (!fp)
    {
        return -1;
    }
    *expire = ntohl(*(const uint32_t *)fp);
    //log_debug("expire=%d", *expire);
    return sizeof(*expire);
}

#if 0
static int rdbLoadTime(const char *fp, time_t *t32)
{
    if (!fp)
    {
        return -1;
    }
    *t32 = *(const int32_t *)fp;
    return sizeof(time_t);
}
#endif

/* Load an encoded length from the DB, see the REDIS_RDB_* defines on the top
 * of this file for a description of how this are stored on disk.
 *
 * isencoded is set to 1 if the readed length is not actually a length but
 * an "encoding type", check the above comments for more info */
static int rdbLoadLen(const char *str, int *isencoded, uint32_t *len)
{
    const char *fp = str;
    int n;
    unsigned char buf[2];
    int type;
    int nbytes = -1;

    if (!fp || !len)
    {
        return nbytes;
    }

    if (isencoded)
    {
        *isencoded = 0;
    }

    n = sread(buf, 1, 1, fp);
    fp += n;
    type = (buf[0]&0xC0)>>6;
    if (type == REDIS_RDB_6BITLEN) {
        /* Read a 6 bit len */
        *len = buf[0]&0x3F;
        nbytes = 1;
    } else if (type == REDIS_RDB_ENCVAL) {
        /* Read a 6 bit len encoding type */
        if (isencoded) *isencoded = 1;
        *len = buf[0]&0x3F;
        nbytes = 1;
    } else if (type == REDIS_RDB_14BITLEN) {
        /* Read a 14 bit len */
        sread(buf + 1, 1, 1, fp);
        *len = ((buf[0]&0x3F)<<8)|buf[1];
        nbytes = 2;
    } else {
        /* Read a 32 bit len */
        sread(len, 4, 1, fp);
        *len = ntohl(*len);
        nbytes = 1 + 4;
    }

    return nbytes;
}

/* Load an integer-encoded object from file 'fp', with the specified
 * encoding type 'enctype'. If encode is true the function may return
 * an integer-encoded object as reply, otherwise the returned object
 * will always be encoded as a raw string. */
static int rdbLoadIntegerObject(const char *fp, int enctype, int encode, robj **o)
{
    unsigned char enc[4];
    long long val;
    int nbytes = -1;

    if (enctype == REDIS_RDB_ENC_INT8) {
        if (sread(enc,1,1,fp) == 0) return nbytes;
        val = (signed char)enc[0];
        nbytes = 1;
    } else if (enctype == REDIS_RDB_ENC_INT16) {
        uint16_t v;
        if (sread(enc,2,1,fp) == 0) return nbytes;
        v = enc[0]|(enc[1]<<8);
        val = (int16_t)v;
        nbytes = 2;
    } else if (enctype == REDIS_RDB_ENC_INT32) {
        uint32_t v;
        if (sread(enc,4,1,fp) == 0) return nbytes;
        v = enc[0]|(enc[1]<<8)|(enc[2]<<16)|(enc[3]<<24);
        val = (int32_t)v;
        nbytes = 4;
    } else {
        val = 0; /* anti-warning */
        log_fatal("Unknown RDB integer encoding type");
        return -1;
    }
    if (encode)
    {
        *o = createStringObjectFromLongLong(val);
    }
    else
    {
        *o = createObject(REDIS_STRING,sdsfromlonglong(val));
    }
    return nbytes;
}

static int rdbLoadLzfStringObject(const char *fp, robj **o)
{
    const char *s = fp;
    unsigned int len, clen;
    sds val = NULL;
    int ret;
    int nbytes = -1;

    if ((ret = rdbLoadLen(s, NULL, &clen)) < 0) return nbytes;
    s += ret;
    if ((ret = rdbLoadLen(s, NULL, &len)) < 0) return nbytes;
    s += ret;
    if ((val = sdsnewlen(NULL,len)) == NULL) goto err;
    if (lzf_decompress(s,clen,val,len) == 0) goto err;
    *o = createObject(REDIS_STRING,val);
    nbytes = s - fp + clen;
    return nbytes;
err:
    if (val)
    {
        sdsfree(val);
    }
    return nbytes;
}

static int rdbGenericLoadStringObject(const char *s, int encode, robj **o)
{
    const char *fp = s;
    int isencoded;
    uint32_t len;
    sds val;
    int ret;

    ret = rdbLoadLen(fp, &isencoded, &len);
    if (ret < 0)
    {
        return -1;
    }
    fp += ret;
    if (isencoded) {
        switch(len) {
        case REDIS_RDB_ENC_INT8:
        case REDIS_RDB_ENC_INT16:
        case REDIS_RDB_ENC_INT32:
            ret = rdbLoadIntegerObject(fp,len,encode,o);
            if (ret <= 0)
            {
                return -1;
            }
            return fp - s + ret;
        case REDIS_RDB_ENC_LZF:
            ret = rdbLoadLzfStringObject(fp,o);
            if (ret <= 0)
            {
                return -1;
            }
            return fp - s + ret;
        default:
            log_fatal("Unknown RDB encoding type");
            return -1;
        }
    }

    val = sdsnewlen(NULL,len);
    if (len && sread(val,len,1,fp) == 0) {
        sdsfree(val);
        return -1;
    }
    *o = createObject(REDIS_STRING,val);
    return fp - s + len;
}

static int rdbLoadStringObject(const char *fp, robj **o)
{
    return rdbGenericLoadStringObject(fp, 0, o);
}

static int rdbLoadEncodedStringObject(const char *fp, robj **o)
{
    return rdbGenericLoadStringObject(fp, 1, o);
}

/* For information about double serialization check rdbSaveDoubleValue() */
static int rdbLoadDoubleValue(const char *str, double *val)
{
    const char *fp = str;
    char buf[128];
    unsigned char len;
    int nbytes = -1;
    int n = 0;

    if ((n = sread(&len,1,1,fp)) == 0) return -1;
    nbytes = n;
    fp += n;
    switch (len)
    {
    case 255:
        *val = R_NegInf;
        break;
    case 254:
        *val = R_PosInf;
        break;
    case 253:
        *val = R_Nan;
        break;
    default:
        if (sread(buf,len,1,fp) == 0) return -1;
        nbytes += len;
        buf[len] = '\0';
        sscanf(buf, "%lg", val);
        break;
    }

    return nbytes;
}

/* Load a Redis object of the specified type from the specified file.
 * On success a newly allocated object is returned, otherwise NULL. */
static int LoadObject(int type, const char *s, robj **obj)
{
    const char *fp = s;
    robj *o, *ele, *dec;
    uint32_t len;
    unsigned int i;
    int nbytes = -1;

    if (type == REDIS_STRING)
    {
        /* Read string value */
        if (((nbytes = rdbLoadEncodedStringObject(fp, obj)) < 0) || !*obj) return -1;
        *obj = tryObjectEncoding(*obj);
    }
    else if (type == REDIS_LIST)
    {
        int ret;

        /* Read list value */
        if ((ret = rdbLoadLen(fp, NULL, &len)) < 0) return -1;
        fp += ret;

        /* Use a real list when there are too many entries */
        if (len > server.list_max_ziplist_entries) {
            o = createListObject();
        } else {
            o = createZiplistObject();
        }

        /* Load every single element of the list */
        while(len--) {
            if ((ret = rdbLoadEncodedStringObject(fp, &ele)) < 0) return -1;
            fp += ret;

            /* If we are using a ziplist and the value is too big, convert
             * the object to a real list. */
            if (o->encoding == REDIS_ENCODING_ZIPLIST &&
                ele->encoding == REDIS_ENCODING_RAW &&
                sdslen(ele->ptr) > server.list_max_ziplist_value)
                    listTypeConvert(o,REDIS_ENCODING_LINKEDLIST);

            if (o->encoding == REDIS_ENCODING_ZIPLIST) {
                dec = getDecodedObject(ele);
                o->ptr = ziplistPush(o->ptr,dec->ptr,sdslen(dec->ptr),REDIS_TAIL);
                decrRefCount(dec);
                decrRefCount(ele);
            } else {
                ele = tryObjectEncoding(ele);
                listAddNodeTail(o->ptr,ele);
            }
        }

        *obj = o;
        nbytes = fp - s;
    }
    else if (type == REDIS_SET)
    {
        int ret;

        /* Read list/set value */
        if ((ret = rdbLoadLen(fp,NULL,&len)) < 0) return -1;
        fp += ret;

        /* Use a regular set when there are too many entries. */
        if (len > server.set_max_intset_entries) {
            o = createSetObject();
            /* It's faster to expand the dict to the right size asap in order
             * to avoid rehashing */
            if (len > DICT_HT_INITIAL_SIZE)
                dictExpand(o->ptr,len);
        } else {
            o = createIntsetObject();
        }

        /* Load every single element of the list/set */
        for (i = 0; i < len; i++) {
            long long llval;
            if ((ret = rdbLoadEncodedStringObject(fp, &ele)) < 0) return -1;
            fp += ret;
            ele = tryObjectEncoding(ele);

            if (o->encoding == REDIS_ENCODING_INTSET) {
                /* Fetch integer value from element */
                if (isObjectRepresentableAsLongLong(ele,&llval) == REDIS_OK) {
                    o->ptr = intsetAdd(o->ptr,llval,NULL);
                } else {
                    setTypeConvert(o,REDIS_ENCODING_HT);
                    dictExpand(o->ptr,len);
                }
            }

            /* This will also be called when the set was just converted
             * to regular hashtable encoded set */
            if (o->encoding == REDIS_ENCODING_HT) {
                dictAdd((dict*)o->ptr,ele,NULL);
            } else {
                decrRefCount(ele);
            }
        }

        *obj = o;
        nbytes = fp - s;
    }
    else if (type == REDIS_ZSET) 
    {
        /* Read list/set value */
        uint32_t zsetlen;
        size_t maxelelen = 0;
        zset *zs;
        int ret;

        if ((ret = rdbLoadLen(fp,NULL,&zsetlen)) < 0) return -1;
        fp += ret;
        o = createZsetObject();
        zs = o->ptr;

        /* Load every single element of the list/set */
        while(zsetlen--) {
            robj *ele;
            double score;
            zskiplistNode *znode;

            if ((ret = rdbLoadEncodedStringObject(fp,&ele)) < 0) return -1;
            fp += ret;
            ele = tryObjectEncoding(ele);
            if ((ret = rdbLoadDoubleValue(fp,&score)) < 0) return -1;
            fp += ret;

            /* Don't care about integer-encoded strings. */
            if (ele->encoding == REDIS_ENCODING_RAW &&
                sdslen(ele->ptr) > maxelelen)
            {
                maxelelen = sdslen(ele->ptr);
            }

            znode = zslInsert(zs->zsl,score,ele);
            dictAdd(zs->dict,ele,&znode->score);
            incrRefCount(ele); /* added to skiplist */
        }

        /* Convert *after* loading, since sorted sets are not stored ordered. */
        if (zsetLength(o) <= server.zset_max_ziplist_entries &&
            maxelelen <= server.zset_max_ziplist_value)
        {
            zsetConvert(o,REDIS_ENCODING_ZIPLIST);
        }

        *obj = o;
        nbytes = fp - s;
    }
    else if (type == REDIS_HASH)
    {
        int ret;
        uint32_t hashlen;

        if ((ret = rdbLoadLen(fp,NULL,&hashlen)) < 0) return -1;
        fp += ret;
        o = createHashObject();
        /* Too many entries? Use an hash table. */
        if (hashlen > server.hash_max_zipmap_entries)
            convertToRealHash(o);
        /* Load every key/value, then set it into the zipmap or hash
         * table, as needed. */
        while(hashlen--) {
            robj *key, *val;

            if ((ret = rdbLoadEncodedStringObject(fp,&key)) < 0 || !key) return -1;
            fp += ret;
            if ((ret = rdbLoadEncodedStringObject(fp,&val)) < 0 || !val) return -1;
            fp += ret;
            /* If we are using a zipmap and there are too big values
             * the object is converted to real hash table encoding. */
            if (o->encoding != REDIS_ENCODING_HT &&
               ((key->encoding == REDIS_ENCODING_RAW &&
                sdslen(key->ptr) > server.hash_max_zipmap_value) ||
                (val->encoding == REDIS_ENCODING_RAW &&
                sdslen(val->ptr) > server.hash_max_zipmap_value)))
            {
                convertToRealHash(o);
            }

            if (o->encoding == REDIS_ENCODING_ZIPMAP) {
                unsigned char *zm = o->ptr;
                robj *deckey, *decval;

                /* We need raw string objects to add them to the zipmap */
                deckey = getDecodedObject(key);
                decval = getDecodedObject(val);
                zm = zipmapSet(zm,deckey->ptr,sdslen(deckey->ptr),
                                  decval->ptr,sdslen(decval->ptr),NULL);
                o->ptr = zm;
                decrRefCount(deckey);
                decrRefCount(decval);
                decrRefCount(key);
                decrRefCount(val);
            } else {
                key = tryObjectEncoding(key);
                val = tryObjectEncoding(val);
                dictAdd((dict*)o->ptr,key,val);
            }
        }

        *obj = o;
        nbytes = fp - s;
    }
    else if (type == REDIS_HASH_ZIPMAP ||
             type == REDIS_LIST_ZIPLIST ||
             type == REDIS_SET_INTSET ||
             type == REDIS_ZSET_ZIPLIST)
    {
        robj *aux = 0;
        int ret = rdbLoadStringObject(fp, &aux);

        if (ret < 0 || aux == NULL) return -1;
        fp += ret;
        o = createObject(REDIS_STRING,NULL); /* string is just placeholder */
        o->ptr = zmalloc(sdslen(aux->ptr));
        memcpy(o->ptr,aux->ptr,sdslen(aux->ptr));
        decrRefCount(aux);

        /* Fix the object encoding, and make sure to convert the encoded
         * data type into the base type if accordingly to the current
         * configuration there are too many elements in the encoded data
         * type. Note that we only check the length and not max element
         * size as this is an O(N) scan. Eventually everything will get
         * converted. */
        switch(type) {
            case REDIS_HASH_ZIPMAP:
                o->type = REDIS_HASH;
                o->encoding = REDIS_ENCODING_ZIPMAP;
                if (zipmapLen(o->ptr) > server.hash_max_zipmap_entries)
                    convertToRealHash(o);
                break;
            case REDIS_LIST_ZIPLIST:
                o->type = REDIS_LIST;
                o->encoding = REDIS_ENCODING_ZIPLIST;
                if (ziplistLen(o->ptr) > server.list_max_ziplist_entries)
                    listTypeConvert(o,REDIS_ENCODING_LINKEDLIST);
                break;
            case REDIS_SET_INTSET:
                o->type = REDIS_SET;
                o->encoding = REDIS_ENCODING_INTSET;
                if (intsetLen(o->ptr) > server.set_max_intset_entries)
                    setTypeConvert(o,REDIS_ENCODING_HT);
                break;
            case REDIS_ZSET_ZIPLIST:
                o->type = REDIS_ZSET;
                o->encoding = REDIS_ENCODING_ZIPLIST;
                if (zsetLength(o) > server.zset_max_ziplist_entries)
                    zsetConvert(o,REDIS_ENCODING_SKIPLIST);
                break;
            default:
                log_fatal("Unknown encoding");
                return -1;
                break;
        }

        *obj = o;
        nbytes = fp - s;
    }
    else
    {
        log_fatal("Unknown object type");
        return -1;
    }
    return nbytes;
}

robj *unserializeObj(const char *s, int len, time_t *expire)
{
    log_debug("unserializeObj(): buf=%p, len=%d", s, len);
    log_buffer(s, len);

    robj *val = 0;
    unsigned char type;
    int ret;
    const char *fp = s;

    if (!s || len <= 0)
    {
        return 0;
    }

    /* Read type */
    if ((ret = rdbLoadType(fp, &type)) < 0)
    {
        return 0;
    }
    fp += ret;
    len -= ret;

    const unsigned char rsvd_bit = type & 0x10;
    const unsigned char ts_bit = type & 0x20;
    type = type & 0x0f;

    /* Read version */
    uint64_t version;
    if ((ret = rdbLoadVersion(fp, &version)) < 0)
    {
        return 0;
    }
    fp += ret;
    len -= ret;

    /* Read reserved (optional) */
    uint32_t reserved = 0;
    if (rsvd_bit)
    {
        if ((ret = rdbLoadRsvd(fp, &reserved)) < 0)
        {
            return 0;
        }
        fp += ret;
        len -= ret;
    }

    /* Read expire */
    uint32_t exp = 0;
    if ((ret = rdbLoadExpire(fp, &exp)) < 0)
    {
        return 0;
    }
    fp += ret;
    len -= ret;
    if (expire)
    {
        *expire = exp;
    }

    /* Read value */
    if ((ret = LoadObject(type, fp, &val)) < 0)
    {
        return 0;
    }

    if (val)
    {
        val->ts_bit = 0;
        val->timestamp = 0;
        val->version = version;
        if (rsvd_bit)
        {
            val->rsvd_bit = 1;
            val->reserved = reserved;
        }
        if (server.has_dbe == 0 && ts_bit)
        {
            uint64_t ts;
            if ((ret = rdbLoadTimestamp(fp + ret, &ts)) > 0)
            {
                //log_prompt("unserializeObj: ts=%"PRIu64, ts);
                val->ts_bit = 1;
                val->timestamp = ts;
            }
        }
        log_debug("unserializeObj() succ, "
                  "version=%lld, expire=%d, rsvd_bit=%d, ts_bit=%d, reserved=%d, ts=%"PRIu64
                  , version, exp, val->rsvd_bit, val->ts_bit, val->reserved, val->timestamp);
    }
    
    return val;
}

/* return:
 * 0 - succ
 * 1 - fail
 */
int getVersion(const char *s, int len, uint64_t *version, time_t *expire)
{
    log_debug("getVersion(): buf=%p, len=%d", s, len);
    //log_buffer(s, len);

    unsigned char type;
    int ret;
    const char *fp = s;

    if (!s || len <= 0 || !version)
    {
        return 1;
    }

    /* Read type */
    if ((ret = rdbLoadType(fp, &type)) < 0)
    {
        return 1;
    }
    fp += ret;
    len -= ret;

    const unsigned char rsvd_bit = type & 0x10;
    type = type & 0x0f;

    /* Read version */
    if ((ret = rdbLoadVersion(fp, version)) < 0)
    {
        return 1;
    }
    fp += ret;
    len -= ret;

    /* Read reserved (optional) */
    uint32_t reserved = 0;
    if (rsvd_bit)
    {
        if ((ret = rdbLoadRsvd(fp, &reserved)) < 0)
        {
            return 1;
        }
        fp += ret;
        len -= ret;
    }

    /* Read expire */
    uint32_t exp = 0;
    if ((ret = rdbLoadExpire(fp, &exp)) < 0)
    {
        return 1;
    }
    fp += ret;
    len -= ret;
    if (expire)
    {
        *expire = exp;
    }

    return 0;
}

/* return:
 * 0 - succ
 * 1 - fail
 */
static int getValExpire(const char *s, int len, int *expire)
{
    log_debug("getValExpire(): buf=%p, len=%d", s, len);
    //log_buffer(s, len);

    unsigned char type;
    int ret;
    const char *fp = s;

    if (!s || len <= 0)
    {
        return 1;
    }

    /* Read type */
    if ((ret = rdbLoadType(fp, &type)) < 0)
    {
        return 1;
    }
    fp += ret;
    len -= ret;

    const unsigned char rsvd_bit = type & 0x10;
    type = type & 0x0f;

    /* Read version */
    ret = sizeof(uint64_t);
    fp += ret;
    len -= ret;

    /* Read reserved (optional) */
    if (rsvd_bit)
    {
        ret = sizeof(uint32_t);
        fp += ret;
        len -= ret;
    }

    /* Read expire */
    uint32_t exp = 0;
    if ((ret = rdbLoadExpire(fp, &exp)) < 0)
    {
        return 1;
    }
    fp += ret;
    len -= ret;
    if (expire)
    {
        *expire = exp;
    }

    return 0;
}


void log_buffer(const char *buf, int len)
{
    if (buf == 0 || len <= 0 || len > 128 || server.verbosity != REDIS_DEBUG)
    {
        return;
    }

    char log_buf[2048];
    int i;
    for (i = 0; i < len; i++)
    {
        sprintf(log_buf + i * 3, "%02x ", (unsigned char)buf[i]);
    }
    log_debug("%s", log_buf);
}

void log_string(const char *buf, int len)
{
    if (buf == 0 || len <= 0 || server.verbosity != REDIS_DEBUG)
    {
        return;
    }

    char log_buf[4096];
    snprintf(log_buf, sizeof(log_buf) - 1, "%*.s", len, buf);
    log_debug("%s", log_buf);
}

/* thread safe */
void log_buffer2(const char *buf, int len, int tag)
{
    if (buf == 0 || len <= 0 || server.verbosity != REDIS_DEBUG)
    {
        return;
    }

    char *tmp = (char *)zmalloc(len * 3 + 1);
    int i;
    for (i = 0; i < len; i++)
    {
        sprintf(tmp + i * 3, "%02x ", (unsigned char)buf[i]);
    }
    log_debug2(tag, "%s", tmp);
    zfree(tmp);
}

/* thread safe */
void log_string2(const char *buf, int len, int tag)
{
    if (buf == 0 || len <= 0 || server.verbosity != REDIS_DEBUG)
    {
        return;
    }

    char *tmp = (char *)zmalloc(len + 1);
    sprintf(tmp, "%*.s", len, buf);
    log_debug2(tag, "%s", tmp);
    zfree(tmp);
}

char *serializeStrExp(const char *buf, int buf_len, int *pLen, int expire)
{
    sds buf_sds = sdsnewlen(buf, buf_len);

    robj v;
    initObject(&v, REDIS_STRING, buf_sds);

    char *val = serializeObjExp(&v, pLen, expire);
    sdsfree(buf_sds);
    return val;
}

void freeStr(char *str)
{
    zfree(str);
}

int32_t value_filter(const char *val, size_t len, int *exp_time)
{
    int expire = 0;
    int32_t result = 0;
    if (*(const unsigned char *)val != 128)
    {
        /* string */
        int ret = getValExpire(val, len, &expire);
        if (exp_time)
        {
            *exp_time = ret == 0 ? expire : 0;
        }
        if (ret == 0 && expire > 0 && expire < time(0))
        {
            /* expired */
            result = 1;
        }
    }
    return result;
}

