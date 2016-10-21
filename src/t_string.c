#include "redis.h"

#include "dbmng.h"
#include "op_cmd.h"
#include "ds_log.h"
#include "serialize.h"
#include "ds_zmalloc.h"

/*-----------------------------------------------------------------------------
 * String Commands
 *----------------------------------------------------------------------------*/

static int checkStringLength(redisClient *c, long long size)
{
    if (size > 512*1024*1024) { // redis: 512M
        addReplyError(c,"string exceeds maximum allowed size (512MB)");
        return REDIS_ERR;
    }
    return REDIS_OK;
}

static int checkStringLength_mc(redisClient *c, long long size)
{
    if (size > 10*1024*1024) { // mc: 10M
        addReplyError(c, "object too large for cache");
        return REDIS_ERR;
    }
    return REDIS_OK;
}

/* nx:
 * 0 - set when exist or not exist
 * 1 - set only when not exist
 * 2 - set only when exist
 */
static void setGenericCommand(redisClient *c, int nx, robj *key, robj *val, robj *expire, robj *flags)
{
    long seconds = 0; /* initialized to avoid an harmness warning */
    unsigned int reserved = 0;

    if (expire)
    {
        /* expire is a number of seconds starting from current */
        log_debug("in req, expire=%s", expire->ptr);
        if (getLongFromObjectOrReply(c, expire, &seconds, "invalid expire") != REDIS_OK)
        {
            log_error("expire=%s invalid", expire->ptr);
            return;
        }
        if (seconds < 0)
        {
            log_error("expire=%ld, invalid", seconds);
            addReplyError(c, "invalid expire");
            return;
        }
        log_debug("seconds=%ld", seconds);
        if (seconds)
        {
            seconds += time(0);
            if (seconds < 0)
            {
                log_error("expire unix time=%ld, now=%ld, invalid parameter", seconds, time(0));
                addReplyError(c, "invalid expire");
                return;
            }
        }
    }

    if (flags)
    {
        if (getu32IntFromObjectOrReply(c, flags, &reserved, "invalid parameter") != REDIS_OK)
        {
            log_error("in req flags=%s invalid", flags->ptr);
            return;
        }
        log_debug("in req, flags=%s, reserved=%u", flags->ptr, reserved);
        val->rsvd_bit = 1;
        val->reserved = reserved;
    }

    if (server.has_cache == 0 && server.has_dbe == 1)
    {
        /* dbe only */

        uint64_t ver;
        const int exist_flg = dbmng_key_exist(c->tag, key, &ver);
        if ((nx == 1 && 1 == exist_flg)
            || (nx == 2 && 0 == exist_flg))
        {
            log_info("key=%s do not meet the condition", key->ptr);
            addReply(c, shared.czero);
            return;
        }
        if (nx == 2)
        {
            val->visited_bit = 1;
        }
        if (exist_flg == 1)
        {
            val->version = ver + 1;
        }
        if (seconds)
        {
            robj *argv[2];
            argv[0] = val;
            argv[1] = createStringObjectFromLongLong((long long)(seconds));
            dbmng_wr_bl(c->tag, OP_SETEX, key, 2, argv, c->ds_id, c->db->id);
            dbmng_set_key(c->tag, key, 2, argv);
            decrRefCount(argv[1]);
        }
        else
        {
            dbmng_wr_bl(c->tag, OP_SET, key, 1, &val, c->ds_id, c->db->id);
            dbmng_set_key(c->tag, key, 1, &val);
        }
        addReply(c, nx != 0 ? shared.cone : shared.ok);
        return;
    }

    robj *o = lookupKeyWrite(c->db, key);
    if (nx == 1)
    {
        // need the key not exist
        if (o)
        {
            log_info("key=%s do not meet the condition", key->ptr);
            addReply(c,shared.czero);
            return;
        }
    }
    else if (nx == 2)
    {
        // need the key exist
        if (o == NULL)
        {
            log_info("key=%s do not meet the condition", key->ptr);
            addReply(c,shared.czero);
            return;
        }
    }

    if (nx == 2)
    {
        val->visited_bit = 1;
    }

    setKey(c->db,key,val);
    server.dirty++;
    //log_debug("set key->refcount=%d, val->refcount=%d", key->refcount, val->refcount);
    if (seconds)
    {
        setExpire(c->db,key,seconds);

        robj *argv[2];
        argv[0] = val;
        argv[1] = createStringObjectFromLongLong((long long)(seconds));
        dbmng_save_op(c->tag, OP_SETEX, key, 2, argv, c->ds_id, c->db->id);
        decrRefCount(argv[1]);
    }
    else
    {
        dbmng_save_op(c->tag, OP_SET, key, 1, &val, c->ds_id, c->db->id);
    }
    //log_debug("==set key=%p, val=%p, key->refcount=%d, val->refcount=%d", key, val, key->refcount, val->refcount);
    addReply(c, nx != 0 ? shared.cone : shared.ok);
}

void setCommand(redisClient *c)
{
    /* set key value [expire] */
    const size_t val_len = sdslen(c->argv[2]->ptr);
    log_debug("set: key=%s, value_len=%d", c->argv[1]->ptr, val_len);

    if (c->read_only)
    {
        addReplyError(c, "forbidden");
        log_error("read_only mode, forbidden");
        return;
    }

    if (checkStringLength(c, val_len) != REDIS_OK)
    {
        log_error("value_len=%d invalid", val_len);
        return;
    }

    robj *expire = 0;
    if (c->argc == 4)
    {
        expire = c->argv[3];
    }
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    setGenericCommand(c,0,c->argv[1],c->argv[2],expire,0);
}

void setxCommand(redisClient *c)
{
    /* mc cmd */
    /* setx key value flags expire */
    const size_t val_len = sdslen(c->argv[2]->ptr);
    log_debug("setx: key=%s, value_len=%d, flags=%s, expire=%s",
              c->argv[1]->ptr, val_len, c->argv[3]->ptr, c->argv[4]->ptr);

    if (c->read_only)
    {
        addReplyError(c, "forbidden");
        log_error("read_only mode, forbidden");
        return;
    }

    server.stat_set_cmd++;
    if (checkStringLength_mc(c, val_len) != REDIS_OK)
    {
        log_error("value_len=%d invalid", val_len);
        return;
    }

    robj *flags = c->argv[3];
    robj *expire = c->argv[4];
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    setGenericCommand(c, 0, c->argv[1], c->argv[2], expire, flags);
}

void setnxCommand(redisClient *c)
{
    /* setnx key value [expire] */
    const size_t val_len = sdslen(c->argv[2]->ptr);
    log_debug("setnx: key=%s, value_len=%d", c->argv[1]->ptr, val_len);

    if (c->read_only)
    {
        addReplyError(c, "forbidden");
        log_error("read_only mode, forbidden");
        return;
    }

    if (checkStringLength(c, val_len) != REDIS_OK)
    {
        log_error("value_len=%d invalid", val_len);
        return;
    }

    robj *expire = 0;
    if (c->argc == 4)
    {
        expire = c->argv[3];
    }
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    setGenericCommand(c,1,c->argv[1],c->argv[2],expire,0);
}

void addCommand(redisClient *c)
{
    /* mc cmd */
    /* add key value flags expire */
    const size_t val_len = sdslen(c->argv[2]->ptr);
    log_debug("add: key=%s, value_len=%d, flags=%s, expire=%s",
              c->argv[1]->ptr, val_len, c->argv[3]->ptr, c->argv[4]->ptr);

    if (c->read_only)
    {
        addReplyError(c, "forbidden");
        log_error("read_only mode, forbidden");
        return;
    }

    if (checkStringLength_mc(c, val_len) != REDIS_OK)
    {
        log_error("value_len=%d invalid", val_len);
        return;
    }

    robj *flags = c->argv[3];
    robj *expire = c->argv[4];
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    setGenericCommand(c, 1, c->argv[1], c->argv[2], expire, flags);
}

void replaceCommand(redisClient *c)
{
    /* mc cmd */
    /* replace key value flags expire */
    const size_t val_len = sdslen(c->argv[2]->ptr);
    log_debug("replace: key=%s, value_len=%d, flags=%s, expire=%s",
              c->argv[1]->ptr, val_len, c->argv[3]->ptr, c->argv[4]->ptr);

    if (c->read_only)
    {
        addReplyError(c, "forbidden");
        log_error("read_only mode, forbidden");
        return;
    }

    if (checkStringLength_mc(c, val_len) != REDIS_OK)
    {
        log_error("value_len=%d invalid", val_len);
        return;
    }

    robj *flags = c->argv[3];
    robj *expire = c->argv[4];
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    setGenericCommand(c, 2, c->argv[1], c->argv[2], expire, flags);
}

void setexCommand(redisClient *c)
{
    /* setex key expire value */
    const size_t val_len = sdslen(c->argv[3]->ptr);
    log_debug("setex: key=%s, value_len=%d", c->argv[1]->ptr, val_len);

    if (c->read_only)
    {
        addReplyError(c, "forbidden");
        log_error("read_only mode, forbidden");
        return;
    }

    if (checkStringLength(c, val_len) != REDIS_OK)
    {
        log_error("value_len=%d invalid", val_len);
        return;
    }

    c->argv[3] = tryObjectEncoding(c->argv[3]);
    setGenericCommand(c,0,c->argv[1],c->argv[3],c->argv[2],0);
}

static robj *query_key(redisClient *c)
{
    robj *o;

    if (server.has_cache == 0 && server.has_dbe == 1)
    {
        /* dbe only */
        o = dbmng_lookup_key(c->tag, c->argv[1], 0);
    }
    else
    {
        o = lookupKeyRead(c->db, c->argv[1]);
    }

    return o;
}

static int getGenericCommand(redisClient *c, int *exist_flg, uint64_t *ver)
{
    robj *o;

    if (exist_flg)
    {
        *exist_flg = 0;
    }

    if (server.has_cache == 0 && server.has_dbe == 1)
    {
        /* dbe only */
        o = dbmng_lookup_key(c->tag, c->argv[1], 0);
    }
    else
    {
        o = lookupKeyRead(c->db, c->argv[1]);
    }
    if (!o)
    {
        addReply(c, shared.nullbulk);
        return REDIS_OK;
    }

    if (o->type != REDIS_STRING)
    {
        addReply(c,shared.wrongtypeerr);
        if (server.has_cache == 0 && server.has_dbe == 1)
        {
            decrRefCount(o);
        }
        return REDIS_ERR;
    }
    else
    {
        if (exist_flg && ver)
        {
            *exist_flg = 1;
            *ver = o->version;
        }
        //log_debug("get o->refcount=%d", o->refcount);
        addReplyBulk(c,o);
        if (server.has_cache == 0 && server.has_dbe == 1)
        {
            decrRefCount(o);
        }
        return REDIS_OK;
    }
}

void getCommand(redisClient *c)
{
    getGenericCommand(c, 0, 0);
}

void getsetCommand(redisClient *c)
{
    int exist_flg;
    uint64_t ver;
    if (getGenericCommand(c, &exist_flg, &ver) == REDIS_ERR) return;
    c->argv[2] = tryObjectEncoding(c->argv[2]);

    if (server.has_cache == 0 && server.has_dbe == 1)
    {
        /* dbe only */
        if (exist_flg)
        {
            c->argv[2]->version = ver + 1;
        }
        dbmng_wr_bl(c->tag, OP_SET, c->argv[1], 1, &c->argv[2], c->ds_id, c->db->id);
        dbmng_set_key(c->tag, c->argv[1], 1, &c->argv[2]);
        return;
    }

    setKey(c->db,c->argv[1],c->argv[2]);
    dbmng_save_op(c->tag, OP_SET, c->argv[1], 1, &c->argv[2], c->ds_id, c->db->id);
    server.dirty++;
}

void casCommand(redisClient *c)
{
    /* mc cmd */
    /* cas key value flags expire_time cas_unique */
    /* expire_time: (exchange by proxy or binlogCommand())
     * a number of seconds starting from current
     * 0 mean never expired
     */
    long seconds = 0; /* initialized to avoid an harmness warning */
    unsigned int flags = 0;

    log_debug("cas: key=%s, value_len=%d, flags=%s, expire=%s, cas_unique=%s",
              c->argv[1]->ptr, sdslen(c->argv[2]->ptr), c->argv[3]->ptr, c->argv[4]->ptr, c->argv[5]->ptr);

    if (c->read_only)
    {
        addReplyError(c, "forbidden");
        log_error("read_only mode, forbidden");
        return;
    }

    if (getu32IntFromObjectOrReply(c, c->argv[3], &flags, "invalid parameter") != REDIS_OK)
    {
        log_error("cas: flags=%s invalid, key=%s", c->argv[3]->ptr, c->argv[1]->ptr);
        return;
    }

    if (getLongFromObjectOrReply(c, c->argv[4], &seconds, "invalid parameter") != REDIS_OK)
    {
        log_error("cas: expire=%s invalid, key=%s", c->argv[4]->ptr, c->argv[1]->ptr);
        return;
    }
    if (seconds < 0)
    {
        log_error("cas: expire=%ld, invalid, key=%s", seconds, c->argv[1]->ptr);
        addReplyError(c, "invalid parameter");
        return;
    }
    else if (seconds != 0)
    {
        const time_t now = time(0);
        seconds += now;
        if (seconds < 0)
        {
            log_error("expire unix time=%ld, now=%ld, invalid parameter", seconds, now);
            addReplyError(c, "invalid parameter");
            return;
        }
    }

    const size_t val_len = sdslen(c->argv[2]->ptr);
    if (checkStringLength_mc(c, val_len) != REDIS_OK)
    {
        log_error("cas: value's len = %d invalid", val_len);
        return;
    }

    unsigned long long ver;
    if (getuLongLongFromObjectOrReply(c, c->argv[5], &ver, "invalid cas_unique") != REDIS_OK)
    {
        log_error("cas: version=%s in cmd is illegal", c->argv[5]->ptr);
        return;
    }

    robj *val = query_key(c);
    if (val == 0)
    {
        log_debug("can't find the key=%s", c->argv[1]->ptr);
        addReply(c, shared.cnegone);
        server.stat_cas_misses++;
        return;
    }
    if (val->version != (uint64_t)ver)
    {
        log_debug("cas: %llu not match %llu in db, key=%s"
                , ver, val->version, c->argv[1]->ptr);
        addReply(c, shared.czero);
        server.stat_cas_badval++;
        return;
    }
    addReplyLongLong(c, 1);

    c->argv[2] = tryObjectEncoding(c->argv[2]);
    c->argv[2]->version = ver + 1;
    c->argv[2]->reserved = (uint32_t)flags;
    c->argv[2]->rsvd_bit = 1;
    c->argv[2]->visited_bit = 1;

    setKey(c->db,c->argv[1],c->argv[2]);
    server.dirty++;
    if (seconds > 0)
    {
        setExpire(c->db, c->argv[1], seconds);
    }

    robj *argv[4];
    argv[0] = c->argv[2];
    argv[1] = c->argv[3];
    argv[2] = createStringObjectFromLongLong((long long)(seconds));
    argv[3] = c->argv[5];
    dbmng_save_op(c->tag, OP_CAS, c->argv[1], 4, argv, c->ds_id, c->db->id);
    decrRefCount(argv[2]);

    server.stat_cas_hits++;
}

static int getBitOffsetFromArgument(redisClient *c, robj *o, size_t *offset)
{
    long long loffset;
    char *err = "bit offset is not an integer or out of range";

    if (getLongLongFromObjectOrReply(c,o,&loffset,err) != REDIS_OK)
        return REDIS_ERR;

    /* Limit offset to 512MB in bytes */
    if ((loffset < 0) || ((unsigned long long)loffset >> 3) >= (512*1024*1024))
    {
        addReplyError(c,err);
        return REDIS_ERR;
    }

    *offset = (size_t)loffset;
    return REDIS_OK;
}

void setbitCommand(redisClient *c)
{
    /* setbit key offset value */
    robj *o;
    char *err = "bit is not an integer or out of range";
    size_t bitoffset;
    int byte, bit;
    int byteval, bitval;
    long on;

    if (c->read_only)
    {
        addReplyError(c, "forbidden");
        log_error("read_only mode, forbidden");
        return;
    }

    if (getBitOffsetFromArgument(c,c->argv[2],&bitoffset) != REDIS_OK)
        return;

    if (getLongFromObjectOrReply(c,c->argv[3],&on,err) != REDIS_OK)
        return;

    /* Bits can only be set or cleared... */
    if (on & ~1)
    {
        addReplyError(c,err);
        return;
    }

    if (server.has_cache == 0 && server.has_dbe == 1)
    {
        /* dbe only */
        o = dbmng_lookup_key(c->tag, c->argv[1], 0);
    }
    else
    {
        o = lookupKeyWrite(c->db,c->argv[1]);
    }

    if (!o)
    {
        o = createObject(REDIS_STRING, sdsempty());
        if (!(server.has_cache == 0 && server.has_dbe == 1))
        {
            dbAdd(c->db,c->argv[1],o);
        }
    }
    else
    {
        if (checkType(c,o,REDIS_STRING))
        {
            goto setbit_over;
        }

        /* Create a copy when the object is shared or encoded. */
        if (o->refcount != 1 || o->encoding != REDIS_ENCODING_RAW)
        {
            robj *decoded = getDecodedObject(o);
            o = createStringObject(decoded->ptr, sdslen(decoded->ptr));
            o->version = decoded->version;
            decrRefCount(decoded);
            dbOverwrite(c->db,c->argv[1],o);
        }
        else
        {
            o->version++;
        }
    }

    /* Grow sds value to the right length if necessary */
    byte = bitoffset >> 3;
    if (byte + 1 > stringObjectLen(o))
    {
        ds_update_mem_stat(byte + 1 - stringObjectLen(o));
    }
    o->ptr = sdsgrowzero(o->ptr,byte+1);

    /* Get current values */
    byteval = ((char*)o->ptr)[byte];
    bit = 7 - (bitoffset & 0x7);
    bitval = byteval & (1 << bit);

    /* Update byte with new bit value and return original value */
    byteval &= ~(1 << bit);
    byteval |= ((on & 0x1) << bit);
    ((char*)o->ptr)[byte] = byteval;

    if (server.has_cache == 0 && server.has_dbe == 1)
    {
        dbmng_wr_bl(c->tag, OP_SETBIT, c->argv[1], c->argc - 2, &c->argv[2], c->ds_id, c->db->id);
        dbmng_set_key(c->tag, c->argv[1], 1, &o);
        decrRefCount(o);
    }
    else
    {
        dbmng_save_op(c->tag, OP_SETBIT, c->argv[1], c->argc - 2, &c->argv[2], c->ds_id, c->db->id);
        signalModifiedKey(c->db,c->argv[1]);
        server.dirty++;
    }

    addReply(c, bitval ? shared.cone : shared.czero);

setbit_over:
    if (o && server.has_cache == 0 && server.has_dbe == 1)
    {
        decrRefCount(o);
    }
}

void getbitCommand(redisClient *c)
{
    robj *o;
    char llbuf[32];
    size_t bitoffset;
    size_t byte, bit;
    size_t bitval = 0;

    if (getBitOffsetFromArgument(c,c->argv[2],&bitoffset) != REDIS_OK)
        return;

    if (server.has_cache == 0 && server.has_dbe == 1)
    {
        /* dbe only */
        o = dbmng_lookup_key(c->tag, c->argv[1], 0);
        if (!o)
        {
            addReply(c, shared.czero);
            return;
        }
        if (checkType(c, o, REDIS_STRING))
        {
            decrRefCount(o);
            return;
        }
    }
    else
    {
        o = lookupKeyRead(c->db, c->argv[1]);
        if (!o)
        {
            addReply(c, shared.czero);
            return;
        }
        if (checkType(c, o, REDIS_STRING))
        {
            return;
        }
    }

    byte = bitoffset >> 3;
    bit = 7 - (bitoffset & 0x7);
    if (o->encoding != REDIS_ENCODING_RAW)
    {
        if (byte < (size_t)ll2string(llbuf,sizeof(llbuf),(long)o->ptr))
            bitval = llbuf[byte] & (1 << bit);
    }
    else
    {
        if (byte < sdslen(o->ptr))
            bitval = ((char*)o->ptr)[byte] & (1 << bit);
    }

    addReply(c, bitval ? shared.cone : shared.czero);

    if (server.has_cache == 0 && server.has_dbe == 1)
    {
        decrRefCount(o);
    }
}

void setrangeCommand(redisClient *c)
{
    /* setrange key offset value */
    robj *o;
    long offset;
    sds value = c->argv[3]->ptr;

    if (c->read_only)
    {
        addReplyError(c, "forbidden");
        log_error("read_only mode, forbidden");
        return;
    }

    if (getLongFromObjectOrReply(c,c->argv[2],&offset,NULL) != REDIS_OK)
        return;

    if (offset < 0)
    {
        addReplyError(c,"offset is out of range");
        return;
    }

    if (server.has_cache == 0 && server.has_dbe == 1)
    {
        o = dbmng_lookup_key(c->tag, c->argv[1], 0);
    }
    else
    {
        o = lookupKeyWrite(c->db,c->argv[1]);
    }

    if (o == NULL)
    {
        /* Return 0 when setting nothing on a non-existing string */
        if (sdslen(value) == 0)
        {
            addReply(c,shared.czero);
            return;
        }

        /* Return when the resulting string exceeds allowed size */
        if (checkStringLength(c,offset+sdslen(value)) != REDIS_OK)
            return;

        o = createObject(REDIS_STRING,sdsempty());
        if (!(server.has_cache == 0 && server.has_dbe == 1))
        {
            dbAdd(c->db,c->argv[1],o);
        }
    }
    else
    {
        size_t olen;

        /* Key exists, check type */
        if (checkType(c,o,REDIS_STRING))
            goto setrange_over;

        /* Return existing string length when setting nothing */
        olen = stringObjectLen(o);
        if (sdslen(value) == 0)
        {
            addReplyLongLong(c,olen);
            goto setrange_over;
        }

        /* Return when the resulting string exceeds allowed size */
        if (checkStringLength(c,offset+sdslen(value)) != REDIS_OK)
            goto setrange_over;

        /* Create a copy when the object is shared or encoded. */
        if ((!(server.has_cache == 0 && server.has_dbe == 1)) && (o->refcount != 1 || o->encoding != REDIS_ENCODING_RAW))
        {
            robj *decoded = getDecodedObject(o);
            o = createStringObject(decoded->ptr, sdslen(decoded->ptr));
            o->version = decoded->version;
            decrRefCount(decoded);
            dbOverwrite(c->db,c->argv[1],o);
        }
        else
        {
            o->version++;
        }
    }

    if (sdslen(value) > 0)
    {
        const long old_size = stringObjectLen(o);
        const long new_size = offset + sdslen(value);
        if (new_size > old_size)
        {
            ds_update_mem_stat(new_size - old_size);
        }
        o->ptr = sdsgrowzero(o->ptr, new_size);
        memcpy((char*)o->ptr+offset,value,sdslen(value));
        if (!(server.has_cache == 0 && server.has_dbe == 1))
        {
            signalModifiedKey(c->db,c->argv[1]);
            server.dirty++;
        }
    }

    if (server.has_cache == 0 && server.has_dbe == 1)
    {
        dbmng_wr_bl(c->tag, OP_SETRANGE, c->argv[1], c->argc - 2, &c->argv[2], c->ds_id, c->db->id);
        dbmng_set_key(c->tag, c->argv[1], 1, &o);
    }
    else
    {
        dbmng_save_op(c->tag, OP_SETRANGE, c->argv[1], c->argc - 2, &c->argv[2], c->ds_id, c->db->id);
    }

    addReplyLongLong(c,sdslen(o->ptr));

setrange_over:
    if (o && server.has_cache == 0 && server.has_dbe == 1)
    {
        decrRefCount(o);
    }
}

void getrangeCommand(redisClient *c)
{
    robj *o;
    long start, end;
    char *str, llbuf[32];
    size_t strlen;

    if (getLongFromObjectOrReply(c,c->argv[2],&start,NULL) != REDIS_OK)
        return;
    if (getLongFromObjectOrReply(c,c->argv[3],&end,NULL) != REDIS_OK)
        return;

    if (server.has_cache == 0 && server.has_dbe == 1)
    {
        o = dbmng_lookup_key(c->tag, c->argv[1], 0);
        if (o == 0)
        {
            addReply(c, shared.emptybulk);
            return;
        }
        if (checkType(c, o, REDIS_STRING))
        {
            decrRefCount(o);
            return;
        }
    }
    else
    {
        o = lookupKeyRead(c->db,c->argv[1]);
        if (o == 0)
        {
            addReply(c, shared.emptybulk);
            return;
        }
        if (checkType(c, o, REDIS_STRING))
        {
            return;
        }
    }

    if (o->encoding == REDIS_ENCODING_INT)
    {
        str = llbuf;
        strlen = ll2string(llbuf,sizeof(llbuf),(long)o->ptr);
    }
    else
    {
        str = o->ptr;
        strlen = sdslen(str);
    }

    /* Convert negative indexes */
    if (start < 0) start = strlen+start;
    if (end < 0) end = strlen+end;
    if (start < 0) start = 0;
    if (end < 0) end = 0;
    if ((unsigned)end >= strlen) end = strlen-1;

    /* Precondition: end >= 0 && end < strlen, so the only condition where
     * nothing can be returned is: start > end. */
    if (start > end)
    {
        addReply(c,shared.emptybulk);
    }
    else
    {
        addReplyBulkCBuffer(c,(char*)str+start,end-start+1);
    }

    if (server.has_cache == 0 && server.has_dbe == 1)
    {
        decrRefCount(o);
    }
}

static void mgetCommandEx(redisClient *c, int ver_flg, int mc_flg, int ts_flg)
{
    int j;
    robj *o = 0;

    addReplyMultiBulkLen(c, (c->argc-1) * (1 + (ver_flg ? 1 : 0) + (mc_flg ? 1 : 0) + (ts_flg ? 1 : 0)));
    for (j = 1; j < c->argc; j++)
    {
        if (server.has_cache == 0 && server.has_dbe == 1)
        {
            o = dbmng_lookup_key(c->tag, c->argv[j], 0);
        }
        else
        {
            o = lookupKeyRead(c->db,c->argv[j]);
        }
        if (o == 0 && mc_flg && ver_flg == 0)
        {
            server.stat_get_misses++;
            if (server.log_get_miss == 1)
            {
                log_prompt("get_miss=%llu, key_len=%d, key=%s"
                    , server.stat_get_misses, sdslen(c->argv[j]->ptr), c->argv[j]->ptr);
            }
            else
            {
                log_info("get_miss=%llu, key_len=%d, key=%s"
                    , server.stat_get_misses, sdslen(c->argv[j]->ptr), c->argv[j]->ptr);
            }
        }

        if (o == NULL)
        {
            addReply(c,shared.nullbulk);
            if (ver_flg)
            {
                addReply(c,shared.nullbulk);
            }
            if (mc_flg)
            {
                addReply(c,shared.nullbulk);
            }
            if (ts_flg)
            {
                addReply(c,shared.nullbulk);
            }
        }
        else
        {
            if (o->type != REDIS_STRING)
            {
                log_error("No.%d: value's type=%d no REDIS_STRING, key=%s",
                          j, o->type, c->argv[j]->ptr);
                addReply(c,shared.nullbulk);
                if (ver_flg)
                {
                    addReply(c,shared.nullbulk);
                }
                if (mc_flg)
                {
                    addReply(c,shared.nullbulk);
                }
                if (ts_flg)
                {
                    addReply(c,shared.nullbulk);
                }
            }
            else
            {
                addReplyBulk(c,o);
                if (mc_flg)
                {
                    o->visited_bit = 1;
                    addReplyBulkLongLong_u(c, (unsigned long long)(o->rsvd_bit ? o->reserved : 0));
                    log_debug("No.%d: rsvd_bit=%d, reserved=%u", j, o->rsvd_bit, o->reserved);
                }
                if (ver_flg)
                {
                    addReplyBulkLongLong_u(c, (unsigned long long)o->version);
                    log_debug("No.%d: version=%"PRIu64, j, o->version);
                }
                if (ts_flg)
                {
                    addReplyBulkLongLong_u(c, o->ts_bit ? (unsigned long long)o->timestamp : 0);
                    log_debug("No.%d: ts_bit=%d, timestamp=%"PRIu64, j, o->ts_bit, o->timestamp);
                }
                if (mc_flg && ver_flg == 0)
                {
                    server.stat_get_hits++;
                }
            }

            if (server.has_cache == 0 && server.has_dbe == 1)
            {
                decrRefCount(o);
            }
        }
    }
}

void mgetCommand(redisClient *c)
{
    /* mget key1 key2 ... */
    mgetCommandEx(c, 0, 0, 0);
}

void mgetxCommand(redisClient *c)
{
    /* mc cmd */
    /* mgetx key1 key2 ... */
    server.stat_get_cmd++;
    mgetCommandEx(c, 0, 1, 0);
}

void mgetsCommand(redisClient *c)
{
    /* mc cmd */
    /* mgets key1 key2 ... */
    mgetCommandEx(c, 1, 1, 0);
}

void xgetCommand(redisClient *c)
{
    /* extend cmd , for get more info of value */
    /* xget key1 key2 ... */
    server.stat_get_cmd++;
    mgetCommandEx(c, 0, 1, 1);
}

void msetGenericCommand(redisClient *c, int nx)
{
    int j, busykeys = 0;

    if ((c->argc % 2) == 0)
    {
        addReplyError(c,"wrong number of arguments for MSET");
        return;
    }
    /* Handle the NX flag. The MSETNX semantic is to return zero and don't
     * set nothing at all if at least one already key exists. */
    if (nx)
    {
        if (server.has_cache == 0 && server.has_dbe == 1)
        {
            for (j = 1; j < c->argc; j += 2)
            {
                uint64_t ver;
                if (1 == dbmng_key_exist(c->tag, c->argv[j], &ver))
                {
                    busykeys++;
                }
            }
        }
        else
        {
            robj *o;
            for (j = 1; j < c->argc; j += 2)
            {
                o = lookupKeyWrite(c->db,c->argv[j]);
                if (o)
                {
                    busykeys++;
                }
            }
        }
        if (busykeys)
        {
            addReply(c, shared.czero);
            return;
        }
    }

    if (server.has_cache == 0 && server.has_dbe == 1)
    {
        for (j = 1; j < c->argc; j += 2)
        {
            uint64_t ver;
            if (1 == dbmng_key_exist(c->tag, c->argv[j], &ver))
            {
                c->argv[j+1]->version = ver + 1;
            }
            dbmng_wr_bl(c->tag, OP_SET, c->argv[j], 1, &c->argv[j+1], c->ds_id, c->db->id);
            dbmng_set_key(c->tag, c->argv[j], 1, &c->argv[j+1]);
        }
    }
    else
    {
        for (j = 1; j < c->argc; j += 2)
        {
            c->argv[j+1] = tryObjectEncoding(c->argv[j+1]);
            setKey(c->db,c->argv[j],c->argv[j+1]);
            dbmng_save_op(c->tag, OP_SET, c->argv[j], 1, &c->argv[j+1], c->ds_id, c->db->id);
        }
        server.dirty += (c->argc-1)/2;
    }
    addReply(c, nx ? shared.cone : shared.ok);
}

void msetCommand(redisClient *c)
{
    /* mset key value key value ... */
    if (c->read_only)
    {
        addReplyError(c, "forbidden");
        log_error("read_only mode, forbidden");
        return;
    }

    msetGenericCommand(c,0);
}

void msetnxCommand(redisClient *c)
{
    /* msetnx key value key value ... */
    if (c->read_only)
    {
        addReplyError(c, "forbidden");
        log_error("read_only mode, forbidden");
        return;
    }

    msetGenericCommand(c,1);
}

static void incrDecrCommand(redisClient *c, long long offset)
{
    long long value, oldvalue;
    robj *o, *new;
    long seconds = 0;

    if (server.has_cache == 0 && server.has_dbe == 1)
    {
        time_t expire;
        o = dbmng_lookup_key(c->tag, c->argv[1], &expire);
        if (o)
        {
            seconds = expire;
        }
    }
    else
    {
        o = lookupKeyWrite(c->db,c->argv[1]);
        if (o)
        {
            seconds = getExpire(c->db, c->argv[1]);
        }
    }
    if (o != NULL && checkType(c,o,REDIS_STRING))
    {
        goto incr_decr_fin;
    }
    if (getLongLongFromObjectOrReply(c,o,&value,0) != REDIS_OK) goto incr_decr_fin;

    oldvalue = value;
    value += offset;
    if ((offset < 0 && value > oldvalue) || (offset > 0 && value < oldvalue))
    {
        addReplyError(c,"increment or decrement would overflow");
        goto incr_decr_fin;
    }

    if (server.has_cache == 0 && server.has_dbe == 1)
    {
        new = createStringObjectFromLongLong2(value);
        new->visited_bit = 1;
        if (o)
        {
            new->version = o->version + 1;
            new->rsvd_bit = o->rsvd_bit;
            new->reserved = o->reserved;
        }
        if (seconds > 0)
        {
            robj *argv[2];
            argv[0] = new;
            argv[1] = createStringObjectFromLongLong((long long)(seconds));
            dbmng_wr_bl(c->tag, OP_SETEX, c->argv[1], 2, argv, c->ds_id, c->db->id);
            dbmng_set_key(c->tag, c->argv[1], 2, argv);
            decrRefCount(argv[1]);
        }
        else
        {
            dbmng_wr_bl(c->tag, OP_SET, c->argv[1], 1, &new, c->ds_id, c->db->id);
            dbmng_set_key(c->tag, c->argv[1], 1, &new);
        }
    }
    else
    {
        new = createStringObjectFromLongLong2(value);
        new->visited_bit = 1;
        if (o)
        {
            new->version = o->version;
            new->rsvd_bit = o->rsvd_bit;
            new->reserved = o->reserved;

            dbOverwrite(c->db,c->argv[1],new);
        }
        else
        {
            dbAdd(c->db,c->argv[1],new);
        }
        if (seconds > 0)
        {
            robj *argv[2];
            argv[0] = new;
            argv[1] = createStringObjectFromLongLong((long long)(seconds));
            dbmng_save_op(c->tag, OP_SETEX, c->argv[1], 2, argv, c->ds_id, c->db->id);
            decrRefCount(argv[1]);
        }
        else
        {
            dbmng_save_op(c->tag, OP_SET, c->argv[1], 1, &new, c->ds_id, c->db->id);
        }
        signalModifiedKey(c->db,c->argv[1]);
        server.dirty++;
    }
    addReply(c,shared.colon);
    addReply(c,new);
    addReply(c,shared.crlf);

incr_decr_fin:
    if (o && server.has_cache == 0 && server.has_dbe == 1)
    {
        decrRefCount(o);
    }
}

static void incrDecrCommandx(redisClient *c, unsigned long long offset, int incr)
{
    unsigned long long value, oldvalue;
    robj *o, *new;
    long seconds = 0;

    if (server.has_cache == 0 && server.has_dbe == 1)
    {
        time_t expire;
        o = dbmng_lookup_key(c->tag, c->argv[1], &expire);
        if (o)
        {
            seconds = expire;
        }
    }
    else
    {
        o = lookupKeyWrite(c->db,c->argv[1]);
        if (o)
        {
            seconds = getExpire(c->db, c->argv[1]);
        }
        else
        {
            if (incr)
            {
                server.stat_incr_misses++;
            }
            else
            {
                server.stat_decr_misses++;
            }
        }
    }
    if (o == 0)
    {
        addReply(c, shared.nullbulk);
        log_info("%s not exist", c->argv[1]->ptr);
        goto incr_decr_fin_x;
    }
    if (o != NULL && checkType(c,o,REDIS_STRING))
    {
        log_error("value of %s is non-string, type is %d", c->argv[1]->ptr, o->type);
        goto incr_decr_fin_x;
    }
    char *const msg = "cannot increment or decrement non-numeric value";
    if (getuLongLongFromObjectOrReply(c,o,&value,msg) != REDIS_OK)
    {
        log_error("value of %s is non-numeric: %s"
                , c->argv[1]->ptr, o->encoding == REDIS_ENCODING_RAW ? o->ptr : "");
        goto incr_decr_fin_x;
    }

    oldvalue = value;
    if (!incr)
    {
        if (value <= offset)
        {
            value = 0;
        }
        else
        {
            value -= offset;
        }
    }
    else
    {
        value += offset;
    }
    if (oldvalue == value)
    {
        /* do nothing */
        addReplyLongLong_u(c, value);
        goto incr_decr_fin_x;
    }

    if (server.has_cache == 0 && server.has_dbe == 1)
    {
        new = createStringObjectFromuLongLong2(value);
        new->visited_bit = 1;
        if (o)
        {
            new->version = o->version + 1;
            new->rsvd_bit = o->rsvd_bit;
            new->reserved = o->reserved;
        }
        if (seconds > 0)
        {
            robj *argv[2];
            argv[0] = new;
            argv[1] = createStringObjectFromLongLong((long long)(seconds));
            dbmng_wr_bl(c->tag, OP_SETEX, c->argv[1], 2, argv, c->ds_id, c->db->id);
            dbmng_set_key(c->tag, c->argv[1], 2, argv);
            decrRefCount(argv[1]);
        }
        else
        {
            dbmng_wr_bl(c->tag, OP_SET, c->argv[1], 1, &new, c->ds_id, c->db->id);
            dbmng_set_key(c->tag, c->argv[1], 1, &new);
        }
    }
    else
    {
        new = createStringObjectFromuLongLong2(value);
        new->visited_bit = 1;
        if (o)
        {
            new->version = o->version;
            new->rsvd_bit = o->rsvd_bit;
            new->reserved = o->reserved;

            dbOverwrite(c->db,c->argv[1],new);
        }
        else
        {
            dbAdd(c->db,c->argv[1],new);
        }
        if (seconds > 0)
        {
            robj *argv[2];
            argv[0] = new;
            argv[1] = createStringObjectFromLongLong((long long)(seconds));
            dbmng_save_op(c->tag, OP_SETEX, c->argv[1], 2, argv, c->ds_id, c->db->id);
            decrRefCount(argv[1]);
        }
        else
        {
            dbmng_save_op(c->tag, OP_SET, c->argv[1], 1, &new, c->ds_id, c->db->id);
        }
        signalModifiedKey(c->db,c->argv[1]);
        server.dirty++;

        if (incr)
        {
            server.stat_incr_hits++;
        }
        else
        {
            server.stat_decr_hits++;
        }
    }
    addReply(c,shared.colon);
    addReply_unsigned(c,new);
    addReply(c,shared.crlf);

incr_decr_fin_x:
    if (o && server.has_cache == 0 && server.has_dbe == 1)
    {
        decrRefCount(o);
    }
}

void incrCommand(redisClient *c)
{
    /* incr key */
    if (c->read_only)
    {
        addReplyError(c, "forbidden");
        log_error("read_only mode, forbidden");
        return;
    }

    incrDecrCommand(c, 1);
}

void decrCommand(redisClient *c)
{
    /* decr key */
    if (c->read_only)
    {
        addReplyError(c, "forbidden");
        log_error("read_only mode, forbidden");
        return;
    }

    incrDecrCommand(c, -1);
}

void incrxCommand(redisClient *c)
{
    /* mc cmd */
    /* incrx key increment */
    log_debug("incrx: key=%s, value=%s", c->argv[1]->ptr, c->argv[2]->ptr);

    if (c->read_only)
    {
        addReplyError(c, "forbidden");
        log_error("read_only mode, forbidden");
        return;
    }

    server.stat_incr_cmd++;
    unsigned long long incr;
    if (getuLongLongFromObjectOrReply(c, c->argv[2], &incr, "invalid numeric delta argument") != REDIS_OK)
    {
        log_error("value=%s invalid", c->argv[2]->ptr);
        return;
    }
    incrDecrCommandx(c, incr, 1);
}

void decrxCommand(redisClient *c)
{
    /* mc cmd */
    /* decrx key value */
    log_debug("decrx: key=%s, value=%s", c->argv[1]->ptr, c->argv[2]->ptr);

    if (c->read_only)
    {
        addReplyError(c, "forbidden");
        log_error("read_only mode, forbidden");
        return;
    }

    server.stat_decr_cmd++;
    unsigned long long incr;
    if (getuLongLongFromObjectOrReply(c, c->argv[2], &incr, "invalid numeric delta argument") != REDIS_OK)
    {
        log_error("value=%s invalid", c->argv[2]->ptr);
        return;
    }
    incrDecrCommandx(c, incr, 0);
}

void incrbyCommand(redisClient *c)
{
    /* incrby key increment */
    long long incr;

    if (c->read_only)
    {
        addReplyError(c, "forbidden");
        log_error("read_only mode, forbidden");
        return;
    }

    if (getLongLongFromObjectOrReply(c, c->argv[2], &incr, NULL) != REDIS_OK) return;
    incrDecrCommand(c, incr);
}

void decrbyCommand(redisClient *c)
{
    /* decrby key decrement */
    long long incr;

    if (c->read_only)
    {
        addReplyError(c, "forbidden");
        log_error("read_only mode, forbidden");
        return;
    }

    if (getLongLongFromObjectOrReply(c, c->argv[2], &incr, NULL) != REDIS_OK) return;
    incrDecrCommand(c, -incr);
}

static void appendCommand_ex(redisClient *c, int ex, int prepend)
{
    size_t totlen = 0;
    robj *o = 0, *append = 0;
    int ret = 0;
    robj *val_obj = 0;
    time_t exp = 0;

    const size_t append_len = stringObjectLen(c->argv[2]);
    ret = ex ? checkStringLength_mc(c, append_len) : checkStringLength(c, append_len);
    if (ret != REDIS_OK)
    {
        log_error("append: value_len=%ld invalid, key=%s", append_len, c->argv[1]->ptr);
        return;
    }

    if (server.has_cache == 0 && server.has_dbe == 1)
    {
        o = dbmng_lookup_key(c->tag, c->argv[1], &exp);
    }
    else
    {
        o = lookupKeyWrite(c->db,c->argv[1]);
        if (o)
        {
            exp = getExpire(c->db, c->argv[1]);
        }
    }

    if (o == NULL)
    {
        if (ex)
        {
            /* not exist but append when exist */
            log_info("key=%s do not meet the condition", c->argv[1]->ptr);
            addReply(c, shared.czero);
            return;
        }

        /* Create the key */
        if (!(server.has_cache == 0 && server.has_dbe == 1))
        {
            c->argv[2] = tryObjectEncoding(c->argv[2]);
            dbAdd(c->db,c->argv[1],c->argv[2]);
            incrRefCount(c->argv[2]);
        }
        totlen = append_len;
        val_obj = c->argv[2];
    }
    else
    {
        /* Key exists, check type */
        if (checkType(c,o,REDIS_STRING))
            goto append_error;

        /* "append" is an argument, so always an sds */
        append = c->argv[2];
        totlen = stringObjectLen(o) + append_len;
        ret = ex ? checkStringLength_mc(c, totlen) : checkStringLength(c, totlen);
        if (ret != REDIS_OK)
        {
            log_error("append: total_len=%ld invalid, key=%s", totlen, c->argv[1]->ptr);
            goto append_error;
        }

        if (ex)
        {
            o->visited_bit = 1;
        }

        if (server.has_cache == 0 && server.has_dbe == 1)
        {
            o->version++;
        }
        else
        {
            /* If the object is shared or encoded, we have to make a copy */
            if (o->refcount != 1 || o->encoding != REDIS_ENCODING_RAW) {
                robj *decoded = getDecodedObject(o);
                o = createStringObject(decoded->ptr, sdslen(decoded->ptr));

                o->version = decoded->version;
                o->rsvd_bit = decoded->rsvd_bit;
                o->reserved = decoded->reserved;
                o->visited_bit = decoded->visited_bit;
                o->no_used = decoded->no_used;

                decrRefCount(decoded);
                dbOverwrite(c->db,c->argv[1],o);
            }
            else
            {
                o->version++;
            }
        }

        /* Append the value */
        ds_update_mem_stat(sdslen(append->ptr));
        if (prepend)
        {
            o->ptr = sdscatlen_prepend(o->ptr,append->ptr,sdslen(append->ptr));
        }
        else
        {
            o->ptr = sdscatlen(o->ptr,append->ptr,sdslen(append->ptr));
        }
        totlen = sdslen(o->ptr);
        val_obj = o;
    }

    {
        const char *op = 0;
        int argc = 1;
        robj *argv[2];
        argv[0] = val_obj;
        if (exp > 0)
        {
            op = OP_SETEX;
            argv[argc++] = createStringObjectFromLongLong((long long)(exp));
        }
        else
        {
            op = OP_SET;
            argv[argc] = 0;
        }

        if (server.has_cache == 0 && server.has_dbe == 1)
        {
            dbmng_wr_bl(c->tag, op, c->argv[1], argc, argv, c->ds_id, c->db->id);
            dbmng_set_key(c->tag, c->argv[1], 1, &val_obj);
        }
        else
        {
            dbmng_save_op(c->tag, op, c->argv[1], argc, argv, c->ds_id, c->db->id);
            signalModifiedKey(c->db,c->argv[1]);
            server.dirty++;
        }

        if (argv[1])
        {
            decrRefCount(argv[1]);
        }
    }

    addReplyLongLong(c,totlen);

append_error:
    if (o && server.has_cache == 0 && server.has_dbe == 1)
    {
        decrRefCount(o);
    }
}

void appendCommand(redisClient *c)
{
    /* append key value */
    log_debug("append: key=%s, value_len=%d", c->argv[1]->ptr, sdslen(c->argv[2]->ptr));

    if (c->read_only)
    {
        addReplyError(c, "forbidden");
        log_error("read_only mode, forbidden");
        return;
    }

    appendCommand_ex(c, 0, 0);
}

void appendxCommand(redisClient *c)
{
    /* mc cmd */
    /* appendx key value */
    log_debug("appendx: key=%s, value_len=%d", c->argv[1]->ptr, sdslen(c->argv[2]->ptr));

    if (c->read_only)
    {
        addReplyError(c, "forbidden");
        log_error("read_only mode, forbidden");
        return;
    }

    appendCommand_ex(c, 1, 0);
}

void prependCommand(redisClient *c)
{
    /* mc cmd */
    /* prepend key value */
    log_debug("prepend: key=%s, value_len=%d", c->argv[1]->ptr, sdslen(c->argv[2]->ptr));

    if (c->read_only)
    {
        addReplyError(c, "forbidden");
        log_error("read_only mode, forbidden");
        return;
    }

    appendCommand_ex(c, 1, 1);
}

void strlenCommand(redisClient *c)
{
    /* strlen key */
    robj *o = 0;

    if (server.has_cache == 0 && server.has_dbe == 1)
    {
        o = dbmng_lookup_key(c->tag, c->argv[1], 0);
        if (o == 0)
        {
            addReply(c, shared.czero);
            return;
        }
        if (checkType(c, o, REDIS_STRING))
        {
            decrRefCount(o);
            return;
        }
    }
    else
    {
        o = lookupKeyRead(c->db,c->argv[1]);
        if (o == 0)
        {
            addReply(c, shared.czero);
            return;
        }
        if (checkType(c, o, REDIS_STRING))
        {
            return;
        }
    }

    addReplyLongLong(c,stringObjectLen(o));

    if (server.has_cache == 0 && server.has_dbe == 1)
    {
        decrRefCount(o);
    }
}

/* support by memcached v1.4.8 */
void touchCommand(redisClient *c)
{
    /* mc cmd */
    /* touch key expire */
    log_debug("touch: key=%s, expire=%s", c->argv[1]->ptr, c->argv[2]->ptr);

    if (c->read_only)
    {
        addReplyError(c, "forbidden");
        log_error("read_only mode, forbidden");
        return;
    }

    long seconds = 0; /* initialized to avoid an harmness warning */

    server.stat_touch_cmd++;

    if (c->argc == 3)
    {
        if (getLongFromObjectOrReply(c, c->argv[2], &seconds, "invalid parameter") != REDIS_OK)
        {
            log_error("expire=%s invalid", c->argv[2]->ptr);
            return;
        }
        if (seconds < 0) 
        {
            log_error("expire=%ld invalid", seconds);
            addReplyError(c, "invalid parameter");
            return;
        }
        if (seconds > 0)
        {
            seconds += time(0);
            if (seconds < 0)
            {
                log_error("expire unix time=%ld, now=%ld, invalid parameter", seconds, time(0));
                addReplyError(c, "invalid parameter");
                return;
            }
        }
    }
    robj *const key = c->argv[1];

    if (server.has_cache == 0 && server.has_dbe == 1)
    {
        /* dbe only */

        time_t expire;
        robj *val = dbmng_lookup_key(c->tag, key, &expire);
        if (0 == val)
        {
            log_info("%s not exist", key->ptr);
            addReply(c, shared.czero);
            return;
        }
        val->visited_bit = 1;
        if (seconds != 0)
        {
            robj *argv[2];
            argv[0] = val;
            argv[1] = createStringObjectFromLongLong((long long)(seconds));
            dbmng_wr_bl(c->tag, OP_SETEX, key, 2, argv, c->ds_id, c->db->id);
            dbmng_set_key(c->tag, key, 2, argv);
            decrRefCount(argv[1]);
        }
        else if (expire != 0)
        {
            dbmng_wr_bl(c->tag, OP_SET, key, 1, &val, c->ds_id, c->db->id);
            dbmng_set_key(c->tag, key, 1, &val);
        }
        decrRefCount(val);
        addReply(c, shared.cone);
        return;
    }

    robj *val = lookupKeyWrite(c->db,key);
    if (val == NULL)
    {
        log_info("%s not exist", key->ptr);
        addReply(c,shared.czero);
        server.stat_touch_misses++;
        return;
    }
    val->visited_bit = 1;
    if (seconds)
    {
        setExpire(c->db,key,seconds);

        robj *argv[2];
        argv[0] = val;
        argv[1] = createStringObjectFromLongLong((long long)(seconds));
        dbmng_save_op(c->tag, OP_SETEX, key, 2, argv, c->ds_id, c->db->id);
        decrRefCount(argv[1]);
    }
    else if (getExpire(c->db, key) > 0)
    {
        removeExpire(c->db, key);
        dbmng_save_op(c->tag, OP_SET, key, 1, &val, c->ds_id, c->db->id);
    }
    addReply(c, shared.cone);
    server.stat_touch_hits++;
}

