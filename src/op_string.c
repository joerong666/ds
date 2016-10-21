#include "redis.h"

#include "ds_log.h"
#include "ds_zmalloc.h"

/*-----------------------------------------------------------------------------
 * String Commands
 *----------------------------------------------------------------------------*/

static int checkStringLength(long long size)
{
    if (size > 512*1024*1024)
    {
        return REDIS_ERR;
    }
    return REDIS_OK;
}

static void setGenericCommand(redisClient *c, robj *key, robj *val, robj *expire) 
{
    long seconds = 0; /* initialized to avoid an harmness warning */

    if (expire)
    {
        /* expire is timestamp format */
        if (getLongFromObject(expire, &seconds) != REDIS_OK)
        {
            log_error("get expire from binlog fail, key=%s", key->ptr);
            return;
        }
        if (seconds < 0)
        {
            log_error("expire=%ld invalid in binlog, key=%s", seconds, key->ptr);
            return;
        }
        if (seconds)
        {
            const time_t now = time(0);
            if (seconds <= now)
            {
                /* expired, ignore it */
                log_error("expire=%ld, now=%d, key=%s", seconds, now, key->ptr);
                return;
            }
        }
    }

    setKey(c->db,key,val);
    if (seconds)
    {
        setExpire(c->db,key,seconds);
    }
}

void op_setCommand(redisClient *c)
{
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    setGenericCommand(c,c->argv[1],c->argv[2],NULL);
}

void op_setexCommand(redisClient *c) 
{
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    setGenericCommand(c,c->argv[1],c->argv[2],c->argv[3]);
}

static int getBitOffsetFromArgument(redisClient *c, robj *o, size_t *offset)
{
    REDIS_NOTUSED(c);
    long long loffset;

    if (getLongLongFromObject(o,&loffset) != REDIS_OK)
        return REDIS_ERR;

    /* Limit offset to 512MB in bytes */
    if ((loffset < 0) || ((unsigned long long)loffset >> 3) >= (512*1024*1024))
    {
        return REDIS_ERR;
    }

    *offset = (size_t)loffset;
    return REDIS_OK;
}

void op_setbitCommand(redisClient *c) 
{
    robj *o;
    size_t bitoffset;
    int byte, bit;
    int byteval, bitval;
    long on;

    if (getBitOffsetFromArgument(c,c->argv[2],&bitoffset) != REDIS_OK)
    {
        return;
    }

    if (getLongFromObject(c->argv[3],&on) != REDIS_OK)
    {
        return;
    }
    /* Bits can only be set or cleared... */
    if (on & ~1)
    {
        return;
    }

    o = lookupKeyWrite(c->db,c->argv[1]);
    if (o == NULL)
    {
        o = createObject(REDIS_STRING,sdsempty());
        dbAdd(c->db,c->argv[1],o);
    }
    else
    {
        if (o->type != REDIS_STRING) return;

        /* Create a copy when the object is shared or encoded. */
        if (o->refcount != 1 || o->encoding != REDIS_ENCODING_RAW)
        {
            robj *decoded = getDecodedObject(o);
            o = createStringObject(decoded->ptr, sdslen(decoded->ptr));
            decrRefCount(decoded);
            dbOverwrite(c->db,c->argv[1],o);
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

    signalModifiedKey(c->db,c->argv[1]);
    server.dirty++;
}

void op_setrangeCommand(redisClient *c) 
{
    robj *o;
    long offset;
    int value_len = stringObjectLen(c->argv[3]);

    if (value_len == 0)
    {
        return;
    }

    if (getLongFromObject(c->argv[2],&offset) != REDIS_OK)
    {
        return;
    }
    if (offset < 0)
    {
        log_error("offset is out of range, offset=%d", offset);
        return;
    }
    if (checkStringLength(offset+value_len) != REDIS_OK)
    {
        return;
    }

    o = lookupKeyWrite(c->db,c->argv[1]);
    if (o == NULL)
    {
        o = createObject(REDIS_STRING,sdsempty());
        dbAdd(c->db,c->argv[1],o);
    }
    else
    {
        /* Key exists, check type */
        if (o->type != REDIS_STRING)
            return;

        /* Create a copy when the object is shared or encoded. */
        if (o->refcount != 1 || o->encoding != REDIS_ENCODING_RAW)
        {
            robj *decoded = getDecodedObject(o);
            o = createStringObject(decoded->ptr, sdslen(decoded->ptr));
            decrRefCount(decoded);
            dbOverwrite(c->db,c->argv[1],o);
        }
    }

    const long old_size = stringObjectLen(o);
    const long new_size = offset + value_len;
    if (new_size > old_size)
    {
        ds_update_mem_stat(new_size - old_size);
    }
    o->ptr = sdsgrowzero(o->ptr, new_size);
    robj *value_obj = getDecodedObject(c->argv[3]);
    memcpy((char*)o->ptr+offset,value_obj->ptr,value_len);
    decrRefCount(value_obj);
    signalModifiedKey(c->db,c->argv[1]);
    server.dirty++;
}

static void incrDecrCommand(redisClient *c, long long incr) 
{
    long long value, oldvalue;
    robj *o, *new;

    o = lookupKeyWrite(c->db,c->argv[1]);
    if (o != NULL && checkType(c,o,REDIS_STRING)) return;
    if (getLongLongFromObject(o,&value) != REDIS_OK) return;

    oldvalue = value;
    value += incr;
    if ((incr < 0 && value > oldvalue) || (incr > 0 && value < oldvalue)) {
        log_error("%s","increment or decrement would overflow");
        return;
    }
    new = createStringObjectFromLongLong(value);
    if (o)
        dbOverwrite(c->db,c->argv[1],new);
    else
        dbAdd(c->db,c->argv[1],new);
}

/* it will not be called */
void op_incrbyCommand(redisClient *c)
{
    long long incr;

    if (getLongLongFromObject(c->argv[2], &incr) != REDIS_OK) return;
    incrDecrCommand(c,incr);
}

void op_appendCommand_ex(redisClient *c, int prepend) 
{
    robj *o, *append;

    o = lookupKeyWrite(c->db,c->argv[1]);
    if (o == NULL) {
        /* Create the key */
        c->argv[2] = tryObjectEncoding(c->argv[2]);
        dbAdd(c->db,c->argv[1],c->argv[2]);
        incrRefCount(c->argv[2]);
    } else {
        /* Key exists, check type */
        if (checkType(c,o,REDIS_STRING))
            return;

        append = getDecodedObject(c->argv[2]);
        if (append == 0) return;
        size_t totlen = stringObjectLen(o)+sdslen(append->ptr);
        if (checkStringLength(totlen) != REDIS_OK)
        {
            decrRefCount(append);
            return;
        }

        /* If the object is shared or encoded, we have to make a copy */
        if (o->refcount != 1 || o->encoding != REDIS_ENCODING_RAW) {
            robj *decoded = getDecodedObject(o);
            o = createStringObject(decoded->ptr, sdslen(decoded->ptr));
            decrRefCount(decoded);
            dbOverwrite(c->db,c->argv[1],o);
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
        decrRefCount(append);
    }
}

/* it will not be called */
void op_appendCommand(redisClient *c) 
{
    op_appendCommand_ex(c, 0);
}

/* it will not be called */
void op_prependCommand(redisClient *c) 
{
    op_appendCommand_ex(c, 1);
}

void op_casCommand(redisClient *c) 
{
    /* cas key value flags expire cas_unique */
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    setGenericCommand(c,c->argv[1],c->argv[2],c->argv[4]);
}

