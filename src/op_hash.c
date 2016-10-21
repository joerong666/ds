#include "redis.h"
#include "ds_log.h"

void op_hmsetCommand(redisClient *c)
{
    int i;
    robj *o;
    robj *key = c->argv[1];

    if ((c->argc % 2) == 1)
    {
        log_error("op_hmsetCommand: argc=%d invalid, key=%s", c->argc, key->ptr);
        return;
    }

    o = lookupKeyWrite(c->db,key);
    if (o == NULL)
    {
        o = createHashObject();
        dbAdd(c->db,key,o);
    }
    else
    {
        if (o->type != REDIS_HASH)
        {
            log_error("op_hmsetCommand: type=%d invalid, key=%s", o->type, key->ptr);
            return;
        }
    }
    hashTypeTryConversion(o,c->argv,2,c->argc-1);
    for (i = 2; i < c->argc; i += 2)
    {
        hashTypeTryObjectEncoding(o,&c->argv[i], &c->argv[i+1]);
        hashTypeSet(o,c->argv[i],c->argv[i+1]);
    }
    signalModifiedKey(c->db,c->argv[1]);
    server.dirty++;
}

void op_hdelCommand(redisClient *c)
{
    robj *o;
    int j, deleted = 0;

    o = lookupKeyWrite(c->db, c->argv[1]);
    if (o == NULL)
    {
        return;
    }
    if (o->type != REDIS_HASH)
    {
        log_error("op_hdelCommand: type=%d invalid, key=%s", o->type, c->argv[1]->ptr);
        return;
    }

    for (j = 2; j < c->argc; j++)
    {
        if (hashTypeDelete(o,c->argv[j]))
        {
            deleted++;
            if (hashTypeLength(o) == 0)
            {
                dbDelete(c->db,c->argv[1]);
                break;
            }
        }
    }
    if (deleted)
    {
        signalModifiedKey(c->db,c->argv[1]);
        server.dirty += deleted;
    }
}

