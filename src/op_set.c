#include "redis.h"

#include "ds_log.h"

void op_saddCommand(redisClient *c)
{
    robj *set;
    int j, added = 0;

    set = lookupKeyWrite(c->db,c->argv[1]);
    if (set == NULL) 
    {
        set = setTypeCreate(c->argv[2]);
        dbAdd(c->db,c->argv[1],set);
    }
    else
    {
        if (set->type != REDIS_SET)
        {
            log_error("op_saddCommand: type=%d invalid, key=%s"
                    , set->type, c->argv[1]->ptr);
            return;
        }
    }

    for (j = 2; j < c->argc; j++)
    {
        c->argv[j] = tryObjectEncoding(c->argv[j]);
        if (setTypeAdd(set,c->argv[j])) added++;
    }
    if (added) signalModifiedKey(c->db,c->argv[1]);
    server.dirty += added;
}

void op_sremCommand(redisClient *c)
{
    robj *set;
    int j, deleted = 0;

    set = lookupKeyWrite(c->db, c->argv[1]);
    if (set == NULL)
    {
        return;
    }
    if (set->type != REDIS_SET)
    {
        log_error("op_sremCommand: type=%d invalid, key=%s", set->type, c->argv[1]->ptr);
        return;
    }

    for (j = 2; j < c->argc; j++)
    {
        if (setTypeRemove(set,c->argv[j]))
        {
            deleted++;
            if (setTypeSize(set) == 0)
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

