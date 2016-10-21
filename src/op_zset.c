#include "t_zset.h"
#include "ds_log.h"

void op_zaddCommand(redisClient *c)
{
    robj *key = c->argv[1];
    robj *ele;
    robj *zobj;
    robj *curobj;
    double score = 0, *scores, curscore = 0.0;
    int j, elements = (c->argc-2)/2;

    //log_prompt("op_zadd: %s", key->ptr);

    if (c->argc % 2)
    {
        log_error("op_zaddCommand: argc=%d invalid, key=%s", c->argc, key->ptr);
        return;
    }

    /* Start parsing all the scores, we need to emit any syntax error
     * before executing additions to the sorted set, as the command should
     * either execute fully or nothing at all. */
    scores = zmalloc(sizeof(double)*elements);
    for (j = 0; j < elements; j++)
    {
        if (getDoubleFromObject(c->argv[2+j*2],&scores[j]) != REDIS_OK)
        {
            log_error("op_zaddCommand: No.%d score invalid, key=%s", j, key->ptr);
            zfree(scores);
            elements = j;
            goto op_zadd_over;
        }

        c->argv[2+j*2+1] = getDecodedObject(c->argv[2+j*2+1]);
    }

    /* Lookup the key and create the sorted set if does not exist. */
    zobj = lookupKeyWrite(c->db,key);
    if (zobj == NULL)
    {
        if (server.zset_max_ziplist_entries == 0 ||
            server.zset_max_ziplist_value < sdslen(c->argv[3]->ptr))
        {
            zobj = createZsetObject();
        }
        else
        {
            zobj = createZsetZiplistObject();
        }
        dbAdd(c->db,key,zobj);
    }
    else
    {
        if (zobj->type != REDIS_ZSET)
        {
            log_error("op_zaddCommand: type=%d invalid, key=%s", zobj->type, key->ptr);
            zfree(scores);
            goto op_zadd_over;
        }
    }

    for (j = 0; j < elements; j++)
    {
        score = scores[j];

        if (zobj->encoding == REDIS_ENCODING_ZIPLIST)
        {
            unsigned char *eptr;

            /* Prefer non-encoded element when dealing with ziplists. */
            ele = c->argv[3+j*2];
            if ((eptr = zzlFind(zobj->ptr,ele,&curscore)) != NULL)
            {
                /* Remove and re-insert when score changed. */
                if (score != curscore)
                {
                    zobj->ptr = zzlDelete(zobj->ptr,eptr);
                    zobj->ptr = zzlInsert(zobj->ptr,ele,score);

                    signalModifiedKey(c->db,key);
                    server.dirty++;
                }
            }
            else
            {
                /* Optimize: check if the element is too large or the list
                 * becomes too long *before* executing zzlInsert. */
                zobj->ptr = zzlInsert(zobj->ptr,ele,score);
                if (zzlLength(zobj->ptr) > server.zset_max_ziplist_entries)
                    zsetConvert(zobj,REDIS_ENCODING_SKIPLIST);
                if (sdslen(ele->ptr) > server.zset_max_ziplist_value)
                    zsetConvert(zobj,REDIS_ENCODING_SKIPLIST);

                signalModifiedKey(c->db,key);
                server.dirty++;
            }
        }
        else if (zobj->encoding == REDIS_ENCODING_SKIPLIST)
        {
            zset *zs = zobj->ptr;
            zskiplistNode *znode;
            dictEntry *de;

            ele = c->argv[3+j*2] = tryObjectEncoding(c->argv[3+j*2]);
            de = dictFind(zs->dict,ele);
            if (de != NULL)
            {
                curobj = dictGetEntryKey(de);
                curscore = *(double*)dictGetEntryVal(de);

                /* Remove and re-insert when score changed. We can safely
                 * delete the key object from the skiplist, since the
                 * dictionary still has a reference to it. */
                if (score != curscore)
                {
                    redisAssert(zslDelete(zs->zsl,curscore,curobj));
                    znode = zslInsert(zs->zsl,score,curobj);
                    incrRefCount(curobj); /* Re-inserted in skiplist. */
                    dictGetEntryVal(de) = &znode->score; /* Update score ptr. */

                    signalModifiedKey(c->db,key);
                    server.dirty++;
                }
            }
            else
            {
                znode = zslInsert(zs->zsl,score,ele);
                incrRefCount(ele); /* Inserted in skiplist. */
                redisAssert(dictAdd(zs->dict,ele,&znode->score) == DICT_OK);
                incrRefCount(ele); /* Added to dictionary. */

                signalModifiedKey(c->db,key);
                server.dirty++;
            }
        }
        else
        {
            log_error("op_zaddCommand: encoding=%d invalid, key=%s"
                    , zobj->encoding, key->ptr);
        }
    }

    zfree(scores);

op_zadd_over:
    for (j = 0; j < elements; j++)
    {
        decrRefCount(c->argv[2+j*2+1]);
    }
}

void op_zremCommand(redisClient *c)
{
    robj *key = c->argv[1];
    robj *zobj;
    int deleted = 0, j;

    //log_prompt("op_zrem: %s", key->ptr);

    for (j = 2; j < c->argc; j++)
    {
        c->argv[j] = getDecodedObject(c->argv[j]);
    }

    zobj = lookupKeyWrite(c->db,key);
    if (zobj == NULL)
    {
        goto op_zrem_over;
    }
    if (zobj->type != REDIS_ZSET)
    {
        log_error("op_zremCommand: type=%d invalid, key=%s", zobj->type, key->ptr);
        goto op_zrem_over;
    }

    if (zobj->encoding == REDIS_ENCODING_ZIPLIST)
    {
        unsigned char *eptr;

        for (j = 2; j < c->argc; j++)
        {
            if ((eptr = zzlFind(zobj->ptr,c->argv[j],NULL)) != NULL)
            {
                deleted++;
                zobj->ptr = zzlDelete(zobj->ptr,eptr);
                if (zzlLength(zobj->ptr) == 0)
                {
                    dbDelete(c->db,key);
                    break;
                }
            }
        }
    }
    else if (zobj->encoding == REDIS_ENCODING_SKIPLIST)
    {
        zset *zs = zobj->ptr;
        dictEntry *de;
        double score;

        for (j = 2; j < c->argc; j++)
        {
            de = dictFind(zs->dict,c->argv[j]);
            if (de != NULL)
            {
                deleted++;

                /* Delete from the skiplist */
                score = *(double*)dictGetEntryVal(de);
                redisAssert(zslDelete(zs->zsl,score,c->argv[j]));

                /* Delete from the hash table */
                dictDelete(zs->dict,c->argv[j]);
                if (htNeedsResize(zs->dict)) dictResize(zs->dict);
                if (dictSize(zs->dict) == 0)
                {
                    dbDelete(c->db,key);
                    break;
                }
            }
        }
    }
    else
    {
        log_error("op_zaddCommand: encoding=%d invalid, key=%s" , zobj->encoding, key->ptr);
    }

    if (deleted)
    {
        signalModifiedKey(c->db,key);
        server.dirty += deleted;
    }

op_zrem_over:
    for (j = 2; j < c->argc; j++)
    {
        decrRefCount(c->argv[j]);
    }
}

