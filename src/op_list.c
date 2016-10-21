#include "redis.h"

void op_pushGenericCommand(redisClient *c, int where)
{
    int j, pushed = 0;
    robj *lobj = lookupKeyWrite(c->db,c->argv[1]);

    if (lobj && lobj->type != REDIS_LIST) 
    {
        return;
    }

    for (j = 2; j < c->argc; j++)
    {
        c->argv[j] = tryObjectEncoding(c->argv[j]);
        if (!lobj)
        {
            lobj = createZiplistObject();
            dbAdd(c->db,c->argv[1],lobj);
        }
        listTypePush(lobj,c->argv[j],where);
        pushed++;
    }
    if (pushed) signalModifiedKey(c->db,c->argv[1]);
    server.dirty += pushed;
}

void op_lpushCommand(redisClient *c)
{
    op_pushGenericCommand(c,REDIS_HEAD);
}

void op_rpushCommand(redisClient *c)
{
    op_pushGenericCommand(c,REDIS_TAIL);
}

void op_pushxGenericCommand(redisClient *c, robj *refval, robj *val, int where)
{
    robj *subject;
    listTypeIterator *iter;
    listTypeEntry entry;
    int inserted = 0;

    subject = lookupKeyRead(c->db,c->argv[1]);
    if (subject == NULL || subject->type != REDIS_LIST) return;

    if (refval != NULL) 
    {
        /* We're not sure if this value can be inserted yet, but we cannot
         * convert the list inside the iterator. We don't want to loop over
         * the list twice (once to see if the value can be inserted and once
         * to do the actual insert), so we assume this value can be inserted
         * and convert the ziplist to a regular list if necessary. */
        listTypeTryConversion(subject,val);

        /* Seek refval from head to tail */
        iter = listTypeInitIterator(subject,0,REDIS_TAIL);
        while (listTypeNext(iter,&entry))
        {
            if (listTypeEqual(&entry,refval))
            {
                listTypeInsert(&entry,val,where);
                inserted = 1;
                break;
            }
        }
        listTypeReleaseIterator(iter);

        if (inserted)
        {
            /* Check if the length exceeds the ziplist length threshold. */
            if (subject->encoding == REDIS_ENCODING_ZIPLIST &&
                ziplistLen(subject->ptr) > server.list_max_ziplist_entries)
                    listTypeConvert(subject,REDIS_ENCODING_LINKEDLIST);
            signalModifiedKey(c->db,c->argv[1]);
            server.dirty++;
        }
    }
}

void op_linsertCommand(redisClient *c)
{
    c->argv[4] = tryObjectEncoding(c->argv[4]);
    robj *pivot = getDecodedObject(c->argv[3]);
    if (strcasecmp(c->argv[2]->ptr,"after") == 0)
    {
        op_pushxGenericCommand(c,pivot,c->argv[4],REDIS_TAIL);
    }
    else if (strcasecmp(c->argv[2]->ptr,"before") == 0)
    {
        op_pushxGenericCommand(c,pivot,c->argv[4],REDIS_HEAD);
    }
    decrRefCount(pivot);
}

void op_lsetCommand(redisClient *c)
{
    robj *o = lookupKeyWrite(c->db,c->argv[1]);
    if (o == NULL || o->type != REDIS_LIST) return;
    robj *idx_obj = getDecodedObject(c->argv[2]);
    int index = atoi(idx_obj->ptr);
    decrRefCount(idx_obj);
    robj *value = (c->argv[3] = tryObjectEncoding(c->argv[3]));

    listTypeTryConversion(o,value);
    if (o->encoding == REDIS_ENCODING_ZIPLIST)
    {
        unsigned char *p, *zl = o->ptr;
        p = ziplistIndex(zl,index);
        if (p)
        {
            o->ptr = ziplistDelete(o->ptr,&p);
            value = getDecodedObject(value);
            o->ptr = ziplistInsert(o->ptr,p,value->ptr,sdslen(value->ptr));
            decrRefCount(value);
            signalModifiedKey(c->db,c->argv[1]);
            server.dirty++;
        }
    }
    else if (o->encoding == REDIS_ENCODING_LINKEDLIST)
    {
        listNode *ln = listIndex(o->ptr,index);
        if (ln)
        {
            decrRefCount((robj*)listNodeValue(ln));
            listNodeValue(ln) = value;
            incrRefCount(value);
            signalModifiedKey(c->db,c->argv[1]);
            server.dirty++;
        }
    }
}

void op_popGenericCommand(redisClient *c, int where)
{
    robj *o = lookupKeyWrite(c->db,c->argv[1]);
    if (o == NULL || o->type != REDIS_LIST) return;

    robj *value = listTypePop(o,where);
    if (value) 
    {
        decrRefCount(value);
        if (listTypeLength(o) == 0) dbDelete(c->db,c->argv[1]);
        signalModifiedKey(c->db,c->argv[1]);
        server.dirty++;
    }
}

void op_lpopCommand(redisClient *c)
{
    op_popGenericCommand(c,REDIS_HEAD);
}

void op_rpopCommand(redisClient *c)
{
    op_popGenericCommand(c,REDIS_TAIL);
}

void op_ltrimCommand(redisClient *c)
{
    robj *o;
    robj *start_obj = getDecodedObject(c->argv[2]);
    int start = atoi(start_obj->ptr);
    decrRefCount(start_obj);
    robj *end_obj = getDecodedObject(c->argv[3]);
    int end = atoi(end_obj->ptr);
    decrRefCount(end_obj);
    int llen;
    int j, ltrim, rtrim;
    list *list;
    listNode *ln;

    o = lookupKeyWrite(c->db,c->argv[1]);
    if (o == NULL || o->type != REDIS_LIST) return;
    llen = listTypeLength(o);

    /* convert negative indexes */
    if (start < 0) start = llen+start;
    if (end < 0) end = llen+end;
    if (start < 0) start = 0;

    /* Invariant: start >= 0, so this test will be true when end < 0.
     * The range is empty when start > end or start >= length. */
    if (start > end || start >= llen)
    {
        /* Out of range start or start > end result in empty list */
        ltrim = llen;
        rtrim = 0;
    }
    else
    {
        if (end >= llen) end = llen-1;
        ltrim = start;
        rtrim = llen-end-1;
    }

    /* Remove list elements to perform the trim */
    if (o->encoding == REDIS_ENCODING_ZIPLIST)
    {
        o->ptr = ziplistDeleteRange(o->ptr,0,ltrim);
        o->ptr = ziplistDeleteRange(o->ptr,-rtrim,rtrim);
    }
    else if (o->encoding == REDIS_ENCODING_LINKEDLIST)
    {
        list = o->ptr;
        for (j = 0; j < ltrim; j++)
        {
            ln = listFirst(list);
            listDelNode(list,ln);
        }
        for (j = 0; j < rtrim; j++)
        {
            ln = listLast(list);
            listDelNode(list,ln);
        }
    }
    else
    {
        return;
    }
    if (listTypeLength(o) == 0) dbDelete(c->db,c->argv[1]);
    signalModifiedKey(c->db,c->argv[1]);
    server.dirty++;
}

void op_lremCommand(redisClient *c)
{
    robj *subject, *obj;
    obj = c->argv[3] = tryObjectEncoding(c->argv[3]);
    robj *count_obj = getDecodedObject(c->argv[2]);
    int toremove = atoi(count_obj->ptr);
    decrRefCount(count_obj);
    int removed = 0;
    listTypeEntry entry;

    subject = lookupKeyWrite(c->db,c->argv[1]);
    if (subject == NULL || subject->type != REDIS_LIST) return;

    /* Make sure obj is raw when we're dealing with a ziplist */
    if (subject->encoding == REDIS_ENCODING_ZIPLIST)
        obj = getDecodedObject(obj);

    listTypeIterator *li;
    if (toremove < 0)
    {
        toremove = -toremove;
        li = listTypeInitIterator(subject,-1,REDIS_HEAD);
    }
    else
    {
        li = listTypeInitIterator(subject,0,REDIS_TAIL);
    }

    while (listTypeNext(li,&entry))
    {
        if (listTypeEqual(&entry,obj))
        {
            listTypeDelete(&entry);
            server.dirty++;
            removed++;
            if (toremove && removed == toremove) break;
        }
    }
    listTypeReleaseIterator(li);

    /* Clean up raw encoded object */
    if (subject->encoding == REDIS_ENCODING_ZIPLIST)
        decrRefCount(obj);

    if (listTypeLength(subject) == 0) dbDelete(c->db,c->argv[1]);
    if (removed) signalModifiedKey(c->db,c->argv[1]);
}

void op_rpoplpushCommand(redisClient *c)
{
    robj *sobj, *value;
    sobj = lookupKeyWrite(c->db, c->argv[1]);
    if (sobj == NULL || sobj->type != REDIS_LIST) return;

    if (listTypeLength(sobj) > 0)
    {
        robj *dobj = lookupKeyWrite(c->db,c->argv[2]);
        robj *touchedkey = c->argv[1];

        if (dobj && dobj->type != REDIS_LIST) return;
        value = listTypePop(sobj,REDIS_TAIL);
        /* We saved touched key, and protect it, since rpoplpushHandlePush
         * may change the client command argument vector. */
        incrRefCount(touchedkey);
    
        if (!dobj)
        {
            dobj = createZiplistObject();
            dbAdd(c->db,c->argv[2],dobj);
        }
        listTypePush(dobj,value,REDIS_HEAD);

        /* listTypePop returns an object with its refcount incremented */
        decrRefCount(value);

        /* Delete the source list when it is empty */
        if (listTypeLength(sobj) == 0) dbDelete(c->db,touchedkey);
        signalModifiedKey(c->db,touchedkey);
        signalModifiedKey(c->db,c->argv[2]);
        decrRefCount(touchedkey);
        server.dirty += 2;
    }
}

