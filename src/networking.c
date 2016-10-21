#include "redis.h"
#include <sys/uio.h>

#include "ds_log.h"
#include "ds_ctrl.h"

int g_max_fd = 0;

void *dupClientReplyValue(void *o) {
    incrRefCount((robj*)o);
    return o;
}

int listMatchObjects(void *a, void *b) {
    return equalStringObjects(a,b);
}

size_t g_tid = -1;
int g_tag = -1;
static size_t alloc_new_tid()
{
    static size_t sc_tid = 0;
    return sc_tid++;
}

redisClient *createClient(int fd) {
    redisClient *c = zmalloc(sizeof(redisClient));
    c->bufpos = 0;

    anetNonBlock(NULL,fd);
    anetTcpNoDelay(NULL,fd);
    if (aeCreateFileEvent(server.el,fd,AE_READABLE,
        readQueryFromClient, c) == AE_ERR)
    {
        close(fd);
        zfree(c);
        return NULL;
    }

    if (fd > g_max_fd)
    {
        g_max_fd = fd;
    }

    c->tag = 0;

    c->db = &server.db[0];
    c->fd = fd;
    c->querybuf = sdsempty();
    c->reqtype = 0;
    c->argc = 0;
    c->argv = NULL;
    c->cmd = c->lastcmd = NULL;
    c->multibulklen = 0;
    c->bulklen = -1;
    c->sentlen = 0;
    c->flags = 0;
    c->lastinteraction = time(NULL);
    c->authenticated = 0;
    c->replstate = REDIS_REPL_NONE;
    c->reply = listCreate();
    listSetFreeMethod(c->reply,decrRefCount);
    listSetDupMethod(c->reply,dupClientReplyValue);
    c->bpop.keys = NULL;
    c->bpop.count = 0;
    c->bpop.timeout = 0;
    c->bpop.target = NULL;
    c->io_keys = listCreate();
    c->watched_keys = listCreate();
    listSetFreeMethod(c->io_keys,decrRefCount);
    c->pubsub_channels = dictCreate(&setDictType,NULL);
    c->pubsub_patterns = listCreate();
    listSetFreeMethod(c->pubsub_patterns,decrRefCount);
    listSetMatchMethod(c->pubsub_patterns,listMatchObjects);
    listAddNodeTail(server.clients,c);
    initClientMultiState(c);

    c->read_only = server.read_only ? 1 : 0;
    c->ds_id = server.ds_key_num;
    c->dbe_get_keys = listCreate();
    c->stat = 0;
    c->recv_dur = 0;
    c->call_dur = 0;
    c->dbe_get_block_dur = 0;
    c->wr_bl_block_dur = 0;
    c->wr_bl_dur = 0;

    return c;
}

/* Set the event loop to listen for write events on the client's socket.
 * Typically gets called every time a reply is built. */
int _installWriteEvent(redisClient *c) {
    if (c->fd <= 0)
    {
        log_error("_installWriteEvent() fail because of c->fd=%d", c->fd);
        return REDIS_ERR;
    }
    if (c->bufpos == 0 && listLength(c->reply) == 0 &&
        (c->replstate == REDIS_REPL_NONE ||
         c->replstate == REDIS_REPL_ONLINE) &&
        aeCreateFileEvent(server.el, c->fd, AE_WRITABLE,
        sendReplyToClient, c) == AE_ERR)
    {
        log_error("_installWriteEvent() fail because of aeCreateFileEvent() fail");
        return REDIS_ERR;
    }
    return REDIS_OK;
}

/* Create a duplicate of the last object in the reply list when
 * it is not exclusively owned by the reply list. */
robj *dupLastObjectIfNeeded(list *reply) {
    robj *new, *cur;
    listNode *ln;
    redisAssert(listLength(reply) > 0);
    ln = listLast(reply);
    cur = listNodeValue(ln);
    if (cur->refcount > 1) {
        new = dupStringObject(cur);
        decrRefCount(cur);
        listNodeValue(ln) = new;
    }
    return listNodeValue(ln);
}

/* -----------------------------------------------------------------------------
 * Low level functions to add more data to output buffers.
 * -------------------------------------------------------------------------- */

int _addReplyToBuffer(redisClient *c, char *s, size_t len) {
    size_t available = sizeof(c->buf)-c->bufpos;

    if (c->flags & REDIS_CLOSE_AFTER_REPLY) return REDIS_OK;

    /* If there already are entries in the reply list, we cannot
     * add anything more to the static buffer. */
    if (listLength(c->reply) > 0) return REDIS_ERR;

    /* Check that the buffer has enough space available for this string. */
    if (len > available) return REDIS_ERR;

    memcpy(c->buf+c->bufpos,s,len);
    c->bufpos+=len;
    return REDIS_OK;
}

void _addReplyObjectToList(redisClient *c, robj *o) {
    robj *tail;

    if (c->flags & REDIS_CLOSE_AFTER_REPLY) return;

    if (listLength(c->reply) == 0) {
        incrRefCount(o);
        listAddNodeTail(c->reply,o);
    } else {
        tail = listNodeValue(listLast(c->reply));

        /* Append to this object when possible. */
        if (tail->ptr != NULL &&
            sdslen(tail->ptr)+sdslen(o->ptr) <= REDIS_REPLY_CHUNK_BYTES)
        {
            tail = dupLastObjectIfNeeded(c->reply);
            tail->ptr = sdscatlen(tail->ptr,o->ptr,sdslen(o->ptr));
        } else {
            incrRefCount(o);
            listAddNodeTail(c->reply,o);
        }
    }
}

/* This method takes responsibility over the sds. When it is no longer
 * needed it will be free'd, otherwise it ends up in a robj. */
void _addReplySdsToList(redisClient *c, sds s) {
    robj *tail;

    if (c->flags & REDIS_CLOSE_AFTER_REPLY) {
        sdsfree(s);
        return;
    }

    if (listLength(c->reply) == 0) {
        listAddNodeTail(c->reply,createObject(REDIS_STRING,s));
    } else {
        tail = listNodeValue(listLast(c->reply));

        /* Append to this object when possible. */
        if (tail->ptr != NULL &&
            sdslen(tail->ptr)+sdslen(s) <= REDIS_REPLY_CHUNK_BYTES)
        {
            tail = dupLastObjectIfNeeded(c->reply);
            tail->ptr = sdscatlen(tail->ptr,s,sdslen(s));
            sdsfree(s);
        } else {
            listAddNodeTail(c->reply,createObject(REDIS_STRING,s));
        }
    }
}

void _addReplyStringToList(redisClient *c, char *s, size_t len) {
    robj *tail;

    if (c->flags & REDIS_CLOSE_AFTER_REPLY) return;

    if (listLength(c->reply) == 0) {
        listAddNodeTail(c->reply,createStringObject(s,len));
    } else {
        tail = listNodeValue(listLast(c->reply));

        /* Append to this object when possible. */
        if (tail->ptr != NULL &&
            sdslen(tail->ptr)+len <= REDIS_REPLY_CHUNK_BYTES)
        {
            tail = dupLastObjectIfNeeded(c->reply);
            tail->ptr = sdscatlen(tail->ptr,s,len);
        } else {
            listAddNodeTail(c->reply,createStringObject(s,len));
        }
    }
}

/* -----------------------------------------------------------------------------
 * Higher level functions to queue data on the client output buffer.
 * The following functions are the ones that commands implementations will call.
 * -------------------------------------------------------------------------- */

void addReply(redisClient *c, robj *obj) {
    if (_installWriteEvent(c) != REDIS_OK) return;
    redisAssert(!server.vm_enabled || obj->storage == REDIS_VM_MEMORY);

    /* This is an important place where we can avoid copy-on-write
     * when there is a saving child running, avoiding touching the
     * refcount field of the object if it's not needed.
     *
     * If the encoding is RAW and there is room in the static buffer
     * we'll be able to send the object to the client without
     * messing with its page. */
    if (obj->encoding == REDIS_ENCODING_RAW) {
        if (_addReplyToBuffer(c,obj->ptr,sdslen(obj->ptr)) != REDIS_OK)
            _addReplyObjectToList(c,obj);
    } else {
        /* FIXME: convert the long into string and use _addReplyToBuffer()
         * instead of calling getDecodedObject. As this place in the
         * code is too performance critical. */
        obj = getDecodedObject(obj);
        if (_addReplyToBuffer(c,obj->ptr,sdslen(obj->ptr)) != REDIS_OK)
            _addReplyObjectToList(c,obj);
        decrRefCount(obj);
    }
}

void addReply_unsigned(redisClient *c, robj *obj)
{
    if (_installWriteEvent(c) != REDIS_OK) return;
    redisAssert(!server.vm_enabled || obj->storage == REDIS_VM_MEMORY);

    /* This is an important place where we can avoid copy-on-write
     * when there is a saving child running, avoiding touching the
     * refcount field of the object if it's not needed.
     *
     * If the encoding is RAW and there is room in the static buffer
     * we'll be able to send the object to the client without
     * messing with its page. */
    if (obj->encoding == REDIS_ENCODING_RAW) {
        if (_addReplyToBuffer(c,obj->ptr,sdslen(obj->ptr)) != REDIS_OK)
            _addReplyObjectToList(c,obj);
    } else {
        /* FIXME: convert the long into string and use _addReplyToBuffer()
         * instead of calling getDecodedObject. As this place in the
         * code is too performance critical. */
        obj = getDecodedObject_unsigned(obj);
        if (_addReplyToBuffer(c,obj->ptr,sdslen(obj->ptr)) != REDIS_OK)
            _addReplyObjectToList(c,obj);
        decrRefCount(obj);
    }
}

void addReplySds(redisClient *c, sds s) {
    if (_installWriteEvent(c) != REDIS_OK) {
        /* The caller expects the sds to be free'd. */
        sdsfree(s);
        return;
    }
    if (_addReplyToBuffer(c,s,sdslen(s)) == REDIS_OK) {
        sdsfree(s);
    } else {
        /* This method free's the sds when it is no longer needed. */
        _addReplySdsToList(c,s);
    }
}

void addReplyString(redisClient *c, char *s, size_t len) {
    if (_installWriteEvent(c) != REDIS_OK) return;
    if (_addReplyToBuffer(c,s,len) != REDIS_OK)
        _addReplyStringToList(c,s,len);
}

void _addReplyError(redisClient *c, char *s, size_t len) {
    addReplyString(c,"-ERR ",5);
    addReplyString(c,s,len);
    addReplyString(c,"\r\n",2);
}

void addReplyError(redisClient *c, char *err) {
    _addReplyError(c,err,strlen(err));
}

void addReplyErrorFormat(redisClient *c, const char *fmt, ...) {
    va_list ap;
    va_start(ap,fmt);
    sds s = sdscatvprintf(sdsempty(),fmt,ap);
    va_end(ap);
    _addReplyError(c,s,sdslen(s));
    sdsfree(s);
}

void _addReplyStatus(redisClient *c, char *s, size_t len) {
    addReplyString(c,"+",1);
    addReplyString(c,s,len);
    addReplyString(c,"\r\n",2);
}

void addReplyStatus(redisClient *c, char *status) {
    _addReplyStatus(c,status,strlen(status));
}

void addReplyStatusFormat(redisClient *c, const char *fmt, ...) {
    va_list ap;
    va_start(ap,fmt);
    sds s = sdscatvprintf(sdsempty(),fmt,ap);
    va_end(ap);
    _addReplyStatus(c,s,sdslen(s));
    sdsfree(s);
}

/* Adds an empty object to the reply list that will contain the multi bulk
 * length, which is not known when this function is called. */
void *addDeferredMultiBulkLength(redisClient *c) {
    /* Note that we install the write event here even if the object is not
     * ready to be sent, since we are sure that before returning to the
     * event loop setDeferredMultiBulkLength() will be called. */
    if (_installWriteEvent(c) != REDIS_OK) return NULL;
    listAddNodeTail(c->reply,createObject(REDIS_STRING,NULL));
    return listLast(c->reply);
}

/* Populate the length object and try glueing it to the next chunk. */
void setDeferredMultiBulkLength(redisClient *c, void *node, long length) {
    listNode *ln = (listNode*)node;
    robj *len, *next;

    /* Abort when *node is NULL (see addDeferredMultiBulkLength). */
    if (node == NULL) return;

    len = listNodeValue(ln);
    len->ptr = sdscatprintf(sdsempty(),"*%ld\r\n",length);
    if (ln->next != NULL) {
        next = listNodeValue(ln->next);

        /* Only glue when the next node is non-NULL (an sds in this case) */
        if (next->ptr != NULL) {
            len->ptr = sdscatlen(len->ptr,next->ptr,sdslen(next->ptr));
            listDelNode(c->reply,ln->next);
        }
    }
}

/* Add a duble as a bulk reply */
void addReplyDouble(redisClient *c, double d) {
    char dbuf[128], sbuf[128];
    int dlen, slen;
    dlen = snprintf(dbuf,sizeof(dbuf),"%.17g",d);
    slen = snprintf(sbuf,sizeof(sbuf),"$%d\r\n%s\r\n",dlen,dbuf);
    addReplyString(c,sbuf,slen);
}

/* Add a long long as integer reply or bulk len / multi bulk count.
 * Basically this is used to output <prefix><long long><crlf>. */
void _addReplyLongLong(redisClient *c, long long ll, char prefix) {
    char buf[128];
    int len;
    buf[0] = prefix;
    len = ll2string(buf+1,sizeof(buf)-1,ll);
    buf[len+1] = '\r';
    buf[len+2] = '\n';
    addReplyString(c,buf,len+3);
}

void addReplyLongLong(redisClient *c, long long ll) {
    if (ll == 0)
        addReply(c,shared.czero);
    else if (ll == 1)
        addReply(c,shared.cone);
    else
        _addReplyLongLong(c,ll,':');
}

void _addReplyLongLong_u(redisClient *c, unsigned long long ll, char prefix)
{
    char buf[128];
    int len;
    buf[0] = prefix;
    len = ull2string(buf+1,sizeof(buf)-1,ll);
    buf[len+1] = '\r';
    buf[len+2] = '\n';
    addReplyString(c,buf,len+3);
}

void addReplyLongLong_u(redisClient *c, unsigned long long ll)
{
    if (ll == 0)
        addReply(c,shared.czero);
    else if (ll == 1)
        addReply(c,shared.cone);
    else
        _addReplyLongLong_u(c,ll,':');
}

void addReplyMultiBulkLen(redisClient *c, long length) {
    _addReplyLongLong(c,length,'*');
}

/* Create the length prefix of a bulk reply, example: $2234 */
void addReplyBulkLen(redisClient *c, robj *obj) {
    size_t len;

    if (obj->encoding == REDIS_ENCODING_RAW) {
        len = sdslen(obj->ptr);
    } else {
        long n = (long)obj->ptr;

        /* Compute how many bytes will take this integer as a radix 10 string */
        len = 1;
        if (n < 0) {
            len++;
            n = -n;
        }
        while((n = n/10) != 0) {
            len++;
        }
    }
    log_debug("bulk_len=%zd", len);
    _addReplyLongLong(c,len,'$');
}

/* Create the length prefix of a bulk reply, example: $2234, as unsigned */
void addReplyBulkLen_u(redisClient *c, robj *obj)
{
    size_t len;

    if (obj->encoding == REDIS_ENCODING_RAW) {
        len = sdslen(obj->ptr);
    } else {
        unsigned long n = (unsigned long)obj->ptr;

        /* Compute how many bytes will take this integer as a radix 10 string */
        len = 1;
        while((n = n/10) != 0) { len++; }
    }
    log_debug("bulk_len=%zd", len);
    _addReplyLongLong(c,len,'$');
}

/* Add a Redis Object as a bulk reply */
void addReplyBulk(redisClient *c, robj *obj) {
    addReplyBulkLen(c,obj);
    addReply(c,obj);
    addReply(c,shared.crlf);
}

/* Add a Redis Object as a bulk reply, as unsigned */
void addReplyBulk_u(redisClient *c, robj *obj)
{
    addReplyBulkLen_u(c,obj);
    addReply_unsigned(c,obj);
    addReply_unsigned(c,shared.crlf);
}

/* Add a C buffer as bulk reply */
void addReplyBulkCBuffer(redisClient *c, void *p, size_t len) {
    _addReplyLongLong(c,len,'$');
    addReplyString(c,p,len);
    addReply(c,shared.crlf);
}

/* Add a C nul term string as bulk reply */
void addReplyBulkCString(redisClient *c, char *s) {
    if (s == NULL) {
        addReply(c,shared.nullbulk);
    } else {
        addReplyBulkCBuffer(c,s,strlen(s));
    }
}

/* Add a long long as a bulk reply */
void addReplyBulkLongLong(redisClient *c, long long ll) {
    char buf[64];
    int len;

    len = ll2string(buf,64,ll);
    addReplyBulkCBuffer(c,buf,len);
}

void addReplyBulkLongLong_u(redisClient *c, unsigned long long ll)
{
    char buf[64];
    int len;

    len = ull2string(buf,64,ll);
    addReplyBulkCBuffer(c,buf,len);
}

static void acceptCommonHandler(int fd, int master_flag)
{
    if (server.app_status != 0)
    {
        if (server.app_status == 1)
        {
            redisLog(REDIS_WARNING, "the app is shutdowning...");
        }
        else
        {
            redisLog(REDIS_WARNING, "the app has shutdown,can't serve");
            if (master_flag == 0)
            {
                /* master, need to release rdb */
                static int moved = 0;
                if (moved == 0)
                {
                    emptyDb();
                    moved = 1;
                }
            }
            close(fd);
            return;
        }
    }

    redisClient *c;
    if ((c = createClient(fd)) == NULL) {
        redisLog(REDIS_WARNING,"Error allocating resoures for the client, fd=%d", fd);
        close(fd); /* May be already closed, just ingore errors */
        return;
    }
    c->master = master_flag;
    /* If maxclient directive is set and this is one client more... close the
     * connection. Note that we create the client instead to check before
     * for this condition, since now the socket is already set in nonblocking
     * mode and we can send an error for free using the Kernel I/O */
    if (server.maxclients && listLength(server.clients) > server.maxclients) {
        char *err = "-ERR max number of clients reached\r\n";

        /* That's a best effort error message, don't check write errors */
        if (write(c->fd,err,strlen(err)) == -1) {
            /* Nothing to do, Just to avoid the warning... */
        }
        else
        {
            server.bytes_written += strlen(err);
        }
        freeClient(c);
        server.rejected_conns += 1;
        server.accepting_conns = 0;
        return;
    }
    server.accepting_conns = 1;
    server.stat_numconnections++;
}

void acceptTcpHandler(aeEventLoop *el, int fd, void *privdata, int mask)
{
    int cport, cfd;
    char cip[128];
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(mask);
    //REDIS_NOTUSED(privdata);
    int *master = (int *)(privdata);

    cfd = anetTcpAccept(server.neterr, fd, cip, &cport);
    if (cfd == AE_ERR) {
        redisLog(REDIS_WARNING,"#%d# Accepting client connection: %s", *master, server.neterr);
        return;
    }
    redisLog(REDIS_VERBOSE,"#%d# Accepted %s:%d, fd=%d", *master, cip, cport, cfd);
    acceptCommonHandler(cfd, *master);
}

void acceptUnixHandler(aeEventLoop *el, int fd, void *privdata, int mask)
{
    int cfd;
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(mask);
    REDIS_NOTUSED(privdata);

    cfd = anetUnixAccept(server.neterr, fd);
    if (cfd == AE_ERR) {
        redisLog(REDIS_WARNING,"Accepting client connection: %s", server.neterr);
        return;
    }
    redisLog(REDIS_VERBOSE,"Accepted connection to %s", server.unixsocket);
    acceptCommonHandler(cfd, 2);
}


static void freeClientArgv(redisClient *c) {
    int j;
    for (j = 0; j < c->argc; j++)
    {
        //redisLog(REDIS_VERBOSE,"%d: %p, argv->refcount=%d, %s", j, c->argv[j], c->argv[j]->refcount, (j == 0 || j == 1) ? c->argv[j]->ptr : "");
        decrRefCount(c->argv[j]);
    }
    c->argc = 0;
    c->cmd = NULL;
}

void freeClientOutLoop(redisClient *c)
{
    redisLog(REDIS_PROMPT, "freeClientOutLoop...c=%p, fd=%d", c, c->fd);
    anetNonBlock(NULL, c->fd);
    anetTcpNoDelay(NULL, c->fd);
    push_back_to_clean_list(c);
#if 0
    listNode *ln;

    /* Note that if the client we are freeing is blocked into a blocking
     * call, we have to set querybuf to NULL *before* to call
     * unblockClientWaitingData() to avoid processInputBuffer() will get
     * called. Also it is important to remove the file events after
     * this, because this call adds the READABLE event. */
    sdsfree(c->querybuf);
    c->querybuf = NULL;
    //if (c->flags & REDIS_BLOCKED)
    //    unblockClientWaitingData(c);

    /* UNWATCH all the keys */
    //unwatchAllKeys(c);
    listRelease(c->watched_keys);
    /* Unsubscribe from all the pubsub channels */
    //pubsubUnsubscribeAllChannels(c,0);
    //pubsubUnsubscribeAllPatterns(c,0);
    dictRelease(c->pubsub_channels);
    listRelease(c->pubsub_patterns);
    /* Obvious cleanup */
    //aeDeleteFileEvent(server.el,c->fd,AE_READABLE);
    //aeDeleteFileEvent(server.el,c->fd,AE_WRITABLE);
    listRelease(c->reply);
    freeClientArgv(c);
    listRelease(c->io_keys);
    /* Remove from the list of clients */
    //ln = listSearchKey(server.clients,c);
    //redisAssert(ln != NULL);
    //listDelNode(server.clients,ln);
    /* When client was just unblocked because of a blocking operation,
     * remove it from the list with unblocked clients. */
    if (c->flags & REDIS_UNBLOCKED) {
        ln = listSearchKey(server.unblocked_clients,c);
        redisAssert(ln != NULL);
        listDelNode(server.unblocked_clients,ln);
    }
    /* Remove from the list of clients waiting for swapped keys, or ready
     * to be restarted, but not yet woken up again. */
    if (c->flags & REDIS_IO_WAIT) {
        redisAssert(server.vm_enabled);
        if (listLength(c->io_keys) == 0) {
            ln = listSearchKey(server.io_ready_clients,c);

            /* When this client is waiting to be woken up (REDIS_IO_WAIT),
             * it should be present in the list io_ready_clients */
            redisAssert(ln != NULL);
            listDelNode(server.io_ready_clients,ln);
        } else {
            while (listLength(c->io_keys)) {
                ln = listFirst(c->io_keys);
                dontWaitForSwappedKey(c,ln->value);
            }
        }
        server.vm_blocked_clients--;
    }
    listRelease(c->io_keys);

    /* Master/slave cleanup.
     * Case 1: we lost the connection with a slave. */
    if (c->flags & REDIS_SLAVE) {
        if (c->replstate == REDIS_REPL_SEND_BULK && c->repldbfd != -1)
            close(c->repldbfd);
        list *l = (c->flags & REDIS_MONITOR) ? server.monitors : server.slaves;
        ln = listSearchKey(l,c);
        redisAssert(ln != NULL);
        listDelNode(l,ln);
    }

    /* Case 2: we lost the connection with the master. */
    if (c->flags & REDIS_MASTER) {
        server.master = NULL;
        server.replstate = REDIS_REPL_CONNECT;
        server.repl_down_since = time(NULL);
        /* Since we lost the connection with the master, we should also
         * close the connection with all our slaves if we have any, so
         * when we'll resync with the master the other slaves will sync again
         * with us as well. Note that also when the slave is not connected
         * to the master it will keep refusing connections by other slaves.
         *
         * We do this only if server.masterhost != NULL. If it is NULL this
         * means the user called SLAVEOF NO ONE and we are freeing our
         * link with the master, so no need to close link with slaves. */
        if (server.masterhost != NULL) {
            while (listLength(server.slaves)) {
                ln = listFirst(server.slaves);
                freeClient((redisClient*)ln->value);
            }
        }
    }
    /* Release memory */
    close(c->fd);
    zfree(c->argv);
    //freeClientMultiState(c);
    zfree(c);
#endif
}

void freeClient(redisClient *c)
{
    //redisLog(REDIS_PROMPT, "freeClient...fd=%d", c->fd);
    listNode *ln;

    /* Note that if the client we are freeing is blocked into a blocking
     * call, we have to set querybuf to NULL *before* to call
     * unblockClientWaitingData() to avoid processInputBuffer() will get
     * called. Also it is important to remove the file events after
     * this, because this call adds the READABLE event. */
    sdsfree(c->querybuf);
    c->querybuf = NULL;
    if (c->flags & REDIS_BLOCKED)
        unblockClientWaitingData(c);

    /* UNWATCH all the keys */
    unwatchAllKeys(c);
    listRelease(c->watched_keys);
    /* Unsubscribe from all the pubsub channels */
    pubsubUnsubscribeAllChannels(c,0);
    pubsubUnsubscribeAllPatterns(c,0);
    dictRelease(c->pubsub_channels);
    listRelease(c->pubsub_patterns);
    /* Obvious cleanup */
    aeDeleteFileEvent(server.el,c->fd,AE_READABLE);
    aeDeleteFileEvent(server.el,c->fd,AE_WRITABLE);
    listRelease(c->reply);
    freeClientArgv(c);
    close(c->fd);
    /* Remove from the list of clients */
    ln = listSearchKey(server.clients,c);
    redisAssert(ln != NULL);
    listDelNode(server.clients,ln);

    /* Remove from the list of write_bl_clients */
    ln = listSearchKey(server.write_bl_clients,c);
    if (ln)
    {
        listDelNode(server.write_bl_clients,ln);
    }

    /* When client was just unblocked because of a blocking operation,
     * remove it from the list with unblocked clients. */
    if (c->flags & REDIS_UNBLOCKED) {
        ln = listSearchKey(server.unblocked_clients,c);
        redisAssert(ln != NULL);
        listDelNode(server.unblocked_clients,ln);
    }
    /* Remove from the list of clients waiting for swapped keys, or ready
     * to be restarted, but not yet woken up again. */
    if (c->flags & REDIS_IO_WAIT) {
        redisAssert(server.vm_enabled);
        if (listLength(c->io_keys) == 0) {
            ln = listSearchKey(server.io_ready_clients,c);

            /* When this client is waiting to be woken up (REDIS_IO_WAIT),
             * it should be present in the list io_ready_clients */
            redisAssert(ln != NULL);
            listDelNode(server.io_ready_clients,ln);
        } else {
            while (listLength(c->io_keys)) {
                ln = listFirst(c->io_keys);
                dontWaitForSwappedKey(c,ln->value);
            }
        }
        server.vm_blocked_clients--;
    }
    listRelease(c->io_keys);
    listRelease(c->dbe_get_keys);
    /* Master/slave cleanup.
     * Case 1: we lost the connection with a slave. */
    if (c->flags & REDIS_SLAVE) {
        if (c->replstate == REDIS_REPL_SEND_BULK && c->repldbfd != -1)
            close(c->repldbfd);
        list *l = (c->flags & REDIS_MONITOR) ? server.monitors : server.slaves;
        ln = listSearchKey(l,c);
        redisAssert(ln != NULL);
        listDelNode(l,ln);
    }

    /* Case 2: we lost the connection with the master. */
    if (c->flags & REDIS_MASTER) {
        server.master = NULL;
        server.replstate = REDIS_REPL_CONNECT;
        server.repl_down_since = time(NULL);
        /* Since we lost the connection with the master, we should also
         * close the connection with all our slaves if we have any, so
         * when we'll resync with the master the other slaves will sync again
         * with us as well. Note that also when the slave is not connected
         * to the master it will keep refusing connections by other slaves.
         *
         * We do this only if server.masterhost != NULL. If it is NULL this
         * means the user called SLAVEOF NO ONE and we are freeing our
         * link with the master, so no need to close link with slaves. */
        if (server.masterhost != NULL) {
            while (listLength(server.slaves)) {
                ln = listFirst(server.slaves);
                freeClient((redisClient*)ln->value);
            }
        }
    }
    /* Release memory */
    zfree(c->argv);
    freeClientMultiState(c);
    zfree(c);
}

void sendReplyToClient(aeEventLoop *el, int fd, void *privdata, int mask) {
    redisClient *c = privdata;
    int nwritten = 0, totwritten = 0, objlen;
    robj *o;
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(mask);

    while(c->bufpos > 0 || listLength(c->reply)) {
        if (c->bufpos > 0) {
            if (c->flags & REDIS_MASTER) {
                /* Don't reply to a master */
                nwritten = c->bufpos - c->sentlen;
            } else {
                nwritten = write(fd,c->buf+c->sentlen,c->bufpos-c->sentlen);
                if (nwritten <= 0) break;
                server.bytes_written += nwritten;
            }
            c->sentlen += nwritten;
            totwritten += nwritten;

            /* If the buffer was sent, set bufpos to zero to continue with
             * the remainder of the reply. */
            if (c->sentlen == c->bufpos) {
                c->bufpos = 0;
                c->sentlen = 0;
            }
        } else {
            o = listNodeValue(listFirst(c->reply));
            objlen = sdslen(o->ptr);

            if (objlen == 0) {
                listDelNode(c->reply,listFirst(c->reply));
                continue;
            }

            if (c->flags & REDIS_MASTER) {
                /* Don't reply to a master */
                nwritten = objlen - c->sentlen;
            } else {
                nwritten = write(fd, ((char*)o->ptr)+c->sentlen,objlen-c->sentlen);
                if (nwritten <= 0) break;
                server.bytes_written += nwritten;
            }
            c->sentlen += nwritten;
            totwritten += nwritten;

            /* If we fully sent the object on head go to the next one */
            if (c->sentlen == objlen) {
                listDelNode(c->reply,listFirst(c->reply));
                c->sentlen = 0;
            }
        }
        /* Note that we avoid to send more thank REDIS_MAX_WRITE_PER_EVENT
         * bytes, in a single threaded server it's a good idea to serve
         * other clients as well, even if a very large request comes from
         * super fast link that is always able to accept data (in real world
         * scenario think about 'KEYS *' against the loopback interfae) */
        if (totwritten > REDIS_MAX_WRITE_PER_EVENT) break;
    }
    if (nwritten == -1) {
        if (errno == EAGAIN) {
            nwritten = 0;
        } else {
            redisLog(REDIS_VERBOSE,
                "Error writing to client: %s", strerror(errno));
            freeClient(c);
            return;
        }
    }
    if (totwritten > 0) c->lastinteraction = time(NULL);
    if (c->bufpos == 0 && listLength(c->reply) == 0) {
        c->sentlen = 0;
        aeDeleteFileEvent(server.el,c->fd,AE_WRITABLE);

        /* Close connection after entire reply has been sent. */
        if (c->flags & REDIS_CLOSE_AFTER_REPLY) freeClient(c);
    }
}

/* resetClient prepare the client to process the next command */
void resetClient(redisClient *c) {
    const long long diff = server.ustime - c->start_time;
    if (server.slow_log && (c->recv_dur + diff) >= server.slowlog_log_slower_than)
    {
        log_prompt("\"%s\"(us): %d, rcv=%lld"
                ", l=%d, proc=%lld, call=%lld"
                ", bl=%"PRId64" (get=%lld, wr=%lld)"
            , c->argv ? (char*)c->argv[0]->ptr : "", c->stat, c->recv_dur
            , c->req_len, diff
            , c->call_dur, c->wr_bl_dur, c->dbe_get_block_dur, c->wr_bl_block_dur
            );
    }

    freeClientArgv(c);
    c->reqtype = 0;
    c->multibulklen = 0;
    c->bulklen = -1;
    c->recv_dur = 0;
    c->call_dur = 0;
    c->dbe_get_block_dur = 0;
    c->wr_bl_block_dur = 0;
    c->wr_bl_dur = 0;
    if (sdslen(c->querybuf))
    {
        c->start_time = server.ustime;
    }
    c->stat = sdslen(c->querybuf) == 0 ? 1 : 2;

    //log_debug("c->querybuf: len=%d, avail=%d, %p"
    //          , sdslen(c->querybuf), sdsavail(c->querybuf), c->querybuf);
    if (sdslen(c->querybuf) == 0
        && (server.querybuf_reuse == 0 || (server.maxidletime == 0 && sdsavail(c->querybuf) > 4096)))
    {
        sdsfree(c->querybuf);
        c->querybuf = NULL;
        c->querybuf = sdsempty();
    }
}

void closeTimedoutClients(void) {
    redisClient *c;
    listNode *ln;
    time_t now = time(NULL);
    listIter li;

    listRewind(server.clients,&li);
    while ((ln = listNext(&li)) != NULL) {
        c = listNodeValue(ln);
        if (server.maxidletime &&
            !(c->flags & REDIS_SLAVE) &&    /* no timeout for slaves */
            !(c->flags & REDIS_MASTER) &&   /* no timeout for masters */
            !(c->flags & REDIS_BLOCKED) &&  /* no timeout for BLPOP */
            !(c->flags & REDIS_DBE_GET_WAIT) &&  /* no timeout for wait for dbe_get */
            !(c->flags & REDIS_WR_BL_WAIT) &&  /* no timeout for wait for write bl */
            dictSize(c->pubsub_channels) == 0 && /* no timeout for pubsub */
            listLength(c->pubsub_patterns) == 0 &&
            (now - c->lastinteraction > server.maxidletime))
        {
            redisLog(REDIS_VERBOSE,"Closing idle client");
            freeClient(c);
        } else if (c->flags & REDIS_BLOCKED) {
            if (c->bpop.timeout != 0 && c->bpop.timeout < now) {
                addReply(c,shared.nullmultibulk);
                unblockClientWaitingData(c);
            }
        }
    }
}

int processInlineBuffer(redisClient *c) {
    char *newline = strstr(c->querybuf,"\r\n");
    int argc, j;
    sds *argv;
    size_t querylen;

    /* Nothing to do without a \r\n */
    if (newline == NULL)
        return REDIS_ERR;

    /* Split the input buffer up to the \r\n */
    querylen = newline-(c->querybuf);
    c->req_len = querylen;
    //log_string(c->querybuf, querylen);
    argv = sdssplitlen(c->querybuf,querylen," ",1,&argc);

    /* Leave data after the first line of the query in the buffer */
    c->querybuf = sdsrange(c->querybuf,querylen+2,-1);

    /* Setup argv array on client structure */
    if (c->argv) zfree(c->argv);
    c->argv = zmalloc(sizeof(robj*)*argc);

    /* Create redis objects for all arguments. */
    for (c->argc = 0, j = 0; j < argc; j++) {
        if (sdslen(argv[j])) {
            c->argv[c->argc] = createObject(REDIS_STRING,argv[j]);
            c->argc++;
        } else {
            sdsfree(argv[j]);
        }
    }
    zfree(argv);
    return REDIS_OK;
}

/* Helper function. Trims query buffer to make the function that processes
 * multi bulk requests idempotent. */
static void setProtocolError(redisClient *c, int pos) {
    if (server.verbosity >= REDIS_VERBOSE) {
        sds client = getClientInfoString(c);
        redisLog(REDIS_VERBOSE,
            "Protocol error from client: %s", client);
        sdsfree(client);
    }
    c->flags |= REDIS_CLOSE_AFTER_REPLY;
    c->querybuf = sdsrange(c->querybuf,pos,-1);
}

int processMultibulkBuffer(redisClient *c)
{
    char *newline = NULL;
    int pos = 0, ok;
    long long ll;

    if (c->multibulklen == 0) {
        /* The client should have been reset */
        redisAssert(c->argc == 0);

        /* Multi bulk length cannot be read without a \r\n */
        newline = strchr(c->querybuf,'\r');
        if (newline == NULL)
        {
            log_info("%s", "Multi bulk without a \\r");
            return REDIS_ERR;
        }

        /* Buffer should also contain \n */
        if (newline-(c->querybuf) > ((signed)sdslen(c->querybuf)-2))
        {
            log_info("%s", "Multi bulk without a \\n");
            return REDIS_ERR;
        }

        /* We know for sure there is a whole line since newline != NULL,
         * so go ahead and find out the multi bulk length. */
        redisAssert(c->querybuf[0] == '*');
        ok = string2ll(c->querybuf+1,newline-(c->querybuf+1),&ll);
        if (!ok || ll > 1024*1024) {
            log_error("%s", "Protocol error: invalid multibulk length");
            addReplyError(c,"Protocol error: invalid multibulk length");
            setProtocolError(c,pos);
            return REDIS_ERR;
        }
        log_debug("multibulklen=%d", ll);

        pos = (newline-c->querybuf)+2;
        if (ll <= 0) {
            c->querybuf = sdsrange(c->querybuf,pos,-1);
            c->req_len += pos;
            return REDIS_OK;
        }

        c->multibulklen = ll;

        /* Setup argv array on client structure */
        if (c->argv) zfree(c->argv);
        c->argv = zmalloc(sizeof(robj*)*c->multibulklen);
    }

    redisAssert(c->multibulklen > 0);
    while(c->multibulklen) {
        /* Read bulk length if unknown */
        if (c->bulklen == -1) {
            newline = strchr(c->querybuf+pos,'\r');
            if (newline == NULL)
            {
                log_info("%s", "Multi bulk without a \\r");
                break;
            }

            /* Buffer should also contain \n */
            if (newline-(c->querybuf) > ((signed)sdslen(c->querybuf)-2))
            {
                log_info("%s", "Multi bulk without a \\n");
                break;
            }

            if (c->querybuf[pos] != '$') {
                log_error("Protocol error: expected '$', got '%c'", c->querybuf[pos]);
                addReplyErrorFormat(c,
                    "Protocol error: expected '$', got '%c'",
                    c->querybuf[pos]);
                setProtocolError(c,pos);
                return REDIS_ERR;
            }

            ok = string2ll(c->querybuf+pos+1,newline-(c->querybuf+pos+1),&ll);
            if (!ok || ll < 0 || ll > 512*1024*1024) {
                log_error("%s", "Protocol error: invalid bulk length");
                addReplyError(c,"Protocol error: invalid bulk length");
                setProtocolError(c,pos);
                return REDIS_ERR;
            }
            log_debug("bulklen=%d", ll);

            pos += newline-(c->querybuf+pos)+2;
            c->bulklen = ll;
        }

        /* Read bulk argument */
        if (sdslen(c->querybuf)-pos < (unsigned)(c->bulklen+2)) {
            /* Not enough data (+2 == trailing \r\n) */
            //log_debug("%s", "Not enough data");
            break;
        } else {
            c->argv[c->argc++] = createStringObject(c->querybuf+pos,c->bulklen);
            pos += c->bulklen+2;
            c->bulklen = -1;
            c->multibulklen--;
            //log_debug("argv[%d] : %s", c->argc - 1, c->argv[c->argc - 1]->ptr);
        }
    }

    /* Trim to pos */
    if (pos)
    {
        c->req_len += pos;
        c->querybuf = sdsrange(c->querybuf,pos,-1);
    }

    /* We're done when c->multibulk == 0 */
    if (c->multibulklen == 0) {
        return REDIS_OK;
    }
    return REDIS_ERR;
}

void processInputBuffer(redisClient *c) {
    /* Keep processing while there is something in the input buffer */
    while(sdslen(c->querybuf)) {
        /* Immediately abort if the client is in the middle of something. */
        if (c->flags & REDIS_BLOCKED || c->flags & REDIS_IO_WAIT) return;
        if (c->flags & REDIS_DBE_GET_WAIT || c->flags & REDIS_WR_BL_WAIT) return;

        /* REDIS_CLOSE_AFTER_REPLY closes the connection once the reply is
         * written to the client. Make sure to not let the reply grow after
         * this flag has been set (i.e. don't process more commands). */
        if (c->flags & REDIS_CLOSE_AFTER_REPLY) return;

        /* Determine request type when unknown. */
        if (!c->reqtype) {
            g_tid = c->tid = alloc_new_tid();
            redisLog(REDIS_DEBUG, "tid=%d, fd=%d", c->tid, c->fd);
            c->req_len = 0;
            if (c->querybuf[0] == '*') {
                c->reqtype = REDIS_REQ_MULTIBULK;
            } else {
                c->reqtype = REDIS_REQ_INLINE;
            }
        }

        if (c->reqtype == REDIS_REQ_INLINE) {
            if (processInlineBuffer(c) != REDIS_OK) break;
        } else if (c->reqtype == REDIS_REQ_MULTIBULK) {
            if (processMultibulkBuffer(c) != REDIS_OK) break;
        } else {
            redisPanic("Unknown request type");
        }

        /* Multibulk processing could see a <= 0 length. */
        if (c->argc == 0) {
            resetClient(c);
        } else {
            /* Only reset the client when the command was executed. */
            if (processCommand(c) == REDIS_OK)
            {
                //log_debug("processCommand() finish,resetClient...");
                resetClient(c);
            }
        }
    }
}

void readQueryFromClient(aeEventLoop *el, int fd, void *privdata, int mask) {
    redisClient *c = (redisClient*) privdata;
    char buf[REDIS_IOBUF_LEN];
    int nread;
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(mask);

    if (c->flags & REDIS_DBE_GET_WAIT) return;
    if (c->stat != 2)
    {
        c->start_time = server.ustime;
    }

    nread = read(fd, buf, REDIS_IOBUF_LEN);
    if (nread == -1) {
        if (errno == EAGAIN) {
            nread = 0;
        } else {
            redisLog(REDIS_VERBOSE, "Reading from client: %s",strerror(errno));
            freeClient(c);
            return;
        }
    } else if (nread == 0) {
        redisLog(REDIS_VERBOSE, "Client closed connection, fd=%d", fd);
        freeClient(c);
        return;
    }
    if (nread) {
        //const size_t querybuf_len = sdslen(c->querybuf);
        c->querybuf = sdscatlen(c->querybuf,buf,nread);
        server.bytes_read += nread;
        //redisLog(REDIS_DEBUG, "read() %d from %d, %d -> %d", nread, fd, querybuf_len, sdslen(c->querybuf));
        c->lastinteraction = time(NULL);
    } else {
        return;
    }
    if (sdslen(c->querybuf) > server.client_max_querybuf_len) {
        sds ci = getClientInfoString(c), bytes = sdsempty();

        bytes = sdscatrepr(bytes,c->querybuf,64);
        redisLog(REDIS_WARNING,"Closing client that reached max query buffer length: %s (qbuf initial bytes: %s)", ci, bytes);
        sdsfree(ci);
        sdsfree(bytes);
        freeClient(c);
        return;
    }
    g_tid = c->tid;
    g_tag = c->master;
    processInputBuffer(c);
    g_tid = -1;
    g_tag = -1;
}

void getClientsMaxBuffers(unsigned long *longest_output_list,
                          unsigned long *biggest_input,
                          unsigned long *biggest_input_buffer)
{
    redisClient *c;
    listNode *ln;
    listIter li;
    unsigned long lol = 0, bib = 0, buf_len = 0;

    listRewind(server.clients,&li);
    while ((ln = listNext(&li)) != NULL) {
        c = listNodeValue(ln);

        if (listLength(c->reply) > lol) lol = listLength(c->reply);
        const size_t il = sdslen(c->querybuf);
        const size_t len = il + sdsavail(c->querybuf);
        if (il > bib) bib = il;
        if (len > buf_len) buf_len = len;
    }
    *longest_output_list = lol;
    *biggest_input = bib;
    *biggest_input_buffer = buf_len;
}

/* Turn a Redis client into an sds string representing its state. */
sds getClientInfoString(redisClient *client) {
    char ip[32], flags[16], events[3], *p;
    int port;
    time_t now = time(NULL);
    int emask;

    if (anetPeerToString(client->fd,ip,&port) == -1) {
        ip[0] = '?';
        ip[1] = '\0';
        port = 0;
    }
    p = flags;
    if (client->flags & REDIS_SLAVE) {
        if (client->flags & REDIS_MONITOR)
            *p++ = 'O';
        else
            *p++ = 'S';
    }
    if (client->flags & REDIS_MASTER) *p++ = 'M';
    if (client->flags & REDIS_MULTI) *p++ = 'x';
    if (client->flags & REDIS_BLOCKED) *p++ = 'b';
    if (client->flags & REDIS_IO_WAIT) *p++ = 'i';
    if (client->flags & REDIS_DIRTY_CAS) *p++ = 'd';
    if (client->flags & REDIS_CLOSE_AFTER_REPLY) *p++ = 'c';
    if (client->flags & REDIS_UNBLOCKED) *p++ = 'u';
    if (p == flags) *p++ = 'N';
    *p++ = '\0';

    emask = client->fd == -1 ? 0 : aeGetFileEvents(server.el,client->fd);
    p = events;
    if (emask & AE_READABLE) *p++ = 'r';
    if (emask & AE_WRITABLE) *p++ = 'w';
    *p = '\0';
    return sdscatprintf(sdsempty(),
        "addr=%s:%d fd=%d idle=%ld flags=%s db=%d sub=%d psub=%d qbuf=%lu obl=%lu oll=%lu events=%s cmd=%s",
        ip,port,client->fd,
        (long)(now - client->lastinteraction),
        flags,
        client->db->id,
        (int) dictSize(client->pubsub_channels),
        (int) listLength(client->pubsub_patterns),
        (unsigned long) sdslen(client->querybuf),
        (unsigned long) client->bufpos,
        (unsigned long) listLength(client->reply),
        events,
        client->lastcmd ? client->lastcmd->name : "NULL");
}

sds getAllClientsInfoString(void) {
    listNode *ln;
    listIter li;
    redisClient *client;
    sds o = sdsempty();

    listRewind(server.clients,&li);
    while ((ln = listNext(&li)) != NULL) {
        client = listNodeValue(ln);
        o = sdscatsds(o,getClientInfoString(client));
        o = sdscatlen(o,"\n",1);
    }
    return o;
}

void clientCommand(redisClient *c) {
    listNode *ln;
    listIter li;
    redisClient *client;

    if (!strcasecmp(c->argv[1]->ptr,"list") && c->argc == 2) {
        sds o = getAllClientsInfoString();
        addReplyBulkCBuffer(c,o,sdslen(o));
        sdsfree(o);
    } else if (!strcasecmp(c->argv[1]->ptr,"kill") && c->argc == 3) {
        listRewind(server.clients,&li);
        while ((ln = listNext(&li)) != NULL) {
            char ip[32], addr[64];
            int port;

            client = listNodeValue(ln);
            if (anetPeerToString(client->fd,ip,&port) == -1) continue;
            snprintf(addr,sizeof(addr),"%s:%d",ip,port);
            if (strcmp(addr,c->argv[2]->ptr) == 0) {
                addReply(c,shared.ok);
                if (c == client) {
                    client->flags |= REDIS_CLOSE_AFTER_REPLY;
                } else {
                    freeClient(client);
                }
                return;
            }
        }
        addReplyError(c,"No such client");
    } else {
        addReplyError(c, "Syntax error, try CLIENT (LIST | KILL ip:port)");
    }
}

void rewriteClientCommandVector(redisClient *c, int argc, ...) {
    va_list ap;
    int j;
    robj **argv; /* The new argument vector */

    argv = zmalloc(sizeof(robj*)*argc);
    va_start(ap,argc);
    for (j = 0; j < argc; j++) {
        robj *a;
        
        a = va_arg(ap, robj*);
        argv[j] = a;
        incrRefCount(a);
    }
    /* We free the objects in the original vector at the end, so we are
     * sure that if the same objects are reused in the new vector the
     * refcount gets incremented before it gets decremented. */
    for (j = 0; j < c->argc; j++) decrRefCount(c->argv[j]);
    zfree(c->argv);
    /* Replace argv and argc with our new versions. */
    c->argv = argv;
    c->argc = argc;
    c->cmd = lookupCommand(c->argv[0]->ptr);
    redisAssert(c->cmd != NULL);
    va_end(ap);
}
