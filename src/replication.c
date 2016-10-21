#include "redis.h"
#include "replication.h"
#include "bl_ctx.h"
#include "ds_ctrl.h"
#include "ds_binlog.h"
#include "dbmng.h"
#include "repl_if.h"
#include "sync_if.h"
#include "pf_util.h"
#include "key_filter.h"

#include <sys/time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <pthread.h>


/* just for compile */
#if 0
int repl_slave_start(const char *master_ip, int master_port, const char *dir, NoticeFunc f)
{
    (void)master_ip;
    (void)master_port;
    (void)dir;
    (void)f;
    return 0;
}

int repl_master_start(const char *dir, int fd, long long bl_tag)
{
    (void)dir;
    (void)fd;
    (void)bl_tag;
    return 0;
}

int sync_slave_start(const char *master_ip, int master_port, const char *dir, long long bl_tag, int port)
{
    (void)master_ip;
    (void)master_port;
    (void)dir;
    (void)bl_tag;
    (void)port;
    return 0;
}

int sync_master_start(const char *bl_dir, const char *prefix, off_t max_size, int max_idx, int fd, long long bl_tag)
{
    (void)bl_dir;
    (void)prefix;
    (void)max_idx;
    (void)fd;
    (void)max_size;
    (void)bl_tag;
    return 0;
}
/* end of temp */
#endif


typedef struct repl_tran_t
{
    int fd;
    int session_id;
    long long bl_tag;
    void *c;
} repl_tran;

static repl_t scarRepl[2][2];

static int bl_relay_append(int sync, int master_slave, const char *buf, int len)
{
    REDIS_NOTUSED(sync);
    REDIS_NOTUSED(master_slave);
    REDIS_NOTUSED(buf);
    REDIS_NOTUSED(len);
    return 0;
}

static int64_t binlogGetLastTs(int sync, int master_slave)
{
    REDIS_NOTUSED(sync);
    REDIS_NOTUSED(master_slave);
    int64_t seq = 0;
    return seq;
}

void blockCommand(redisClient *c)
{
    uint64_t ts;
    const int ret = dbmng_block_upd_cp(c->tag, &ts);
    if (ret == 0)
    {
        addReplyLongLong_u(c, (unsigned long long)ts);
    }
    else
    {
        addReplyLongLong(c, ret);
    }
}

void unblockCommand(redisClient *c)
{
    const int ret = dbmng_unblock_upd_cp(c->tag);
    addReplyLongLong(c, ret);
}

/* ----------------------------- replication master ------------------------------- */
static void *do_repl_master(void *arg);
void processRepl(redisClient *c)
{
    //repl_tran *t = (repl_tran *)zmalloc(sizeof(repl_tran));
    //if (t == 0)
    //{
    //    redisLog(REDIS_WARNING, "malloc() fail for transaction replCommand");
    //    return;
    //}
    //t->fd = c->fd;
    c->bl_tag = bl_get_last_ts(dbmng_get_bl(c->tag), 1);
    //t->c = c;
    //freeClientWithoutFd(c);

    pthread_t tid;
    const int ret = pthread_create(&tid, 0, do_repl_master, c);
    if (ret != 0)
    {
        redisLog(REDIS_WARNING, "pthread_create() fail for replCommand, errcode=%d", ret);
        aeCreateFileEvent(server.el, c->fd, AE_READABLE, readQueryFromClient, c);
    }
    else
    {
        redisLog(REDIS_PROMPT, "pthread_create() succ for replCommand, thread_id=%d", tid);
    }
}

void replCommand(redisClient *c)
{
    aeDeleteFileEvent(server.el, c->fd, AE_READABLE);
    //aeDeleteFileEvent(server.el, c->fd, AE_WRITABLE);

    uint64_t ts;
    dbmng_block_upd_cp(c->tag, &ts);
    if (dbmng_cp_is_active(c->tag) == 0)
    {
        processRepl(c);
    }
    else
    {
        /* add client into list waiting update cp transaction finish */
        listAddNodeTail(server.repl_slaves, c);
    }
}

static void *do_repl_master(void *arg)
{
    redisClient *c = (redisClient *)arg;
    //repl_tran *t = (repl_tran *)arg;
    char peer_ip[64];
    int peer_port;

    anetPeerToString(c->fd, peer_ip, &peer_port);

    char path[256];
    if (anetBlock(path, c->fd) == ANET_OK)
    {
        struct timespec start = pf_get_time_tick();
        const int ret = repl_master_start(get_dbe_path(path, sizeof(path), "local"), c->fd, c->bl_tag);
        struct timespec end = pf_get_time_tick();
        const long long diff = pf_get_time_diff_nsec(start, end);
        redisLog(REDIS_PROMPT, "repl_master over, ret=%d, during=%llds(%lldms), slave=%s:%d",
                 ret, diff / 1000000000, diff / 1000000, peer_ip, peer_port);
    }
    else
    {
        redisLog(REDIS_WARNING, "set fd=%d to block mode fail: %s", c->fd, path);
    }

    //redisClient *c = t->c;
    //close(t->fd);
    //zfree(t);

    freeClientOutLoop(c);

    {
        /* send unblock cmd to main_thread */
        char err[256];
        const int fd = anetTcpConnect(err, "127.0.0.1", server.port);
        if (fd == ANET_ERR)
        {
            redisLog(REDIS_WARNING, "anetTcpConnect() to 127.0.0.1:%d fail: %s",
                     server.port, err);
            return (void*)0;
        }
        anetWrite(fd, "*1\r\n$7\r\nunblock\r\n", 17);
        close(fd);
    }

    return (void*)0;
}

/* ----------------------------- sync master ------------------------------- */
static int bl_filter_gen(const char *bl_ptr, int bl_len, const char *ds_key)
{
    if (bl_ptr == 0 || bl_len <= 0)
    {
        redisLog(REDIS_WARNING, "bl_len=%d, pass", bl_len);
        return 0;
    }

    int rslt = 0;
    op_rec rec;
    const int ret = parse_op_rec_key(bl_ptr, bl_len, &rec);
    if (ret == 0)
    {
        int ds_key_len = 0;
        if (rec.cmd == 0)
        {
            rslt = 1;
        }
        else if (ds_key && (ds_key_len = strlen(ds_key)) > 0)
        {
            rslt = filter_gen(rec.key.ptr, rec.key.len, ds_key);
            if (rslt)
            {
                redisLog(REDIS_DEBUG
                    , "filter_gen enable for the key, ds_key_len=%d, ds_key=%s"
                    , ds_key_len, ds_key ? ds_key : "");
            }
        }

        if (rec.argc != 0)
        {
            /* free memory malloc in parse_op_rec() */
            zfree(rec.argv);
        }

        if (rslt == 0 && server.is_slave && ds_key_len == 0)
        {
            rslt = sync_filter(rec.key.ptr, rec.key.len);
            if (rslt)
            {
                redisLog(REDIS_DEBUG
                    , "sync_filter rule enable for the key, ds_key_len=%d, ds_key=%s"
                    , ds_key_len, ds_key ? ds_key : "");
            }
        }
    }
    redisLog(REDIS_DEBUG
            , "bl_len=%d, ds_key=%s, filter_rslt=%d, ret=%d, cmd=%d"
            , bl_len, ds_key ? ds_key : "", rslt, ret, rec.cmd);
    return rslt;
}

//static void *do_sync_master(void *arg);
/* 
 * cmd format: sync bl_tag ds_id [ds_key]
 */
void syncCommand(redisClient *c)
{
    char bl_path[256];
    get_bl_path(bl_path, sizeof(bl_path), 0);
    //const int fd = c->fd;
    char *eptr;
    c->bl_tag = strtoll(c->argv[1]->ptr, &eptr, 10);
    const unsigned long long ds_id_s = strtoll(c->argv[2]->ptr, &eptr, 10);
    const char *ds_key_s = 0;
    if (c->argc >= 4)
    {
        ds_key_s = c->argv[3]->ptr;
    }
    //freeClientOutLoop(c);
    char peer_ip[64];
    int peer_port;
    anetPeerToString(c->fd, peer_ip, &peer_port);

    redisLog(REDIS_VERBOSE
             , "syncCommand()...bl_tag=%lld, ds_id=%llu, argc=%d, from %s:%d"
             , c->bl_tag, ds_id_s, c->argc, peer_ip, peer_port);

    aeDeleteFileEvent(server.el, c->fd, AE_READABLE);

    const int ret = sync_master_start(bl_path, dbmng_conf.log_prefix, dbmng_conf.binlog_max_size, BL_MAX_IDX, c, freeClientOutLoop, server.ds_key_num, ds_id_s, ds_key_s, bl_filter_gen);
    if (ret != 0)
    {
        redisLog(REDIS_WARNING, "sync_master_start fail, ret=%d, slave=%s:%d",
                 ret, peer_ip, peer_port);
    }
#if 0
    repl_tran *t = (repl_tran *)zmalloc(sizeof(repl_tran));
    if (t == 0)
    {
        redisLog(REDIS_WARNING, "malloc() fail for transaction syncCommand");
        freeClient(c);
        return;
    }
    t->fd = c->fd;
    t->session_id = atoi(c->argv[1]->ptr);
    char *eptr;
    t->bl_tag = strtoll(c->argv[2]->ptr, &eptr, 10);
    freeClientWithoutFd(c);

    pthread_t tid;
    const int ret = pthread_create(&tid, 0, do_sync_master, t);
    if (ret != 0)
    {
        redisLog(REDIS_WARNING, "pthread_create() fail for syncCommand, errcode=%d", ret);
        close(t->fd);
        zfree(t);
    }
    else
    {
        redisLog(REDIS_NOTICE, "pthread_create() succ for syncCommand, thread_id=%d", tid);
    }
#endif
}

#if 0
static void *do_sync_master(void *arg)
{
    repl_tran *t = (repl_tran *)arg;
    char peer_ip[64];
    int peer_port;

    anetPeerToString(t->fd, peer_ip, &peer_port);

    char bl_path[256];
    get_bl_path(bl_path, sizeof(bl_path), 0);

    const time_t start = time(0);
    const int ret = sync_master_start(bl_path, dbmng_conf.log_prefix, 1000, t->fd, t->session_id, t->bl_tag);
    redisLog(REDIS_NOTICE, "sync_master over, ret=%d, during=%ds, slave=%s:%d",
             ret, time(0) - start, peer_ip, peer_port);

    close(t->fd);
    zfree(t);

    return (void*)0;
}
#endif

/* ---------------------------------- MASTER -------------------------------- */

void replicationFeedSlaves(list *slaves, int dictid, robj **argv, int argc)
{
    listNode *ln;
    listIter li;
    int j;

    listRewind(slaves,&li);
    while((ln = listNext(&li))) {
        redisClient *slave = ln->value;

        /* Don't feed slaves that are still waiting for BGSAVE to start */
        if (slave->replstate == REDIS_REPL_WAIT_BGSAVE_START) continue;

        /* Feed slaves that are waiting for the initial SYNC (so these commands
         * are queued in the output buffer until the intial SYNC completes),
         * or are already in sync with the master. */
        if (slave->slaveseldb != dictid) {
            robj *selectcmd;

            switch(dictid) {
            case 0: selectcmd = shared.select0; break;
            case 1: selectcmd = shared.select1; break;
            case 2: selectcmd = shared.select2; break;
            case 3: selectcmd = shared.select3; break;
            case 4: selectcmd = shared.select4; break;
            case 5: selectcmd = shared.select5; break;
            case 6: selectcmd = shared.select6; break;
            case 7: selectcmd = shared.select7; break;
            case 8: selectcmd = shared.select8; break;
            case 9: selectcmd = shared.select9; break;
            default:
                selectcmd = createObject(REDIS_STRING,
                    sdscatprintf(sdsempty(),"select %d\r\n",dictid));
                selectcmd->refcount = 0;
                break;
            }
            addReply(slave,selectcmd);
            slave->slaveseldb = dictid;
        }
        addReplyMultiBulkLen(slave,argc);
        for (j = 0; j < argc; j++) addReplyBulk(slave,argv[j]);
    }
}

void replicationFeedMonitors(list *monitors, int dictid, robj **argv, int argc)
{
    listNode *ln;
    listIter li;
    int j;
    sds cmdrepr = sdsnew("+");
    robj *cmdobj;
    struct timeval tv;

    gettimeofday(&tv,NULL);
    cmdrepr = sdscatprintf(cmdrepr,"%ld.%06ld ",(long)tv.tv_sec,(long)tv.tv_usec);
    if (dictid != 0) cmdrepr = sdscatprintf(cmdrepr,"(db %d) ", dictid);

    for (j = 0; j < argc; j++) {
        if (argv[j]->encoding == REDIS_ENCODING_INT) {
            cmdrepr = sdscatprintf(cmdrepr, "\"%ld\"", (long)argv[j]->ptr);
        } else {
            cmdrepr = sdscatrepr(cmdrepr,(char*)argv[j]->ptr,
                        sdslen(argv[j]->ptr));
        }
        if (j != argc-1)
            cmdrepr = sdscatlen(cmdrepr," ",1);
    }
    cmdrepr = sdscatlen(cmdrepr,"\r\n",2);
    cmdobj = createObject(REDIS_STRING,cmdrepr);

    listRewind(monitors,&li);
    while((ln = listNext(&li))) {
        redisClient *monitor = ln->value;
        addReply(monitor,cmdobj);
    }
    decrRefCount(cmdobj);
}

void sync1Command(redisClient *c)
{
    /* ignore SYNC if aleady slave or in monitor mode */
    if (c->flags & REDIS_SLAVE) return;

    redisLog(REDIS_NOTICE,"Slave ask for synchronization");
    /* Here we need to check if there is a background saving operation
     * in progress, or if it is required to start one */
    if (server.bgsavechildpid != -1)
    {
        /* Ok a background save is in progress. Let's check if it is a good
         * one for replication, i.e. if there is another slave that is
         * registering differences since the server forked to save */
        redisClient *slave;
        listNode *ln;
        listIter li;

        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            slave = ln->value;
            if (slave->replstate == REDIS_REPL_WAIT_BGSAVE_END) break;
        }
        if (ln) {
            /* Perfect, the server is already registering differences for
             * another slave. Set the right state, and copy the buffer. */
            listRelease(c->reply);
            c->reply = listDup(slave->reply);
            c->replstate = REDIS_REPL_WAIT_BGSAVE_END;
            redisLog(REDIS_NOTICE,"Waiting for end of BGSAVE for SYNC");
        } else {
            /* No way, we need to wait for the next BGSAVE in order to
             * register differences */
            c->replstate = REDIS_REPL_WAIT_BGSAVE_START;
            redisLog(REDIS_NOTICE,"Waiting for next BGSAVE for SYNC");
        }
    } else {
        /* Ok we don't have a BGSAVE in progress, let's start one */
        redisLog(REDIS_NOTICE,"Starting BGSAVE for SYNC");
        if (rdbSaveBackground(server.dbfilename) != REDIS_OK) {
            redisLog(REDIS_NOTICE,"Replication failed, can't BGSAVE");
            addReplyError(c,"Unable to perform background save");
            return;
        }
        c->replstate = REDIS_REPL_WAIT_BGSAVE_END;
    }
    c->repldbfd = -1;
    c->flags |= REDIS_SLAVE;
    c->slaveseldb = 0;
    listAddNodeTail(server.slaves,c);
    return;
}

void sendBulkToSlave(aeEventLoop *el, int fd, void *privdata, int mask)
{
    redisClient *slave = privdata;
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(mask);
    char buf[REDIS_IOBUF_LEN];
    ssize_t nwritten, buflen;

    if (slave->repldboff == 0) {
        /* Write the bulk write count before to transfer the DB. In theory here
         * we don't know how much room there is in the output buffer of the
         * socket, but in pratice SO_SNDLOWAT (the minimum count for output
         * operations) will never be smaller than the few bytes we need. */
        sds bulkcount;

        bulkcount = sdscatprintf(sdsempty(),"$%lld\r\n",(unsigned long long)
            slave->repldbsize);
        if (write(fd,bulkcount,sdslen(bulkcount)) != (signed)sdslen(bulkcount))
        {
            sdsfree(bulkcount);
            freeClient(slave);
            return;
        }
        sdsfree(bulkcount);
    }
    lseek(slave->repldbfd,slave->repldboff,SEEK_SET);
    buflen = read(slave->repldbfd,buf,REDIS_IOBUF_LEN);
    if (buflen <= 0) {
        redisLog(REDIS_WARNING,"Read error sending DB to slave: %s",
            (buflen == 0) ? "premature EOF" : strerror(errno));
        freeClient(slave);
        return;
    }
    if ((nwritten = write(fd,buf,buflen)) == -1) {
        redisLog(REDIS_VERBOSE,"Write error sending DB to slave: %s",
            strerror(errno));
        freeClient(slave);
        return;
    }
    slave->repldboff += nwritten;
    if (slave->repldboff == slave->repldbsize) {
        close(slave->repldbfd);
        slave->repldbfd = -1;
        aeDeleteFileEvent(server.el,slave->fd,AE_WRITABLE);
        slave->replstate = REDIS_REPL_ONLINE;
        if (aeCreateFileEvent(server.el, slave->fd, AE_WRITABLE,
            sendReplyToClient, slave) == AE_ERR) {
            freeClient(slave);
            return;
        }
        addReplySds(slave,sdsempty());
        redisLog(REDIS_NOTICE,"Synchronization with slave succeeded");
    }
}

/* This function is called at the end of every backgrond saving.
 * The argument bgsaveerr is REDIS_OK if the background saving succeeded
 * otherwise REDIS_ERR is passed to the function.
 *
 * The goal of this function is to handle slaves waiting for a successful
 * background saving in order to perform non-blocking synchronization. */
void updateSlavesWaitingBgsave(int bgsaveerr)
{
    listNode *ln;
    int startbgsave = 0;
    listIter li;

    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        redisClient *slave = ln->value;

        if (slave->replstate == REDIS_REPL_WAIT_BGSAVE_START) {
            startbgsave = 1;
            slave->replstate = REDIS_REPL_WAIT_BGSAVE_END;
        } else if (slave->replstate == REDIS_REPL_WAIT_BGSAVE_END) {
            struct redis_stat buf;

            if (bgsaveerr != REDIS_OK) {
                freeClient(slave);
                redisLog(REDIS_WARNING,"SYNC failed. BGSAVE child returned an error");
                continue;
            }
            if ((slave->repldbfd = open(server.dbfilename,O_RDONLY)) == -1 ||
                redis_fstat(slave->repldbfd,&buf) == -1) {
                freeClient(slave);
                redisLog(REDIS_WARNING,"SYNC failed. Can't open/stat DB after BGSAVE: %s", strerror(errno));
                continue;
            }
            slave->repldboff = 0;
            slave->repldbsize = buf.st_size;
            slave->replstate = REDIS_REPL_SEND_BULK;
            aeDeleteFileEvent(server.el,slave->fd,AE_WRITABLE);
            if (aeCreateFileEvent(server.el, slave->fd, AE_WRITABLE, sendBulkToSlave, slave) == AE_ERR) {
                freeClient(slave);
                continue;
            }
        }
    }
    if (startbgsave) {
        if (rdbSaveBackground(server.dbfilename) != REDIS_OK) {
            listIter li;

            listRewind(server.slaves,&li);
            redisLog(REDIS_WARNING,"SYNC failed. BGSAVE failed");
            while((ln = listNext(&li))) {
                redisClient *slave = ln->value;

                if (slave->replstate == REDIS_REPL_WAIT_BGSAVE_START)
                    freeClient(slave);
            }
        }
    }
}

/* ----------------------------------- SLAVE -------------------------------- */

/* Abort the async download of the bulk dataset while SYNC-ing with master */
static void replicationAbortSyncTransfer(repl_t *t)
{
    close(t->fd);
    t->status = REDIS_REPL_CONNECT;
}

static void *doReadSyncBulkPayload(void *arg);

/* Asynchronously read the SYNC payload we receive from a master */
static void readSyncBulkPayload(aeEventLoop *el, int fd, void *privdata, int mask)
{
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(mask);

    aeDeleteFileEvent(server.el, fd, AE_READABLE);

    repl_t *t = (repl_t *)privdata;
    t->transfer_left = -1;

    pthread_t tid;
    const int ret = pthread_create(&tid, 0, doReadSyncBulkPayload, t);
    if (ret != 0)
    {
        redisLog(REDIS_WARNING, "pthread_create() fail, errcode=%d", ret);
        replicationAbortSyncTransfer(t);
    }
    else
    {
        redisLog(REDIS_NOTICE, "pthread_create() succ, thread_id=%d", tid);
    }
}

static int doReadSyncCmd(int fd, int cnt)
{
    char buf[128];
    int len;
    int i;
    int ret;

    for (i = 0; i < cnt; i++)
    {
        ret = syncReadLine(fd, buf, sizeof(buf), server.repl_syncio_timeout);
        if (ret < 0)
        {
            break;
        }
        if (buf[0] != '$')
        {
            redisLog(REDIS_WARNING,"Bad protocol from MASTER, the first byte is not '$'");
            ret = -1;
            break;
        }
        len = strtol(buf + 1, 0, 10);
        ret = syncReadLine(fd, buf, sizeof(buf), server.repl_syncio_timeout);
        if (ret < 0)
        {
            break;
        }

        if (ret == 4 && strcmp(buf, "PING") == 0)
        {
            ret = 0;
            break;
        }
        else if (ret == 3 && strcmp(buf, "FIN") == 0)
        {
            ret = 1;
            break;
        }
    }

    return ret;
}

static void *doReadSyncBulkPayload(void *arg)
{
    char buf[4096];
    repl_t *t = (repl_t *)arg;
    int readlen;
    int nread;

    if (t->sync != REPL_SYNC)
    {
        gTran.status[t->master_slave] = TRAN_STATUS_INIT;
    }

    while (1)
    {
        if (t->transfer_left == -1)
        {
            nread = syncReadLine(t->fd, buf, 1024, server.repl_syncio_timeout);
            if (nread == -1)
            {
                redisLog(REDIS_WARNING,
                    "I/O error reading bulk count from MASTER: %s",
                    strerror(errno));
                break;
            }
            else if (nread == 0)
            {
                /* timeout, nothing to recieved */
                continue;
            }

            if (buf[0] == '-')
            {
                redisLog(REDIS_WARNING,
                    "MASTER aborted replication with an error: %s",
                    buf+1);
                break;
            }
            else if (buf[0] == '\0')
            {
                /* At this stage just a newline works as a PING in order to take
                 * the connection live. So we refresh our last interaction
                 * timestamp. */
                t->transfer_lastio = time(NULL);
                continue;
            }
            else if (buf[0] == '*')
            {
                const int cnt = strtol(buf + 1, 0, 10);
                const int ret = doReadSyncCmd(t->fd, cnt);
                if (ret == 0)
                {
                    /* ping */
                    t->transfer_lastio = time(NULL);
                    continue;
                }
                else if (ret == 1)
                {
                    /* fin */
                    gTran.status[t->master_slave] = TRAN_STATUS_FIN;
                    redisLog(REDIS_NOTICE, "finish from MASTER");
                    break;
                }
                else
                {
                    /* error */
                    break;
                }
            }
            else if (buf[0] != '$')
            {
                redisLog(REDIS_WARNING,"Bad protocol from MASTER, the first byte is not '$'");
                break;
            }
            /* format: $len\r\ndata\r\n */
            t->transfer_left = strtol(buf+1,NULL,10);
            redisLog(REDIS_NOTICE,
                "MASTER <-> SLAVE sync: receiving %ld bytes from master",
                t->transfer_left);
        }

        /* Read bulk data */
        readlen = (t->transfer_left < (signed)sizeof(buf)) ?
            t->transfer_left : (signed)sizeof(buf);
        nread = syncRead(t->fd, buf, readlen, server.repl_syncio_timeout);
        if (nread < 0)
        {
            redisLog(REDIS_WARNING,"I/O error trying to sync with MASTER: %s",
                (nread == -1) ? strerror(errno) : "connection lost");
            break;
        }
        if (nread == 0)
        {
            /* timeout */
            continue;
        }

        t->transfer_lastio = time(NULL);
        if (bl_relay_append(t->sync, t->master_slave, buf, nread) != nread)
        {
            redisLog(REDIS_WARNING,"Write error or short write writing to the DB dump file needed for MASTER <-> SLAVE synchrnonization: %s", strerror(errno));
            break;
        }
        t->transfer_left -= nread;
        /* Check if the transfer is now complete */
        if (t->transfer_left == 0)
        {
            t->status = REDIS_REPL_CONNECTED;
            t->transfer_left = -1;
        }
    }

    replicationAbortSyncTransfer(t);
    return t;
}

void syncWithMaster(aeEventLoop *el, int fd, void *privdata, int mask)
{
    char buf[1024];
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(mask);

    repl_t *t = (repl_t *)(privdata);

    redisLog(REDIS_NOTICE,"Non blocking connect for SYNC fired the event.");
    /* This event should only be triggered once since it is used to have a
     * non-blocking connect(2) to the master. It has been triggered when this
     * function is called, so we can delete it. */
    aeDeleteFileEvent(server.el, fd, AE_READABLE|AE_WRITABLE);

    if (t->sync == REPL_TRANSFER)
    {
        t->bl_ctx = bl_init(t->master_slave, 2);
    }
    else
    {
        t->bl_ctx = bl_init(REPL_SLAVE, 1);
    }

    const int64_t ts = binlogGetLastTs(t->sync, t->master_slave);
    snprintf(buf
            , sizeof(buf)
            , "REPL %"PRIu64" %s %d %d \r\n"
            , ts
            , server.app_id
            , t->peer_master_slave
            , t->sync == REPL_SYNC ? 0 : 1
           );

    /* Issue the SYNC command */
    if (syncWrite(fd, buf, strlen(buf),server.repl_syncio_timeout) == -1) {
        redisLog(REDIS_WARNING,"I/O error writing to MASTER: %s",
            strerror(errno));
        goto error;
    }

    /* Prepare a suitable temp file for bulk transfer
    while(maxtries--) {
        snprintf(tmpfile,256,
            "temp-%d.%ld.rdb",(int)time(NULL),(long int)getpid());
        dfd = open(tmpfile,O_CREAT|O_WRONLY|O_EXCL,0644);
        if (dfd != -1) break;
        sleep(1);
    }
    if (dfd == -1) {
        redisLog(REDIS_WARNING,"Opening the temp file needed for MASTER <-> SLAVE synchronization: %s",strerror(errno));
        goto error;
    }
    */

    /* Setup the non blocking download of the bulk file. */
    if (aeCreateFileEvent(server.el, fd, AE_READABLE, readSyncBulkPayload, t) == AE_ERR)
    {
        redisLog(REDIS_WARNING,"Can't create readable event for SYNC");
        goto error;
    }

    t->status = REDIS_REPL_TRANSFER;
    t->transfer_lastio = time(0);
    //server.repl_transfer_left = -1;
    //server.repl_transfer_fd = dfd;
    //server.repl_transfer_lastio = time(NULL);
    //server.repl_transfer_tmpfile = zstrdup(tmpfile);
    return;

error:
    t->status = REDIS_REPL_CONNECT;
    close(fd);
    return;
}

/* 
 * sync: 0 - transfer, 1 - sync
 * master_slave: 0 - master, 1 - slave
 * peer_master_slave: 0 - master, 1 - slave
 */
int connectWithMaster(int sync, int master_slave, int peer_master_slave)
{
    int fd;
    char *addr = 0;
    int port;

    if (sync < 0 || sync > 1 || master_slave < 0 || master_slave > 1 || peer_master_slave < 0 || peer_master_slave > 1)
    {
        redisLog(REDIS_WARNING, "illegal parameter");
        return REDIS_ERR;
    }

    if (sync == 0)
    {
        addr = gTran.peer_address;
        port = gTran.peer_port;

        scarRepl[sync][master_slave].master_slave = master_slave;
        scarRepl[sync][master_slave].peer_master_slave = peer_master_slave;
    }
    else
    {
        /* masterhost & masterport by mngr_server */
        addr = server.masterhost;
        port = server.masterport;

        scarRepl[sync][master_slave].master_slave = REPL_SLAVE;
        scarRepl[sync][master_slave].peer_master_slave = REPL_MASTER;
    }

    scarRepl[sync][master_slave].sync = sync;


    fd = anetTcpNonBlockConnect(NULL, addr, port);
    if (fd == -1)
    {
        redisLog(REDIS_WARNING,"Unable to connect to MASTER(%s:%d): %s",
            addr, port, strerror(errno));
        return REDIS_ERR;
    }

    if (aeCreateFileEvent(server.el,fd,AE_READABLE|AE_WRITABLE,syncWithMaster, &scarRepl[sync][master_slave]) ==
            AE_ERR)
    {
        close(fd);
        redisLog(REDIS_WARNING,"Can't create readable event for SYNC");
        return REDIS_ERR;
    }

    scarRepl[sync][master_slave].fd = fd;
    scarRepl[sync][master_slave].status = REDIS_REPL_CONNECTING;
    //server.repl_transfer_s = fd;
    //server.replstate = REDIS_REPL_CONNECTING;
    return REDIS_OK;
}

void slaveofCommand(redisClient *c)
{
    addReplyError(c, "not support by fooyun");
#if 0
    if (!strcasecmp(c->argv[1]->ptr,"no") &&
        !strcasecmp(c->argv[2]->ptr,"one")) {
        if (server.masterhost) {
            sdsfree(server.masterhost);
            server.masterhost = NULL;
            if (server.master) freeClient(server.master);
            if (server.replstate == REDIS_REPL_TRANSFER)
                replicationAbortSyncTransfer();
            server.replstate = REDIS_REPL_NONE;
            redisLog(REDIS_NOTICE,"MASTER MODE enabled (user request)");
        }
    } else {
        sdsfree(server.masterhost);
        server.masterhost = sdsdup(c->argv[1]->ptr);
        server.masterport = atoi(c->argv[2]->ptr);
        if (server.master) freeClient(server.master);
        if (server.replstate == REDIS_REPL_TRANSFER)
            replicationAbortSyncTransfer();
        server.replstate = REDIS_REPL_CONNECT;
        redisLog(REDIS_NOTICE,"SLAVE OF %s:%d enabled (user request)",
            server.masterhost, server.masterport);
    }
    addReply(c,shared.ok);
#endif
}

/* --------------------------- REPLICATION CRON  ---------------------------- */

void replicationCron(void)
{
    /* Bulk transfer I/O timeout? */
    if (server.masterhost && server.replstate == REDIS_REPL_TRANSFER &&
        (time(NULL)-server.repl_transfer_lastio) > server.repl_timeout)
    {
        redisLog(REDIS_WARNING,"Timeout receiving bulk data from MASTER...");
        //replicationAbortSyncTransfer();
    }

    /* Timed out master when we are an already connected slave? */
    if (server.masterhost && server.replstate == REDIS_REPL_CONNECTED &&
        (time(NULL)-server.master->lastinteraction) > server.repl_timeout)
    {
        redisLog(REDIS_WARNING,"MASTER time out: no data nor PING received...");
        freeClient(server.master);
    }

    /* Check if we should connect to a MASTER */
    if (server.replstate == REDIS_REPL_CONNECT) {
        redisLog(REDIS_NOTICE,"Connecting to MASTER...");
        if (connectWithMaster(0, 1, 1) == REDIS_OK) {
            redisLog(REDIS_NOTICE,"MASTER <-> SLAVE sync started");
        }
    }
    
    /* If we have attached slaves, PING them from time to time.
     * So slaves can implement an explicit timeout to masters, and will
     * be able to detect a link disconnection even if the TCP connection
     * will not actually go down. */
    if (!(server.cronloops % (server.repl_ping_slave_period*10))) {
        listIter li;
        listNode *ln;

        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            redisClient *slave = ln->value;

            /* Don't ping slaves that are in the middle of a bulk transfer
             * with the master for first synchronization. */
            if (slave->replstate == REDIS_REPL_SEND_BULK) continue;
            if (slave->replstate == REDIS_REPL_ONLINE) {
                /* If the slave is online send a normal ping */
                addReplySds(slave,sdsnew("PING\r\n"));
            } else {
                /* Otherwise we are in the pre-synchronization stage.
                 * Just a newline will do the work of refreshing the
                 * connection last interaction time, and at the same time
                 * we'll be sure that being a single char there are no
                 * short-write problems. */
                if (write(slave->fd, "\n", 1) == -1) {
                    /* Don't worry, it's just a ping. */
                }
            }
        }
    }
}

#if 0
void syncCommand(redisClient *c)
{
    /* ignore SYNC if aleady slave or in monitor mode */
    if (c->flags & REDIS_SLAVE) return;

    redisLog(REDIS_NOTICE,"Slave ask for synchronization");
    /* Here we need to check if there is a background saving operation
     * in progress, or if it is required to start one */
    if (server.bgsavechildpid != -1)
    {
        /* Ok a background save is in progress. Let's check if it is a good
         * one for replication, i.e. if there is another slave that is
         * registering differences since the server forked to save */
        redisClient *slave;
        listNode *ln;
        listIter li;

        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            slave = ln->value;
            if (slave->replstate == REDIS_REPL_WAIT_BGSAVE_END) break;
        }
        if (ln) {
            /* Perfect, the server is already registering differences for
             * another slave. Set the right state, and copy the buffer. */
            listRelease(c->reply);
            c->reply = listDup(slave->reply);
            c->replstate = REDIS_REPL_WAIT_BGSAVE_END;
            redisLog(REDIS_NOTICE,"Waiting for end of BGSAVE for SYNC");
        } else {
            /* No way, we need to wait for the next BGSAVE in order to
             * register differences */
            c->replstate = REDIS_REPL_WAIT_BGSAVE_START;
            redisLog(REDIS_NOTICE,"Waiting for next BGSAVE for SYNC");
        }
    } else {
        /* Ok we don't have a BGSAVE in progress, let's start one */
        redisLog(REDIS_NOTICE,"Starting BGSAVE for SYNC");
        if (rdbSaveBackground(server.dbfilename) != REDIS_OK) {
            redisLog(REDIS_NOTICE,"Replication failed, can't BGSAVE");
            addReplyError(c,"Unable to perform background save");
            return;
        }
        c->replstate = REDIS_REPL_WAIT_BGSAVE_END;
    }
    c->repldbfd = -1;
    c->flags |= REDIS_SLAVE;
    c->slaveseldb = 0;
    listAddNodeTail(server.slaves,c);
    return;
}
#endif

