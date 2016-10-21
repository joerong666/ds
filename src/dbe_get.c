#include "dbe_get.h"
#include "dbmng.h"
#include "db_io_engine.h"
#include "checkpoint.h"
#include "binlogtab.h"
#include "dbe_if.h"
#include "serialize.h"
#include "ds_log.h"
#include "write_bl.h"
#include "ds_ctrl.h"
#include "restore_key.h"
#include "key_filter.h"

#include <sys/timeb.h>

#include "dict.h"

typedef struct ItemKeyVal_t
{
    robj *key;
    void *val;
    time_t expire;
    char cmd_type;
} ItemKeyVal;

static int check_block_key(redisClient *c, robj *key);
static int check_multi_block_keys(redisClient *c, struct redisCommand *cmd, int argc, robj **argv);

static int notify_dbe_get(redisClient *c)
{
    const int ret = commit_dbe_get_task(c);
    if (ret != 0)
    {
        /* notify io thread fail */
        server.stat_notify_dbe_get_fail++;
        c->enque_cnt++;
        if (c->enque_cnt >= 8)
        {
            /* terminal the request */
            log_error("terminal the request: c=%p, enque_cnt=%d", c, c->enque_cnt);
            addReplyError(c, "can't notify io thread");
            resetClient(c);
        }
        else
        {
            listAddNodeTail(server.dbe_get_clients, c);
            log_debug("notify fail,enqueue,c=%p,c_cnt=%d,que_len=%d,enque_cnt=%d"
                    , c, server.dbe_get_clients_cnt, listLength(server.dbe_get_clients), c->enque_cnt);
        }
        return 1;
    }
    return 0;
}

/* return:
 * 0 - not need block
 * 1 - need block & block succ
 * 2 - need block but too many clients
 */
int block_client_on_dbe_get(redisClient *c)
{
    //log_test("block_client_on_dbe_get()...");
    if (server.has_dbe == 0 || server.read_dbe == 0)
    {
        return 0;
    }

    const unsigned int clients = server.dbe_get_clients_cnt;
    const int block = check_multi_block_keys(c, c->cmd, c->argc, c->argv);
    if (block == 0)
    {
        return 0;
    }

    if (clients > (unsigned int)server.dbe_get_que_size)
    {
        log_error("too many clients(%d) are queue at dbe_get", clients);
        return 2;
    }

    log_test("need to hang up the client, dbe_get_client=%d", clients);
    c->block_start_time = server.ustime;

    c->flags |= REDIS_DBE_GET_WAIT;
    //aeDeleteFileEvent(server.el, c->fd, AE_READABLE);

    server.dbe_get_clients_cnt++;

    c->enque_cnt = 0;
    c->ctx = get_entry_ctx(0);
#ifdef _UPD_DBE_BY_PERIODIC_
    if (clients == 0)
    {
        /* hangup checkpoint process */
        hangup_cp(c->ctx);
    }
#endif
    notify_dbe_get(c);
    return block;
}

void check_dbe_get_timer()
{
    const int wait_c_cnt = listLength(server.dbe_get_clients);
    if (wait_c_cnt > 0)
    {
        log_debug("check_dbe_get: in queue=%d", wait_c_cnt);
        listNode *ln = NULL;
        redisClient *c = NULL;
        ln = listFirst(server.dbe_get_clients);
        c = listNodeValue(ln);
        listDelNode(server.dbe_get_clients, ln);

        notify_dbe_get(c);
    }
}

static int check_block_key(redisClient *c, robj *key)
{
    if (cache_filter(key->ptr, sdslen(key->ptr)) == 1)
    {
        /* the key belong to cache */
        return 0;
    }

    if (dbmng_check_key_io(0, key) == 0)
    {
        return 0;
    }

    server.stat_misses_in_cache++;
    {
        ItemKeyVal *item = (ItemKeyVal *)zcalloc(sizeof(ItemKeyVal));
        item->key = key;

        if (server.enc_kv == 0)
        {
            /* support string only */
            item->cmd_type = 'K';
        }
        else
        {
            /* can't set according to the cmd, it must search by prefix */
            item->cmd_type = 'k';
        }

        incrRefCount(key);
        listAddNodeTail(c->dbe_get_keys, item);
    }
    return 1;
}

static int check_multi_block_keys(redisClient *c, struct redisCommand *cmd, int argc, robj **argv)
{
    int j, last;
    if (cmd->io_firstkey == 0)
    {
        return 0;
    }
    last = cmd->io_lastkey;
    if (last < 0)
    {
        last = argc+last;
    }

    int ret = 0;
    for (j = cmd->io_firstkey; j <= last; j += cmd->io_keystep)
    {
        if (check_block_key(c, argv[j]))
        {
            ret = 1;
        }
    }
    return ret;
}

/* call by thread in thread_pool */
void do_dbe_get(void *t_ctx)
{
#if 1
    redisClient *c = (redisClient *)t_ctx;
    listNode *ln = 0;
    ItemKeyVal *item;
    listIter li;
    val_attr rslt;

    dbmng_ctx *ctx = (dbmng_ctx*)c->ctx;
    listRewind(c->dbe_get_keys, &li);
    while ((ln = listNext(&li)))
    {
        item = listNodeValue(ln);

        const int ret = restore_key_from_dbe(ctx->db, (const char *)item->key->ptr, sdslen(item->key->ptr), item->cmd_type, &rslt);
        if (ret == 0)
        {
            item->val = rslt.val;
            item->expire = rslt.expire_ms;
        }
        else if (ret != 1)
        {
            log_error("restore_key_from_dbe fail, ret=%d, key=%s"
                    , ret, (const char *)item->key->ptr);
        }
    }
#endif
#if 0
    struct timespec ts;
    dbe_stat *tmp = &gDbeGetTop[0];

    listNode *ln = 0;
    redisClient *c = 0;
    ItemKeyVal *item;
    listIter li;
    int len;
    char *ptr = 0;
    int rslt;

    //long long s = ustime();

    c = ctx->c;
    listRewind(c->dbe_get_keys, &li);
    while ((ln = listNext(&li)))
    {
        item = listNodeValue(ln);

        log_debug2(ctx->tag, "dbe_get() for key=%s", (const char *)item->key->ptr);

        /* get value from db engine */
        ts = pf_get_time_tick();
        rslt = dbe_get(ctx->db, (const char *)item->key->ptr, &ptr, &len, zmalloc);
        tmp->io_dur = pf_get_time_diff_nsec(ts, pf_get_time_tick()) / 1000;
        tmp->buf_len = len;
        if (rslt == DBE_ERR_NOT_FOUND)
        {
            log_info2(ctx->tag, "dbe_get() return null, db=%p, key=%s",
                      ctx->db, (const char *)item->key->ptr);
            continue;
        }
        else if (rslt != DBE_ERR_SUCC)
        {
            /* get fail */
            continue;
        }

        /* unserialize */
        ts = pf_get_time_tick();
        item->val = unserializeObj(ptr, len, &item->expire);
        tmp->unserialize_dur = pf_get_time_diff_nsec(ts, pf_get_time_tick()) / 1000;
        dbe_free_ptr(ptr, zfree);
        if (item->val == 0)
        {
            /* fail */
            log_error2(ctx->tag, "unserializeObj() fail, len=%d", len);
        }

        int min_idx = 0;
        unsigned int k;
        for (k = 1; k < sizeof(gDbeGetTop) / sizeof(gDbeGetTop[0]); k++)
        {
            if (gDbeGetTop[k].valid == 0)
            {
                min_idx = k;
                break;
            }
            if (gDbeGetTop[k].io_dur < gDbeGetTop[min_idx].io_dur)
            {
                min_idx = k;
            }
        }
        memcpy(&gDbeGetTop[min_idx], &gDbeGetTop[0], sizeof(gDbeGetTop[0]));
        gDbeGetTop[min_idx].valid = 1;
        gDbeGetTop[min_idx].time = time(NULL);
        if (tmp->io_dur > gDbeGetMax)
        {
            gDbeGetMax = tmp->io_dur;
        }
        if (tmp->io_dur < gDbeGetMin || gDbeGetMin == 0)
        {
            gDbeGetMin = tmp->io_dur;
        }
        gDbeGetTotal += tmp->io_dur;
        gDbeGetCnt++;
    }

    //log_prompt2(ctx->tag, "*** do_dbe_get: %lld(us)", ustime() - s);

#endif
}

void after_dbe_get(void *t_ctx)
{
    log_test("continue after finishing dbe get...arg=%p", t_ctx);
    unsigned int wait_c_cnt;

    listNode *ln = 0;
    ItemKeyVal *item;
    listIter li;

    redisClient *c = (redisClient *)t_ctx;
    dbmng_ctx *ctx = (dbmng_ctx*)c->ctx;
    listRewind(c->dbe_get_keys, &li);
    while ((ln = listNext(&li)))
    {
        item = listNodeValue(ln);

        robj *const val = lookupKey(ctx->rdb, item->key);
        if (val == 0)
        {
            if (item->val)
            {
                /* get value succ */
                dbAdd(ctx->rdb, item->key, (robj *)item->val);
                if (item->expire > 0)
                {
                    setExpire(ctx->rdb, item->key, item->expire);
                }
                else
                {
                    removeExpire(ctx->rdb, item->key);
                }
            }
            restore_a(ctx->rdb, (binlog_tab *)ctx->binlogtab, ctx->db, item->key, RESTORE_LVL_ACT, 0);
        }
        else
        {
            /* the key has existed in rdb, maybe insert by other cmd, release it */
            if (item->val)
            {
                decrRefCount(item->val);
            }
        }

        /* release resource */
        decrRefCount(item->key);
        listDelNode(c->dbe_get_keys, ln);
        zfree(item);
    }

handle_client:
    server.dbe_get_clients_cnt--;
    wait_c_cnt = listLength(server.dbe_get_clients);
    c->dbe_get_block_dur = server.ustime - c->block_start_time;
    log_test("wait in dbe_get_que: %lld(us), cmd=%s, waiting_c=%d"
              , c->dbe_get_block_dur, c->argv[0]->ptr, wait_c_cnt);

    c->flags &= (~REDIS_DBE_GET_WAIT);
    //aeCreateFileEvent(server.el, c->fd, AE_READABLE, readQueryFromClient, c);
    struct redisCommand *cmd = lookupCommand(c->argv[0]->ptr);
    redisAssert(cmd != NULL);
    int ignore = 0;
    if (cmd->wrbl_flag)
    {
        const int ret = block_client_on_write_bl(c);
        if (ret == 2)
        {
            /* block_client_list is too long */
            log_error("too many client are blocking at write_bl");
            addReplyError(c, "too many clients are blocking at write_bl");
            resetClient(c);
            ignore = 1;
        }
        else if (ret == 1)
        {
            /* need block & blocking succ */
            ignore = 1;
        }
    }
    if (ignore == 0)
    {
        call(c);
        resetClient(c);
        if (c->querybuf && sdslen(c->querybuf) > 0)
        {
            log_info("processInputBuf() after dbe_get...");
            processInputBuffer(c);
        }
    }

    if (wait_c_cnt)
    {
        /* go on startup next c */
        ln = listFirst(server.dbe_get_clients);
        c = listNodeValue(ln);
        listDelNode(server.dbe_get_clients, ln);
        const int block = check_multi_block_keys(c, c->cmd, c->argc, c->argv);
        if (block)
        {
            notify_dbe_get(c);
            return;
        }
        else
        {
            /* do not need to block the client anymore */
            goto handle_client;
        }
    }

#ifdef _UPD_DBE_BY_PERIODIC_
    if (server.dbe_get_clients_cnt <= 0)
    {
        /* list is null, wake up checkpoint process */
        wakeup_cp(ctx);
    }
#endif
}

