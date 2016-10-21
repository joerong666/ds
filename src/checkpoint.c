#include "checkpoint.h"
#include "bl_ctx.h"
#include "binlogtab.h"
#include "dbmng.h"
#include "db_io_engine.h"
#include "dbe_if.h"
#include "ds_log.h"
#include "ds_ctrl.h"
#include "codec_key.h"

#include <sys/timeb.h>

#include "dict.h"

#define CPT_KV_MAX_NUM       32

#define CP_STAT_IDLE         0
#define CP_STAT_WAIT_IO      1
#define CP_STAT_AFTER_IO     2

typedef struct cp_transaction_st
{
    int finished;
    int param_cnt;
    upd_dbe_param param[CPT_KV_MAX_NUM];
    dictIterator *it;
    size_t tid;

    size_t total;
    size_t cnt;
    long long start;
    //struct timeb b_time;
} cp_transaction;

static int update_checkpoint_ex(dbmng_ctx *ctx, int switch_flg);
static void finish_cp_transaction(dbmng_ctx *ctx, int confirm_flg);
static int do_with_key(dbmng_ctx *ctx);

#if 0
void confirm_checkpoint(dbmng_ctx *ctx)
{
    if (ctx == 0)
    {
        /* illegal paramter */
        return;
    }

    if (ctx->block_cp)
    {
        /* blocking, not allow to update */
        log_info("update checkpoint blocked, ignore");
        ctx->pending = 0;
        return;
    }

    if (ctx->t)
    {
        log_info("there is a transaction running, %p", ctx->t);
        ctx->pending = 1;
        return;
    }

    ctx->t = zcalloc(sizeof(cp_transaction));
    if (ctx->t == 0)
    {
        /* system fail */
        log_error("%s", "zcalloc() fail for cp transaction");
        return;
    }

    log_prompt("%s", "begin to update cp...");
    cp_transaction *t = (cp_transaction *)ctx->t;
    t->it = 0;
    t->tid = g_tid;
    ftime(&t->b_time);

    ctx->pending = 0;

    if (tag_new_cp(ctx->binlog, -1, -1) != 0)
    {
        log_error("%s", "tag_new_cp() fail");
        zfree(ctx->t);
        ctx->t = 0;
        return 0;
    }

    do_with_key(ctx);
}
#endif

void update_checkpoint(dbmng_ctx *ctx, int switch_flg)
{
    if (ctx == 0)
    {
        /* illegal paramter */
        return;
    }

    if (ctx->hangup_cp)
    {
        /* hungup, not allow to update */
        log_info("update checkpoint hungup, ignore");
        ctx->pending = 0;
        return;
    }

    if (ctx->block_cp)
    {
        /* blocking, not allow to update */
        log_info("update checkpoint blocked, ignore");
        ctx->pending = 0;
        return;
    }

    binlog_tab *blt = ctx->binlogtab;
    if (blt == 0)
    {
        return;
    }
    dict *const d = switch_flg ? blt->active : blt->immutable;
    if (d == 0)
    {
        return;
    }

#if 0
    /* need not to do anything */
    if (server.save_type == 0)
    {
        /* cache binlog cp */
        if (tag_new_cp(ctx->binlog, -1, -1) != 0)
        {
            return;
        }

        /* confirm binlog cp */
        confirm_new_cp(ctx->binlog);
        return;
    }
#endif

    if (ctx->t)
    {
        log_info("there is a transaction running, %p", ctx->t);
        ctx->pending = 1;
        return;
    }

    ctx->t = zcalloc(sizeof(cp_transaction));
    if (ctx->t == 0)
    {
        /* system fail */
        log_error("%s", "zcalloc() fail for cp transaction");
        return;
    }

    cp_transaction *t = (cp_transaction *)ctx->t;
    t->it = 0;
    t->tid = g_tid;
    t->start = server.mstime;
    //ftime(&t->b_time);

    update_checkpoint_ex(ctx, switch_flg);
}

/* return:
 * 0 - over
 * 1 -doning
 */
static int update_checkpoint_ex(dbmng_ctx *ctx, int switch_flg)
{
    //log_prompt("begin to update cp...flg=%d", switch_flg);
    ctx->pending = 0;

    if (switch_flg)
    {
        /* cache binlog cp */
        int ret;
        if ((ret = tag_new_cp(ctx->binlog, -1, -1)) != 0)
        {
            if (ret < 0)
            {
                log_error("tag_new_cp() fail, ret=%d", ret);
            }
            zfree(ctx->t);
            ctx->t = 0;
            return 0;
        }

        if (0 != switch_blt(ctx->rdb, ctx->binlogtab))
        {
            log_error("%s", "switch_blt() fail");
            zfree(ctx->t);
            ctx->t = 0;
            return 0;
        }
    }

    return do_with_key(ctx);
}

static void finish_cp_transaction(dbmng_ctx *ctx, int confirm_flg)
{
    log_debug("%s", "finish update checkpoint");

    /* confirm binlog cp */
    if (confirm_flg)
    {
        confirm_new_cp(ctx->binlog);
    }

    release_blt_immutable((binlog_tab *)ctx->binlogtab);

    cp_transaction *t = (cp_transaction *)ctx->t;
    const size_t cnt = t->cnt;
    const size_t total = t->total;

    log_info("finish update checkpoint, during=%lld(ms), cnt=%zd, total=%zd",
             server.mstime - t->start, cnt, total);

    if (ctx->pending)
    {
        const size_t tid = t->tid;
        memset(t, 0, sizeof(t));
        t->tid = tid;
        t->start = server.mstime;
        update_checkpoint_ex(ctx, 1);
    }
    else
    {
        /* release cp transaction */
        zfree(ctx->t);
        ctx->t = 0;
        ctx->cp_status = CP_STAT_IDLE;
    }
}

/* call by thread in thread_pool */
void dbe_set_one_op(void *dbe, void *parameter)
{
    struct timespec ts;
    dbe_stat *tmp = &gDbeSetTop[0];

    upd_dbe_param *param = (upd_dbe_param *)parameter;
    int j;
    int ret;
    ts = pf_get_time_tick();

    /* exec pdelete at first if existed */
    if (param->pdel_key && param->pdel_key_len > 0)
    {
        dbe_pdelete(dbe, param->pdel_key, param->pdel_key_len);
        zfree(param->pdel_key);
    }

    if (param->cmd_type == KEY_TYPE_STRING)
    {
        ret = dbe_put(dbe, param->one.k, param->one.ks, param->one.v, param->one.vs);
    }
    else
    {
#ifdef _UPD_DBE_BY_PERIODIC_
        dbe_tran_begin(dbe);
#endif
        for (j = 0; j < 2; j++)
        {
            log_debug("dbe_set_one_op(): %s, dbe=%p, op_cnt=%zu"
                , j == 0 ? "put" : "del", dbe, param->cnt[j]);
            if (j == 0)
            {
                if (param->cnt[j] > 0)
                {
                    ret = dbe_mput(dbe, param->kv[j], param->cnt[j]);
                    if (ret != 0)
                    {
                        log_error("dbe_mput fail, ret=%d, dbe=%p, op_cnt=%zu"
                            , ret, dbe, param->cnt[j]);
                    }
#ifndef _UPD_DBE_BY_PERIODIC_
                    /* need not to try dbe_delete under this mode */
                    break;
#endif
                }
            }
            else
            {
                if (param->cnt[j] > 0)
                {
                    ret = dbe_mdelete(dbe, param->kv[j], param->cnt[j]);
                    if (ret != 0)
                    {
                        log_error("dbe_mdelete fail, ret=%d, dbe=%p, op_cnt=%zu"
                            , ret, dbe, param->cnt[j]);
                    }
                }
            }
        }
#ifdef _UPD_DBE_BY_PERIODIC_
        dbe_tran_commit(dbe);
#endif
    }

    tmp->io_dur = pf_get_time_diff_nsec(ts, pf_get_time_tick()) / 1000;
    log_test("dbe_set_op: dur=%dms", tmp->io_dur);

#if 0   /* free by dbe */
        /* free resource */
        for (j = 0; (size_t)j < param->cnt[0]; j++)
        {
            zfree(param->kv[0][j].k);
            zfree(param->kv[0][j].v);
        }
        zfree(param->kv[0]);
        for (j = 0; (size_t)j < param->cnt[1]; j++)
        {
            zfree(param->kv[1][j].k);
            zfree(param->kv[1][j].v);
        }
        zfree(param->kv[1]);
#endif

    {
        /* for stat. */
        int min_idx = 0;
        unsigned int k;
        for (k = 1; k < sizeof(gDbeSetTop) / sizeof(gDbeSetTop[0]); k++)
        {
            if (gDbeSetTop[k].valid == 0)
            {
                min_idx = k;
                break;
            }
            if (gDbeSetTop[k].io_dur < gDbeSetTop[min_idx].io_dur)
            {
                min_idx = k;
            }
        }
        if (min_idx != 0)
        {
            memcpy(&gDbeSetTop[min_idx], &gDbeSetTop[0], sizeof(gDbeSetTop[0]));
            gDbeSetTop[min_idx].valid = 1;
            gDbeSetTop[min_idx].time = time(NULL);
        }
        if (tmp->io_dur > gDbeSetMax)
        {
            gDbeSetMax = tmp->io_dur;
        }
        if (tmp->io_dur < gDbeSetMin || gDbeSetMin == 0)
        {
            gDbeSetMin = tmp->io_dur;
        }
        gDbeSetTotal += tmp->io_dur;
        gDbeSetCnt++;
        memset(&gDbeSetTop[0], 0, sizeof(gDbeSetTop[0]));
    }
}

void do_dbe_set(dbmng_ctx *ctx)
{
#if 1
    cp_transaction *t = ctx->t;

    int i;
    for (i = 0; i < t->param_cnt; i++)
    {
        dbe_set_one_op(ctx->db, &t->param[i]);
    }
#endif
}

void after_dbe_set(dbmng_ctx *ctx)
{
    ctx->cp_status = CP_STAT_AFTER_IO;
    if (ctx->hangup_cp)
    {
        //log_prompt("%s", "continue after finishing db io for cp, but hungup...");
        return;
    }

    cp_transaction *t = (cp_transaction *)ctx->t;
    g_tid = t ? t->tid : (size_t)-1;

    log_debug("%s", "continue after finishing db io for cp...");

    int i = 0;
    for (; i < t->param_cnt; i++)
    {
        bl_write_confirm(ctx->binlog, t->param[i].key, t->param[i].db_id);
        finish_ble(t->param[i].ble);
    }

    if (ctx->stop)
    {
        ctx->stop = 0;
        finish_cp_transaction(ctx, 1);
        dbmng_uninit(ctx->tag);
        return;
    }
    if (t->finished)
    {
        finish_cp_transaction(ctx, 1);
        return;
    }
    if (ctx->giveup)
    {
        ctx->giveup = 0;
        finish_cp_transaction(ctx, 0);
        return;
    }

    /* go on next key */
    do_with_key(ctx);
}

/* return:
 * 0 - finished
 * 1 - doing
 */
static int do_with_key(dbmng_ctx *ctx)
{
    cp_transaction *t = (cp_transaction *)ctx->t;
    int ret;
    int i;

scan_next:

    i = 0;
    for (; i < CPT_KV_MAX_NUM; i++)
    {
        t->it = scan_immutable(ctx->rdb, ctx->db, (binlog_tab *)ctx->binlogtab, t->it, &t->param[i]);
        if (t->it == 0)
        {
            t->finished = 1;
            break;
        }
    }
    if (i == 0)
    {
        /* scan over */
        finish_cp_transaction(ctx, 1);
        return 0;
    }
    t->param_cnt = i;

    log_debug("updating %d keys checkpoint (updated %zd)...", i, t->total);
    t->total += i;

    if ((ret = commit_dbe_set_task(ctx)) != 0)
    {
        log_error("commit_dbe_set_task() fail, ret=%d", ret);
        goto scan_next;
    }

    ctx->cp_status = CP_STAT_WAIT_IO;
    t->cnt += i;

    return 1;
}

void hangup_cp(dbmng_ctx *ctx)
{
    ctx->hangup_cp = 1;
}

void wakeup_cp(dbmng_ctx *ctx)
{
    ctx->hangup_cp = 0;
    if (ctx->cp_status == CP_STAT_AFTER_IO)
    {
        /* go on cp process */
        //log_prompt("%s", "continue process cp after wakeup...");
        after_dbe_set(ctx);
    }
}

