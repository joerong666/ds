#include "dbmng.h"
#include "bl_ctx.h"
#include "binlogtab.h"
#include "checkpoint.h"
#include "serialize.h"
#include "uthash.h"
#include "dbe_if.h"

#include "ds_log.h"
#include "op_cmd.h"
#include "ds_util.h"
#include "key_filter.h"
#include "ds_ctrl.h"
#include "ds_zmalloc.h"
#include "pf_util.h"
#include "codec_key.h"

#include <stdio.h>
#include <pthread.h>
#include <unistd.h>


typedef struct dbmng_ctx_hash_t
{
    int tag;
    dbmng_ctx *ctx;
    UT_hash_handle hh;
} dbmng_ctx_hash;

static dbmng_ctx_hash *hash_head = 0;

struct dbmng_conf_st dbmng_conf;
/*
struct dbmng_conf_st dbmng_conf = 
{
    "/home/wujian/kvc/run/server0/db",
    "/home/wujian/kvc/run/server0/binlog",
    "binlog",
    60,
    102400,
    3
};
*/

static void dbmng_destroy(dbmng_ctx_hash *s);


static dbmng_ctx_hash *add_hash_entry(int tag)
{
    dbmng_ctx_hash *s = 0;
    HASH_FIND_INT(hash_head, &tag, s);
    if (!s)
    {
        s = (dbmng_ctx_hash *)zcalloc(sizeof(dbmng_ctx_hash));
        if (s)
        {
            s->ctx = (dbmng_ctx *)zcalloc(sizeof(dbmng_ctx));
            if (s->ctx)
            {
                s->tag = tag;
                HASH_ADD_INT(hash_head, tag, s);
            }
            else
            {
                zfree(s);
                s = 0;
            }
        }
    }

    return s;
}

static dbmng_ctx_hash *find_hash_entry(int tag)
{
    dbmng_ctx_hash *s = 0;
    HASH_FIND_INT(hash_head, &tag, s);

    return s;
}

static void free_hash_entry(dbmng_ctx_hash *s)
{
    HASH_DEL(hash_head, s);
    zfree(s->ctx);
    zfree(s);
}

dbmng_ctx *get_entry_ctx(int tag)
{
    dbmng_ctx_hash *s = find_hash_entry(tag);
    return s ? s->ctx : 0;
}

const char *get_dbe_path(char *path, int path_len, const char *name)
{
    snprintf(path
            , path_len
            , "%s%s"
            , dbmng_conf.dbe_root_path
            , name
            );

    return path;
}

const char *make_dbe_path(char *path, int path_len, const char *name)
{
    get_dbe_path(path, path_len, name);
    check_and_make_dir(path);

    return path;
}

int dbmng_init(int tag, int slave)
{
    log_prompt("dbmng_init...");
    (void)slave;
    dbmng_ctx_hash *s = find_hash_entry(tag);
    if (s)
    {
        /* init already */
        return 0;
    }
    s = add_hash_entry(tag);
    if (!s)
    {
        /* system fail */
        log_error("%s", "add_hash_entry() fail");
        return 1;
    }

    s->ctx->rdb = &server.db[0];
    s->ctx->tag = tag;

    int ret;
    time_t start, end;

    /* init binlog */
    start = time(0);
    s->ctx->binlog = bl_init(1, 0);
    if (!s->ctx->binlog)
    {
        log_error("%s", "bl_init() fail");
        dbmng_uninit(tag);
        return 1;
    }
    end = time(0);
    log_prompt("bl_init during=%ds, ctx=%p, rdb=%p, bl=%p"
               , end - start, s->ctx, s->ctx->rdb, s->ctx->binlog);

    if (server.has_dbe == 1)
    {
        /* init dbe */
        start = end;
        char path[256];
        const char *db_path = 0;
        db_path = make_dbe_path(path, sizeof(path), "local");
        ret = dbe_init(db_path, &s->ctx->db, server.read_only, server.auto_purge, server.dbe_fsize);
        if (ret != DBE_ERR_SUCC)
        {
            log_error("dbe_init() fail, ret=%d, db_path=%s", ret, db_path);
            dbmng_uninit(tag);
            return 1;
        }
        dbe_set_filter(s->ctx->db, (dbeFilterFunc)master_filter_4_dbe);
        dbe_set_val_filter(s->ctx->db, value_filter);
        end = time(0);
        log_prompt("dbe_init during=%ds, dbe=%p, path=%s"
                   , end - start, s->ctx->db, db_path);

        void *it = 0;
        if (server.has_cache == 1 && server.dbe_hot_level > 0)
        {
            const int lvl = server.dbe_hot_level;
            it = dbe_create_it(s->ctx->db, lvl);
        }
        if (it)
        {
            // load hot keys from dbe
            start = end;
            long long cnt = 0;
            size_t ignore_cnt = 0;
            char *k = 0;
            int k_len;
            char *v = 0;
            int v_len;
            robj *key = 0;
            robj *val = 0;
            size_t val_total_len = 0;
            time_t diff = 0;
            long long uns_diff = 0;

            while ((server.load_hot_key_max_num < 0 || server.load_hot_key_max_num > cnt)
                   && dbe_next_key(it, &k, &k_len, &v, &v_len, zmalloc, NULL) == 0)
            {
                const size_t mem_size = ds_zmalloc_used_memory();
                if (mem_size * 10 > (size_t)server.db_max_size * 9)
                {
                    log_prompt("stop loading, mem=%zd, db_max_size=%llu",
                               mem_size, server.db_max_size);
                    zfree(k);
                    zfree(v);
                    break;
                }

                dbe_key_attr attr;
                if (decode_dbe_key(k, k_len, &attr) != 0)
                {
                    zfree(k);
                    zfree(v);
                    continue;
                }
                if (attr.type != KEY_TYPE_STRING)
                {
                    log_test("\'%c\' not string, ignore, dbe_key=%s", attr.type, k);
                    zfree(k);
                    zfree(v);
                    continue;
                }

                struct timespec start1 = pf_get_time_tick();
                key = createStringObject(attr.key, attr.key_len);
                val = lookupKey(s->ctx->rdb, key);
                if (!val)
                {
                    time_t expire;
                    struct timespec start_t = pf_get_time_tick();
                    val = unserializeObj(v, v_len, &expire);
                    struct timespec end_t = pf_get_time_tick();
                    uns_diff += pf_get_time_diff_nsec(start_t, end_t);

                    const time_t now = time(0);
                    if (val && expire > 0 && expire < now)
                    {
                        /* expire, ignore */
                        log_error("\"%s\" expired, ignore it, expire=%d, now=%d",
                                  key->ptr, expire, now);
                        decrRefCount(val);
                    }
                    else if (val)
                    {
                        if (val->type == REDIS_STRING
                            && val->encoding == REDIS_ENCODING_RAW)
                        {
                            val_total_len += sdslen(val->ptr);
                        }
                        cnt++;

                        dbAdd(s->ctx->rdb, key, val);
                        if (expire > 0)
                        {
                            setExpire(s->ctx->rdb, key, expire);
                        }
                    }
                    else
                    {
                        log_error("load from dbe fail, key=%s", key->ptr);
                    }
                }
                else
                {
                    /* the has exist, ignore it */
                    log_info("\"%s\" existed, ignore", key->ptr);
                    ignore_cnt++;
                }

                decrRefCount(key);
                zfree(k);
                zfree(v);

                struct timespec end1 = pf_get_time_tick();
                diff += pf_get_time_diff_nsec(start1, end1);
            }

            dbe_destroy_it(it);

            end = time(0);
            log_prompt("load k-v into rdb from dbe during=%ds(%lldms, %lldms)"
                       ", cnt=%lld, ignore=%zd, val_total_len=%zd"
                       , end - start, diff / 1000000, uns_diff / 1000000
                       , cnt, ignore_cnt, val_total_len);
        }

        ret = dbe_startup(s->ctx->db);
        if (ret != DBE_ERR_SUCC)
        {
            log_error("dbe_startup() fail, ret=%d", ret);
            dbmng_uninit(tag);
            return 1;
        }
    }

    if (server.has_dbe == 1 && server.has_cache == 1)
    {
        /* init binlog tab */
        start = end;
        s->ctx->binlogtab = blt_init();
        end = time(0);
        log_prompt("blt_init during=%ds, binlogtab=%p", end - start, s->ctx->binlogtab);

        /* load into binlog tab from binlog file */
        if (0 != bl_load_to_mem(s->ctx->binlog, s->ctx->binlogtab))
        {
            log_error("%s", "bl_load_to_mem() fail");
            //dbmng_uninit(tag);
            //return 1;
        }

        redo_blt(s->ctx->rdb, s->ctx->binlogtab, s->ctx->db);

        /* continue unfinished checkpoint at immutable blt */
        update_checkpoint(s->ctx, 0);
    }
    else if (server.has_dbe == 0 && server.has_cache == 1 && server.load_bl == 1 && server.load_bl_cnt > 0)
    {
        redo_bl(s->ctx->rdb, s->ctx->binlog, server.load_bl_cnt);
    }

    return 0;
}

int dbmng_uninit(int tag)
{
    dbmng_ctx_hash *s = find_hash_entry(tag);
    if (!s)
    {
        /* not exist */
        log_info("%s", "dbmng_uninit() can't find the ctx");
        return 1;
    }

    if (s->ctx->t)
    {
        /* can't uninit now */
        s->ctx->stop = 1;
        log_info("%s", "cp transaction is running");
        return 2;
    }

    dbmng_destroy(s);
    return 0;
}

static void dbmng_destroy(dbmng_ctx_hash *s)
{
    if (s->ctx->db)
    {
        dbe_uninit(s->ctx->db);
    }
    if (s->ctx->binlogtab)
    {
        blt_uninit(s->ctx->binlogtab);
    }

    if (s->ctx->db_slave)
    {
        dbe_uninit(s->ctx->db_slave);
    }

    free_hash_entry(s);

    return;
}

void dbmng_set_rdb(int tag, redisDb *rdb)
{
    dbmng_ctx_hash *s = find_hash_entry(tag);
    if (!s)
    {
        return;
    }
    s->ctx->rdb = rdb;
}

void dbmng_check_key(int tag, const robj *key)
{
    (void)tag;
    (void)key;
#if 0
    dbmng_ctx_hash *s = find_hash_entry(tag);
    if (!s || s->ctx->rdb == 0)
    {
        return;
    }

    robj *value = lookupKey(s->ctx->rdb, (robj *)key);
    if (!value)
    {
        if (s->ctx->stop)
        {
            return;
        }
        /* try to restore at first */
        //restore(s->ctx->rdb, (binlog_tab *)s->ctx->binlogtab,
        //        s->ctx->db, key, RESTORE_LVL_ACT);
    }
#endif
}

/* return: 0 - not need io; 1 - need io */
int dbmng_check_key_io(int tag, const robj *key)
{
    dbmng_ctx_hash *s = find_hash_entry(tag);
    if (!s || s->ctx->rdb == 0 || s->ctx->stop)
    {
        return 0;
    }

    int ret = 0;
    robj *value = lookupKey(s->ctx->rdb, (robj *)key);
    if (!value)
    {
        ret = restore_a(s->ctx->rdb, (binlog_tab *)s->ctx->binlogtab,
                s->ctx->db, key, RESTORE_LVL_ACT, 1);
        if (ret == -1)
        {
            /* fail, set to not need io */
            ret = 0;
        }
    }
    return ret;
}

int dbmng_save_op(int tag, const char *cmd, const robj *key, int argc, robj **argv, unsigned long long ds_id, unsigned char db_id)
{
    //return 0;
    log_debug("dbmng_save_op...cmd=%s, key=%s", cmd, key->ptr);

    if (server.is_slave == 0
        && ((server.has_cache == 1 && server.has_dbe == 0)
         || (server.has_cache == 0 && server.has_dbe == 1))
        && server.wr_bl == 0)
    {
        /* pure cache & pure dbe may ignore binlog */
        return 0;
    }

    dbmng_ctx *ctx = get_entry_ctx(tag);
    if (ctx == 0)
    {
        /* fail */
        log_error("%s", "get_entry_ctx() fail");
        return 1;
    }

    const int command = get_cmd(cmd);
    if (command < 0)
    {
        /* illegal command */
        log_error("illegal command, ret=%d, cmd=%s", command, cmd);
        return 1;
    }
    log_debug("op: %s --> %d", cmd, command);

    //robj *value = lookupKey(ctx->rdb, (robj *)key);
    //if (value)
    //{
    //    value->version++;
    //}

    const int app_type = server.has_dbe == 1 ? 1 : 0;
    int type = app_type;
    if (app_type == 0)
    {
        // cache
        if (bl_filter(key->ptr, sdslen(key->ptr)) == 1)
        {
            //log_prompt("cache-key %s not allow to write binlog", key->ptr);
            return 0;
        }
    }
    else
    {
        // persistence
        if (cache_filter(key->ptr, sdslen(key->ptr)) == 1)
        {
            //log_prompt("%s belong to cache", key->ptr);
            type = 0; // cache
            if (bl_filter(key->ptr, sdslen(key->ptr)) == 1)
            {
                //log_prompt("cache-key %s not allow to write binlog", key->ptr);
                return 0;
            }
        }
    }

    if (0 != bl_write_op(ctx->binlog, command, key, argc, (const robj **)argv, ds_id, db_id, type))
    {
        return 1;
    }
    if (server.has_cache == 1 && type == 1)
    {
        blt_add_op_generic((binlog_tab *)ctx->binlogtab, command, key, argc, argv);
    }
    return 0;
}

void dbmng_try_cp(int tag)
{
    if (server.has_dbe == 0)
    {
        return;
    }

    dbmng_ctx *ctx = get_entry_ctx(tag);
    if (ctx == 0)
    {
        log_error("get_entry_ctx() fail, tag=%d", tag);
        return;
    }
    if (ctx->binlogtab == 0)
    {
        return;
    }
    const size_t op_cnt = ((binlog_tab *)ctx->binlogtab)->op_cnt_act;
    if (op_cnt > (size_t)server.op_max_num)
    {
        /* begin to update checkpoint */
        log_debug("start to update checkpoint, op_cnt=%d, max=%d"
                 , op_cnt, server.op_max_num);
        update_checkpoint(ctx, 1);
    }
}

void dbmng_start_cp(int tag)
{
    if (!(server.has_cache == 1 && server.has_dbe == 1))
    {
        return;
    }

    const unsigned int bl_writing_cnt = listLength(server.bl_writing);
    if (bl_writing_cnt > 0)
    {
        log_info("dbmng_start_cp, but bl is writing, bl_writing_cnt=%u", bl_writing_cnt);
        return;
    }

    //log_debug("dbmng_start_cp...(tag=%d)", tag);

    dbmng_ctx *ctx = get_entry_ctx(tag);
    if (ctx)
    {
        const size_t op_cnt = ((binlog_tab *)ctx->binlogtab)->op_cnt_act;
        if (op_cnt > 0)
        {
            update_checkpoint(ctx, 1);
        }
        else
        {
            //log_debug("op_cnt of bl_tab is %d, need not update cp, tag=%d", op_cnt, tag);
        }
    }
    else
    {
        log_error("get_entry_ctx() fail, tag=%d", tag);
    }
}

void dbmng_print_dbeinfo(int tag)
{
    if (server.has_dbe == 0)
    {
        return;
    }

    //log_debug("dbmng_print_dbeinfo...(tag=%d)", tag);
    static unsigned long long get, put, del;
    unsigned long long t, g, p, d;
    g = server.stat_dbeio_get;
    p = server.stat_dbeio_put;
    d = server.stat_dbeio_del;
    t = g + p + d;

    log_prompt("dbeio:%llu(%llu), get:%llu(%llu), put:%llu(%llu), del:%llu(%llu)"
            , t, t - get - put - del
            , g, g - get
            , p, p - put
            , d, d - del
            );
    get = g;
    put = p;
    del = d;

    dbmng_ctx *ctx = get_entry_ctx(tag);
    if (ctx)
    {
        dbe_print_info(ctx->db);
    }
    else
    {
        log_error("get_entry_ctx() fail, tag=%d", tag);
    }
}

int initDbmngConfig()
{
    dbmng_conf.dbe_root_path = server.dbe_root_path;
    dbmng_conf.bl_root_path = server.bl_root_path;
    dbmng_conf.log_prefix = server.binlog_prefix;

    if (server.binlog_max_size <= 0)
    {
        log_error("binlog_max_size=%d parameter illegal,set to default 200M"
                , server.binlog_max_size);
        server.binlog_max_size = 200 * 1024 * 1024;
        //return 1;
    }
    dbmng_conf.binlog_max_size = server.binlog_max_size;

#if 0
    log_debug("app_path=%s", dbmng_conf.app_path);
    log_debug("binlog_prefix=%s", dbmng_conf.log_prefix);
    log_debug("binlog_max_size=%d", dbmng_conf.binlog_max_size);
#endif

    return 0;
}

void *dbmng_get_db(int tag, int master)
{
    (void)master;
    dbmng_ctx *ctx = get_entry_ctx(tag);
    if (ctx == 0)
    {
        /* fail */
        log_error("%s", "get_entry_ctx() fail");
        return 0;
    }

    return ctx->db;
}

void *dbmng_get_bl(int tag)
{
    dbmng_ctx *ctx = get_entry_ctx(tag);
    if (ctx == 0)
    {
        /* fail */
        log_error("%s", "get_entry_ctx() fail");
        return 0;
    }

    return ctx->binlog;
}

int dbmng_cp_is_active(int tag)
{
    dbmng_ctx *ctx = get_entry_ctx(tag);
    if (ctx == 0)
    {
        /* fail */
        log_error("%s", "get_entry_ctx() fail");
        return 0;
    }

    return ctx->t ? 1 : 0;
}

int dbmng_cp_is_blocked(int tag)
{
    dbmng_ctx *ctx = get_entry_ctx(tag);
    if (ctx == 0)
    {
        /* fail */
        log_error("%s", "get_entry_ctx() fail");
        return 0;
    }

    return ctx->block_cp ? 1 : 0;
}

int dbmng_block_upd_cp(int tag, uint64_t *ts)
{
    if (ts == 0)
    {
        return -1;
    }

    dbmng_ctx *ctx = get_entry_ctx(tag);
    if (ctx == 0)
    {
        /* fail */
        log_error("%s", "get_entry_ctx() fail");
        return -1;
    }

    ctx->block_cp = 1;

    if (ctx->db)
    {
        int ret;
        if ((ret = dbe_freeze(ctx->db)) != DBE_ERR_SUCC)
        {
            log_error("dbe_freeze() fail, ret=%d", ret);
            return -3;
        }
    }

    *ts = bl_get_last_ts(ctx->binlog, 1);
    log_prompt("last bl with cp: %"PRIu64", cp_idx=%d, cp_offset=%d"
               , *ts
               , bl_get_cp_idx(ctx->binlog)
               , bl_get_cp_offset(ctx->binlog)
              );

    return 0;
}

int dbmng_unblock_upd_cp(int tag)
{
    /* check whether allow to unblock */
    if (lock_repl_status() != 0)
    {
        log_error("%s", "lock_repl_status() fail");
        return -1;
    }
    repl_status_node *node = gReplStatusMngr.head;
    while (node)
    {
        if (node->status == REPL_STATUS_INIT
            || node->status == REPL_STATUS_DOING
           )
        {
            log_error("%s:%d status=%d, not allow to unblock",
                      node->master_ip, node->master_port, node->status);
            unlock_repl_status();
            return -2;
        }
        node = node->next;
    }
    unlock_repl_status();

    dbmng_ctx *ctx = get_entry_ctx(tag);
    if (ctx == 0)
    {
        /* fail */
        log_error("%s", "get_entry_ctx() fail");
        return -1;
    }

    ctx->block_cp = 0;

    if (ctx->db)
    {
        int ret;
        if ((ret = dbe_unfreeze(ctx->db)) != DBE_ERR_SUCC)
        {
            log_error("dbe_unfreeze() fail, ret=%d", ret);
            return -3;
        }
    }

    return 0;
}

int dbmng_key_exist(int tag, const robj *key, uint64_t *ver)
{
    log_debug("dbmng_key_exist...key=%s", key->ptr);

    dbmng_ctx *ctx = get_entry_ctx(tag);
    if (ctx == 0)
    {
        /* fail */
        log_error("%s", "get_entry_ctx() fail");
        return 2;
    }
    (void)ver;

    return 0;
}

robj *dbmng_lookup_key(int tag, const robj *key, time_t *exp)
{
    log_debug("dbmng_lookup_key...key=%s", key->ptr);

    dbmng_ctx *ctx = get_entry_ctx(tag);
    if (ctx == 0)
    {
        /* fail */
        log_error("%s", "get_entry_ctx() fail");
        return 0;
    }
    (void)exp;

    return 0;
}

int dbmng_set_key(int tag, const robj *key, int argc, robj **argv)
{
    log_debug("dbmng_set_key...key=%s", key->ptr);

    dbmng_ctx *ctx = get_entry_ctx(tag);
    if (ctx == 0)
    {
        /* fail */
        log_error("%s", "get_entry_ctx() fail");
        return 1;
    }

    int expire = 0;
    if (argc == 2)
    {
        long long exp;
        if (getLongLongFromObject(argv[1], &exp) == REDIS_OK)
        {
            expire = (int)exp;
        }
    }

    int len;
    char *val = 0;
    val = serializeObjExp(argv[0], &len, expire);
    if (val)
    {
        zfree(val);
        return 0;
    }
    else
    {
        return 1;
    }
}

int dbmng_wr_bl(int tag, const char *cmd, const robj *key, int argc, robj **argv, unsigned long long ds_id, unsigned char db_id)
{
    log_debug("dbmng_wr_bl...cmd=%s, key=%s", cmd, key->ptr);

    if (server.is_slave == 0
        && ((server.has_cache == 1 && server.has_dbe == 0)
         || (server.has_cache == 0 && server.has_dbe == 1))
        && server.wr_bl == 0)
    {
        /* pure cache & pure dbe may ignore binlog */
        return 0;
    }

    dbmng_ctx *ctx = get_entry_ctx(tag);
    if (ctx == 0)
    {
        /* fail */
        log_error("%s", "get_entry_ctx() fail");
        return 1;
    }

    const int command = get_cmd(cmd);
    if (command < 0)
    {
        /* illegal command */
        log_error("illegal command, ret=%d, cmd=%s", command, cmd);
        return 1;
    }
    log_debug("op: %s --> %d", cmd, command);

    bl_write_op(ctx->binlog, command, key, argc, (const robj **)argv, ds_id, db_id, 1);

    return 0;
}

int dbmng_del_key(int tag, const robj *key)
{
    log_debug("dbmng_del_key...key=%s", key->ptr);

    dbmng_ctx *ctx = get_entry_ctx(tag);
    if (ctx == 0)
    {
        /* fail */
        log_error("%s", "get_entry_ctx() fail");
        return 1;
    }

    return 0;
}

void dbmng_release_bl(int tag)
{
    dbmng_ctx *ctx = get_entry_ctx(tag);
    if (ctx == 0)
    {
        /* fail */
        log_error("%s", "get_entry_ctx() fail");
        return;
    }

    if (tag_new_cp(ctx->binlog, -1, -1) == 0)
    {
        confirm_new_cp(ctx->binlog);
    }

    reset_blt_active(ctx->binlogtab);

    ctx->giveup = 1;
    ctx->pending = 0;
}

void dbmng_reset_bl(int tag)
{
    dbmng_ctx *ctx = get_entry_ctx(tag);
    if (ctx == 0)
    {
        /* fail */
        log_error("%s", "get_entry_ctx() fail");
        return;
    }

    reset_bl(ctx->binlog);
}

