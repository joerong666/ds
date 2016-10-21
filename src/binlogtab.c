#include "binlogtab.h"
#include "serialize.h"
#include "dbe_if.h"
#include "dbmng.h"

#include "ds_log.h"
#include "op_cmd.h"
#include "restore_key.h"
#include "codec_key.h"

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

#include <sys/sysinfo.h>

static dict *create_bl_tab();
static binlogEntry *query_bl_tab_ex(dict *t, const void *k);
static int restore_from_dbe(redisDb *rdb, void *db, const robj *key);
static int restore_from_blt(redisDb *rdb, const binlogEntry *e, const robj *key);
static int add_op_to_oplist(binlogEntry *e, int cmd, int argc, const binlog_str *argv);
static int get_mirror(redisDb *rdb, dict *d);
static int need_restore_from_dbe(const binlogEntry *e);
static int is_del_op(list *oplist);
static void make_zset_dbe_data(sds key, listNode *node, upd_dbe_param *param);

static unsigned int blt_dictSdsHash(const void *key)
{
    return dictGenHashFunction((unsigned char*)key, sdslen((char*)key));
}

static int blt_dictSdsKeyCompare(void *privdata, const void *key1, const void *key2)
{
    //log_debug("blt_dictSdsKeyCompare()...key1=\"%s\", key2=\"%s\"",
    //          (const char *)key1, (const char *)key2);

    int l1,l2;
    DICT_NOTUSED(privdata);

    l1 = sdslen((sds)key1);
    l2 = sdslen((sds)key2);
    if (l1 != l2) return 0;
    return memcmp(key1, key2, l1) == 0;
}

static void blt_dictSdsDestructor(void *privdata, void *val)
{
    //log_debug("blt_dictSdsDestructor()...val=%s", val);
    //const size_t a = zmalloc_used_memory();
    //log_debug("before: blt_dictSdsDestructor()...mem=%d", a);
    DICT_NOTUSED(privdata);
    sdsfree(val);
    //const size_t b = zmalloc_used_memory();
    //log_debug("after: blt_dictSdsDestructor()...mem=%d, diff=%d", b, a - b);
}

static void blt_dictStDestructor(void *privdata, void *val)
{
    //log_debug("blt_dictStDestructor()...val=%p", val);
    //const size_t a = zmalloc_used_memory();
    //log_debug("before blt_dictStDestructor()...mem=%d", a);
    DICT_NOTUSED(privdata);
    binlogEntry *v = (binlogEntry *)val;
    if (v->oplist)
    {
        listRelease(v->oplist);
    }
    if (v->cache_byte)
    {
        zfree(v->cache_byte);
    }
    zfree(v);
    //const size_t b = zmalloc_used_memory();
    //log_debug("after blt_dictStDestructor()...mem=%d, diff=%d", b, a - b);
}

/* for listSetFreeMethod(l, m) */
static void oplistDestructor(void *ptr)
{
    if (!ptr)
    {
        return;
    }

    opAttr *op = (opAttr *)ptr;
    if (op->argv)
    {
        int i;
        for (i = 0; i < op->argc; i++)
        {
            if (op->argv[i])
            {
                decrRefCount(op->argv[i]);
            }
        }
        zfree(op->argv);
    }
    zfree(op);
}

#ifdef _UPD_DBE_BY_PERIODIC_
static opAttr *gen_op_generic(unsigned char cmd, int argc, robj **argv)
{
    opAttr *op = (opAttr *)zmalloc(sizeof(opAttr));
    if (op == 0)
    {
        log_error("zmalloc() fail for opAttr, cmd=%d", cmd);
        return op;
    }
    op->cmd = cmd;
    op->argc = argc;
    op->argv = 0;
    if (argc == 0)
    {
        return op;
    }

    op->argv = (robj **)zcalloc(sizeof(robj *) * op->argc);
    if (op->argv == 0)
    {
        log_error("zmalloc() fail for opAttr->argv, argc=%d", argc);
        zfree(op);
        return 0;
    }
    //log_debug("gen_op_generic: argc=%d", argc);
    int i;
    for (i = 0; i < argc; i++)
    {
        incrRefCount(argv[i]);
        op->argv[i] = argv[i];
    }
    return op;
}
#endif

static opAttr *gen_op_generic2(unsigned char cmd, int argc, const binlog_str *argv)
{
    opAttr *op = (opAttr *)zmalloc(sizeof(opAttr));
    if (op == 0)
    {
        log_error("%s", "zmalloc() fail for opAttr");
        return op;
    }
    op->cmd = cmd;
    op->argc = argc;
    op->argv = 0;
    if (argc == 0)
    {
        return op;
    }

    op->argv = (robj **)zcalloc(sizeof(robj *) * op->argc);
    if (op->argv == 0)
    {
        log_error("%s", "zmalloc() fail for opAttr->argv");
        zfree(op);
        return 0;
    }
    int i;
    for (i = 0; i < argc; i++)
    {
        op->argv[i] = unserializeObj(argv[i].ptr, argv[i].len, 0);
        if (op->argv[i] == 0)
        {
            log_error("unserializeObj() fail for opAttr->argv[%d], argc=%d", i, argc);
            oplistDestructor(op);
            return 0;
        }
        //log_debug("after unserialize, refcount=%d", op->argv[i]->refcount);
        //print_obj(op->argv[i]);
    }

    return op;
}

static dictType bltabDictType = 
{
    blt_dictSdsHash,            /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    blt_dictSdsKeyCompare,      /* key compare */
    blt_dictSdsDestructor,      /* key destructor */
    blt_dictStDestructor        /* val destructor */
};

binlog_tab *blt_init()
{
    binlog_tab *blt = (binlog_tab *)zcalloc(sizeof(binlog_tab));
    blt->active = create_bl_tab();
    return blt;
}

static void blt_destroy(binlog_tab *blt)
{
    if (blt->active)
    {
        dictRelease(blt->active);
    }
    if (blt->immutable)
    {
        dictRelease(blt->immutable);
    }
    zfree(blt);
}

void blt_uninit(binlog_tab *blt)
{
    if (blt == 0)
    {
        return;
    }

    blt_destroy(blt);
}

static dict *create_bl_tab()
{
    return dictCreate(&bltabDictType, 0);
}

/* k : sds */
static binlogEntry *query_bl_tab_ex(dict *t, const void *k)
{
    if (t == 0 || k == 0)
    {
        return 0;
    }

    binlogEntry *v = (binlogEntry *)dictFetchValue(t, k);
    return v;
}

/* blt : dict
 * key : sds
 * value : binlogEntry
 * */
static binlogEntry *blt_add_e(dict *t, void *k)
{
    binlogEntry *v = 0;
    v = (binlogEntry *)zcalloc(sizeof(binlogEntry));
    if (!v)
    {
        return 0;
    }
    v->cache_len = -1; /* init */
    v->oplist = listCreate();
    if (v->oplist == 0)
    {
        zfree(v);
        return 0;
    }
    listSetFreeMethod(v->oplist, oplistDestructor);
    const int ret = dictAdd(t, k, v);
    if (ret != DICT_OK)
    {
        listRelease(v->oplist);
        zfree(v);
        return 0;
    }

    return v;
}

/* return:
 * -1 : fail
 *  0 : succ, not need get from dbe
 *  1 : succ, need get from dbe
 */
int restore_a(redisDb *rdb, binlog_tab *blt, void *db, const robj *key, int lvl, int check)
{
    if (!rdb || !db || !key || lvl < RESTORE_LVL_DBE || lvl > RESTORE_LVL_ACT)
    {
        log_error("%s", "illegal parameter in restore_a()");
        return -1;
    }

    log_debug("restore_a: check=%d, key=%s", check, (const char*)key->ptr);

    int ret;

    if (lvl == RESTORE_LVL_DBE || blt == 0)
    {
        /* restore from dbe directly */
        log_debug("restore from dbe only, key=%s", (const char*)key->ptr);
        return 1;
    }

    binlogEntry *e_act = 0;
    int rst_imm = 1;
    if (lvl == RESTORE_LVL_ACT)
    {
        e_act = query_bl_tab_ex(blt->active, key->ptr);
        rst_imm = need_restore_from_dbe(e_act);
    }
    if (rst_imm == 1)
    {
        /* restore according to immutable_tab */
        binlogEntry *e = query_bl_tab_ex(blt->immutable, key->ptr);
        if (e && e->giveup == 0)
        {
            log_debug("restore from immutable blt, binlogEntry.status=%d, key=%s",
                      e->status, (const char*)key->ptr);

            if (e->status == BLE_STATUS_INIT)
            {
                if (0 == is_del_op(e->oplist))
                {
                    if (check == 1 && need_restore_from_dbe(e))
                    {
                        return 1;
                    }
                    log_debug("restore from immutable, key=%s", (const char*)key->ptr);
                    ret = restore_from_blt(rdb, e, key);
                }
            }
            else if (e->status == BLE_STATUS_DONE)
            {
                if (check == 1)
                {
                    return 1;
                }
            }
            else
            {
                log_info("restore by unserialize: cache_byte=%p, cache_len=%d",
                         e->cache_byte, e->cache_len);

                if (e->cache_byte && e->cache_len > 0)
                {
                time_t expire;
                robj *v = unserializeObj((const char *)e->cache_byte, e->cache_len, &expire);
                if (v)
                {
                    dbAdd(rdb, (robj *)key, v);
                    setExpire(rdb, (robj *)key, expire);
                }
                else
                {
                    /* fail */
                    log_error("unserializeObj() fail, cache_len=%d, status=%d, key=%s",
                              e->cache_len, e->status, (const char*)key->ptr);
                    return -1;
                }
                }
            }
        }
        else
        {
            /* restore from dbe directly */
            if (check)
            {
                return 1;
            }
        }
    }

    /* restore according to active_tab */
    if (e_act)
    {
        log_debug("restore from active blt, key=%s", (const char*)key->ptr);
        restore_from_blt(rdb, e_act, key);
    }

    return 0;
}

/* sync interface, only call by init */
int restore(redisDb *rdb, binlog_tab *blt, void *db, const robj *key, int lvl)
{
    if (!rdb || !db || !key || lvl < RESTORE_LVL_DBE || lvl > RESTORE_LVL_ACT)
    {
        log_error("%s", "illegal parameter in restore()");
        return -1;
    }

    int ret;

    if (lvl == RESTORE_LVL_DBE || blt == 0)
    {
        /* restore from dbe directly */
        log_debug("restore from dbe only, key=%s", (const char*)key->ptr);

        ret = restore_from_dbe(rdb, db, key);
        if (ret < 0)
        {
            log_error("restore_from_dbe() fail, ret=%d, key=%s", ret, (const char*)key->ptr);
            return -1;
        }
        return 1;
    }

    binlogEntry *e_act = 0;
    int rst_imm = 1;
    if (lvl == RESTORE_LVL_ACT)
    {
        e_act = query_bl_tab_ex(blt->active, key->ptr);
        rst_imm = need_restore_from_dbe(e_act);
    }
    if (rst_imm == 0)
    {
        log_debug("restore from active blt only, key=%s", (const char*)key->ptr);

        binlogEntry *e = query_bl_tab_ex(blt->immutable, key->ptr);
        if (e && e->giveup == 0 && e->status == BLE_STATUS_INIT)
        {
            log_debug("restore from immutable first for updating cp, key=%s", (const char*)key->ptr);
            if (is_del_op(e->oplist))
            {
                e->cache_byte = 0;
                e->cache_len = 0;
            }
            else
            {
            if (need_restore_from_dbe(e))
            {
                ret = restore_from_dbe(rdb, db, key);
                if (ret < 0)
                {
                    log_error("restore_from_dbe() fail, ret=%d, key=%s", ret, (const char*)key->ptr);
                }
            }
            log_debug("restore from immutable, key=%s", (const char*)key->ptr);
            ret = restore_from_blt(rdb, e, key);
            robj *value = lookupKey(rdb, (robj *)key);
            if (value)
            {
                const int expire = getExpire(rdb, (robj *)key);
                e->cache_byte = serializeObjExp(value, &e->cache_len, expire == -1 ? 0 : expire);

                dbDelete(rdb, (robj *)key);
            }
            else
            {
                //e->cache_byte = 0;
                //e->cache_len = 0;
                log_error("impossible: loopupKey() null after restore_from_blt(), key=%s", (const char*)key->ptr);
            }
            }
        }
    }
    else
    {
        /* restore according to immutable_tab */
        binlogEntry *e = query_bl_tab_ex(blt->immutable, key->ptr);
        if (e && e->giveup == 0)
        {
            log_debug("restore from immutable blt, binlogEntry.status=%d, key=%s",
                      e->status, (const char*)key->ptr);

            if (e->status == BLE_STATUS_INIT)
            {
                if (is_del_op(e->oplist))
                {
                    e->cache_byte = 0;
                    e->cache_len = 0;
                }
                else
                {
                if (need_restore_from_dbe(e))
                {
                    ret = restore_from_dbe(rdb, db, key);
                    if (ret < 0)
                    {
                        log_error("restore_from_dbe() fail, ret=%d, key=%s", ret, (const char*)key->ptr);
                        return -1;
                    }
                }
                log_debug("restore from immutable, key=%s", (const char*)key->ptr);
                ret = restore_from_blt(rdb, e, key);
                robj *value = lookupKey(rdb, (robj *)key);
                if (value)
                {
                    const int expire = getExpire(rdb, (robj *)key);
                    e->cache_byte = serializeObjExp(value, &e->cache_len, expire == -1 ? 0 : expire);
                }
                else
                {
                    //e->cache_byte = 0;
                    //e->cache_len = 0;
                    log_error("impossible: loopupKey() null after restore_from_blt(), key=%s", (const char*)key->ptr);
                }
                }
            }
            else if (e->status == BLE_STATUS_DONE)
            {
                ret = restore_from_dbe(rdb, db, key);
                if (ret < 0)
                {
                    log_error("restore_from_dbe() fail, ret=%d, key=%s", ret, (const char*)key->ptr);
                    return -1;
                }
            }
            else
            {
                log_info("restore by unserialize: cache_byte=%p, cache_len=%d",
                         e->cache_byte, e->cache_len);

                if (e->cache_byte && e->cache_len > 0)
                {
                time_t expire;
                robj *v = unserializeObj((const char *)e->cache_byte, e->cache_len, &expire);
                if (v)
                {
                    dbAdd(rdb, (robj *)key, v);
                    setExpire(rdb, (robj *)key, expire);
                }
                else
                {
                    /* fail */
                    log_error("unserializeObj() fail, cache_len=%d, status=%d, key=%s",
                              e->cache_len, e->status, (const char*)key->ptr);
                    return -1;
                }
                }
            }
        }
        else
        {
            /* restore from dbe directly */
            ret = restore_from_dbe(rdb, db, key);
            if (ret < 0)
            {
                log_error("restore_from_dbe() fail, ret=%d, key=%s", ret, (const char*)key->ptr);
                return -1;
            }
        }
    }

    /* restore according to active_tab */
    if (e_act)
    {
        log_debug("restore from active blt, key=%s", (const char*)key->ptr);
        restore_from_blt(rdb, e_act, key);
    }

    return 0;
}

/* return:
 * -1 : fail
 *  0 : get nothing from dbe
 *  1 : succ
 *  2 : the key expired
 */
static int restore_from_dbe(redisDb *rdb, void *db, const robj *key)
{
    if (!key)
    {
        /* illegal argument */
        return -1;
    }

    log_debug("restore from dbe, key=%s", (const char*)key->ptr);

    /* get value from db engine */
    val_attr rslt;
    const int ret = restore_key_from_dbe(db, (const char *)key->ptr, sdslen(key->ptr), 'k', &rslt);
    if (ret == 1)
    {
        log_info("dbe_get() return null, db=%p, key=%s", db, (const char *)key->ptr);
        return 0;
    }
    else if (ret != 0)
    {
        /* get fail */
        return -1;
    }

    time_t expire = rslt.expire_ms;
    robj *v = rslt.val;
    const time_t now = time(0);
    if (expire > 0 && expire <= now)
    {
        /* the key expired, delete it from dbe as well */
        log_info("expired, key=%s, expire=%ld, now=%ld"
                , (const char*)key->ptr, expire, now);
        decrRefCount(v);
        //dbe_delete(db, (const char *)key->ptr);
        return 2;
    }

    dbAdd(rdb, (robj *)key, v);
    if (expire > 0)
    {
        setExpire(rdb, (robj *)key, expire);
    }
    else
    {
        removeExpire(rdb, (robj *)key);
    }
    signalModifiedKey(rdb, (robj *)key);

    log_debug("restore from dbe succ, key=%s, version=%"PRIu64", expire=%ld",
              (const char*)key->ptr, v->version, expire);

    return 1;
}

/* return:
 * 0 - succ
 */
static int restore_from_blt(redisDb *rdb, const binlogEntry *e, const robj *key)
{
    int ret = 0;
    listIter *it = 0;
    if (e->oplist)
    {
        it = listGetIterator(e->oplist, AL_START_HEAD);
    }

    if (it)
    {
        listNode *node = 0;

        while ((node = listNext(it)) != 0)
        {
            opAttr *op = (opAttr *)node->value;
            if (op->cmd != 0)
            {
                redo_op(rdb, key, op->cmd, op->argc, op->argv);
            }
        }

        listReleaseIterator(it);
    }
    else
    {
        log_error("op_list is null, key=%s", (const char*)key->ptr);
    }

    return ret;
}

int switch_blt_only(redisDb *rdb, binlog_tab *blt)
{
    if (blt->active == 0)
    {
        log_error("switch_blt() but active is null, rdb=%p, blt=%p"
                , (void*)rdb, (void*)blt);
        return 1;
    }

    if (blt->immutable)
    {
        log_warn("%s", "switch_blt() but immutable isn't null");
        release_blt_immutable(blt);
    }

    dict *n = create_bl_tab();
    if (n == 0)
    {
        log_error("create_bl_tab() fail for active, blt=%p", (void*)blt);
        return 1;
    }

    blt->immutable = blt->active;
    blt->active = n;
    blt->op_cnt_act = 0;

    return 0;
}

int switch_blt(redisDb *rdb, binlog_tab *blt)
{
    int ret;
    if ((ret = switch_blt_only(rdb, blt)) == 0)
    {
    }

    return ret;
}

void release_blt_immutable(binlog_tab *blt)
{
    if (!blt || !blt->immutable)
    {
        return;
    }

    log_info("release_blt_immutable()...rec_cnt=%lu", dictSize(blt->immutable));
    dictRelease(blt->immutable);
    blt->immutable = 0;
}

void reset_blt_active(binlog_tab *blt)
{
    if (!blt || !blt->active)
    {
        return;
    }

    log_info("reset_blt_active(): dictSize()=%lu, op_cnt_act=%zd",
             dictSize(blt->active), blt->op_cnt_act);
    dictRelease(blt->active);
    blt->active = create_bl_tab();
    blt->op_cnt_act = 0;
}

static int get_mirror(redisDb *rdb, dict *d)
{
    if (rdb == 0 || d == 0)
    {
        return 0;
    }

    dictIterator *it = dictGetSafeIterator(d);
    if (it == 0)
    {
        /* system fail */
        log_error("dictGetSafeIterator() fail, dict=%p", (void*)d);
        return -1;
    }

    int cnt = 0;
    int cnt_ok = 0;

    dictEntry *node = 0;
    while ((node = dictNext(it)) != 0)
    {
        cnt++;

        sds key = node->key; // sds
        binlogEntry *e = (binlogEntry *)node->val;

        if (!key || !e)
        {
            log_error("%s", "illegal data, skip the node");
            continue;
        }

        if (e->status != BLE_STATUS_INIT)
        {
            log_info("skip the node for status=%d, key=%s", e->status, key);
            continue;
        }

        if (e->giveup)
        {
            log_info("skip the node for giveup, key=%s", key);
            continue;
        }

        if (e->cache_len >= 0)
        {
            continue;
        }

        if (is_del_op(e->oplist))
        {
            e->cache_byte = 0;
            e->cache_len = 0;
        }
        else
        {
            get_value_from_rdb(rdb, key, &e->cache_byte, &e->cache_len);
        }
        cnt_ok++;
    }

    dictReleaseIterator(it);
    it = 0;

    log_prompt("get_mirror: cnt=%d, cnt_ok=%d", cnt, cnt_ok);

    return 0;
}

int get_value_from_rdb(redisDb *rdb, sds key, char **ptr, int *len)
{
    robj k;
    initObject(&k, REDIS_STRING, key);

    robj *value = lookupKey(rdb, &k);
    if (value)
    {
        const int expire = getExpire(rdb, &k);
        *ptr = serializeObjExp(value, len, expire == -1 ? 0 : expire);
        return 0;
    }
    else
    {
        log_debug("not exist in rdb, key=%s", key);
        *ptr = 0;
        *len = 0;
        return 1;
    }
}

/* 
 * check whether it is del op at tail of list
 * return: 0 - isn't; 1 - yes
 */
static int is_del_op(list *oplist)
{
    if (oplist == 0)
    {
        return 0;
    }
    listNode *node = listLast(oplist);
    if (node == 0)
    {
        return 0;
    }
    opAttr *op = (opAttr *)node->value;
    if (op == 0)
    {
        return 0;
    }
    return op->cmd == OP_CMD_DEL ? 1 : 0;
}

dictIterator *scan_immutable(redisDb *rdb, void *db, binlog_tab *blt, dictIterator *dit, upd_dbe_param *param)
{
    if (!rdb || !db || !blt || !blt->immutable || param)
    {
        return 0;
    }
    memset(param, 0, sizeof(*param));

    if (dit == 0)
    {
        blt->imm_size = dictSize(blt->immutable);
        blt->imm_scan_cnt = -1;
        blt->imm_do_cnt = 0;
        blt->imm_done_cnt = 0;
        blt->imm_doing_cnt = 0;
        blt->imm_giveup_cnt = 0;
        blt->imm_error_cnt = 0;
        blt->imm_read_rdb_cnt = 0;
        log_info("begin to scan immutable, dict_size=%zd", blt->imm_size);
    }

    dictIterator *it = dit ? dit : dictGetSafeIterator(blt->immutable);
    if (it == 0)
    {
        /* system fail */
        return it;
    }

    dictEntry *node = 0;
    while ((node = dictNext(it)) != 0)
    {
        blt->imm_scan_cnt++;

        void *key = node->key;
        binlogEntry *e = (binlogEntry *)node->val;

        if (!key || !e)
        {
            log_error("illegal data, skip the node, scan_no=%zd", blt->imm_scan_cnt);
            blt->imm_error_cnt++;
            continue;
        }

        if (e->status == BLE_STATUS_DOING)
        {
            log_info("skip the node, scan_no=%zd, status=%d, key=%s",
                      blt->imm_scan_cnt, e->status, (char*)key);
            blt->imm_doing_cnt++;
            continue;
        }

        if (e->status == BLE_STATUS_DONE)
        {
            log_info("skip the node, scan_no=%zd, status=%d, key=%s",
                      blt->imm_scan_cnt, e->status, (char*)key);
            blt->imm_done_cnt++;
            continue;
        }

        if (e->giveup)
        {
            log_info("skip the node giveup, scan_no=%zd, key=%s", blt->imm_scan_cnt, (char*)key);
            blt->imm_giveup_cnt++;
            continue;
        }

        opAttr *op = 0;
        listNode *node = 0;
        node = e->oplist ? listFirst(e->oplist) : 0;
        //log_prompt("oplist_len=%d, key=%s"
        //        , e->oplist ? listLength(e->oplist) : 0, (const char *)key);
        if (node)
        {
            op = (opAttr *)(node->value);
        }
        else
        {
            log_error("oplist is null, impossible, key=%s", (const char *)key);
            continue;
        }
        if (op->cmd == OP_CMD_DEL)
        {
            param->pdel_key = encode_prefix_key((const char*)key, sdslen((sds)key), 0, &param->pdel_key_len);

            node = listNextNode(node);
            op = node ? (opAttr*)(node->value) : NULL;
        }
        if (op)
        {
            param->cmd_type = get_blcmd_type(op->cmd);
            if (param->cmd_type == KEY_TYPE_STRING)
            {
                if (e->cache_len < 0)
                {
                    blt->imm_read_rdb_cnt++;
                    get_value_from_rdb(rdb, key, &e->cache_byte, &e->cache_len);
                }

                param->one.k = encode_string_key((const char*)key, sdslen((sds)key), &(param->one.ks));
                param->one.v = e->cache_byte;
                param->one.vs = e->cache_len;
                e->cache_byte = NULL;
                e->cache_len = -1;
            }
            else if (param->cmd_type == KEY_TYPE_LIST)
            {
                log_error("unsupport list now");
            }
            else if (param->cmd_type == KEY_TYPE_SET)
            {
                log_error("unsupport set now");
            }
            else if (param->cmd_type == KEY_TYPE_ZSET)
            {
                make_zset_dbe_data((sds)key, node, param);
            }
            else if (param->cmd_type == KEY_TYPE_HASH)
            {
                log_error("unsupport hash now");
            }
            else
            {
                log_error("wrong cmd_type=%c, cmd=%d, key=%s"
                        , param->cmd_type, op->cmd, (const char*)key);
            }
        }
        param->db_id = 0; /* defualt */
        param->key = key;
        param->ble = e;

        e->status = BLE_STATUS_DOING;
        blt->imm_do_cnt++;
        break;
    }

    if (node == 0)
    {
        /* scan over */
        dictReleaseIterator(it);
        it = 0;

        log_info("finish scan immutable, dict_size=%zd, scan_cnt=%zd, do_cnt=%zd, "
                   "doing_cnt=%zd, done_cnt=%zd, giveup_cnt=%zd, error_cnt=%zd, "
                   "read_rdb_cnt=%zd"
                   , blt->imm_size, blt->imm_scan_cnt + 1, blt->imm_do_cnt
                   , blt->imm_doing_cnt, blt->imm_done_cnt, blt->imm_giveup_cnt
                   , blt->imm_error_cnt, blt->imm_read_rdb_cnt
                  );
    }

    return it;
}

void finish_ble(void *ble)
{
    if (ble == 0)
    {
        return;
    }

    binlogEntry *e = (binlogEntry *)ble;
    e->status = BLE_STATUS_DONE;
    e->giveup = 1;
    if (e->cache_byte)
    {
        zfree(e->cache_byte);
        e->cache_byte = 0;
        e->cache_len = -1;
    }
    
    if (e->oplist)
    {
        listRelease(e->oplist);
        e->oplist = 0;
    }
}

int blt_exist_key(binlog_tab *blt, const robj *key)
{
    if (!blt || !key)
    {
        return 0;
    }
    return query_bl_tab_ex(blt->active, key->ptr) ? 1 : 0;
}

static int add_op_to_oplist(binlogEntry *e, int cmd, int argc, const binlog_str *argv)
{
    opAttr *op = gen_op_generic2(cmd, argc, argv);
    if (op == 0)
    {
        log_error("%s", "gen_op_generic2() fail");
        return 1;
    }
    if (listAddNodeTail(e->oplist, op) == 0)
    {
        log_error("%s", "listAddNodeTail() fail");
        oplistDestructor(op);
        return 1;
    }

    return 0;
}

int blt_add_op_generic(binlog_tab *blt, unsigned char cmd, const robj *key, int argc, robj **argv)
{
#ifndef _UPD_DBE_BY_PERIODIC_
    (void)blt;
    (void)cmd;
    (void)key;
    (void)argc;
    (void)argv;
    return 0;
#else
    if (!(server.has_cache == 1 && server.has_dbe == 1))
    {
        return 0;
    }

    if (!blt || !key || argc < 0 || (argc > 0 && argv == 0))
    {
        return 1;
    }

    binlogEntry *e = query_bl_tab_ex(blt->active, key->ptr);
    if (!e)
    {
        /* not exist */
        sds k = sdsdup(key->ptr);
        if (k == 0)
        {
            log_error("sdsdup() fail, key=%s", (const char*)key->ptr);
            return 1;
        }
        e = blt_add_e(blt->active, k);
        if (!e)
        {
            log_error("blt_add_e() fail, key=%s", (const char*)key->ptr);
            sdsfree(k);
            return 1;
        }
    }
    else
    {
        e->cache_len = -1;
        if (e->cache_byte)
        {
            zfree(e->cache_byte);
            e->cache_byte = 0;
        }

        const unsigned int op_cnt = listLength(e->oplist);
        if (is_entirety_cmd(cmd) && (op_cnt > 0))
        {
            log_debug("cmd=%d, will free %d cmd before, k=%s", cmd, op_cnt, (const char*)key->ptr);

            /* release curren op */
            listRelease(e->oplist);

            /* create oplist for new op */
            e->oplist = listCreate();
            if (e->oplist == 0)
            {
                log_error("listCreate() fail, cmd=%d, key=%s", cmd, (const char*)key->ptr);
                return 1;
            }
            listSetFreeMethod(e->oplist, oplistDestructor);
        }
    }

    opAttr *op = gen_op_generic(cmd, argc, argv);
    if (op == 0)
    {
        log_error("gen_op_generic() fail, key=%s", (const char*)key->ptr);
        return 1;
    }
    if (listAddNodeTail(e->oplist, op) == 0)
    {
        log_error("listAddNodeTail() fail, key=%s", (const char*)key->ptr);
        oplistDestructor(op);
        return 1;
    }

    blt->op_cnt_act++;
    log_debug("blt add op succ, cmd=%d, key=%s, op_cnt_act=%zd",
              cmd, (const char*)key->ptr, blt->op_cnt_act);

    return 0;
#endif
}

int blt_add_op(binlog_tab *blt, unsigned char cmd, const binlog_str *key, int argc, const binlog_str *argv)
{
    if (!(server.has_cache == 1 && server.has_dbe == 1))
    {
        return 0;
    }

    if (!blt || !key || argc < 0 || (argc > 0 && argv == 0))
    {
        return 1;
    }

    int ret = 1;
    int c_flg = 0;
    sds k = sdsnewlen(key->ptr, key->len);
    binlogEntry *e = query_bl_tab_ex(blt->active, k);
    if (!e)
    {
        /* not exist */
        e = blt_add_e(blt->active, k);
        if (!e)
        {
            log_error("blt_add_e() fail, key=%s", k);
            goto blt_add_op_fail;
        }
        c_flg = 1;
    }
    else
    {
        const unsigned int op_cnt = listLength(e->oplist);
        if (is_entirety_cmd(cmd) && (op_cnt > 0))
        {
            log_debug("cmd=%d, will free %d cmd before, k=%s", cmd, op_cnt, k);

            /* release curren op */
            listRelease(e->oplist);

            /* create oplist for new op */
            e->oplist = listCreate();
            if (e->oplist == 0)
            {
                log_error("listCreate() fail, cmd=%d, key=%s", cmd, k);
                goto blt_add_op_fail;
            }
            listSetFreeMethod(e->oplist, oplistDestructor);
        }
    }

    if (0 != add_op_to_oplist(e, cmd, argc, argv))
    {
        log_error("add_op_to_oplist() fail, key=%s, cmd=%d", k, cmd);
        goto blt_add_op_fail;
    }

    blt->op_cnt_act++;
    log_debug("blt add op succ, key=%s, cmd=%d, op_cnt_act=%zd", k, cmd, blt->op_cnt_act);
    ret = 0;

blt_add_op_fail:
    if (c_flg == 0)
    {
        sdsfree(k);
    }
    return ret;
}

static int need_restore_from_dbe(const binlogEntry *e)
{
    if (e == 0)
    {
        return 1;
    }

    opAttr *op = 0;
    listNode *head = e->oplist ? listFirst(e->oplist) : 0;
    if (head)
    {
        op = (opAttr *)(head->value);
    }
    else
    {
        log_error("can't get head of oplist, status=%d", e->status);
        return 1;
    }

    if (op && is_entirety_cmd(op->cmd))
    {
        return 0;
    }

    return 1;
}

int get_blt_cnt(binlog_tab *blt)
{
    int cnt = 0;

    if (blt->active)
    {
        cnt += dictSize(blt->active);
    }
    if (blt->immutable)
    {
        cnt += dictSize(blt->immutable);
    }

    return cnt;
}

int blt_cp_confirmed(binlog_tab *blt, const binlog_str *key)
{
    if (blt && blt->immutable && key && key->ptr && key->len > 0)
    {
        sds k = sdsnewlen(key->ptr, key->len);
        binlogEntry *e = 0;
        e = query_bl_tab_ex(blt->immutable, k);
        if (e)
        {
            finish_ble(e);
        }
        sdsfree(k);
    }
    return 0;
}

/*
 * restore_flg:
 * -1 : need not restore when not exist
 *  0 : restore to dbe
 *  1 : restore to immutable blt
 *  2 : restore to active blt
 */
static void blt_redo_from_dict(redisDb *rdb, dict *tbl, binlog_tab *blt, void *db, int restore_flg)
{
    if (!rdb || !tbl || dictSize(tbl) <= 0)
    {
        return;
    }

    dictIterator *it = dictGetSafeIterator(tbl);
    if (it == 0)
    {
        /* system fail */
        log_error("dictGetSafeIterator() fail, dict=%p", (void*)tbl);
        return;
    }

    robj k;
    initObject(&k, REDIS_STRING, 0);

    size_t scan_cnt = 0;
    size_t do_cnt = 0;
    size_t done_cnt = 0;
    size_t giveup_cnt = 0;
    size_t notfound_cnt = 0;
    dictEntry *node = 0;
    while ((node = dictNext(it)) != 0)
    {
        scan_cnt++;

        void *key = node->key;
        binlogEntry *e = (binlogEntry *)node->val;

        if (!key || !e)
        {
            log_error("illegal data, skip the node, scan_cnt=%zd", scan_cnt);
            continue;
        }

        if (e->status == BLE_STATUS_DONE)
        {
            log_info("skip the node(done), scan_cnt=%zd, key=%s", scan_cnt, (char*)key);
            done_cnt++;
            continue;
        }
        if (e->giveup)
        {
            log_info("skip the node giveup, scan_cnt=%zd, key=%s", scan_cnt, (char*)key);
            giveup_cnt++;
            continue;
        }

        k.ptr = key;

        robj *value = lookupKey(rdb, &k);
        if (!value)
        {
            notfound_cnt++;
            if (restore_flg >= 0)
            {
                restore(rdb, blt, db, &k, restore_flg);
            }
            else
            {
                log_info("scan_cnt=%zd, key=%s not exist and not restore", scan_cnt, (char*)key);
            }
        }
        else
        {
            restore_from_blt(rdb, e, &k);
            do_cnt++;
        }
    }

    /* scan over */
    dictReleaseIterator(it);

    log_prompt("finish scan blt dict, dict_size=%zd, scan_cnt=%zd, do_cnt=%zd, "
               "done_cnt=%zd, giveup_cnt=%zd, notfound_cnt=%zd",
               dictSize(tbl), scan_cnt, do_cnt, done_cnt, giveup_cnt, notfound_cnt);

    return;
}

void redo_blt(redisDb *rdb, binlog_tab *blt, void *db)
{
    if (!rdb || !blt || !db)
    {
        return;
    }

    if (blt->immutable)
    {
        blt_redo_from_dict(rdb, blt->immutable, blt, db, RESTORE_LVL_IMM);
        get_mirror(rdb, blt->immutable);
    }
    blt_redo_from_dict(rdb, blt->active, blt, db, RESTORE_LVL_ACT);
}

#ifdef _UPD_DBE_BY_PERIODIC_
static void save_key_mirror(redisDb *rdb, dict *immutable, const robj *key)
{
    binlogEntry *e = 0;
    e = query_bl_tab_ex(immutable, key->ptr);
    if (e)
    {
        if (e->cache_len == -1)
        {
            /* key in immutable & never serialize */
            log_debug("save mirror in immutable for key=%s", (const char*)key->ptr);
            get_value_from_rdb(rdb, key->ptr, &e->cache_byte, &e->cache_len);
        }
    }
}
#endif

void save_key_mirror_evict(const robj *key)
{
#ifdef _UPD_DBE_BY_PERIODIC_
    if (server.has_dbe == 0)
    {
        return;
    }

    binlogEntry *e = 0;
    dbmng_ctx *ctx = 0;
    binlog_tab *blt = 0;

    ctx = get_entry_ctx(0);
    blt = (binlog_tab *)ctx->binlogtab;

    if (blt == 0)
    {
        return;
    }

    if (blt->immutable)
    {
        e = query_bl_tab_ex(blt->immutable, key->ptr);
    }
    if (e)
    {
        if (e->cache_len == -1)
        {
            /* key in immutable & never serialize */
            get_value_from_rdb(ctx->rdb, key->ptr, &e->cache_byte, &e->cache_len);
            log_info("save mirror in immutable when evict for key=%s, cache_len=%d"
                    , (const char*)key->ptr, e->cache_len);
            return;
        }
    }
    if (blt->active
        && (e = query_bl_tab_ex(blt->active, key->ptr))
        && e->cache_len == -1)
    {
        get_value_from_rdb(ctx->rdb, key->ptr, &e->cache_byte, &e->cache_len);
        log_info("save mirror in active when evict for key=%s, cache_len=%d"
                , (const char*)key->ptr, e->cache_len);
    }
#else
    (void)key;
#endif
}

void save_keys_mirror(redisClient *c)
{
#ifdef _UPD_DBE_BY_PERIODIC_
    if (server.has_dbe == 0)
    {
        return;
    }

    dbmng_ctx *ctx = 0;
    binlog_tab *blt = 0;
    ctx = get_entry_ctx(0);
    blt = (binlog_tab *)ctx->binlogtab;
    if (blt == 0 || blt->immutable == 0)
    {
        return;
    }

    struct redisCommand *cmd = c->cmd;
    int j, last;
    if (cmd->firstkey == 0)
    {
        return;
    }
    last = cmd->lastkey;
    if (last < 0)
    {
        last = c->argc + last;
    }

    for (j = cmd->firstkey; j <= last; j += cmd->keystep)
    {
        save_key_mirror(ctx->rdb, blt->immutable, c->argv[j]);
    }
#else
    (void)c;
#endif
}

extern unsigned int dictSdsHash(const void *key);
extern int dictSdsKeyCompare(void *privdata, const void *key1, const void *key2);
/* Db->dict, keys are sds strings, vals are Redis objects. */
dictType dbHashType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    NULL,                       /* key destructor */
    NULL                        /* val destructor */
};

static void make_zset_dbe_data(sds key, listNode *node, upd_dbe_param *param)
{
    size_t i = 0;
    const int zset_add_cmd = get_cmd(OP_ZADD);
    const int zset_rem_cmd = get_cmd(OP_ZREM);

    dict *dict_add = dictCreate(&dbHashType, NULL);
    dict *dict_rem = dictCreate(&dbHashType, NULL);
    opAttr *op = node ? (opAttr*)(node->value) : NULL;
    while (node && op)
    {
        if ((int)op->cmd == zset_add_cmd)
        {
            log_debug("No.%zd item of oplist: zset_add, argc=%d, key=%s"
                    , i, op->argc, (const char*)key);
            if (op->argc % 2)
            {
                log_error("op->argc=%d invalid", op->argc);
                return;
            }
            int k = 0;
            while (k < op->argc)
            {
                //log_prompt("mmeber(op->argv[%d]): %s", k + 1, op->argv[k + 1]->ptr);
                // format: score1 member1 score2 member2 ...
                dictAdd(dict_add, op->argv[k + 1]->ptr, op->argv[k]);
                dictDelete(dict_rem, op->argv[k + 1]->ptr);
                k += 2;
            }
        }
        else if ((int)op->cmd == zset_rem_cmd)
        {
            log_debug("No.%zd item of oplist: zset_rem, argc=%d, key=%s"
                    , i, op->argc, (const char*)key);
            int k = 0;
            for (k= 0; k < op->argc; k++)
            {
                // format: member1 member2 ...
                dictAdd(dict_rem, op->argv[k]->ptr, NULL);
                dictDelete(dict_add, op->argv[k]->ptr);
            }
        }
        else
        {
            log_error("op->cmd=%d (%s) impossible for zset, key=%s"
                    , op->cmd, get_cmdstr(op->cmd), (const char*)key);
        }
        i++;

        /* next */
        node = listNextNode(node);
        op = node ? (opAttr*)(node->value) : NULL;
    }

    dictIterator *it = 0;
    dictEntry *de = 0;

    param->cnt[0] = dictSize(dict_add);
    param->cnt[1] = dictSize(dict_rem);
    if (param->cnt[0] > 0)
    {
        param->kv[0] = (kvec_t *)zcalloc(sizeof(kvec_t) * param->cnt[0]);
    }
    if (param->cnt[1] > 0)
    {
        param->kv[1] = (kvec_t *)zcalloc(sizeof(kvec_t) * param->cnt[1]);
    }

    it = dictGetIterator(dict_add);
    i = 0;
    while ((de = dictNext(it)) != NULL && i < param->cnt[0])
    {
        sds member = dictGetEntryKey(de);
        robj *score_obj = dictGetEntryVal(de);
        double score;
        getDoubleFromObject(score_obj, &score);

        param->kv[0][i].k = encode_zset_key((const char*)key, sdslen(key), (const char*)member, sdslen(member), &param->kv[0][i].ks);
        param->kv[0][i].v = encode_zset_val((const char*)member, sdslen(member), score, &param->kv[0][i].vs);

        i++;
    }
    dictReleaseIterator(it);
    it = 0;

    it = dictGetIterator(dict_rem);
    i = 0;
    while ((de = dictNext(it)) != NULL && i < param->cnt[1])
    {
        sds member = dictGetEntryKey(de);

        param->kv[1][i].k = encode_zset_key((const char*)key, sdslen(key), (const char*)member, sdslen(member), &param->kv[1][i].ks);

        i++;
    }
    dictReleaseIterator(it);
    it = 0;

    dictRelease(dict_add);
    dictRelease(dict_rem);
}

void free_upd_dbe_param(upd_dbe_param *param)
{
    int j;

    zfree(param->pdel_key);
    param->pdel_key = NULL;
    param->pdel_key_len = 0;

    for (j = 0; (size_t)j < param->cnt[0]; j++)
    {
        zfree(param->kv[0][j].k);
        zfree(param->kv[0][j].v);
    }
    zfree(param->kv[0]);
    param->kv[0] = NULL;
    param->cnt[0] = 0;

    for (j = 0; (size_t)j < param->cnt[1]; j++)
    {
        zfree(param->kv[1][j].k);
        zfree(param->kv[1][j].v);
    }
    zfree(param->kv[1]);
    param->kv[1] = NULL;
    param->cnt[1] = 0;

    zfree(param->one.k);
    param->one.k = NULL;
    zfree(param->one.v);
    param->one.v = NULL;
}

