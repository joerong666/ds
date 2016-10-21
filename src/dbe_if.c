#include "redis.h"

#include <stdlib.h>
#include <string.h>

#include "dbe_if.h"
#include "log.h"
#include "ds_ctrl.h"

#ifdef _DBE_LEVEL_DB_
#include "leveldb/dbe_cif.h"
#else
#include "hidb/db/include/db.h"
#include "hidb2/src/db/db.h"
#endif

#define CHECK_DB_CTX(Ctx) \
{\
    if (Ctx == 0) return DBE_ERR_OTHER_FAIL;\
}


int dbe_init(const char *dbname, void **pdb, int read_only, int auto_purge, int dbe_fsize)
{
    log_prompt("dbe_init: dbname=%s, read_only=%d, auto_purge=%d, dbe_fsize=%d"
            , dbname, read_only, auto_purge, dbe_fsize);

#ifdef _DBE_LEVEL_DB_
    (void)read_only;
    (void)auto_purge;
    (void)dbe_fsize;
    server.dbe_ver = DBE_VER_HIDB;
    return leveldbInit(dbname, pdb);
#else
    const int dbe_ver = dbe_version((char*)dbname);
    if (dbe_ver == DBVER_V1)
    {
        log_prompt("%s hidb!", dbname);
        server.dbe_ver = DBE_VER_HIDB;
    }
    else if (dbe_ver == DBVER_V2)
    {
        log_prompt("%s hidb2!", dbname);
        server.dbe_ver = DBE_VER_HIDB2;
    }
    else if (dbe_ver == DBVER_EMPTY)
    {
        /* use hidb2 for creating new dbe */
        log_prompt("%s null, set to hidb%s"
                , dbname, server.dbe_ver == DBE_VER_HIDB ? "" : "2");
    }
    else
    {
        log_error("unknown format of dbe & set to hidb%s, ret=%d, dbname=%s"
                , server.dbe_ver == DBE_VER_HIDB ? "" : "2"
                , dbe_ver, dbname);
    }

    db_attr_t attr;
    memset(&attr, 0, sizeof(attr));
    DBATTR_INIT(&attr);
    attr.wait_compact = false;
    attr.build_manifest = false;
    attr.readonly = read_only ? true : false; // dbe merge or not
    attr.auto_purge = auto_purge ? true : false;
    attr.split_size = dbe_fsize;
    db_t *db = NULL;
    if (server.dbe_ver == DBE_VER_HIDB)
    {
        db = db_open((char *)dbname, &attr);
    }
    else
    {
        db = db_open_v2((char *)dbname, &attr);
    }
    if (db)
    {
        *pdb = db;
        return DBE_ERR_SUCC;
    }
    else
    {
        log_error("db_open%s fail, dbname=%s"
                , server.dbe_ver == DBE_VER_HIDB ? "" : "_v2", dbname);
        return DBE_ERR_OTHER_FAIL;
    }
#endif
}

void dbe_uninit(void *db_ctx)
{
    log_prompt("dbe_uninit()...db_ctx=%p", db_ctx);
    if (db_ctx == 0)
    {
        return;
    }

#ifdef _DBE_LEVEL_DB_
    leveldbUninit(db_ctx);
#else
    if (server.dbe_ver == DBE_VER_HIDB)
    {
        db_close((db_t *)db_ctx);
    }
    else
    {
        db_close_v2((db_t *)db_ctx);
    }
#endif
}

int dbe_startup(void *db_ctx)
{
    log_prompt("dbe_startup()...db_ctx=%p", db_ctx);
    if (db_ctx == 0)
    {
        return DBE_ERR_OTHER_FAIL;
    }

#ifdef _DBE_LEVEL_DB_
    (void)db_ctx;
    return DBE_ERR_SUCC;
#else
    int ret;
    if (server.dbe_ver == DBE_VER_HIDB)
    {
        ret = db_run((db_t *)db_ctx);
    }
    else
    {
        ret = db_run_v2((db_t *)db_ctx);
    }

    if (ret == 0)
    {
        return DBE_ERR_SUCC;
    }
    else
    {
        log_error("db_run%s fail, ret=%d"
                , server.dbe_ver == DBE_VER_HIDB ? "" : "_v2", ret);
        return DBE_ERR_OTHER_FAIL;
    }
#endif
}

int dbe_get(void *db_ctx, const char *key, size_t key_len, char **val, int *val_len, dbeMallocFunc f, int64_t *dur)
{
    server.stat_dbeio_get++;
    if (db_ctx == 0)
    {
        return DBE_ERR_ILLEGAL_ARG;
    }
    struct timespec ts;
    if (dur)
    {
        ts = pf_get_time_tick();
    }

#ifdef _DBE_LEVEL_DB_
    (void)key_len;
    *val = leveldbGet(db_ctx, key, val_len, f);
    const int32_t ret = *val != NULL ? 0 : 1;
#else
    int32_t ret;
    if (server.dbe_ver == DBE_VER_HIDB)
    {
        lockDbeGet();
        ret = db_get((db_t *)db_ctx
                           , (char *)key
                           , (uint16_t)key_len + 1
                           , val
                           , (uint32_t *)val_len
                           , f
                          );
        unlockDbeGet();
    }
    else
    {
        ret = db_get_v2((db_t *)db_ctx
                           , (char *)key
                           , (uint16_t)key_len
                           , val
                           , (uint32_t *)val_len
                           , f
                          );
    }
#endif

    if (dur)
    {
        *dur = pf_get_time_diff_nsec(ts, pf_get_time_tick()) / 1000;
    }

    if (ret == 0)
    {
        /* exist the key */
#ifdef _DBE_IF_DEBUG_
        log_prompt("db_get%s succ: key=%s, val_len=%d, db_ctx=%p"
                , server.dbe_ver == DBE_VER_HIDB ? "" : "_v2", key, *val_len, db_ctx);
#endif
        return DBE_ERR_SUCC;
    }
    else if (ret == 1)
    {
        /* not exist the key */
#ifdef _DBE_IF_DEBUG_
        log_prompt("db_get%s: key=%s not exist, db_ctx=%p"
                , server.dbe_ver == DBE_VER_HIDB ? "" : "_v2", key, db_ctx);
#endif
        *val = 0;
        *val_len = 0;
        return DBE_ERR_NOT_FOUND;
    }
    else
    {
        log_error("db_get%s fail, ret=%d, key=%s, db_ctx=%p"
                , server.dbe_ver == DBE_VER_HIDB ? "" : "_v2", ret, key, db_ctx);
        return DBE_ERR_OTHER_FAIL;
    }
}

int dbe_put(void *db_ctx, char *key, int key_len, char *val, int val_len)
{
#if 0
    // just for test
    (void)db_ctx;
    (void)key_len;
    (void)val_len;
    zfree(key);
    zfree(val);
    return DBE_ERR_SUCC;
#else
    if (key == NULL || val == NULL || key_len <=0 || val_len <= 0)
    {
        log_error("dbe_put() illegal parameter: key=%p, key_len=%d, val=%p, val_len=%d"
                , key, key_len, val, val_len);
        zfree(key);
        zfree(val);
        return DBE_ERR_OTHER_FAIL;
    }

#ifdef _DBE_IF_DEBUG_
    log_prompt("dbe_put(): db_ctx=%p, key=%s, val=%p, val_len=%d"
            , db_ctx, key, val, val_len);
#endif

    server.stat_dbeio_put++;
    CHECK_DB_CTX(db_ctx);
#ifdef _DBE_LEVEL_DB_
    int ret = leveldbPut(db_ctx, key, val, val_len);
    if (ret != LEVELDB_ERR_SUCC)
    {
        log_error("leveldbPut() fail, ret=%d, key=%s, val_len=%d", ret, key, val_len);
        ret = DBE_ERR_OTHER_FAIL;
    }
    else
    {
        ret = DBE_ERR_SUCC;
    }
    zfree(key);
    zfree(val);
    return ret;
#else
    int ret;
    if (server.dbe_ver == DBE_VER_HIDB)
    {
        ret = db_put((db_t *)db_ctx
                         , (char *)key
                         , (uint16_t)key_len + 1
                         , (char *)val
                         , (uint32_t)val_len
                          );
    }
    else
    {
        ret = db_put_v2((db_t *)db_ctx
                         , (char *)key
                         , (uint16_t)key_len
                         , (char *)val
                         , (uint32_t)val_len
                          );
    }
    if (ret == 0)
    {
        return DBE_ERR_SUCC;
    }
    else
    {
        log_error("db_put%s fail, ret=%d, val_len=%d"
                , server.dbe_ver == DBE_VER_HIDB ? "" : "_v2", ret, val_len);
        return DBE_ERR_OTHER_FAIL;
    }
#endif
#endif
}

int dbe_mput(void *db_ctx, kvec_t *data, size_t cnt)
{
    if (data == NULL || cnt == 0)
    {
        log_error("dbe_mput() illegal parameter: data=%p,cnt=%zd", data, cnt);
        return DBE_ERR_OTHER_FAIL;
    }

    size_t i;
    i = 0;
#ifdef _DBE_IF_DEBUG_
    log_prompt("dbe_mput(): db_ctx=%p, kvec_cnt=%zd", db_ctx, cnt);
    for (i = 0; i < cnt; i++)
    {
        log_prompt("No.%d: key=%s, val_len=%zd", i, data[i].k, data[i].vs);
        //log_buffer2(data[i].v, data[i].vs, 0);
    }
#endif

    server.stat_dbeio_put++;
    CHECK_DB_CTX(db_ctx);
#ifdef _DBE_LEVEL_DB_
    for (i = 0; i < cnt; i++)
    {
        const int ret = leveldbPut(db_ctx, data[i].k, data[i].v, data[i].vs);
        if (ret != LEVELDB_ERR_SUCC)
        {
            log_error("No.%d: leveldbPut() fail, ret=%d, key=%s", i, ret, data[i].k);
        }
        zfree(data[i].k);
        zfree(data[i].v);
    }
    zfree(data);
    return DBE_ERR_SUCC;
#else
    if (server.dbe_ver == DBE_VER_HIDB)
    {
        log_error("hidb don't support dbe_mput");
        return DBE_ERR_OTHER_FAIL;
    }
    const int32_t ret = db_mput_v2((db_t *)db_ctx
                                , (kvec_t *)data
                                , cnt
                               );
    if (ret == 0)
    {
        return DBE_ERR_SUCC;
    }
    else
    {
        log_error("db_mput_v2() fail, ret=%d, key_cnt=%zu", ret, cnt);
        return DBE_ERR_OTHER_FAIL;
    }
#endif
}

int dbe_mdelete(void *db_ctx, kvec_t *data, size_t cnt)
{
    if (data == NULL && cnt == 0)
    {
        log_error("dbe_mdelete() illegal parameter: data=%p, cnt=%zd", data, cnt);
        return DBE_ERR_OTHER_FAIL;
    }

    size_t i;
    i = 0;
#ifdef _DBE_IF_DEBUG_
    log_prompt("dbe_mdelete(): db_ctx=%p, kvec_cnt=%zd", db_ctx, cnt);
    for (i = 0; i < cnt; i++)
    {
        log_prompt("No.%d: key=%s", i, data[i].k);
    }
#endif

    server.stat_dbeio_del++;
    CHECK_DB_CTX(db_ctx);
#ifdef _DBE_LEVEL_DB_
    for (i = 0; i < cnt; i++)
    {
        const int ret = leveldbDelete(db_ctx, data[i].k);
        if (ret != LEVELDB_ERR_SUCC)
        {
            log_error("No.%d: leveldbDelete() fail, ret=%d, key=%s", i, ret, data[i].k);
        }
        zfree(data[i].k);
        zfree(data[i].v);
    }
    zfree(data);
    return DBE_ERR_SUCC;
#else
    if (server.dbe_ver == DBE_VER_HIDB)
    {
        log_error("hidb don't support dbe_mdelete");
        return DBE_ERR_OTHER_FAIL;
    }
    const int32_t ret = db_mdel_v2((db_t *)db_ctx , (kvec_t *)data , cnt);
    if (ret == 0)
    {
        return DBE_ERR_SUCC;
    }
    else
    {
        log_error("db_mdel_v2() fail, ret=%d, key_cnt=%zu", ret, cnt);
        return DBE_ERR_OTHER_FAIL;
    }
#endif
}

void dbe_free_ptr(void *ptr, dbeFreeFunc f)
{
#ifdef _DBE_LEVEL_DB_
    leveldbFreePtr(ptr, f);
#else
    f(ptr);
#endif
}

int dbe_set_filter(void *db_ctx, dbeFilterFunc f)
{
    CHECK_DB_CTX(db_ctx);
#ifdef _DBE_LEVEL_DB_
    (void)(f);
    return DBE_ERR_SUCC;
#else
    int32_t ret;
    if (server.dbe_ver == DBE_VER_HIDB)
    {
        ret = db_set_rulefilter((db_t *)db_ctx , (DB_KFILTER)f);
    }
    else
    {
        ret = db_set_rulefilter_v2((db_t *)db_ctx , (DB_KFILTER)f);
    }
    if (ret == 0)
    {
        return DBE_ERR_SUCC;
    }
    else
    {
        log_error("db_set_rulefilter%s fail, ret=%d"
                , server.dbe_ver == DBE_VER_HIDB ? "" : "_v2", ret);
        return DBE_ERR_OTHER_FAIL;
    }
#endif
}

int dbe_set_val_filter(void *db_ctx, dbeValFilterFunc f)
{
    CHECK_DB_CTX(db_ctx);
#ifdef _DBE_LEVEL_DB_
    (void)(f);
    return DBE_ERR_SUCC;
#else
    int32_t ret;
    if (server.dbe_ver == DBE_VER_HIDB)
    {
        ret = db_set_valfilter((db_t *)db_ctx , (DB_VFILTER)f);
    }
    else
    {
        ret = db_set_valfilter_v2((db_t *)db_ctx , (DB_VFILTER)f);
    }
    if (ret == 0)
    {
        return DBE_ERR_SUCC;
    }
    else
    {
        log_error("db_set_valfilter%s fail, ret=%d"
                , server.dbe_ver == DBE_VER_HIDB ? "" : "_v2", ret);
        return DBE_ERR_OTHER_FAIL;
    }
#endif
}

int dbe_clean(void *db_ctx)
{
    log_prompt("dbe_clean: db_ctx=%p", db_ctx);

    CHECK_DB_CTX(db_ctx);
#ifdef _DBE_LEVEL_DB_
    return DBE_ERR_SUCC;
#else
    int ret;
    if (server.dbe_ver == DBE_VER_HIDB)
    {
        ret = db_clear((db_t *)db_ctx);
    }
    else
    {
        ret = db_clear_v2((db_t *)db_ctx);
    }
    if (ret == 0)
    {
        return DBE_ERR_SUCC;
    }
    else
    {
        log_error("db_clean%s fail, ret=%d"
                , server.dbe_ver == DBE_VER_HIDB ? "" : "_v2", ret);
        return DBE_ERR_OTHER_FAIL;
    }
#endif
}

int dbe_clean_file(void *db_ctx, const void *tm)
{
    log_prompt("dbe_clean_file: db_ctx=%p", db_ctx);

    CHECK_DB_CTX(db_ctx);
#ifdef _DBE_LEVEL_DB_
    (void)(tm);
    return DBE_ERR_SUCC;
#else
    int32_t ret;
    if (server.dbe_ver == DBE_VER_HIDB)
    {
        ret = db_clean_file((db_t *)db_ctx , (struct tm *)tm);
    }
    else
    {
        ret = db_clean_file_v2((db_t *)db_ctx , (struct tm *)tm);
    }
    if (ret == 0)
    {
        return DBE_ERR_SUCC;
    }
    else
    {
        log_error("db_clean_file%s fail, ret=%d"
                , server.dbe_ver == DBE_VER_HIDB ? "" : "_v2", ret);
        return DBE_ERR_OTHER_FAIL;
    }
#endif
}

int dbe_freeze(void *db_ctx)
{
    log_prompt("dbe_freeze: db_ctx=%p", db_ctx);

    CHECK_DB_CTX(db_ctx);
#ifdef _DBE_LEVEL_DB_
    return DBE_ERR_SUCC;
#else
    int32_t ret;
    if (server.dbe_ver == DBE_VER_HIDB)
    {
        ret = db_freeze((db_t *)db_ctx);
    }
    else
    {
        ret = db_freeze_v2((db_t *)db_ctx);
    }
    if (ret == 0)
    {
        return DBE_ERR_SUCC;
    }
    else
    {
        log_error("db_freeze%s fail, ret=%d"
                , server.dbe_ver == DBE_VER_HIDB ? "" : "_v2", ret);
        return DBE_ERR_OTHER_FAIL;
    }
#endif
}

int dbe_unfreeze(void *db_ctx)
{
    log_prompt("dbe_unfreeze: db_ctx=%p", db_ctx);

    CHECK_DB_CTX(db_ctx);
#ifdef _DBE_LEVEL_DB_
    return DBE_ERR_SUCC;
#else
    int32_t ret;
    if (server.dbe_ver == DBE_VER_HIDB)
    {
        ret = db_unfreeze((db_t *)db_ctx);
    }
    else
    {
        ret = db_unfreeze_v2((db_t *)db_ctx);
    }
    if (ret == 0)
    {
        return DBE_ERR_SUCC;
    }
    else
    {
        log_error("db_unfreeze%s fail, ret=%d"
                , server.dbe_ver == DBE_VER_HIDB ? "" : "_v2", ret);
        return DBE_ERR_OTHER_FAIL;
    }
#endif
}

int dbe_merge(void *db_ctx, const char *path)
{
    log_prompt("dbe_merge: db_ctx=%p, path=%s", db_ctx, path);

    CHECK_DB_CTX(db_ctx);
#ifdef _DBE_LEVEL_DB_
    (void)db_ctx;
    (void)path;
    return DBE_ERR_SUCC;
#else
    int32_t ret;
    if (server.dbe_ver == DBE_VER_HIDB)
    {
        ret = db_load_dir((db_t *)db_ctx , (char *)path);
    }
    else
    {
        ret = db_load_dir_v2((db_t *)db_ctx , (char *)path);
    }
    if (ret == 0)
    {
        return DBE_ERR_SUCC;
    }
    else
    {
        log_error("db_load_dir%s fail, ret=%d, path=%s"
                , server.dbe_ver == DBE_VER_HIDB ? "" : "_v2", ret);
        return DBE_ERR_OTHER_FAIL;
    }
#endif
}

int dbe_purge(void *db_ctx)
{
    log_prompt("dbe_purge: db_ctx=%p", db_ctx);

    CHECK_DB_CTX(db_ctx);
#ifdef _DBE_LEVEL_DB_
    (void)db_ctx;
    return DBE_ERR_SUCC;
#else
    if (server.dbe_ver == DBE_VER_HIDB)
    {
        db_manual_purge((db_t *)db_ctx);
    }
    else
    {
        db_manual_purge_v2((db_t *)db_ctx);
    }
    return DBE_ERR_SUCC;
#endif
}

void *dbe_pget(void *db_ctx, const char *key_prefix, size_t len)
{
    if (db_ctx == NULL || key_prefix == NULL || len == 0)
    {
        log_error("dbe_pget() illegal parameter: db_ctx=%p, key_prefix=%p, len=%zd"
                , db_ctx, key_prefix, len);
        return NULL;
    }

    void *it = 0;

#ifdef _DBE_LEVEL_DB_
    server.stat_dbeio_it++;
    (void)key_prefix;
    (void)len;
    it = (void*)1;
#else
    if (server.dbe_ver == DBE_VER_HIDB)
    {
        log_error("hidb don't support dbe_pget");
        return NULL;
    }
    it = db_pget_v2((db_t *)db_ctx, (char*)key_prefix, len);
    if (it == NULL)
    {
        log_error("db_pget_v2: dbe return NULL, key_prefix=%s", key_prefix); 
    }
    else
    {
        server.stat_dbeio_it++;
    }
#endif

#ifdef _DBE_IF_DEBUG_
    log_prompt("dbe_pget_v2: db_ctx=%p, key_prefix=%s, it=%p", db_ctx, key_prefix, it);
#endif
    return it;
}

int dbe_pdelete(void *db_ctx, const char *key_prefix, size_t len)
{
#ifdef _DBE_IF_DEBUG_
    log_prompt("dbe_pdelete(): db_ctx=%p, key_prefix=%s, len=%zd"
            , db_ctx, key_prefix, len);
#endif

    if (db_ctx == 0)
    {
        log_error("dbe_pdelete() illegal parameter: db_ctx=%p", db_ctx);
        return DBE_ERR_OTHER_FAIL;
    }

#ifdef _DBE_LEVEL_DB_
    (void)key_prefix;
    (void)len;
    return DBE_ERR_SUCC;
#else
    int ret;
    if (server.dbe_ver == DBE_VER_HIDB)
    {
        ret = db_del((db_t *)db_ctx , (char *)key_prefix, len + 1);
    }
    else
    {
        ret = db_pdel_v2((db_t *)db_ctx, (char*)key_prefix, len);
    }
    if (ret != 0)
    {
        log_error("%s: dbe return %d, key=%s"
                , server.dbe_ver == DBE_VER_HIDB ? "db_del" : "db_pdel_v2"
                , ret, key_prefix); 
        return DBE_ERR_OTHER_FAIL;
    }
    else
    {
        return DBE_ERR_SUCC;
    }
#endif
}

void *dbe_create_it(void *db_ctx, int level)
{
    if (db_ctx == 0)
    {
        log_error("dbe_create_it() illegal parameter: db_ctx=%p, level=%d"
                , db_ctx, level);
        return NULL;
    }

    void *it = 0;

#ifdef _DBE_LEVEL_DB_
    server.stat_dbeio_it++;
    (void)db_ctx;
    (void)level;
    it = (void*)1;
#else
    if (server.dbe_ver == DBE_VER_HIDB)
    {
        it = db_create_it((db_t *)db_ctx, level);
    }
    else
    {
        it = db_create_it_v2((db_t *)db_ctx, level);
    }
    if (it == NULL)
    {
        log_error("db_create_it%s: dbe return NULL, db_ctx=%p, level=%d"
                , server.dbe_ver == DBE_VER_HIDB ? "" : "_v2", db_ctx, level); 
    }
    else
    {
        server.stat_dbeio_it++;
    }
#endif

#ifdef _DBE_IF_DEBUG_
    log_prompt("dbe_create_it%s: db_ctx=%p, level=%d, it=%p"
                , server.dbe_ver == DBE_VER_HIDB ? "" : "_v2", db_ctx, level, it);
#endif
    return it;
}

#ifdef _DBE_LEVEL_DB_
/* iterator */
static int sc_idx = 0;
#endif

int dbe_next_key(void *it, char **k, int *k_len, char **v, int *v_len, dbeMallocFunc f, int64_t *dur)
{
    if (k)
    {
        *k = 0;
    }
    if (k_len)
    {
        *k_len = 0;
    }
    if (v)
    {
        *v = 0;
    }
    if (v_len)
    {
        *v_len = 0;
    }

#ifdef _DBE_LEVEL_DB_
    (void)it;
    (void)f;
    if (dur)
    {
        *dur = 0;
    }
    //return DBE_ERR_OTHER_FAIL;

    /* z: m1 10, m2 20, m3 30 */
    static const char *key[3] = {
        "01zZe68bdb94952e717e"
      , "01zZ167a5c58962e7311"
      , "01zZ296491b0942e6feb"
    };
    static const char val[3][12] = {
        { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x6d, 0x31, 0x32, 0x30 }
      , { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x6d, 0x33, 0x31, 0x30 }
      , { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x6d, 0x32, 0x33, 0x30 }
    };
    const int key_cnt = (sizeof(key) / sizeof(key[0]));
    if (sc_idx >= key_cnt)
    {
#ifdef _DBE_IF_DEBUG_
        log_error("dbe_next_key() over, key_cnt=%d, idx=%d", key_cnt, sc_idx);
#endif
        sc_idx = sc_idx % key_cnt;
        return DBE_ERR_OTHER_FAIL;
    }
    else
    {
        *k_len = strlen(key[sc_idx]);
        *v_len = 12;
        *k = f(*k_len + 1);
        memcpy(*k, key[sc_idx], *k_len);
        (*k)[*k_len] = 0;
        *v = f(*v_len);
        memcpy(*v, val[sc_idx], *v_len);

        sc_idx += 1;
        return DBE_ERR_SUCC;
    }
#else
    struct timespec ts;
    if (dur)
    {
        ts = pf_get_time_tick();
    }
    int ret;
    if (server.dbe_ver == DBE_VER_HIDB)
    {
        ret = db_iter((iter_t *)it, k, k_len, v, v_len, f);
    }
    else
    {
        ret = db_iter_v2((iter_t *)it, k, k_len, v, v_len, f);
    }
    if (dur)
    {
        *dur = pf_get_time_diff_nsec(ts, pf_get_time_tick()) / 1000;
    }

    if (ret == 0)
    {
#ifdef _DBE_IF_DEBUG_
        log_prompt("db_iter%s succ: key=%s, val_len=%d"
                , server.dbe_ver == DBE_VER_HIDB ? "" : "_v2", *k, *v_len);
#endif
        return DBE_ERR_SUCC;
    }
    else
    {
#ifdef _DBE_IF_DEBUG_
        log_error("db_iter() ret=%d, it=%p", ret, it);
#endif
        return DBE_ERR_OTHER_FAIL;
    }
#endif
}

void dbe_destroy_it(void *it)
{
    server.stat_dbeio_it--;
#ifdef _DBE_LEVEL_DB_
    (void)it;
    sc_idx = 0;
    return;
#else
    if (server.dbe_ver == DBE_VER_HIDB)
    {
        db_destroy_it((iter_t *)it);
    }
    else
    {
        db_destroy_it_v2((iter_t *)it);
    }
#endif
}

void dbe_print_info(void *db_ctx)
{
#ifdef _DBE_LEVEL_DB_
    (void)db_ctx;
    return;
#else
    if (db_ctx)
    {
        if (server.dbe_ver == DBE_VER_HIDB)
        {
            db_print_state((db_t *)db_ctx);
        }
        else
        {
            db_print_state_v2((db_t *)db_ctx);
        }
    }
#endif
}

extern int dbe_tran_begin(void *db_ctx)
{
    int ret = 0;
#ifndef _DBE_LEVEL_DB_
    if (server.dbe_ver == DBE_VER_HIDB)
    {
        return 0;
    }
    if (db_ctx)
    {
        ret = db_gtx_begin_v2((db_t *)db_ctx);
    }
#else
    (void)db_ctx;
#endif
    return ret;
}

extern int dbe_tran_commit(void *db_ctx)
{
    int ret = 0;
#ifndef _DBE_LEVEL_DB_
    if (server.dbe_ver == DBE_VER_HIDB)
    {
        return 0;
    }
    if (db_ctx)
    {
        ret = db_gtx_comit_v2((db_t *)db_ctx);
    }
#else
    (void)db_ctx;
#endif
    return ret;
}

