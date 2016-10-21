#include "restore_key.h"
#include "codec_key.h"
#include "ds_log.h"
#include "ds_ctrl.h"
#include "dbe_if.h"
#include "serialize.h"
#include "t_zset.h"

static void stat_dbe_get_op();
static int make_string(const char *val, int v_len, val_attr *rslt, dbe_stat *tmp);
static int make_list(robj *subject, char *key, int k_len, char *val, int v_len);
static int make_set(robj *set, char *key, int k_len, char *val, int v_len, robj *vobj);
static int make_zset(robj *zobj, char *key, int k_len, char *val, int v_len);
static int make_hash(robj *set, char *key, int k_len, char *val, int v_len);
static int restore_key_fuzzy(void *db, const char *k, size_t k_len, val_attr *rslt);
static int restore_string_key(void *db, const char *k, size_t k_len, val_attr *rslt);
static int restore_list_key(void *db, const char *k, size_t k_len, val_attr *rslt);
static int restore_set_key(void *db, const char *k, size_t k_len, val_attr *rslt);
static int restore_zset_key(void *db, const char *k, size_t k_len, val_attr *rslt);
static int restore_hash_key(void *db, const char *k, size_t k_len, val_attr *rslt);

/*
 * key_type: 'k" mean key_prefix
 * ret:
 * 0 - succ
 * 1 - not found
 * 2 - dbe fail
 * 3 - process fail
 */
int restore_key_from_dbe(void *db, const char *key, size_t key_len, char key_type, val_attr *rslt)
{
#if 0
    // just for test main framework of get operation
    (void)db;
    (void)key;
    (void)key_len;
    (void)key_type;
    rslt->val = createStringObject("a", 1);
    rslt->expire_ms = 0;
    return 0;
#else
    log_test("restore_key_from_dbe: key=%s, key_type=%c", key, key_type);

    memset(rslt, 0, sizeof(*rslt));

    int ret = 3;
    if (key_type == 'K')
    {
        ret = restore_string_key(db, key, key_len, rslt);
    }
    else if (key_type == 'L')
    {
        ret = restore_list_key(db, key, key_len, rslt);
    }
    else if (key_type == 'S')
    {
        ret = restore_set_key(db, key, key_len, rslt);
    }
    else if (key_type == 'Z')
    {
        ret = restore_zset_key(db, key, key_len, rslt);
    }
    else if (key_type == 'H')
    {
        ret = restore_hash_key(db, key, key_len, rslt);
    }
    else if (key_type == 'k')
    {
        ret = restore_key_fuzzy(db, key, key_len, rslt);
    }
    else
    {
        log_error("unknown key_type=\'%c\'", key_type);
    }
    return ret;
#endif
}

static int restore_key_fuzzy(void *db, const char *k, size_t k_len, val_attr *rslt)
{
    dbe_stat *tmp = &gDbeGetTop[0];
    int ret = 3;
    char *key = 0;
    size_t key_len;

    key = encode_prefix_key(k, k_len, 0, &key_len);
    if (key == 0)
    {
        return 3;
    }

    void *it = dbe_pget(db, key, key_len);
    if (it == 0)
    {
        log_error("dbe_pget fail, dbe_key_prefix=%s", key);
        zfree(key);
        return 3;
    }
    zfree(key);
    key = 0;

    /* get one and parse type from it */
    char *val = 0;
    size_t val_len;
    int key_len_;
    int val_len_;
    int tmp_ret;
    tmp_ret = dbe_next_key(it, &key, &key_len_, &val, &val_len_, zmalloc, &tmp->io_dur);
    if (tmp_ret != 0)
    {
        log_info("restore_key_fuzzy: dbe_next_key fail, ret=%d, key=%s"
                , tmp_ret, k);
        dbe_destroy_it(it);
        return 1;
    }
    key_len = key_len_;
    val_len = val_len_;

    dbe_key_attr attr;
    tmp_ret = decode_dbe_key(key, key_len, &attr);
    if (tmp_ret != 0)
    {
        log_error("restore_key_fuzzy: decode_key fail, dbe_key=%s", key);
#ifdef _DBE_LEVEL_DB_
        zfree(key);
        zfree(val);
#else
        dbe_free_ptr(key, zfree);
        dbe_free_ptr(val, zfree);
#endif
        dbe_destroy_it(it);
        return 3;
    }

    if (attr.type == KEY_TYPE_STRING)
    {
        dbe_destroy_it(it);

        ret = make_string(val, val_len, rslt, tmp) == 0 ? 0 : 3;

#ifdef _DBE_LEVEL_DB_
        zfree(key);
        zfree(val);
#else
        dbe_free_ptr(key, zfree);
        dbe_free_ptr(val, zfree);
#endif
    }
    else if (attr.type == KEY_TYPE_LIST)
    {
        robj *subject = createListObject();
        ret = make_list(subject, key, key_len_, val, val_len_);
        if (ret == 0)
        {
            while (dbe_next_key(it, &key, &key_len_, &val, &val_len_, zmalloc, &tmp->io_dur) == 0)
            {
                ret = make_list(subject, key, key_len_, val, val_len_);
                if (ret == 1)
                {
                    break;
                }
            }
        }
        dbe_destroy_it(it);
        if (ret == 0)
        {
            rslt->val = subject;
        }
        else
        {
            decrRefCount(subject);
            log_error("make_list fail, key=%s", k);
            ret = 3;
        }
    }
    else if (attr.type == KEY_TYPE_ZSET)
    {
        robj *zobj = createZsetZiplistObject();
        ret = make_zset(zobj, key, key_len_, val, val_len_);
        if (ret == 0)
        {
            while (dbe_next_key(it, &key, &key_len_, &val, &val_len_, zmalloc, &tmp->io_dur) == 0)
            {
                ret = make_zset(zobj, key, key_len_, val, val_len_);
                if (ret == 1)
                {
                    break;
                }
            }
        }
        dbe_destroy_it(it);
        if (ret == 0)
        {
            rslt->val = zobj;
        }
        else
        {
            decrRefCount(zobj);
            log_error("make_zset fail, key=%s", k);
            ret = 3;
        }
    }
    else if (attr.type == KEY_TYPE_SET)
    {
        robj *member = createStringObject(val + 1, val_len - 1);
        robj *set = setTypeCreate(member);
        ret = make_set(set, key, key_len_, val, val_len_, member);
        if (ret == 0)
        {
            while (dbe_next_key(it, &key, &key_len_, &val, &val_len_, zmalloc, &tmp->io_dur) == 0)
            {
                ret = make_set(set, key, key_len_, val, val_len_, NULL);
                if (ret == 1)
                {
                    break;
                }
            }
        }
        dbe_destroy_it(it);
        if (ret == 0)
        {
            rslt->val = set;
        }
        else
        {
            decrRefCount(set);
            log_error("make_set fail, key=%s", k);
            ret = 3;
        }
    }
    else if (attr.type == KEY_TYPE_HASH)
    {
        robj *hash = createHashObject();
        ret = make_hash(hash, key, key_len_, val, val_len_);
        if (ret == 0)
        {
            while (dbe_next_key(it, &key, &key_len_, &val, &val_len_, zmalloc, &tmp->io_dur) == 0)
            {
                ret = make_hash(hash, key, key_len_, val, val_len_);
                if (ret == 1)
                {
                    break;
                }
            }
        }
        dbe_destroy_it(it);
        if (ret == 0)
        {
            rslt->val = hash;
        }
        else
        {
            decrRefCount(hash);
            log_error("make_hash fail, key=%s", k);
            ret = 3;
        }
    }
    else
    {
        dbe_destroy_it(it);

#ifdef _DBE_LEVEL_DB_
        zfree(key);
        zfree(val);
#else
        dbe_free_ptr(key, zfree);
        dbe_free_ptr(val, zfree);
#endif
        ret = restore_key_from_dbe(db, k, k_len, attr.type, rslt);
    }

    return ret;
}

static int make_string(const char *val, int v_len, val_attr *rslt, dbe_stat *tmp)
{
    struct timespec ts;
    ts = pf_get_time_tick();
    rslt->val = unserializeObj(val, v_len, (time_t*)&rslt->expire_ms);
    tmp->unserialize_dur = pf_get_time_diff_nsec(ts, pf_get_time_tick()) / 1000;
    stat_dbe_get_op();
    int ret = 0;
    if (rslt->val == NULL)
    {
        ret = 1;
        log_error("unserializeObj() ret NULL, buf_len=%d", v_len);
    }
    return ret;
}

/* ret:
 * 0 - succ
 * 1 - not found
 * 2 - dbe fail
 * 3 - process fail
 */
static int restore_string_key(void *db, const char *k, size_t k_len, val_attr *rslt)
{
    dbe_stat *tmp = &gDbeGetTop[0];
    char *key = 0;
    size_t key_len;

    if (server.enc_kv == 0)
    {
        key = (char *)k;
        key_len = k_len;
    }
    else
    {
        key = encode_string_key(k, k_len, &key_len);
        if (key == 0)
        {
            log_error("encode fail, key=%s", k);
            return 3;
        }
    }

    /* get value from db engine */
    int len;
    char *ptr = 0;
    const int dbe_ret = dbe_get(db, key, key_len, &ptr, &len, zmalloc, &tmp->io_dur);
    if (dbe_ret == DBE_ERR_NOT_FOUND)
    {
        log_info("dbe_get() return null, db=%p, dbe_key=%s", db, key);
        if (server.enc_kv)
        {
            zfree(key);
        }
        return 1;
    }
    else if (dbe_ret != DBE_ERR_SUCC)
    {
        /* get fail */
        if (server.enc_kv)
        {
            zfree(key);
        }
        return 2;
    }
    tmp->buf_len = len;

    int ret = 0;
    if (make_string(ptr, len, rslt, tmp) != 0)
    {
        /* fail */
        log_error("make_string() fail, val_len=%d, dbe_key=%s", len, key);
        ret = 3;
    }

    if (server.enc_kv)
    {
        zfree(key);
    }
    dbe_free_ptr(ptr, zfree);

    return ret;
}

static int restore_list_key(void *db, const char *k, size_t k_len, val_attr *rslt)
{
    (void)db;
    (void)k;
    (void)k_len;
    (void)rslt;
    return 3;
}

static int make_list(robj *subject, char *key, int k_len, char *val, int v_len)
{
    size_t key_len = k_len;
    size_t val_len = v_len;

    dbe_key_attr key_attr;
    decode_dbe_key(key, key_len, &key_attr);
    uint32_t seq = key_attr.seq;
#ifdef _DBE_LEVEL_DB_
    zfree(key);
#else
    dbe_free_ptr(key, zfree);
#endif

    robj *val_obj = createStringObject(val + 1, val_len - 1);
    val_obj->reserved = seq;

#ifdef _DBE_LEVEL_DB_
    zfree(val);
#else
    dbe_free_ptr(val, zfree);
#endif

    listIter *iter = NULL;
    listNode *node = NULL;
    int cnt = 0;
    iter = listGetIterator(subject->ptr, AL_START_HEAD);
    while (iter && (node = listNext(iter)) != NULL)
    {
        cnt++;
        robj *cur = (robj*)listNodeValue(node);
        if (seq <= cur->reserved)
        {
            log_test("No.%d: seq(%d) <= Node(%d)", cnt, seq, cur->reserved);
            break;
        }
    }
    listReleaseIterator(iter);
    if (node)
    {
        log_test("listInsertNode: seq=%u", seq);
        subject->ptr = listInsertNode(subject->ptr, node, val_obj, AL_START_HEAD);
    }
    else if (cnt > 0)
    {
        log_test("listAddNodeTail: seq=%u", seq);
        subject->ptr = listAddNodeTail(subject->ptr, val_obj);
    }
    else
    {
        log_test("listAddNodeHead: seq=%u", seq);
        subject->ptr = listAddNodeHead(subject->ptr, val_obj);
    }

    stat_dbe_get_op();

    return 0;
}

static int make_set(robj *set, char *key, int k_len, char *val, int v_len, robj *vobj)
{
    (void)k_len;

    /* the key is dbe_key include md5 of member, useless */
#ifdef _DBE_LEVEL_DB_
    zfree(key);
#else
    dbe_free_ptr(key, zfree);
#endif

    int ret = 1;
    robj *member = vobj ? vobj : createStringObject(val + 1, v_len - 1);
    if (member)
    {
        if (setTypeAdd(set, member) == 0)
        {
            /* fail */
            decrRefCount(member);
            ret = 1;
        }
        else
        {
            ret = 0;
        }
    }

#ifdef _DBE_LEVEL_DB_
    zfree(val);
#else
    dbe_free_ptr(val, zfree);
#endif

    stat_dbe_get_op();

    return ret;
}

static int make_hash(robj *set, char *key, int k_len, char *val, int v_len)
{
    (void)k_len;
    size_t val_len = v_len;

    /* the key is dbe_key include md5 of member, useless */
#ifdef _DBE_LEVEL_DB_
    zfree(key);
#else
    dbe_free_ptr(key, zfree);
#endif

    hash_val_attr attr;
    const int ret = decode_hash_val(val, val_len, &attr);
    if (ret == 0)
    {
        robj *f_obj = createStringObject(attr.field, attr.f_len);
        robj *v_obj = createStringObject(attr.val, attr.v_len);
        hashTypeSet(set, f_obj, v_obj);
    }

#ifdef _DBE_LEVEL_DB_
    zfree(val);
#else
    dbe_free_ptr(val, zfree);
#endif

    stat_dbe_get_op();

    return ret;
}

static int restore_set_key(void *db, const char *k, size_t k_len, val_attr *rslt)
{
    (void)db;
    (void)k;
    (void)k_len;
    (void)rslt;
    return 3;
}

static int make_zset(robj *zobj, char *key, int k_len, char *val, int v_len)
{
    (void)k_len;
    size_t val_len = v_len;

    /* the key is dbe_key include md5 of member, useless */
#ifdef _DBE_LEVEL_DB_
    zfree(key);
#else
    dbe_free_ptr(key, zfree);
#endif

    zset_val_attr attr;
    const int ret = decode_zset_val(val, val_len, &attr);
    if (ret == 0)
    {
        zaddMember(zobj, attr.member, attr.m_len, attr.score);
    }

#ifdef _DBE_LEVEL_DB_
    zfree(val);
#else
    dbe_free_ptr(val, zfree);
#endif

    stat_dbe_get_op();

    return ret ? 1 : 0;
}

static int restore_zset_key(void *db, const char *k, size_t k_len, val_attr *rslt)
{
    dbe_stat *tmp = &gDbeGetTop[0];
    char *key = 0;
    size_t key_len;

    key = encode_prefix_key(k, k_len, 'Z', &key_len);
    if (key == 0)
    {
        return 3;
    }

    void *it = dbe_pget(db, key, key_len);
    if (it == 0)
    {
        log_error("dbe_pget fail, dbe_key_prefix=%s", key);
        zfree(key);
        return 3;
    }
    zfree(key);
    key = 0;

    char *val = 0;
    int key_len_;
    int val_len_;
    int m_cnt = 0;
    robj *zobj = createZsetZiplistObject();
    while (dbe_next_key(it, &key, &key_len_, &val, &val_len_, zmalloc, &tmp->io_dur) == 0)
    {
        const int ret = make_zset(zobj, key, key_len_, val, val_len_);
        if (ret == 1)
        {
            decrRefCount(zobj);
            dbe_destroy_it(it);
            log_error("make_zset fail, key=%s", k);
            return 3;
        }

        m_cnt++;
    }

    dbe_destroy_it(it);

    log_test("get %d member from dbe for zset, key=%s", m_cnt, k);
    if (m_cnt)
    {
        rslt->val = zobj;
        return 0;
    }
    else
    {
        /* no fount */
        decrRefCount(zobj);
        return 1;
    }
}

static int restore_hash_key(void *db, const char *k, size_t k_len, val_attr *rslt)
{
    (void)db;
    (void)k;
    (void)k_len;
    (void)rslt;
    int ret = 3;
    return ret;
}

static void stat_dbe_get_op()
{
    /* sort according to io_dur */
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
    if (min_idx != 0)
    {
        memcpy(&gDbeGetTop[min_idx], &gDbeGetTop[0], sizeof(gDbeGetTop[0]));
        gDbeGetTop[min_idx].valid = 1;
        gDbeGetTop[min_idx].time = time(NULL);
    }

    if (gDbeGetTop[0].io_dur > gDbeGetMax)
    {
        gDbeGetMax = gDbeGetTop[0].io_dur;
    }
    if (gDbeGetTop[0].io_dur < gDbeGetMin || gDbeGetMin == 0)
    {
        gDbeGetMin = gDbeGetTop[0].io_dur;
    }
    gDbeGetTotal += gDbeGetTop[0].io_dur;
    gDbeGetCnt++;
    memset(&gDbeGetTop[0], 0, sizeof(gDbeGetTop[0]));
}

