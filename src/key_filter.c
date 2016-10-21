#include "redis.h"

#include "ds_ctrl.h"
#include "ch.h"
#include "ds_log.h"
#include "ds_util.h"
#include "codec_key.h"

#include <pthread.h>

typedef struct filter_list_tag
{
    int permission;
    int list_cnt;
    char **list;
} filter_list;

static filter_list sc_bl_list = { -1, 0, 0};
static filter_list sc_sync_list = { -1, 0, 0};
static filter_list sc_cache_list = { -1, 0, 0};

static pthread_rwlock_t g_rwlock;
static serv_node_info_t *g_cont = 0;
static serv_node_info_t *g_cont2 = 0;


int init_filter()
{
    pthread_rwlock_init(&g_rwlock, 0);
    return 0;
}

int gen_filter_server()
{
    int rslt = 0;
    char **server_keys = 0;
    int sk_count = 0;
    int i;

    lock_ctrl_info_rd();
    log_prompt("gen_server: ip_cnt=%d, g_cont=%p", gDsCtrl.ip_cnt, g_cont);
    if (gDsCtrl.ip_cnt > 0)
    {
        server_keys = (char **)zmalloc(gDsCtrl.ip_cnt * sizeof(char *));
        sk_count = gDsCtrl.ip_cnt;

        ds_ip_node *node = gDsCtrl.ds_ip_list;
        for (i = 0; i < gDsCtrl.ip_cnt && node; i++)
        {
            server_keys[i] = (char *)zmalloc(CH_SERVER_KEY_MAX_LENGT + 1);
            strncpy(server_keys[i], node->ds_ip, CH_SERVER_KEY_MAX_LENGT);
            node = node->next;
        }
    }
    unlock_ctrl_info();

    if (g_cont)
    {
        /* destroy it at first */
        consistent_hash_destory(g_cont);
        g_cont = 0;
    }
    if (server_keys)
    {
        g_cont = consistent_hash_init((const char *const*)server_keys, sk_count);
        if (g_cont == 0)
        {
            /* log error */
            log_error("consistent_hash_init() fail");
        }
    }

    pthread_rwlock_wrlock(&g_rwlock);
    if (g_cont2)
    {
        /* destroy it at first */
        consistent_hash_destory(g_cont2);
        g_cont2 = 0;
    }
    if (server_keys)
    {
        g_cont2 = consistent_hash_init((const char *const*)server_keys, sk_count);
        if (g_cont2 == 0)
        {
            /* log error */
            log_error("consistent_hash_init() fail for g_cont2");
        }
    }
    log_prompt("gen_server: g_cont=%p, g_cont2=%p", g_cont, g_cont2);
    pthread_rwlock_unlock(&g_rwlock);

    /* release source */
    if (server_keys)
    {
        for (i = 0; i < sk_count; i++)
        {
            zfree(server_keys[i]);
        }
        zfree(server_keys);
    }

    return rslt;
}

static int get_server(serv_node_info_t *cont, const char *key, int key_len, serv_node_t *s)
{
    int rslt = 0;
    if (cont)
    {
        serv_node_t *serv = ch_get_server(cont, key, key_len);
        if (serv)
        {
            s->point = serv->point;
            strcpy(s->server_key, serv->server_key);
        }
        else
        {
            /* it maybe impossible */
            rslt = 1;
        }
    }
    else
    {
        rslt = 1;
    }
    return rslt;
}

/*
 * return: 0 - allow; 1 - forbidden
 */
static int ds_filter(const char *key, int key_len, const char *ds_key)
{
    if (key == 0 || key_len == 0 || ds_key == 0)
    {
        return 0;
    }

    serv_node_t serv;
    int ret;

    ret = get_server(g_cont, key, key_len, &serv);
    if (ret == 0)
    {
        //log_debug("\"%s\" on %s, ds_key=%s", key, serv.server_key, server.ds_key);
        if (strcmp(serv.server_key, ds_key) == 0)
        {
            /* the key belong to this node */
            //log_debug("master_filter() : valid key=%s", key);
            return 0;
        }
        else
        {
            //log_debug("master_filter() : invalid key=%s", key);
            return 1;
        }
    }
    else
    {
        /* fail, but regard as valid key */
        //log_error("master_filter() : get_server() fail for \"%s\"", key);
        return 0;
    }
}

/*
 * return: 0 - allow; 1 - forbidden
 */
static int ds_filter2(const char *key, int key_len, const char *ds_key)
{
    if (key == 0 || key_len == 0 || ds_key == 0)
    {
        return 0;
    }

    serv_node_t serv;
    int ret;

    pthread_rwlock_rdlock(&g_rwlock);
    ret = get_server(g_cont2, key, key_len, &serv);
    pthread_rwlock_unlock(&g_rwlock);
    if (ret == 0)
    {
        //log_debug("\"%s\" on %s, ds_key=%s", key, serv.server_key, server.ds_key);
        if (strcmp(serv.server_key, ds_key) == 0)
        {
            /* the key belong to this node */
            //log_debug("master_filter() : valid key=%s", key);
            return 0;
        }
        else
        {
            //log_debug("master_filter() : invalid key=%s", key);
            return 1;
        }
    }
    else
    {
        /* fail, but regard as valid key */
        //log_error("master_filter() : get_server() fail for \"%s\"", key);
        return 0;
    }
}

int master_filter_keylen(const char *key, int key_len)
{
    return server.is_slave == 1 ? 0 : ds_filter(key, key_len, server.ds_key);
}

/* for cache
 * return: 0 - permissible; 1 - forbidden
 */
int master_filter(const char *key)
{
    return master_filter_keylen(key, strlen(key));
}

/* for dbe
 * return: 0 - permissible; 1 - forbidden
 */
int master_filter_4_dbe(const char *dbe_key, size_t dbe_key_len)
{
    if (server.is_slave == 1)
    {
        return 0;
    }

    int ret;
    dbe_key_attr attr;
    ret = decode_dbe_key(dbe_key, dbe_key_len, &attr);
    if (ret == 0)
    {
        ret = ds_filter2(attr.key, attr.key_len, server.ds_key);
#ifdef _DBE_IF_DEBUG_
        log_prompt("filter_4_dbe: len=%zd, dbe_key=%s, key_len=%zd, key_type=%c, ret=%d"
                , dbe_key_len, dbe_key, attr.key_len, attr.type, ret);
#endif
    }
    else
    {
        /* fail, allow */
#ifdef _DBE_IF_DEBUG_
        log_prompt("filter_4_dbe: len=%zd, dbe_key=%s, decode fail:%d"
                , dbe_key_len, dbe_key, ret);
#endif
        ret = 0;
    }
    return ret;
}

/* for sync, ds_key belong to slave_sync
 * return: 0 - permissible; 1 - forbidden
 */
int filter_gen(const char *key, int key_len, const char *ds_key)
{
    if (key == 0 || key_len <= 0 || ds_key == 0)
    {
        return 0;
    }
    const int ret = ds_filter2(key, key_len, ds_key);
    return ret;
}

/*
 * permission: -1 - disable; 0 - forbidden_write_bl prefix; 1 - write_bl prefix
 */
void set_bl_filter_list(const char *list, int list_len, int permission)
{
    static int sc_permission = -1;
    static int sc_list_len = 0;
    static char *sc_list = 0;

    if (sc_permission != permission
        || sc_list_len != list_len
        || (sc_list && list && strcmp(sc_list, list) != 0)
       )
    {
        /* need to change current config */
        int cnt = 0;
        char **list_ar = 0;
        if (sc_list)
        {
            zfree(sc_list);
            sc_list = 0;
            sc_list_len = 0;
        }
        if (permission != -1)
        {
            if (list && list_len > 0)
            {
                sc_list = zstrdup(list);
                sc_list_len = list_len;
                list_ar = parse_list_str(sc_list, &cnt, "\r \n,");
            }
        }
        sc_permission = permission;
        log_prompt("bl_filter change: perm=%d, book_cnt=%d, book_list=%s"
                , permission, cnt, list ? list : "");

        lock_bl_filter();

        if (sc_bl_list.list)
        {
            int i = 0;
            for (; i < sc_bl_list.list_cnt; i++)
            {
                zfree(sc_bl_list.list[i]);
            }
            zfree(sc_bl_list.list);
            sc_bl_list.list = 0;
            sc_bl_list.list_cnt = 0;
        }
        sc_bl_list.permission = permission;
        sc_bl_list.list_cnt = cnt;
        sc_bl_list.list = list_ar;

        unlock_bl_filter();
    }
}

/*
 * return: 0 - write bl, 1 - forbidden write bl
 */
int bl_filter(const char *key, int key_len)
{
    int rslt;

    lock_bl_filter();

    if (sc_bl_list.permission != -1)
    {
        int find = 0;
        if (sc_bl_list.list)
        {
            find = 0;
            int i;
            for (i= 0; i < sc_bl_list.list_cnt; i++)
            {
                const int prefix_len = strlen(sc_bl_list.list[i]);
                if (key_len >= prefix_len
                    && strncmp(sc_bl_list.list[i], key, prefix_len) == 0)
                {
                    find = 1;
                    break;
                }
            }
        }
        else
        {
            find = 0;
        }

        if (sc_bl_list.permission == 0)
        { 
            /* black list */
            rslt = find == 0 ? 0 : 1;
        }
        else
        {
            /* white list */
            rslt = find == 0 ? 1 : 0;
        }
    }
    else
    {
        /* disable list */
        rslt = 0;
    }

    unlock_bl_filter();

    //log_prompt("bl_filter: key_len=%d, result=%d", key_len, rslt);
    return rslt;
}

/*
 * permission: -1 mean disable
 */
void set_sync_filter_list(const char *list, int list_len, int permission)
{
    static int sc_permission = -1;
    static int sc_list_len = 0;
    static char *sc_list = 0;

    if (sc_permission != permission
        || sc_list_len != list_len
        || (sc_list && list && strcmp(sc_list, list) != 0)
       )
    {
        /* need to change current config */
        int cnt = 0;
        char **list_ar = 0;
        if (sc_list)
        {
            zfree(sc_list);
            sc_list = 0;
            sc_list_len = 0;
        }
        if (permission != -1)
        {
            if (list && list_len > 0)
            {
                sc_list = zstrdup(list);
                sc_list_len = list_len;
                list_ar = parse_list_str(sc_list, &cnt, "\r \n,");
            }
        }
        sc_permission = permission;
        log_prompt("sync_filter change: perm=%d, book_cnt=%d, book_list=%s"
                , permission, cnt, list ? list : "");

        lock_sync_filter();

        if (sc_sync_list.list)
        {
            int i = 0;
            for (; i < sc_sync_list.list_cnt; i++)
            {
                zfree(sc_sync_list.list[i]);
            }
            zfree(sc_sync_list.list);
            sc_sync_list.list = 0;
            sc_sync_list.list_cnt = 0;
        }
        sc_sync_list.permission = permission;
        sc_sync_list.list_cnt = cnt;
        sc_sync_list.list = list_ar;

        unlock_sync_filter();
    }
}

/*
 * return: 0 - allow, 1 - forbidden
 */
int sync_filter(const char *key, int key_len)
{
    int rslt;

    lock_sync_filter();

    if (sc_sync_list.permission != -1)
    {
        int find = 0;
        if (sc_sync_list.list)
        {
            find = 0;
            int i;
            for (i= 0; i < sc_sync_list.list_cnt; i++)
            {
                const int prefix_len = strlen(sc_sync_list.list[i]);
                if (key_len >= prefix_len
                    && strncmp(sc_sync_list.list[i], key, prefix_len) == 0)
                {
                    find = 1;
                    break;
                }
            }
        }
        else
        {
            find = 0;
        }

        if (sc_sync_list.permission == 0)
        { 
            /* black list */
            rslt = find == 0 ? 0 : 1;
        }
        else
        {
            /* white list */
            rslt = find == 0 ? 1 : 0;
        }
    }
    else
    {
        /* disable list */
        rslt = 0;
    }

    unlock_sync_filter();

    //log_prompt("sync_filter: key_len=%d, result=%d", key_len, rslt);
    return rslt;
}

/*
 * permission: -1 - disable; 0 - define cache prefix; 1 - define persistence prefix
 */
void set_cache_filter_list(const char *list, int list_len, int permission)
{
    static int sc_permission = -1;
    static int sc_list_len = 0;
    static char *sc_list = 0;

    if (sc_permission != permission
        || sc_list_len != list_len
        || (sc_list && list && strcmp(sc_list, list) != 0)
       )
    {
        /* need to change current config */
        int cnt = 0;
        char **list_ar = 0;
        if (sc_list)
        {
            zfree(sc_list);
            sc_list = 0;
            sc_list_len = 0;
        }
        if (permission != -1)
        {
            if (list && list_len > 0)
            {
                sc_list = zstrdup(list);
                sc_list_len = list_len;
                list_ar = parse_list_str(sc_list, &cnt, "\r \n,");
            }
        }
        sc_permission = permission;
        log_prompt("cache_filter change: perm=%d, book_cnt=%d, book_list=%s"
                , permission, cnt, list ? list : "");

        lock_cache_filter();

        if (sc_cache_list.list)
        {
            int i = 0;
            for (; i < sc_cache_list.list_cnt; i++)
            {
                zfree(sc_cache_list.list[i]);
            }
            zfree(sc_cache_list.list);
            sc_cache_list.list = 0;
            sc_cache_list.list_cnt = 0;
        }
        sc_cache_list.permission = permission;
        sc_cache_list.list_cnt = cnt;
        sc_cache_list.list = list_ar;

        unlock_cache_filter();
    }
}

/*
 * return: 0 - persistence, 1 - cache
 */
int cache_filter(const char *key, int key_len)
{
    int rslt;

    lock_cache_filter();

    if (sc_cache_list.permission != -1)
    {
        int find = 0;
        if (sc_cache_list.list)
        {
            find = 0;
            int i;
            for (i= 0; i < sc_cache_list.list_cnt; i++)
            {
                const int prefix_len = strlen(sc_cache_list.list[i]);
                if (key_len >= prefix_len
                    && strncmp(sc_cache_list.list[i], key, prefix_len) == 0)
                {
                    find = 1;
                    break;
                }
            }
        }
        else
        {
            find = 0;
        }

        if (sc_cache_list.permission == 0)
        { 
            /* black list */
            rslt = find == 0 ? 0 : 1;
        }
        else
        {
            /* white list */
            rslt = find == 0 ? 1 : 0;
        }
    }
    else
    {
        /* disable list */
        rslt = 0;
    }

    unlock_cache_filter();

    //log_prompt("cache_filter: key_len=%d, result=%d", key_len, rslt);
    return rslt;
}

