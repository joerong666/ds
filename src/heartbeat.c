#include "ds_log.h"
#include "http_client.h"
#include "heartbeat.h"
#include "ds_ctrl.h"
#include "pf_json.h"
#include "zmalloc.h"
#include "util.h"
#include "ds_util.h"
#include "ds_zmalloc.h"
#include "ds_binlog.h"
#include "key_filter.h"

#include <string.h>
#include <stdlib.h>
#include <unistd.h>

static int get_log_level(const char *str);
extern int notify_main_server(int fd, const void *buf, size_t buf_size, int tag);
extern void rsp_hb(aeEventLoop *el, int fd, void *privdata, int mask);

typedef struct MngrListTag
{
    char **Mngrs;
    int MngrCnt;
    int idx;
} MngrList;

static MngrList *scpMngrs = 0;
static MngrList *gpMngrs = 0;
static int scHbLogFlg = 0;
static int64_t gHbVersion = -1;

static MngrList *set_mngr_list_ex(const char *mngr_list, const MngrList *pList);
static void set_mngr_list2(const char *mngr_list);

static void *heart_beat_task(void *arg);
static int make_ds_hb_url(char *url, int url_len, const char *pMngrAddr)
{
    return snprintf(url, url_len, "http://%s/DataServerHeartBeat", pMngrAddr);
}

static int write_hbrsp_to_file(const char *rsp)
{
    char tmp[256];
    char f[256];
    snprintf(tmp, sizeof(tmp), "./.%d-%s-tmp.txt", server.port, server.host);
    snprintf(f, sizeof(f), "./.%d-%s.txt", server.port, server.host);

    FILE *fp = fopen(tmp, "w");
    if (!fp)
    {
        log_error("fopen() fail for write hb-rsp, errno=%d, file=%s", errno, tmp);
        return -1;
    }
    //log_prompt("write hb-rsp to %s", tmp);

    const int cnt =fprintf(fp, "%s", rsp);
    if (cnt <= 0)
    {
        log_error("fprintf()=%d, fail, file=%s", cnt, tmp);
        fclose(fp);
        return -1;
    }
    int ret;
    ret = fflush(fp);
    if (ret != 0)
    {
        log_error("fflush() fail, file=%s, errno=%d", tmp, errno);
        fclose(fp);
        return -1;
    }
    ret = fsync(fileno(fp));
    if (ret != 0)
    {
        log_error("fsync() fail, file=%s, errno=%d", tmp, errno);
        fclose(fp);
        return -1;
    }
    ret = fclose(fp);
    if (ret != 0)
    {
        log_error("fclose() fail, errno=%d, file=%s", errno, tmp);
        return -1;
    }

    if (rename(tmp, f) != 0)
    {
        log_error("rename() fail, errno=%d, file=%s", errno, tmp);
        return -1;
    }
    return 0;
}

static char * read_hbrsp_from_file()
{
    char f[256];
    snprintf(f, sizeof(f), "./.%d-%s.txt", server.port, server.host);

    FILE *fp = fopen(f, "r");
    if (!fp)
    {
        log_error("fopen() fail for read hb-rsp, errno=%d, file=%s", errno, f);
        return 0;
    }

    fseek(fp, 0, SEEK_END);
    const int RspLen = ftell(fp);
    fseek(fp, 0, SEEK_SET);
    char *pRsp = zmalloc(RspLen + 1);
    fread(pRsp, 1, RspLen, fp);
    pRsp[RspLen] = 0;
    //log_prompt("read from %s: %d\n%s", f, RspLen, pRsp);

    fclose(fp);
    return pRsp;
}

static pf_json_object_t *send_hb_to_mngr(pf_json_object_t *req_obj, int main)
{
    if (scpMngrs == 0)
    {
        log_debug("mngr_list is null, req_obj=%p", (void*)req_obj);
        return 0;
    }

    int ret;
    char url[512];
    const char *body = 0;
    int body_len;
    char *rsp = 0;
    int rsp_len;
    int rsp_code;

    int try_cnt = 0;

    /* make heart-beat req msg */
    body = pf_json_obj_to_json_str(req_obj);
    body_len = strlen(body);
    if (main)
    {
        log_prompt("heart_beat req: len=%d\n%s", body_len, body);
    }
    else
    {
        if (scHbLogFlg)
        {
            log_prompt2(-1, "heart_beat req: len=%d\n%s", body_len, body);
        }
    }


    while (1)
    {
        /* make url request to mngr */
        make_ds_hb_url(url, (int)(sizeof(url)), scpMngrs->Mngrs[scpMngrs->idx]);

        /* send to mngr and get rsp */
        ret = http_post(url, body, body_len, -1, &rsp, &rsp_len, &rsp_code);
        try_cnt++;
        if (ret == 0)
        {
            if (rsp == 0)
            {
                if (main)
                {
                    log_error("http_post() succ, but rsp=NULL, idx=%d, mngr_cnt=%d, url=%s"
                            , scpMngrs->idx, scpMngrs->MngrCnt, url);
                }
                else
                {
                    log_error2(-1, "http_post() succ, but rsp=NULL, idx=%d, mngr_cnt=%d, url=%s"
                            , scpMngrs->idx, scpMngrs->MngrCnt, url);
                }

                if (try_cnt >= scpMngrs->MngrCnt)
                {
                    /* can't go to try another mngr */
                    return 0;
                }
            }
            else
            {
                break;
            }
        }
        else if (ret == 1)
        {
            if (main)
            {
                log_error("http_post() fail, curl_easy_perform=%d, idx=%d, mngr_cnt=%d, url=%s"
                        , rsp_code, scpMngrs->idx, scpMngrs->MngrCnt, url);
            }
            else
            {
                log_error2(-1, "http_post() fail, curl_easy_perform=%d, idx=%d, mngr_cnt=%d, url=%s"
                        , rsp_code, scpMngrs->idx, scpMngrs->MngrCnt, url);
            }

            if (try_cnt >= scpMngrs->MngrCnt)
            {
                /* can't try another mngr */
                return 0;
            }
        }
        else if (ret == 2)
        {
            if (main)
            {
                log_error("http_post() fail, http_rsp_code=%d, idx=%d, mngr_cnt=%d, url=%s"
                        , rsp_code, scpMngrs->idx, scpMngrs->MngrCnt, url);
            }
            else
            {
                log_error2(-1, "http_post() fail, http_rsp_code=%d, idx=%d, mngr_cnt=%d, url=%s"
                        , rsp_code, scpMngrs->idx, scpMngrs->MngrCnt, url);
            }

            if (try_cnt >= scpMngrs->MngrCnt)
            {
                /* can't try another mngr */
                return 0;
            }
        }
        else
        {
            if (main)
            {
                log_error("http_post() fail, ret=%d(%d), idx=%d, mngr_cnt=%d, url=%s"
                        , ret, rsp_code, scpMngrs->idx, scpMngrs->MngrCnt, url);
            }
            else
            {
                log_error2(-1, "http_post() fail, ret=%d(%d), idx=%d, mngr_cnt=%d, url=%s"
                        , ret, rsp_code, scpMngrs->idx, scpMngrs->MngrCnt, url);
            }
            return 0;
        }

        scpMngrs->idx = (scpMngrs->idx + 1) % scpMngrs->MngrCnt;
        if (main)
        {
            log_prompt("try other mngr[%d]: %s"
                    , scpMngrs->idx, scpMngrs->Mngrs[scpMngrs->idx]);
        }
        else
        {
            log_prompt2(-1, "try other mngr[%d]: %s"
                    , scpMngrs->idx, scpMngrs->Mngrs[scpMngrs->idx]);
        }
    }

    if (main)
    {
        log_prompt("heart_beat rsp: len=%d\n%s", rsp_len, rsp);
    }
    else
    {
        if (scHbLogFlg)
        {
            log_prompt2(-1, "heart_beat rsp: len=%d\n%s", rsp_len, rsp);
        }
    }

    pf_json_object_t *rsp_json = 0;
    rsp_json = pf_json_parse(rsp);
    zfree(rsp);
    return rsp_json;
}

static int find_node(const ds_ip_node *node)
{
    int fount = 0;
    const ds_ip_node *n = gDsCtrl.ds_ip_list;
    while (n)
    {
        if (strcmp(n->ds_ip, node->ds_ip) == 0)
        {
            fount = 1;
            break;
        }
        n = n->next;
    }

    return fount;
}

static char *serialize_ds_ip_list(char *buf, int buf_size, const ds_ip_node *tmp)
{
    buf[0] = 0;
    int total = 0;
    int len = buf_size - total - 1;
    while (tmp && len > 0)
    {
        const int i = snprintf(buf + total, len, "%s%s", total ? ", " : "", tmp->ds_ip);
        if (i < 0)
        {
            /* fail */
            break;
        }
        else if (i > len)
        {
            /* too many output, buf is full */
            total += len;
        }
        else
        {
            total += i;
        }
        len = buf_size - total - 1;
        tmp = tmp->next;
    }
    return buf;
}

/* return:
 * bits:
 * bit0 - result, 1-succ, 0-fail
 * bit1 - ip_list changed, 1-yes, 0-not
 * bit2 - include my_address, 1-yes, 0-not
 * bit3 - ip increment or not, 1-yes, 0-not
 */
static int save_ds_ip_list(pf_json_object_t *obj)
{
    pf_json_object_t *e = 0;
    ds_ip_node **pnode = &gDsCtrl.ds_ip_list_tmp;
    ds_ip_node *node = *pnode;
    const int cnt = pf_json_array_obj_len(obj);
    int i;
    pf_json_object_t *item = 0;
    int found_me = 0;
    int ip_list_changed = gDsCtrl.ip_cnt == cnt ? 0 : 1;
    int is_incr = gDsCtrl.ip_cnt > cnt ? 0 : 1;

    for (i = 0; i < cnt; i++)
    {
        //log_prompt("save_ds_ip_list: for(), ip_cnt=%d, i=%d", cnt, i);
        e = pf_json_array_obj_get_item(obj, i);
        if (e == 0)
        {
            return 0;
        }
        if (node == 0)
        {
            node = (ds_ip_node *)zmalloc(sizeof(ds_ip_node));
            if (node == 0)
            {
                return 0;
            }

            node->next = 0;
            *pnode = node; /* hang upon the node to the tail of the list */
        }

        /* ds_ip */
        item = pf_json_get_sub_obj(e, "ds_ip");
        if (item == 0)
        {
            return 0;
        }
        if (pf_json_get_obj_type(item) != PF_JSON_TYPE_STRING)
        {
            return 0;
        }
        const char *const ds_ip = pf_json_get_str(item);
        if (strlen(ds_ip) >= sizeof(node->ds_ip))
        {
            /* illegal value */
            return 0;
        }
        strcpy(node->ds_ip, ds_ip);
        if (strcmp(node->ds_ip, server.ds_key) == 0)
        {
            found_me = 1;
        }
        /* check ds_ip */
        ds_ip_node *n = gDsCtrl.ds_ip_list_tmp;
        while (n != node)
        {
            if (strcmp(n->ds_ip, node->ds_ip) == 0)
            {
                /* illegal */
                return 0;
            }
            n = n->next;
        }

        /* ds_num */
        item = pf_json_get_sub_obj(e, "ds_num");
        if (item == 0)
        {
            return 0;
        }
        if (pf_json_get_obj_type(item) != PF_JSON_TYPE_INT)
        {
            return 0;
        }
        node->ds_seq = pf_json_get_int(item);;

        if (ip_list_changed == 0 && find_node(node) == 0)
        {
            ip_list_changed = 1;
            is_incr = 1;
        }

        pnode = &node->next;
        node = *pnode;
    }

    /* relase */
    if (node)
    {
        *pnode = 0;
    }
    while (node)
    {
        //log_debug("release node: %p", node);
        ds_ip_node *tmp = node;
        node = node->next;
        zfree(tmp);
    }

    //log_prompt("ip_list_chagned=%d", ip_list_changed);
    int ret = 1;
    if (ip_list_changed)
    {
        ds_ip_node *const tmp = gDsCtrl.ds_ip_list;
        gDsCtrl.ds_ip_list = gDsCtrl.ds_ip_list_tmp;
        gDsCtrl.ds_ip_list_tmp = tmp;

        gDsCtrl.ip_cnt = cnt;

        ret += 2;

        /* print info */
        char buf_old[1024];
        serialize_ds_ip_list(buf_old, sizeof(buf_old) - 1, gDsCtrl.ds_ip_list_tmp);
        char buf_new[1024];
        serialize_ds_ip_list(buf_new, sizeof(buf_new) - 1, gDsCtrl.ds_ip_list);
        log_prompt("ds_ip_list changed: %s ==> %s", buf_old, buf_new);
    }
    if (found_me)
    {
        ret += 4;
    }
    if (is_incr)
    {
        ret += 8;
    }
    return ret;
}

int init_with_mngr(int local_conf)
{
    /* make heart-beat req msg */
    pf_json_object_t *req_obj = pf_json_create_obj();
    pf_json_add_sub_obj(req_obj, "address", pf_json_create_str_obj(server.host));
    pf_json_add_sub_obj(req_obj, "port", pf_json_create_int_obj(server.port));
    pf_json_add_sub_obj(req_obj, "start_time", pf_json_create_str_obj(server.starttime_str));
    char ver[128];
    sprintf(ver, "%s %s %s", REDIS_VERSION, __DATE__, __TIME__);
    pf_json_add_sub_obj(req_obj, "soft_ver", pf_json_create_str_obj(ver));

    /* handle the rsp */
    int tryed = 0;
    pf_json_object_t *rsp_obj = send_hb_to_mngr(req_obj, 1);
    pf_json_put_obj(req_obj);

try_local_config:
    if (rsp_obj == 0)
    {
        /* try to get from file */
        char *pRsp = read_hbrsp_from_file();
        if (pRsp != 0)
        {
            rsp_obj = pf_json_parse(pRsp);
            log_prompt("get hb-rsp from file, rsp_obj=%p:\n%s", (void*)rsp_obj, pRsp);

            zfree(pRsp);
            pRsp = 0;
        }
    }

    if (rsp_obj == 0)
    {
        /* fail */
        log_error("%s", "can't get rsp from mngr after sent req");
        return 1;
    }

    pf_json_object_t *err_msg = pf_json_get_sub_obj(rsp_obj, "err_msg");
    if (tryed == 0
        && err_msg
        && strcmp(pf_json_get_str(err_msg), "db operator fail") == 0
       )
    {
        /* db operator fail, go on try local config */
        log_prompt("hb-rsp db fail, rsp_obj=%s", pf_json_get_str(rsp_obj));
        tryed = 1;
        pf_json_put_obj(rsp_obj);
        rsp_obj = 0;
        goto try_local_config;
    }

    pf_json_object_t *config = pf_json_get_sub_obj(rsp_obj, "config");
    if (config == 0 && local_conf == 0)
    {
        /* fail */
        log_error("json parse config fail, rsp:\n%s", pf_json_get_str(rsp_obj));
        pf_json_put_obj(rsp_obj);
        return 1;
    }

    pf_json_object_t *rsp_list = pf_json_get_sub_obj(rsp_obj, "rsp_list");
    if (rsp_list == 0)
    {
        /* fail */
        log_error("json parse rsp_list fail, rsp:\n%s", pf_json_get_str(rsp_obj));
        pf_json_put_obj(rsp_obj);
        return 1;
    }

    pf_json_object_t *rsp_item = pf_json_array_obj_get_item(rsp_list, 0);
    if (rsp_item == 0)
    {
        /* fail */
        log_error("json get array item 0 from rsp_list fail,rsp_list:\n%s", pf_json_get_str(rsp_list));
        pf_json_put_obj(rsp_obj);
        return 1;
    }

    pf_json_object_t *item = 0;
    int result = 0;
    int ret;
    int prtcl_ver = 1;

    /* prtcl_ver */
    item = pf_json_get_sub_obj(rsp_obj, "prtcl_ver");
    if (item && pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
    {
        prtcl_ver = pf_json_get_int(item);
    }

    /* result */
    item = pf_json_get_sub_obj(rsp_item, "result");
    if (item == 0)
    {
        /* result null,illegal, mandotory */
        log_error("result lose, rsp_item:\n%s", pf_json_get_str(rsp_item));
        result = 1;
        goto init_with_mngr_fail;
    }
    if (pf_json_get_obj_type(item) != PF_JSON_TYPE_INT)
    {
        /* illegal */
        log_error("%s", "result illegal value");
        result = 1;
        goto init_with_mngr_fail;
    }
    const int rslt = pf_json_get_int(item);
    if (rslt != 0)
    {
        log_error("mngr rsp result=%d", rslt);
        result = 1;
        goto init_with_mngr_fail;
    }

    /* app_id */
    item = pf_json_get_sub_obj(rsp_item, "app_id");
    if (item == 0)
    {
        /* app_id null,illegal, mandotory */
        log_error("app_id lose, rsp_item:\n%s", pf_json_get_str(rsp_item));
        result = 1;
        goto init_with_mngr_fail;
    }
    if (pf_json_get_obj_type(item) != PF_JSON_TYPE_STRING)
    {
        /* app_id illegal */
        log_error("%s", "app_id illegal value");
        result = 1;
        goto init_with_mngr_fail;
    }
    strncpy(server.app_id, pf_json_get_str(item), sizeof(server.app_id) - 1);

    /* ds_key */
    item = pf_json_get_sub_obj(rsp_item, "ds_key");
    if (item == 0)
    {
        /* ds_key null,illegal, mandotory */
        log_error("ds_key lose, rsp_item:\n%s", pf_json_get_str(rsp_item));
        result = 1;
        goto init_with_mngr_fail;
    }
    if (pf_json_get_obj_type(item) != PF_JSON_TYPE_STRING)
    {
        /* ds_key illegal */
        log_error("%s", "ds_key illegal value");
        result = 1;
        goto init_with_mngr_fail;
    }
    if (server.ds_key)
    {
        zfree(server.ds_key);
    }
    server.ds_key = zstrdup(pf_json_get_str(item));

    /* ds_key_num */
    item = pf_json_get_sub_obj(rsp_item, "ds_key_num");
    if (item == 0)
    {
        if (prtcl_ver != 1)
        {
            /* ds_key_num null,illegal, mandotory */
            log_error("ds_key_num lose, prtcl_ver=%d, rsp_item:\n%s", prtcl_ver, pf_json_get_str(rsp_item));
            result = 1;
            goto init_with_mngr_fail;
        }
    }
    else
    {
        if (pf_json_get_obj_type(item) != PF_JSON_TYPE_INT)
        {
            /* illegal */
            log_error("%s", "ds_key_num illegal value");
            result = 1;
            goto init_with_mngr_fail;
        }
        server.ds_key_num = pf_json_get_int64(item);
    }

    /* master */
    item = pf_json_get_sub_obj(rsp_item, "master");
    if (item == 0)
    {
        if (prtcl_ver != 1)
        {
            /* master null,illegal, mandotory */
            log_error("master lose, rsp_item:\n%s", pf_json_get_str(rsp_item));
            result = 1;
            goto init_with_mngr_fail;
        }
    }
    else
    {
        if (pf_json_get_obj_type(item) != PF_JSON_TYPE_INT)
        {
            /* illegal */
            log_error("%s", "master illegal value");
            result = 1;
            goto init_with_mngr_fail;
        }
        server.is_slave = pf_json_get_int(item) ? 1 : 0;
    }

    /* has_cache */
    item = pf_json_get_sub_obj(rsp_item, "has_cache");
    if (item == 0)
    {
        /* has_cache null,illegal, mandotory */
        log_error("has_cache lose, rsp_item:\n%s", pf_json_get_str(rsp_item));
        result = 1;
        goto init_with_mngr_fail;
    }
    if (pf_json_get_obj_type(item) != PF_JSON_TYPE_INT)
    {
        /* illegal */
        log_error("%s", "has_cache illegal value");
        result = 1;
        goto init_with_mngr_fail;
    }
    server.has_cache = pf_json_get_int(item);

    /* has_dbe */
    item = pf_json_get_sub_obj(rsp_item, "has_dbe");
    if (item == 0)
    {
        /* has_dbe null,illegal, mandotory */
        log_error("has_dbe lose, rsp_item:\n%s", pf_json_get_str(rsp_item));
        result = 1;
        goto init_with_mngr_fail;
    }
    if (pf_json_get_obj_type(item) != PF_JSON_TYPE_INT)
    {
        /* illegal */
        log_error("%s", "has_dbe illegal value");
        result = 1;
        goto init_with_mngr_fail;
    }
    server.has_dbe = pf_json_get_int(item);

    /* status */
    item = pf_json_get_sub_obj(rsp_item, "status");
    if (item == 0)
    {
        /* status null,illegal, mandotory */
        log_error("status lose, rsp_item:\n%s", pf_json_get_str(rsp_item));
        result = 1;
        goto init_with_mngr_fail;
    }
    if (pf_json_get_obj_type(item) != PF_JSON_TYPE_INT)
    {
        /* illegal */
        log_error("%s", "status illegal value");
        result = 1;
        goto init_with_mngr_fail;
    }
    server.app_status = gDsCtrl.app_status = pf_json_get_int(item);

    /* version */
    item = pf_json_get_sub_obj(rsp_item, "version");
    if (item == 0)
    {
        /* version null,illegal, mandotory */
        log_error("version lose, rsp_item:\n%s", pf_json_get_str(rsp_item));
        result = 1;
        goto init_with_mngr_fail;
    }
    if (pf_json_get_obj_type(item) != PF_JSON_TYPE_INT)
    {
        /* illegal */
        log_error("%s", "version illegal value");
        result = 1;
        goto init_with_mngr_fail;
    }
    gHbVersion = pf_json_get_int64(item);

    /* ds_ip_list */
    item = pf_json_get_sub_obj(rsp_item, "ds_ip_list");
    if (item)
    {
        const char *str = pf_json_get_str(item);
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_ARRAY)
        {
            ret = save_ds_ip_list(item);
            if (ret == 0)
            {
                log_error("%s", "save ds_ip_list fail");
            }
            else
            {
                gDsCtrl.dsip_list = zstrdup(str);
                gen_filter_server();
            }
        }
        else
        {
            gDsCtrl.dsip_list = zstrdup(str);
        }
    }

    /* min_mem */
    item = pf_json_get_sub_obj(rsp_item, "min_mem");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            const size_t tmp = pf_json_get_int64(item);
            if (tmp > 11 * 1024 * 1024)
            {
                server.db_min_size = tmp;
            }
        }
        else
        {
            log_error("%s", "min_mem illegal value");
        }
    }
    gDsCtrl.min_mem = server.db_min_size;

    /* max_mem */
    item = pf_json_get_sub_obj(rsp_item, "max_mem");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            int64_t db_max_size = pf_json_get_int64(item);
            if (server.is_slave == 0 && gDsCtrl.ip_cnt > 1)
            {
                /* master, according to mngr */
                db_max_size = db_max_size * 5 / 4;
                db_max_size = db_max_size / gDsCtrl.ip_cnt;
            }
            if (db_max_size > server.db_min_size && db_max_size != server.db_max_size)
            {
                server.db_max_size = db_max_size;
            }
        }
        else
        {
            log_error("%s", "max_mem illegal value");
        }
    }
    gDsCtrl.max_mem = server.db_max_size;

    /* config argument */

    if (local_conf == 0)
    {
    /* app_path */
    item = pf_json_get_sub_obj(config, "app_path");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_STRING)
        {
            const char *str = pf_json_get_str(item);
            if (*str == '/' || strncmp(str, "./", 2) == 0)
            {
                if (server.app_path)
                {
                    zfree(server.app_path);
                }
                server.app_path = zstrdup(str);
            }
        }
    }

    /* dbe_root_path */
    item = pf_json_get_sub_obj(config, "dbe_root_path");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_STRING)
        {
            const char *str = pf_json_get_str(item);
            if (*str == '/' || strncmp(str, "./", 2) == 0)
            {
                if (server.dbe_root_path)
                {
                    zfree(server.dbe_root_path);
                }
                server.dbe_root_path = zstrdup(str);
            }
        }
    }

    /* binlog_root_path */
    item = pf_json_get_sub_obj(config, "binlog_root_path");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_STRING)
        {
            const char *str = pf_json_get_str(item);
            if (*str == '/' || strncmp(str, "./", 2) == 0)
            {
                if (server.bl_root_path)
                {
                    zfree(server.bl_root_path);
                }
                server.bl_root_path = zstrdup(str);
            }
        }
    }

    /* binlog_prefix */
    item = pf_json_get_sub_obj(config, "binlog_prefix");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_STRING)
        {
            const char *str = pf_json_get_str(item);
            if (*str != '\0')
            {
                if (server.binlog_prefix)
                {
                    zfree(server.binlog_prefix);
                }
                server.binlog_prefix = zstrdup(str);
            }
        }
    }

    /* binlog_max_size */
    item = pf_json_get_sub_obj(config, "binlog_max_size");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            int64_t i = pf_json_get_int64(item);
            if (i > 0)
            {
                if (i < 100 * 1024 * 1024)
                {
                    i = 100 * 1024 * 1024;
                }
                if (i > 2000 * 1024 * 1024)
                {
                    /* file size can't exceed 2G */
                    i = 2000 * 1024 * 1024;
                }
                server.binlog_max_size = (int)i;
            }
        }
    }

    /* update_cp_interval */
    item = pf_json_get_sub_obj(config, "update_cp_interval");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            const int32_t i = pf_json_get_int(item);
            if (i > 0 && i < INT_MAX / 1000)
            {
                server.upd_cp_timeout = i;
            }
        }
    }
    gDsCtrl.upd_cp_timeout = server.upd_cp_timeout;

    /* update_cp_threshold */
    item = pf_json_get_sub_obj(config, "update_cp_threshold");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            const int64_t i = pf_json_get_int64(item);
            if (i > 0)
            {
                server.op_max_num = i;
            }
        }
    }
    gDsCtrl.op_max_num = server.op_max_num;

    /* pid_file */
    item = pf_json_get_sub_obj(config, "pid_file");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_STRING)
        {
            const char *str = pf_json_get_str(item);
            if (*str != '\0')
            {
                if (server.pidfile)
                {
                    zfree(server.pidfile);
                }
                server.pidfile = zstrdup(str);
            }
        }
    }

    /* log_dir */
    item = pf_json_get_sub_obj(config, "log_dir");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_STRING)
        {
            const char *str = pf_json_get_str(item);
            if (*str == '/' || strncmp(str, "./", 2) == 0)
            {
                if (server.log_dir)
                {
                    zfree(server.log_dir);
                }
                server.log_dir = zstrdup(str);
            }
        }
    }

    /* log_file */
    item = pf_json_get_sub_obj(config, "log_file");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_STRING)
        {
            const char *str = pf_json_get_str(item);
            if (*str != '\0')
            {
                if (server.log_prefix)
                {
                    zfree(server.log_prefix);
                }
                server.log_prefix = zstrdup(str);
            }
        }
    }

    /* log_file_len */
    item = pf_json_get_sub_obj(config, "log_file_len");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            server.log_file_len = pf_json_get_int(item);
            if ((sizeof(long) == 4)
                && (server.log_file_len == 0 || server.log_file_len > 2000)
               )
            {
                /* 32bit platform, file size can't exceed 2G */
                server.log_file_len = 2000;
            }
        }
    }

    /* daemonize */
    item = pf_json_get_sub_obj(config, "daemonize");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            server.daemonize = pf_json_get_int(item);
        }
    }

    /* maxclients */
    item = pf_json_get_sub_obj(config, "maxclients");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            const int maxclients = pf_json_get_int(item);
            if (maxclients > 0)
            {
                server.maxclients = maxclients;
            }
            else
            {
                // keep default (10000)
            }
        }
    }

    /* timeout */
    item = pf_json_get_sub_obj(config, "timeout");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            server.maxidletime = pf_json_get_int(item);
        }
    }

    /* maxmemory */
    item = pf_json_get_sub_obj(config, "maxmemory");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            server.maxmemory = pf_json_get_int(item);
        }
    }

    /* maxmemory_policy */
    item = pf_json_get_sub_obj(config, "maxmemory_policy");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_STRING)
        {
            const char* t = pf_json_get_str(item);
            if (!strcasecmp(t, "volatile-lru"))
            {
                server.maxmemory_policy = REDIS_MAXMEMORY_VOLATILE_LRU;
            }
            else if (!strcasecmp(t, "volatile-random"))
            {
                server.maxmemory_policy = REDIS_MAXMEMORY_VOLATILE_RANDOM;
            }
            else if (!strcasecmp(t, "volatile-ttl"))
            {
                server.maxmemory_policy = REDIS_MAXMEMORY_VOLATILE_TTL;
            }
            else if (!strcasecmp(t, "allkeys-lru"))
            {
                server.maxmemory_policy = REDIS_MAXMEMORY_ALLKEYS_LRU;
            }
            else if (!strcasecmp(t, "allkeys-random"))
            {
                server.maxmemory_policy = REDIS_MAXMEMORY_ALLKEYS_RANDOM;
            }
            else if (!strcasecmp(t, "noeviction"))
            {
                server.maxmemory_policy = REDIS_MAXMEMORY_NO_EVICTION;
            }
        }
    }

    /* maxmemory_samples */
    item = pf_json_get_sub_obj(config, "maxmemory_samples");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            server.maxmemory_samples = pf_json_get_int(item);
        }
    }

    /* slowlog_log_slower_than */
    item = pf_json_get_sub_obj(config, "slowlog_log_slower_than");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            server.slowlog_log_slower_than = pf_json_get_int64(item);
        }
    }
    gDsCtrl.slowlog_log_slower_than = server.slowlog_log_slower_than;

    /* slowlog_max_len */
    item = pf_json_get_sub_obj(config, "slowlog_max_len");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            server.slowlog_max_len = (unsigned long)pf_json_get_int64(item);
        }
    }

    /* reuse_querybuf */
    item = pf_json_get_sub_obj(config, "reuse_querybuf");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            server.querybuf_reuse = pf_json_get_int(item);
        }
    }
    }

    /* read_only */
    item = pf_json_get_sub_obj(config, "read_only");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            const int read_only = pf_json_get_int(item);
            server.read_only = read_only ? 1 : 0;
        }
    }

    /* wr_bl */
    item = pf_json_get_sub_obj(config, "wr_bl");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            /* wr_bl */
            server.wr_bl = pf_json_get_int(item) ? 1 : 0;
        }
        else
        {
            log_error("%s", "wr_bl illegal value");
        }
    }
    gDsCtrl.wr_bl = server.wr_bl;

    /* dbe_get_que_size */
    item = pf_json_get_sub_obj(config, "dbe_get_que_size");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            const int tmp = pf_json_get_int(item);
            if (tmp > 0)
            {
                server.dbe_get_que_size = tmp;
            }
        }
    }
    gDsCtrl.dbe_get_que_size = server.dbe_get_que_size;

    /* wr_bl_que_size */
    item = pf_json_get_sub_obj(config, "wr_bl_que_size");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            const int tmp = pf_json_get_int(item);
            if (tmp > 0)
            {
                server.wr_bl_que_size = tmp;
            }
        }
    }
    gDsCtrl.wr_bl_que_size = server.wr_bl_que_size;

    /* slow_log */
    item = pf_json_get_sub_obj(config, "slow_log");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            /* slow_log */
            server.slow_log = pf_json_get_int(item) ? 1 : 0;
        }
        else
        {
            log_error("%s", "slow_log illegal value");
        }
    }
    gDsCtrl.slow_log = server.slow_log;

    /* compress */
    item = pf_json_get_sub_obj(config, "compress");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            /* compress */
            server.rdbcompression = pf_json_get_int(item) ? 1 : 0;
        }
        else
        {
            log_error("%s", "compress illegal value");
        }
    }
    gDsCtrl.rdb_compression = server.rdbcompression;

    /* requirepass */
    item = pf_json_get_sub_obj(config, "requirepass");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_STRING)
        {
            const char* t = pf_json_get_str(item);
            if (strlen(t) > 0)
            {
                server.requirepass = zstrdup(t);
            }
        }
    }

    /* log_get_miss */
    item = pf_json_get_sub_obj(config, "log_get_miss");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            /* log_get_miss */
            server.log_get_miss = pf_json_get_int(item) ? 1 : 0;
        }
        else
        {
            log_error("%s", "log_get_miss illegal value");
        }
    }
    gDsCtrl.log_get_miss = server.log_get_miss;

    /* dbe_hot_level */
    item = pf_json_get_sub_obj(config, "dbe_hot_level");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            const int dbe_hot_level = pf_json_get_int(item);
            if (16 > dbe_hot_level && 0 <= dbe_hot_level)
            {
                server.dbe_hot_level = dbe_hot_level;
            }
        }
    }

    /* load_hot_key_max_num */
    item = pf_json_get_sub_obj(config, "load_hot_key_max_num");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            const long long load_hot_key_max_num = (long long)pf_json_get_int64(item);
            if (0 < load_hot_key_max_num)
            {
                server.load_hot_key_max_num = load_hot_key_max_num;
            }
        }
    }

    /* read_dbe */
    item = pf_json_get_sub_obj(config, "read_dbe");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            const int read_dbe = pf_json_get_int(item);
            server.read_dbe = read_dbe ? 1 : 0;
        }
    }
    gDsCtrl.read_dbe = server.read_dbe;

    /* log_level */
    item = pf_json_get_sub_obj(config, "log_level");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_STRING)
        {
            const char* t = pf_json_get_str(item);
            gDsCtrl.log_level = zstrdup(t);
            const int verbosity = get_log_level(t);
            if (verbosity != -1)
            {
                server.verbosity = verbosity;
                pf_log_set_level(verbosity);
            }
        }
    }

    /* heart_beat_interval */
    item = pf_json_get_sub_obj(config, "heart_beat_interval");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            server.period = pf_json_get_int(item);
            if (server.period <= 0)
            {
                server.period = 1;
            }
        }
    }
    gDsCtrl.period = server.period;

    /* mngr_list */
    item = pf_json_get_sub_obj(config, "mngr_list");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_STRING)
        {
            const char *str = pf_json_get_str(item);
            if (*str != '\0'
                && (!server.mngr_list
                    || (server.mngr_list && strcmp(server.mngr_list, str))
                   )
               )
            {
                if (server.mngr_list)
                {
                    zfree(server.mngr_list);
                }
                server.mngr_list = zstrdup(str);
                set_mngr_list2(server.mngr_list);
            }
        }
    }

    /* log_hb_msg */
    item = pf_json_get_sub_obj(config, "log_hb_msg");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            scHbLogFlg = pf_json_get_int(item) ? 1 : 0;
        }
    }

    /* load_bl */
    item = pf_json_get_sub_obj(config, "load_bl");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            server.load_bl = pf_json_get_int(item) ? 1 : 0;
        }
        else
        {
            log_error("%s", "load_bl illegal value");
        }
    }

    /* load_bl_cnt */
    item = pf_json_get_sub_obj(config, "load_bl_cnt");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            server.load_bl_cnt = pf_json_get_int(item);
            if (server.load_bl_cnt > BL_MAX_IDX)
            {
                server.load_bl_cnt = BL_MAX_IDX;
            }
        }
    }

    /* auto_purge */
    item = pf_json_get_sub_obj(config, "auto_purge");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            server.auto_purge = pf_json_get_int(item);
        }
    }

    /* dbe_fsize */
    item = pf_json_get_sub_obj(config, "dbe_fsize");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            server.dbe_fsize = pf_json_get_int(item);
        }
    }

    /* dbnum */
    item = pf_json_get_sub_obj(config, "dbnum");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            const int dbnum = pf_json_get_int(item);
            if (dbnum < 0x80 && dbnum > 0)
            {
                server.dbnum = dbnum;
            }
        }
    }

    /* prtcl_redis */
    item = pf_json_get_sub_obj(config, "prtcl_redis");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            server.prtcl_redis = pf_json_get_int(item);
            if (server.prtcl_redis == 1)
            {
                server.enc_kv = 1;
            }
        }
    }

    /* bl_filter_perm */
    item = pf_json_get_sub_obj(config, "bl_filter_perm");
    if (item)
    {
        int perm = -2;
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            perm = pf_json_get_int(item);
        }
        if (perm == 0 || perm == 1)
        {
            /* bl_filter_list */
            item = pf_json_get_sub_obj(config, "bl_filter_list");
            if (item)
            {
                const char *str = pf_json_get_str(item);
                set_bl_filter_list(str, strlen(str), perm);
            }
            else
            {
                set_bl_filter_list(0, 0, perm);
            }
        }
        else if (perm == -1)
        {
            set_bl_filter_list(0, 0, -1);
        }
    }

    /* sync_filter_perm */
    item = pf_json_get_sub_obj(config, "sync_filter_perm");
    if (item)
    {
        int perm = -2;
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            perm = pf_json_get_int(item);
        }
        if (perm == 0 || perm == 1)
        {
            /* sync_filter_list */
            item = pf_json_get_sub_obj(config, "sync_filter_list");
            if (item)
            {
                const char *str = pf_json_get_str(item);
                set_sync_filter_list(str, strlen(str), perm);
            }
            else
            {
                set_sync_filter_list(0, 0, perm);
            }
        }
        else if (perm == -1)
        {
            set_sync_filter_list(0, 0, -1);
        }
    }

    /* cache_filter_perm */
    item = pf_json_get_sub_obj(config, "cache_filter_perm");
    if (item)
    {
        int perm = -2;
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            perm = pf_json_get_int(item);
        }
        if (perm == 0 || perm == 1)
        {
            /* cache_filter_list */
            item = pf_json_get_sub_obj(config, "cache_filter_list");
            if (item)
            {
                const char *str = pf_json_get_str(item);
                set_cache_filter_list(str, strlen(str), perm);
            }
            else
            {
                set_cache_filter_list(0, 0, perm);
            }
        }
        else if (perm == -1)
        {
            set_cache_filter_list(0, 0, -1);
        }
    }

#if 0
    /* ignore sync_list in this rsp because system has not been ready yet */
    /* sync_list */
    item = pf_json_get_sub_obj(rsp_item, "sync_list");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_ARRAY)
        {
            pf_json_object_t *sync_item = 0;
            int i = 0;
            while ((sync_item = pf_json_array_obj_get_item(item, i++)) != 0)
            {
                pf_json_object_t *sync_item_member = 0;

                sync_item_member = pf_json_get_sub_obj(sync_item, "master_port");
                if (sync_item_member == 0)
                {
                    continue;
                }
                if (pf_json_get_obj_type(sync_item_member) != PF_JSON_TYPE_INT)
                {
                    pf_json_put_obj(sync_item_member);
                    continue;
                }
                const int port = pf_json_get_int(sync_item_member);
                pf_json_put_obj(sync_item_member);

                sync_item_member = pf_json_get_sub_obj(sync_item, "master_ip");
                if (sync_item_member == 0)
                {
                    continue;
                }
                if (pf_json_get_obj_type(sync_item_member) != PF_JSON_TYPE_STRING)
                {
                    pf_json_put_obj(sync_item_member);
                    continue;
                }
                const char *ip = pf_json_get_str(sync_item_member);
                const int ret = start_sync(ip, port, 1, 1, 0);
                if (ret != 0)
                {
                    log_error("start_sync() fail for %s:%d, ret=%d", ip, port, ret);
                }
                pf_json_put_obj(sync_item_member);

                pf_json_put_obj(sync_item);
            }
        }
        pf_json_put_obj(item);
    }
#endif

    write_hbrsp_to_file(pf_json_get_str(rsp_obj));

init_with_mngr_fail:
    pf_json_put_obj(rsp_obj);

    return result;
}

int start_heart_beat(pthread_t *tid)
{
    int fds[2];
    if (pipe(fds))
    {
        log_error("pipe() fail: %s", strerror(errno));
        return -1;
    }
    server.hb_recv_fd = fds[0];
    server.hb_send_fd = fds[1];
    if (aeCreateFileEvent(server.el, server.hb_recv_fd, AE_READABLE, rsp_hb, 0)
        == AE_ERR)
    {
        log_error("aeCreateFileEvent() fail for hb_recv_fd(%d)", fds[0]);
        return -1;
    }

    int ret;
    do
    {
        ret = pthread_create(tid, 0, heart_beat_task, 0);
        if (ret != 0)
        {
            log_error("%s", "pthread_create() fail for heart beat with mngr-server");
        }
    } while (ret);

    log_prompt("hb_pthread_id=%lu, hb_recv_fd=%d, hb_send_fd=%d"
            , *tid, server.hb_recv_fd, server.hb_send_fd);

    return ret;
}

extern void linux_thread_setname(char const* threadName);
static void hb_with_mngr();
static void *heart_beat_task(void *arg)
{
    linux_thread_setname("heart_beat");
    log_prompt2(-1, "hear_beat_task running...(interval=%ds), pthread_id=%lu"
            , gDsCtrl.period, pthread_self());
    REDIS_NOTUSED(arg);

    for (;;)
    {
        msleep(gDsCtrl.period * 1000);
        hb_with_mngr();
    }

    return (void *)0;
}

static int get_log_level(const char *str)
{
    int log_level = -1;
    if (!strcasecmp(str, "debug"))
    {
        log_level = REDIS_DEBUG;
    }
    else if (!strcasecmp(str, "verbose"))
    {
        log_level = REDIS_VERBOSE;
    }
    else if (!strcasecmp(str, "notice"))
    {
        log_level = REDIS_NOTICE;
    }
    else if (!strcasecmp(str, "warning"))
    {
        log_level = REDIS_WARNING;
    }

    return log_level;
}

static void hb_with_mngr()
{
    ds_stat_info stat;
    ds_status_info status;
    lock_stat_info();
    memcpy(&stat, &gDsStat, sizeof(stat));
    memcpy(&status, &gDsStatus, sizeof(status));
    unlock_stat_info();

    /* make heart-beat req msg */
    pf_json_object_t *req_obj = pf_json_create_obj();
    pf_json_add_sub_obj(req_obj, "address", pf_json_create_str_obj(server.host));
    pf_json_add_sub_obj(req_obj, "port", pf_json_create_int_obj(server.port));

    pf_json_object_t *app_info = pf_json_create_obj();
    pf_json_add_sub_obj(app_info, "app_id", pf_json_create_str_obj(server.app_id));
    pf_json_add_sub_obj(app_info, "version", pf_json_create_int64_obj(gHbVersion));
    pf_json_add_sub_obj(app_info, "id", pf_json_create_int_obj(0));
    pf_json_add_sub_obj(app_info, "used_mem", pf_json_create_int64_obj(stat.db_used_mem));

    if (lock_sync_status() == 0)
    {
        pf_json_object_t *sync_status_list = pf_json_create_array_obj();

        sync_status_node *node = gSyncStatusMngr.head;
        while (node)
        {
            pf_json_object_t *sync_status = pf_json_create_obj();
            pf_json_add_sub_obj(sync_status, "ip", pf_json_create_str_obj(node->master_ip));
            pf_json_add_sub_obj(sync_status, "port", pf_json_create_int_obj(node->master_port));
            pf_json_add_sub_obj(sync_status, "status", pf_json_create_int_obj(node->status));
            if (node->status == SYNC_STATUS_SYNCED
                || node->status == SYNC_STATUS_DOING
               )
            {
                pf_json_add_sub_obj(sync_status, "bl_tag", pf_json_create_int64_obj(node->bl_tag));
            }
            pf_json_array_obj_add_item(sync_status_list, sync_status);

            node = node->next;
        }

        pf_json_add_sub_obj(app_info, "sync_status_list", sync_status_list);

        unlock_sync_status();
    }

    if (lock_repl_status() == 0)
    {
        pf_json_object_t *repl_status_list = pf_json_create_array_obj();

        repl_status_node *node = gReplStatusMngr.head;
        while (node)
        {
            pf_json_object_t *repl_status = pf_json_create_obj();
            pf_json_add_sub_obj(repl_status, "ip", pf_json_create_str_obj(node->master_ip));
            pf_json_add_sub_obj(repl_status, "port", pf_json_create_int_obj(node->master_port));
            pf_json_add_sub_obj(repl_status, "status", pf_json_create_int_obj(node->status));
            pf_json_add_sub_obj(repl_status, "path", pf_json_create_str_obj(node->path));
            if (node->status == REPL_STATUS_STOPED)
            {
                pf_json_add_sub_obj(repl_status, "bl_tag", pf_json_create_int64_obj(node->bl_tag));
            }
            pf_json_array_obj_add_item(repl_status_list, repl_status);

            node = node->next;
        }

        pf_json_add_sub_obj(app_info, "repl_status_list", repl_status_list);

        unlock_repl_status();
    }

    /* add other status info at 2014.04.28 */
    pf_json_object_t *status_info = pf_json_create_obj();
    pf_json_add_sub_obj(status_info, "soft_ver", pf_json_create_str_obj(status.soft_ver));
    pf_json_add_sub_obj(status_info, "pid", pf_json_create_int64_obj(status.pid));
    pf_json_add_sub_obj(status_info, "block_upd_cp", pf_json_create_int_obj(status.block_upd_cp));
    pf_json_add_sub_obj(status_info, "upd_cp_status", pf_json_create_int_obj(status.upd_cp_status));
    pf_json_add_sub_obj(status_info, "arch_bits", pf_json_create_int_obj(status.arch_bits));
    pf_json_add_sub_obj(app_info, "status_info", status_info);

    /* add other stat info at 2014.01.09 */
    pf_json_object_t *stat_info = pf_json_create_obj();
    pf_json_add_sub_obj(stat_info, "uptime", pf_json_create_int64_obj(stat.uptime));
    //pf_json_add_sub_obj(stat_info, "pid", pf_json_create_int64_obj(stat.pid));
    pf_json_add_sub_obj(stat_info, "z_used_mem", pf_json_create_int64_obj(stat.used_mem));
    pf_json_add_sub_obj(stat_info, "peak_mem", pf_json_create_int64_obj(stat.peak_mem));
    pf_json_add_sub_obj(stat_info, "rss_mem", pf_json_create_int64_obj(stat.rss_mem));
    pf_json_add_sub_obj(stat_info, "max_fd", pf_json_create_int_obj(stat.max_fd));
    pf_json_add_sub_obj(stat_info, "active_clients", pf_json_create_int_obj(stat.active_clients));
    pf_json_add_sub_obj(stat_info, "total_connections_received", pf_json_create_int64_obj(stat.total_connections_received));
    pf_json_add_sub_obj(stat_info, "total_commands_processed", pf_json_create_int64_obj(stat.total_commands_processed));
    pf_json_add_sub_obj(stat_info, "total_commands_processed_dur", pf_json_create_int64_obj(stat.total_commands_processed_dur));
    pf_json_add_sub_obj(stat_info, "total_keys", pf_json_create_int64_obj(stat.keys));
    pf_json_add_sub_obj(stat_info, "in_expire_keys", pf_json_create_int64_obj(stat.in_expire_keys));
    pf_json_add_sub_obj(stat_info, "dirty", pf_json_create_int64_obj(stat.dirty));
    pf_json_add_sub_obj(stat_info, "hits", pf_json_create_int64_obj(stat.hits));
    pf_json_add_sub_obj(stat_info, "misses", pf_json_create_int64_obj(stat.misses));
    pf_json_add_sub_obj(stat_info, "del_lru", pf_json_create_int64_obj(stat.del_lru));
    pf_json_add_sub_obj(stat_info, "expired_keys", pf_json_create_int64_obj(stat.expired_keys));
    pf_json_add_sub_obj(stat_info, "evicted_keys", pf_json_create_int64_obj(stat.evicted_keys));
    pf_json_add_sub_obj(stat_info, "get_cmds", pf_json_create_int64_obj(stat.get_cmds));
    pf_json_add_sub_obj(stat_info, "set_cmds", pf_json_create_int64_obj(stat.set_cmds));
    pf_json_add_sub_obj(stat_info, "touch_cmds", pf_json_create_int64_obj(stat.touch_cmds));
    pf_json_add_sub_obj(stat_info, "incr_cmds", pf_json_create_int64_obj(stat.incr_cmds));
    pf_json_add_sub_obj(stat_info, "decr_cmds", pf_json_create_int64_obj(stat.decr_cmds));
    pf_json_add_sub_obj(stat_info, "delete_cmds", pf_json_create_int64_obj(stat.delete_cmds));
    pf_json_add_sub_obj(stat_info, "auth_cmds", pf_json_create_int64_obj(stat.auth_cmds));
    pf_json_add_sub_obj(stat_info, "get_hits", pf_json_create_int64_obj(stat.get_hits));
    pf_json_add_sub_obj(stat_info, "get_misses", pf_json_create_int64_obj(stat.get_misses));
    pf_json_add_sub_obj(stat_info, "touch_hits", pf_json_create_int64_obj(stat.touch_hits));
    pf_json_add_sub_obj(stat_info, "touch_misses", pf_json_create_int64_obj(stat.touch_misses));
    pf_json_add_sub_obj(stat_info, "incr_hits", pf_json_create_int64_obj(stat.incr_hits));
    pf_json_add_sub_obj(stat_info, "incr_misses", pf_json_create_int64_obj(stat.incr_misses));
    pf_json_add_sub_obj(stat_info, "decr_hits", pf_json_create_int64_obj(stat.decr_hits));
    pf_json_add_sub_obj(stat_info, "decr_misses", pf_json_create_int64_obj(stat.decr_misses));
    pf_json_add_sub_obj(stat_info, "cas_hits", pf_json_create_int64_obj(stat.cas_hits));
    pf_json_add_sub_obj(stat_info, "cas_misses", pf_json_create_int64_obj(stat.cas_misses));
    pf_json_add_sub_obj(stat_info, "cas_badval", pf_json_create_int64_obj(stat.cas_badval));
    pf_json_add_sub_obj(stat_info, "delete_hits", pf_json_create_int64_obj(stat.delete_hits));
    pf_json_add_sub_obj(stat_info, "delete_misses", pf_json_create_int64_obj(stat.delete_misses));
    pf_json_add_sub_obj(stat_info, "auth_errors", pf_json_create_int64_obj(stat.auth_errors));

    pf_json_add_sub_obj(stat_info, "expired_unfetched", pf_json_create_int64_obj(stat.expired_unfetched));
    pf_json_add_sub_obj(stat_info, "evicted_unfetched", pf_json_create_int64_obj(stat.evicted_unfetched));
    pf_json_add_sub_obj(stat_info, "bytes_read", pf_json_create_int64_obj(stat.bytes_read));
    pf_json_add_sub_obj(stat_info, "bytes_written", pf_json_create_int64_obj(stat.bytes_written));
    pf_json_add_sub_obj(stat_info, "rejected_conns", pf_json_create_int64_obj(stat.rejected_conns));
    pf_json_add_sub_obj(stat_info, "accepting_conns", pf_json_create_int_obj(stat.accepting_conns));
    pf_json_add_sub_obj(app_info, "stat_info", stat_info);


    pf_json_object_t *app_info_list = pf_json_create_array_obj();
    pf_json_array_obj_add_item(app_info_list, app_info);

    pf_json_add_sub_obj(req_obj, "app_info_list", app_info_list);

    /* handle the rsp */
    pf_json_object_t *rsp_obj = send_hb_to_mngr(req_obj, 0);
    pf_json_put_obj(req_obj);
    if (rsp_obj == 0)
    {
        /* fail */
        log_error2(-1, "%s", "can't get rsp from mngr after sent req");
        return;
    }

    pf_json_object_t *config = pf_json_get_sub_obj(rsp_obj, "config");
    if (config == 0)
    {
        /* fail */
        log_error2(-1, "json parse config fail, rsp:\n%s", pf_json_get_str(rsp_obj));
        pf_json_put_obj(rsp_obj);
        return;
    }

    pf_json_object_t *rsp_list = pf_json_get_sub_obj(rsp_obj, "rsp_list");
    if (rsp_list == 0)
    {
        /* fail */
        log_error2(-1, "json parse rsp_list fail, rsp:\n%s", pf_json_get_str(rsp_obj));
        pf_json_put_obj(rsp_obj);
        return;
    }

    pf_json_object_t *rsp_item = pf_json_array_obj_get_item(rsp_list, 0);
    if (rsp_item == 0)
    {
        /* fail */
        log_error2(-1, "json get array item 0 from rsp_list fail, rsp_list:\n%s", pf_json_get_str(rsp_list));
        pf_json_put_obj(rsp_obj);
        return;
    }

    pf_json_object_t *item = 0;

    /* result */
    item = pf_json_get_sub_obj(rsp_item, "result");
    if (item == 0)
    {
        /* result null,illegal, mandotory */
        log_error2(-1, "result lose\n%s", pf_json_get_str(rsp_item));
        goto illegal_msg;
    }
    if (pf_json_get_obj_type(item) != PF_JSON_TYPE_INT)
    {
        /* illegal */
        log_error2(-1, "%s", "result illegal value");
        goto illegal_msg;
    }
    const int rslt = pf_json_get_int(item);
    if (rslt != 0)
    {
        if (rslt == 12)
        {
            /* app not exist */
            gDsCtrl.app_status = 2;
        }
        log_error2(-1, "mngr rsp result=%d", rslt);
        goto illegal_msg;
    }

    /* app_id */
    item = pf_json_get_sub_obj(rsp_item, "app_id");
    if (item == 0)
    {
        /* app_id null,illegal, mandotory */
        log_error2(-1, "app_id lose\n%s", pf_json_get_str(rsp_item));
        goto illegal_msg;
    }
    if (pf_json_get_obj_type(item) != PF_JSON_TYPE_STRING)
    {
        /* app_id illegal */
        log_error2(-1, "%s", "app_id illegal value");
        goto illegal_msg;
    }
    if (strcmp(server.app_id, pf_json_get_str(item)))
    {
        log_error2(-1, "app_id not match:%s, %s(in req)", server.app_id, pf_json_get_str(item));
        goto illegal_msg;
    }

    /* status */
    item = pf_json_get_sub_obj(rsp_item, "status");
    if (item == 0)
    {
        /* status null,illegal, mandotory */
        log_error2(-1, "status lose\n%s", pf_json_get_str(rsp_item));
        goto illegal_msg;
    }
    if (pf_json_get_obj_type(item) != PF_JSON_TYPE_INT)
    {
        /* illegal */
        log_error2(-1, "%s", "status illegal value");
        goto illegal_msg;
    }
    const int app_status = pf_json_get_int(item);
    if (app_status != gDsCtrl.app_status)
    {
        log_prompt2(-1, "app_status: %d --> %d", gDsCtrl.app_status, app_status);
        server.app_status = gDsCtrl.app_status = app_status;
    }

    /* version */
    item = pf_json_get_sub_obj(rsp_item, "version");
    if (item == 0)
    {
        /* version null,illegal, mandotory */
        log_error2(-1, "version lose\n%s", pf_json_get_str(rsp_item));
        goto illegal_msg;
    }
    if (pf_json_get_obj_type(item) != PF_JSON_TYPE_INT)
    {
        /* illegal */
        log_error2(-1, "%s", "version illegal value");
        goto illegal_msg;
    }
    int64_t const ver = pf_json_get_int64(item);
    if (ver != gHbVersion)
    {
        log_prompt2(-1, "hb version: %"PRId64" --> %"PRId64, gHbVersion, ver);
        gHbVersion = ver;
    }

    /* LRU_flag */
    item = pf_json_get_sub_obj(rsp_item, "LRU_flag");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            gDsCtrl.LRU_flag = pf_json_get_int(item);
            if (gDsCtrl.LRU_flag)
            {
                gDsCtrl.flag |= LRU_MASK;
            }
        }
        else
        {
            log_error2(-1, "%s", "LRU_flag illegal value");
        }
    }

    /* ds_ip_list */
    item = pf_json_get_sub_obj(rsp_item, "ds_ip_list");
    if (item)
    {
        const char *str = pf_json_get_str(item);
        if (*str != '\0'
            && (!gDsCtrl.dsip_list
                || (gDsCtrl.dsip_list && strcmp(gDsCtrl.dsip_list, str))
               )
           )
        {
            log_prompt("ds_ip_list changed: %s -> %s", gDsCtrl.dsip_list, str);
            if (gDsCtrl.dsip_list)
            {
                zfree(gDsCtrl.dsip_list);
            }
            gDsCtrl.dsip_list = zstrdup(str);

            if (strcmp(str, "{ }") != 0
                && pf_json_get_obj_type(item) != PF_JSON_TYPE_ARRAY)
            {
                /* illegal */
                log_error2(-1, "%s", "ds_ip_list illegal value");
            }
            else
            {
                lock_ctrl_info_wr();
                int ret = save_ds_ip_list(item);
                unlock_ctrl_info();
                if (ret == 0)
                {
                    /* illegal */
                    log_error2(-1, "%s", "save ds_ip_list fail");
                }
                else
                {
                    if (scHbLogFlg)
                    {
                        log_prompt2(-1, "save ds_ip_list return: %02x", (unsigned char)ret);
                    }
                    if (ret & 2)
                    {
                        if (ret & 8)
                        {
                            gDsCtrl.flag |= DS_IP_LIST_INCR_MASK;
                        }
                        else
                        {
                            gDsCtrl.flag |= DS_IP_LIST_MASK;
                        }
                    }
                }
            }
        }
    }

    /* clean_cmd */
    item = pf_json_get_sub_obj(rsp_item, "clean_cmd");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            /* clean */
            const int clean_para = pf_json_get_int(item);
            log_prompt("clean_para=%d", clean_para);
            gDsCtrl.clean_size = clean_para;
            gDsCtrl.flag |= CLEAN_MASK;
        }
        else
        {
            log_error2(-1, "%s", "clean_cmd illegal value");
        }
    }

    if (gDsCtrl.flag)
    {
        // notify main-thread to do
        notify_main_server(server.hb_send_fd, (const void*)&gDsCtrl.flag, sizeof(gDsCtrl.flag), 0);
        gDsCtrl.flag = 0;
    }

    /* min_mem */
    item = pf_json_get_sub_obj(rsp_item, "min_mem");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            const int64_t tmp = pf_json_get_int64(item);
            if (tmp > 11 * 1024 * 1024)
            {
                if (gDsCtrl.min_mem != tmp)
                {
                    log_prompt2(-1, "min_mem: %"PRId64" --> %"PRId64
                            , gDsCtrl.min_mem, tmp);
                    server.db_min_size = gDsCtrl.min_mem = tmp;
                }
            }
        }
        else
        {
            log_error2(-1, "%s", "min_mem illegal value");
        }
    }

    /* max_mem */
    item = pf_json_get_sub_obj(rsp_item, "max_mem");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            int64_t db_max_size = pf_json_get_int64(item);
            if (server.is_slave == 0 && gDsCtrl.ip_cnt > 1)
            {
                /* master, according to mngr */
                db_max_size = db_max_size * 5 / 4;
                db_max_size = db_max_size / gDsCtrl.ip_cnt;
            }
            if (db_max_size > gDsCtrl.min_mem && db_max_size != gDsCtrl.max_mem)
            {
                log_prompt2(-1, "db_max_size: %"PRId64" --> %"PRId64
                            , server.db_max_size, db_max_size);
                server.db_max_size = gDsCtrl.max_mem = db_max_size;
            }
        }
        else
        {
            log_error2(-1, "%s", "max_mem illegal value");
        }
    }

    /* update_cp_interval */
    item = pf_json_get_sub_obj(config, "update_cp_interval");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            const int32_t i = pf_json_get_int(item);
            if (i > 0 && i < INT_MAX / 1000 && gDsCtrl.upd_cp_timeout != i)
            {
                log_prompt2(-1, "upd_cp_timeout: %d --> %d", gDsCtrl.upd_cp_timeout, i);
                server.upd_cp_timeout = gDsCtrl.upd_cp_timeout = i;
            }
        }
    }

    /* update_cp_threshold */
    item = pf_json_get_sub_obj(config, "update_cp_threshold");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            const int64_t i = pf_json_get_int64(item);
            if (i > 0 && gDsCtrl.op_max_num != i)
            {
                log_prompt2(-1, "op_max_num: %"PRId64" --> %"PRId64
                        , gDsCtrl.op_max_num, i);
                server.op_max_num = gDsCtrl.op_max_num = i;
            }
        }
    }

    /* read_dbe */
    item = pf_json_get_sub_obj(config, "read_dbe");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            const int read_dbe = pf_json_get_int(item) ? 1 : 0;
            if (gDsCtrl.read_dbe != read_dbe)
            {
                log_prompt2(-1, "read_dbe: %d --> %d", gDsCtrl.read_dbe, read_dbe);
                server.read_dbe = gDsCtrl.read_dbe = read_dbe;
            }
        }
    }

    /* log_level */
    item = pf_json_get_sub_obj(config, "log_level");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_STRING)
        {
            const char* t = pf_json_get_str(item);
            if (!gDsCtrl.log_level
                || (gDsCtrl.log_level && strcmp(t, gDsCtrl.log_level)))
            {
                log_prompt2(-1, "log_level: %s --> %s"
                        , gDsCtrl.log_level ? gDsCtrl.log_level : "", t);
                if (gDsCtrl.log_level)
                {
                    zfree(gDsCtrl.log_level);
                    gDsCtrl.log_level = 0;
                }
                gDsCtrl.log_level = zstrdup(t);
                const int verbosity = get_log_level(t);
                if (verbosity != -1)
                {
                    server.verbosity = verbosity;
                    pf_log_set_level(verbosity);
                }
            }
        }
    }

    /* heart_beat_interval */
    item = pf_json_get_sub_obj(config, "heart_beat_interval");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            const int period = pf_json_get_int(item);
            if (period > 0 && period != gDsCtrl.period)
            {
                log_prompt2(-1, "heart_beat_interval:%ds --> %ds"
                        , gDsCtrl.period, period);
                server.period = gDsCtrl.period = period;
            }
        }
    }

    /* mngr_list */
    item = pf_json_get_sub_obj(config, "mngr_list");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_STRING)
        {
            const char *new_mngr_list = pf_json_get_str(item);

            if (*new_mngr_list != '\0'
                && (!server.mngr_list
                    || (server.mngr_list && strcmp(server.mngr_list, new_mngr_list))
                   )
               )
            {
                lock_ctrl_info_wr();
                if (server.mngr_list)
                {
                    zfree(server.mngr_list);
                }
                server.mngr_list = zstrdup(new_mngr_list);
                unlock_ctrl_info();
                set_mngr_list2(server.mngr_list);
                log_prompt2(-1, "new mngr_list:%s", server.mngr_list);
            }
        }
    }

    /* log_hb_msg */
    item = pf_json_get_sub_obj(config, "log_hb_msg");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            scHbLogFlg = pf_json_get_int(item) ? 1 : 0;
        }
        else
        {
            log_error2(-1, "%s", "log_hb_msg illegal value");
        }
    }

    /* wr_bl */
    item = pf_json_get_sub_obj(config, "wr_bl");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            /* wr_bl */
            const int tmp = pf_json_get_int(item) ? 1 : 0;
            if (tmp != gDsCtrl.wr_bl)
            {
                log_prompt2(-1, "wr_bl: %d -> %d", gDsCtrl.wr_bl, tmp);
                server.wr_bl = gDsCtrl.wr_bl = tmp;
            }
        }
        else
        {
            log_error2(-1, "%s", "wr_bl illegal value");
        }
    }

    /* dbe_get_que_size */
    item = pf_json_get_sub_obj(config, "dbe_get_que_size");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            const int dbe_get_que_size = pf_json_get_int(item);
            if (dbe_get_que_size > 0 && dbe_get_que_size != gDsCtrl.dbe_get_que_size)
            {
                log_prompt2(-1, "dbe_get_que_size: %d -> %d"
                        , gDsCtrl.dbe_get_que_size, dbe_get_que_size);
                server.dbe_get_que_size = gDsCtrl.dbe_get_que_size = dbe_get_que_size;
            }
        }
    }

    /* wr_bl_que_size */
    item = pf_json_get_sub_obj(config, "wr_bl_que_size");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            const int wr_bl_que_size = pf_json_get_int(item);
            if (wr_bl_que_size > 0 && wr_bl_que_size != gDsCtrl.wr_bl_que_size)
            {
                log_prompt2(-1, "wr_bl_que_size: %d -> %d"
                        , gDsCtrl.wr_bl_que_size, wr_bl_que_size);
                server.wr_bl_que_size = gDsCtrl.wr_bl_que_size = wr_bl_que_size;
            }
        }
    }

    /* slowlog_log_slower_than */
    item = pf_json_get_sub_obj(config, "slowlog_log_slower_than");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            const long long tmp = pf_json_get_int64(item);
            if (tmp != gDsCtrl.slowlog_log_slower_than)
            {
                log_prompt2(-1, "slowlog_log_slower_than: %lld -> %lld"
                        , gDsCtrl.slowlog_log_slower_than, tmp);
                server.slowlog_log_slower_than = gDsCtrl.slowlog_log_slower_than = tmp;
            }
        }
    }

    /* slow_log */
    item = pf_json_get_sub_obj(config, "slow_log");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            /* slow_log */
            const int tmp = pf_json_get_int(item) ? 1 : 0;
            if (tmp != gDsCtrl.slow_log)
            {
                log_prompt2(-1, "slow_log: %d -> %d", gDsCtrl.slow_log, tmp);
                server.slow_log = gDsCtrl.slow_log = tmp;
            }
        }
        else
        {
            log_error2(-1, "%s", "slow_log illegal value");
        }
    }

    /* compress */
    item = pf_json_get_sub_obj(config, "compress");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            /* compress */
            const int tmp = pf_json_get_int(item) ? 1 : 0;
            if (tmp != gDsCtrl.rdb_compression)
            {
                log_prompt2(-1, "compress: %d -> %d", gDsCtrl.rdb_compression, tmp);
                server.rdbcompression = gDsCtrl.rdb_compression = tmp;
            }
        }
        else
        {
            log_error2(-1, "%s", "compress illegal value");
        }
    }

    /* log_get_miss */
    item = pf_json_get_sub_obj(config, "log_get_miss");
    if (item)
    {
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            /* log_get_miss */
            const int tmp = pf_json_get_int(item) ? 1 : 0;
            if (tmp != gDsCtrl.log_get_miss)
            {
                log_prompt2(-1, "log_get_miss: %d -> %d", gDsCtrl.log_get_miss, tmp);
                server.log_get_miss = gDsCtrl.log_get_miss = tmp;
            }
        }
        else
        {
            log_error2(-1, "%s", "log_get_miss illegal value");
        }
    }

    /* sync_list */
    if (server.init_ready == 1)
    {
    static int sc_sync_list_len = 0;
    static char *sc_sync_list = 0;
    item = pf_json_get_sub_obj(rsp_item, "sync_list");
    if (item)
    {
        const char *sync_list = pf_json_get_str(item);
        const int sync_list_len = strlen(sync_list);
        if (sc_sync_list_len != sync_list_len
            || (sc_sync_list == 0 && sync_list != 0)
            || strcmp(sc_sync_list, sync_list) != 0)
        {
            log_debug("o_len=%d, %s, n_len=%d, %s"
                       , sc_sync_list_len, sc_sync_list, sync_list_len, sync_list);
            //log_debug("strcmp()=%d", strcmp("{ }", sync_list));
            int stop_all = 0;
            if (sc_sync_list)
            {
                zfree(sc_sync_list);
                sc_sync_list = 0;
                sc_sync_list_len = 0;

                if (strcmp("{ }", sync_list) == 0)
                {
                    stop_all = 1;
                }
            }
            sc_sync_list_len = sync_list_len;
            sc_sync_list = zstrdup(sync_list);

            log_debug("type(sync_list)=%d", pf_json_get_obj_type(item));
            if (stop_all)
            {
                log_debug("stop all sync_list:%s", sc_sync_list);
                start_syncs(0, 0);
            }
            else if (pf_json_get_obj_type(item) == PF_JSON_TYPE_ARRAY)
            {
                sync_addr *masters = 0;
                const int cnt = pf_json_array_obj_len(item);
                if (cnt > 0)
                {
                    masters = (sync_addr *)zmalloc(sizeof(sync_addr) * cnt);
                }

                if (masters)
                {
                    pf_json_object_t *sync_item = 0;
                    int i = 0, j = 0;
                    while ((sync_item = pf_json_array_obj_get_item(item, i++)) != 0)
                    {
                        pf_json_object_t *sync_item_member = 0;

                        sync_item_member = pf_json_get_sub_obj(sync_item, "master_port");
                        if (sync_item_member == 0)
                        {
                            continue;
                        }
                        if (pf_json_get_obj_type(sync_item_member) != PF_JSON_TYPE_INT)
                        {
                            continue;
                        }
                        const int port = pf_json_get_int(sync_item_member);

                        sync_item_member = pf_json_get_sub_obj(sync_item, "master_ip");
                        if (sync_item_member == 0)
                        {
                            continue;
                        }
                        if (pf_json_get_obj_type(sync_item_member) != PF_JSON_TYPE_STRING)
                        {
                            continue;
                        }
                        const char *ip = pf_json_get_str(sync_item_member);
                        strncpy(masters[j].ip, ip, sizeof(masters[j].ip));
                        masters[j].port = port;
                        j++;
                    }
                    if (j == cnt)
                    {
                        start_syncs(masters, j);
                    }
                    zfree(masters);
                }
            }
        }
        else
        {
            //log_debug("sync_list no change, ignore");
        }
    }
    else
    {
        //log_debug("sync_list lose, do nothing about it");
    }
    }

    /* bl_filter_perm */
    item = pf_json_get_sub_obj(config, "bl_filter_perm");
    if (item)
    {
        int perm = -2;
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            perm = pf_json_get_int(item);
        }
        if (perm == 0 || perm == 1)
        {
            /* bl_filter_list */
            item = pf_json_get_sub_obj(config, "bl_filter_list");
            if (item)
            {
                const char *str = pf_json_get_str(item);
                set_bl_filter_list(str, strlen(str), perm);
            }
            else
            {
                set_bl_filter_list(0, 0, perm);
            }
        }
        else if (perm == -1)
        {
            set_bl_filter_list(0, 0, -1);
        }
    }

    /* sync_filter_perm */
    item = pf_json_get_sub_obj(config, "sync_filter_perm");
    if (item)
    {
        int perm = -2;
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            perm = pf_json_get_int(item);
        }
        if (perm == 0 || perm == 1)
        {
            /* sync_filter_list */
            item = pf_json_get_sub_obj(config, "sync_filter_list");
            if (item)
            {
                const char *str = pf_json_get_str(item);
                set_sync_filter_list(str, strlen(str), perm);
            }
            else
            {
                set_sync_filter_list(0, 0, perm);
            }
        }
        else if (perm == -1)
        {
            set_sync_filter_list(0, 0, -1);
        }
    }

    /* cache_filter_perm */
    item = pf_json_get_sub_obj(config, "cache_filter_perm");
    if (item)
    {
        int perm = -2;
        if (pf_json_get_obj_type(item) == PF_JSON_TYPE_INT)
        {
            perm = pf_json_get_int(item);
        }
        if (perm == 0 || perm == 1)
        {
            /* cache_filter_list */
            item = pf_json_get_sub_obj(config, "cache_filter_list");
            if (item)
            {
                const char *str = pf_json_get_str(item);
                set_cache_filter_list(str, strlen(str), perm);
            }
            else
            {
                set_cache_filter_list(0, 0, perm);
            }
        }
        else if (perm == -1)
        {
            set_cache_filter_list(0, 0, -1);
        }
    }

illegal_msg:
    pf_json_put_obj(rsp_obj);

    return;
}

int get_my_addr()
{
    if (server.host == 0)
    {
        char ip[64];
        if (0 == get_host_ipaddr(ip, sizeof(ip)))
        {
            server.host = zstrdup(ip);
        }
        else
        {
            return 1;
        }
    }

    if (server.ds_key == NULL)
    {
        /* set default ds_key when can't get from network config */
        char buf[256];
        sprintf(buf, "%s:%d", server.host, server.port);
        server.ds_key = zstrdup(buf);
    }

    return 0;
}

void set_mngr_list(const char *mngr_list)
{
    gpMngrs = set_mngr_list_ex(mngr_list, 0);
    scpMngrs = set_mngr_list_ex(mngr_list, 0);
    if (scpMngrs)
    {
        scpMngrs->idx = 0;
    }
}

static void set_mngr_list2(const char *mngr_list)
{
    /* free old at first */
    if (scpMngrs)
    {
        int i;
        for (i = 0; i < scpMngrs->MngrCnt; i++)
        {
            zfree(scpMngrs->Mngrs[i]);
            scpMngrs->Mngrs[i] = 0;
        }
        zfree(scpMngrs);
        scpMngrs = 0;
    }

    scpMngrs = set_mngr_list_ex(mngr_list, gpMngrs);
    if (scpMngrs)
    {
        scpMngrs->idx = 0;
    }
}

static MngrList *set_mngr_list_ex(const char *mngr_list, const MngrList *pList)
{
    if (mngr_list == 0)
    {
        return 0;
    }

    int i;
    int MngrCnt = 0;
    char **list_ar = parse_list_str(mngr_list, &MngrCnt, "\r \n,;\t");
    const int cnt = MngrCnt + (pList ? pList->MngrCnt : 0);

    MngrList *pMngrs = (MngrList *)zmalloc(sizeof(MngrList) + cnt * sizeof(char *));
    if (pMngrs == 0)
    {
        if (list_ar)
        {
            for (i = 0; i < MngrCnt; i++)
            {
                zfree(list_ar[i]);
            }
            zfree(list_ar);
        }
        return 0;
    }
    pMngrs->Mngrs = (char **)((char *)pMngrs + sizeof(*pMngrs));
    pMngrs->MngrCnt = 0;

    if (pList)
    {
        for (i = 0; i < pList->MngrCnt; i++)
        {
            pMngrs->Mngrs[pMngrs->MngrCnt++] = zstrdup(pList->Mngrs[i]);
        }
    }

    for (i = 0; i < MngrCnt; i++)
    {
        int j;
        for (j = 0; j < pMngrs->MngrCnt; j++)
        {
            if (strcmp(pMngrs->Mngrs[j], list_ar[i]) == 0)
            {
                break;
            }
        }
        if (j >= pMngrs->MngrCnt)
        {
            pMngrs->Mngrs[pMngrs->MngrCnt++] = list_ar[i];
        }
        else
        {
            zfree(list_ar[i]);
        }
    }
    zfree(list_ar);

    log_prompt("set_mngr_list_ex: MngrCnt_list=%d, total=%d, MngrCnt=%d, list=%s"
            , MngrCnt, cnt, pMngrs->MngrCnt, mngr_list);

    /* just for test
    int i;
    for (i = 0; i < pMngrs->MngrCnt; i++)
    {
        fprintf(stderr, "No.%d: %s\n", i, pMngrs->Mngrs[i]);
    }
    */

    return pMngrs;
}

void set_ds_list(const char *ds_list)
{
    log_prompt("set_ds_list: %s", ds_list);
    if (ds_list == 0)
    {
        return;
    }

    int i;
    int Cnt = 0;
    char **list_ar = parse_list_str(ds_list, &Cnt, ",");
    const int cnt = Cnt;
    int ip_cnt = 0;

    ds_ip_node **pnode = &gDsCtrl.ds_ip_list_tmp;
    ds_ip_node *node = *pnode;
    for (i = 0; i < cnt; i++)
    {
        //log_prompt("save_ds_ip_list: for(), ip_cnt=%d, i=%d", cnt, i);
        /* ds_ip */
        /* check ds_ip */
        ds_ip_node *n = gDsCtrl.ds_ip_list_tmp;
        while (n)
        {
            if (strcmp(n->ds_ip, list_ar[i]) == 0)
            {
                /* existed */
                break;
            }
            n = n->next;
        }
        if (n)
        {
            /* ignore the existed */
            continue;
        }

        if (node == 0)
        {
            node = (ds_ip_node *)zcalloc(sizeof(ds_ip_node));
            node->next = 0;
            *pnode = node; /* hang upon the node to the tail of the list */
        }
        strcpy(node->ds_ip, list_ar[i]);

        pnode = &node->next;
        node = *pnode;
        ip_cnt++;
    }

    /* relase */
    for (i = 0; i < cnt; i++)
    {
        zfree(list_ar[i]);
        list_ar[i] = NULL;
    }
    zfree(list_ar);
    list_ar = NULL;

    if (node)
    {
        *pnode = 0;
    }
    while (node)
    {
        //log_debug("release node: %p", node);
        ds_ip_node *tmp = node;
        node = node->next;
        zfree(tmp);
    }

    //log_prompt("ip_list_chagned=%d", ip_list_changed);
    {
        ds_ip_node *const tmp = gDsCtrl.ds_ip_list;
        gDsCtrl.ds_ip_list = gDsCtrl.ds_ip_list_tmp;
        gDsCtrl.ds_ip_list_tmp = tmp;

        gDsCtrl.ip_cnt = ip_cnt;

        /* print info */
        char buf_old[1024];
        serialize_ds_ip_list(buf_old, sizeof(buf_old) - 1, gDsCtrl.ds_ip_list_tmp);
        char buf_new[1024];
        serialize_ds_ip_list(buf_new, sizeof(buf_new) - 1, gDsCtrl.ds_ip_list);
        log_prompt("ds_ip_list: %s ==> %s", buf_old, buf_new);
    }

    gDsCtrl.dsip_list = zstrdup(ds_list);
    gen_filter_server();
}

