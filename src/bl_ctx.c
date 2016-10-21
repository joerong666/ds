#include "dbmng.h"
#include "bl_ctx.h"
#include "ds_binlog.h"
#include "bin_log.h"
#include "op_cmd.h"
#include "db_io_engine.h"
#include "key_filter.h"
#include "ds_ctrl.h"
#include "checkpoint.h"
#include "codec_key.h"

#include "ds_log.h"
#include "util.h"
#include "ds_util.h"
#include "serialize.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <time.h>


typedef struct bl_ctx_st
{
    char path[256];

    int cur_cp_idx;
    int cur_cp_offset;
    int new_cp_idx;
    int new_cp_offset;
    int cp_taged;

    int cur_bl_idx;
    int fd_cur; /* file of current binlog(write) */
    int flags; /* open() flags argurment */
} bl_ctx;

typedef struct bl_read_iterator_st
{
    int fd;
    int offset;
} bl_read_iterator;

typedef struct wr_bl_info_st
{
    int fd;
    unsigned char op_flag;
    unsigned char cmd;
    unsigned char db_id;
    unsigned char type;
    int32_t ts;
    int argc;
    robj *key;
    robj **argv;
    unsigned long long ds_id;
    bl_ctx *bl;
#ifndef _UPD_DBE_BY_PERIODIC_
    char *buf;     /* value of string type obtain from rdb */
    int buf_len;
    void *dbe;
#endif
} wr_bl_info;

#ifndef _UPD_DBE_BY_PERIODIC_
typedef struct wr_bl_arg_st
{
    unsigned char op_flag;
    unsigned long long ds_id;
    bl_ctx *bl;
    char *buf;     /* value of string type obtain from rdb */
    int buf_len;
} wr_bl_arg;
#endif

typedef struct async_wr_bl_ctx_st
{
    int cnt;
    wr_bl_info *infos[8];
} async_wr_bl_ctx;

#ifdef _UPD_DBE_BY_PERIODIC_
static async_wr_bl_ctx g_wrbl_ctx;
#else
static void make_list_dbe_data(wr_bl_info *info, upd_dbe_param *param);
static void make_set_dbe_data(wr_bl_info *info, upd_dbe_param *param);
static void make_hash_dbe_data(wr_bl_info *info, upd_dbe_param *param);
static void make_zset_dbe_data(wr_bl_info *info, upd_dbe_param *param);
#endif

static int redo_op_rec(redisDb *rdb, const char *buf, int buf_len);
static int bl_open_file(const char *file, int flags);
static int open_binlog_file(const char *path, const char *prefix, int idx, int flags);
static int get_binlog_file_size(int fd);
//static int write_binlog_file(bl_ctx *bl, char *buf, int len, unsigned long long ds_id, int op_flag);
static int write_binlog_file(bl_ctx *bl, unsigned char cmd, const robj *key, int argc, const robj **argv, unsigned long long ds_id, unsigned char db_id, unsigned char type);

#ifdef _UPD_DBE_BY_PERIODIC_
static int do_op_rec(binlog_tab *blt, const char *buf, int buf_len);
static int load_to_mem(binlog_tab *blt, const char *path, int idx, int offset, int end, int *cnf_cnt)
{
    void *it = bl_get_read_it(path, idx, offset);
    if (it == 0)
    {
        log_error("bl_get_read_it() fail, idx=%d, offset=%d", idx, offset);
        return -1;
    }
    if (cnf_cnt)
    {
        *cnf_cnt = 0;
    }

    int cnt = 0;
    int ret;
    char *buf = 0;
    int buf_len;
    int next_offset;
    while ((ret = bl_get_next_rec(it, &buf, &buf_len, &next_offset)) == 0)
    {
        log_debug("bl_get_next_rec(): buf_len=%d, next_offset=%d", buf_len, next_offset);
        ret = do_op_rec(blt, buf, buf_len);
        bl_release_rec(buf);
        cnt++;
        if (cnf_cnt && ret == 0)
        {
            (*cnf_cnt)++;
        }

        if (end != -1 && end <= next_offset)
        {
            /* up to end, over */
            break;
        }
    }

    bl_release_it(it);

    return cnt;
}

/* return:
 * -1 : fail
 *  0 : confirm info
 *  1 : op cmd
 *  2 : ignore
 */
static int do_op_rec(binlog_tab *blt, const char *buf, int buf_len)
{
    op_rec rec;
    int ret = 0;
    
    //log_buffer(buf, buf_len);

    ret = parse_op_rec(buf, buf_len, &rec);
    if (ret != 0)
    {
        log_error("parse_op_rec() fail, len=%d, ret=%d", buf_len, ret);
        return -1;
    }
    char key[1024];
    snprintf(key, sizeof(key), "%.*s", rec.key.len, rec.key.ptr);
    log_debug("op info in binlog: cmd=%d, argc=%d, key=%s", rec.cmd, rec.argc, key);
    if (rec.type == 0)
    {
        log_info("\'%s\' belong to cache, ignore", key);
        return 2;
    }

    if (cache_filter(rec.key.ptr, rec.key.len) == 1)
    {
        log_info("\'%s\' belong to cache, ignore", key);
        return 2;
    }

    if (rec.cmd == 0)
    {
        ret = blt_cp_confirmed(blt, &rec.key);
    }
    else
    {
        ret = blt_add_op(blt, rec.cmd, &rec.key, rec.argc, rec.argv);
    }

    if (rec.argc > 0)
    {
        /* free memory malloc in parse_op_rec() */
        zfree(rec.argv);
    }

    if (ret != 0)
    {
        log_error("add op to blt fail, ret=%d", ret);
        return -1;
    }

    return rec.cmd == 0 ? 0 : 1;
}
#endif

/* return :
 * -1 : fail
 *  0 : succ
 *  2 : ignore
 */
static int redo_op_rec(redisDb *rdb, const char *buf, int buf_len)
{
    op_rec rec;
    int ret = 0;
    
    //log_buffer(buf, buf_len);

    ret = parse_op_rec(buf, buf_len, &rec);
    if (ret != 0)
    {
        log_error("parse_op_rec() fail, len=%d, ret=%d", buf_len, ret);
        return -1;
    }
    char key[1024];
    snprintf(key, sizeof(key), "%.*s", rec.key.len, rec.key.ptr);
    log_debug("op info in binlog: cmd=%d, argc=%d, key=%s", rec.cmd, rec.argc, key);
    if (rec.type == 1)
    {
        log_info("\'%s\' belong to persistence, ignore", key);
        return 2;
    }

    if (rec.db_id >= server.dbnum)
    {
        log_error("db_id(%d) >= server.dbnum(%d), key=%s", rec.db_id, server.dbnum, key);
        if (rec.argc > 0)
        {
            /* free memory malloc in parse_op_rec() */
            zfree(rec.argv);
        }
        return -1;
    }

    if (rec.cmd != 0)
    {
        robj *obj_k = createStringObject((char *)rec.key.ptr, rec.key.len);
        robj **argv = 0;
        int iserr = 0;
        if (rec.argc > 0)
        {
            argv = (robj **)zcalloc(sizeof(robj *) * rec.argc);
            uint32_t i = 0;
            robj *o = 0;
            for (i = 0; i < rec.argc; i++)
            {
                o = unserializeObj(rec.argv[i].ptr, rec.argv[i].len, 0);
                if (o == 0)
                {
                    iserr = 1;
                    log_error("redo_op_rec: No.%d arg unserializeObj() fail"
                            ", cmd=%d, argc=%d, key=%s"
                            , i, rec.cmd, rec.argc, obj_k->ptr);
                    break;
                }
                argv[i] = getDecodedObject(o);
                decrRefCount(o);
            }
        }
        redisDb *const to_rdb = rdb->id == rec.db_id ? rdb : &server.db[rec.db_id];
        ret = iserr == 0 ? redo_op(to_rdb, obj_k, rec.cmd, rec.argc, argv) : -1;
        if (argv)
        {
            uint32_t i = 0;
            for (i = 0; i < rec.argc; i++)
            {
                decrRefCount(argv[i]);
            }
            zfree(argv);
            argv = 0;
        }
        decrRefCount(obj_k);
    }

    if (rec.argc > 0)
    {
        /* free memory malloc in parse_op_rec() */
        zfree(rec.argv);
    }

    if (ret != 0)
    {
        log_error("redo op fail, ret=%d", ret);
        return -1;
    }

    return 0;
}

const char *get_bl_path(char *path, int path_len, int binlog_type)
{
    static const char *const bl_type[2] = {"local", "sync"};

    snprintf(path
            , path_len
            , "%s%s"
            , dbmng_conf.bl_root_path
            , bl_type[binlog_type]
            );

    return path;
}

const char *make_bl_path(char *path, int path_len, int binlog_type)
{
    get_bl_path(path, path_len, binlog_type);
    check_and_make_dir(path);

    return path;
}

void *bl_init(int master, int binlog_type)
{
    (void)master;
    if (binlog_type < 0 || binlog_type > 1)
    {
        log_error("%s", "illegal parameter");
        return 0;
    }

    bl_ctx *bl = (bl_ctx *)zmalloc(sizeof(*bl));
    if (bl == 0)
    {
        log_error("%s", "zmalloc() fail for bl_ctx");
        return 0;
    }

    /* init */
    bl->cur_cp_idx = 0;
    bl->cur_cp_offset = 0;
    bl->new_cp_idx = -1;
    bl->new_cp_offset = -1;
    bl->cp_taged = 0;
    bl->cur_bl_idx = 0;
    bl->fd_cur = -1;
    bl->flags = O_WRONLY | O_CREAT | O_APPEND;

    make_bl_path(bl->path, sizeof(bl->path), binlog_type);

#ifdef _UPD_DBE_BY_PERIODIC_
    char f[256];
    char line[64];
    char *p = 0;
    FILE *fp = 0;
    
    if (server.has_cache == 1 && server.has_dbe == 1)
    {
    make_cp_file(f, sizeof(f), bl->path, dbmng_conf.log_prefix);
    fp = fopen(f, "r");
    if (!fp)
    {
        if (errno == 2)
        {
            /* no exist */
            log_info("%s not exist", f);
        }
        else
        {
            log_error("fopen(\"r\") %s fail:%s", f, strerror(errno));
            //zfree(bl);
            //return 0;
        }
    }
    else
    {
        /* cp: idx offset */
        p = fgets(line, sizeof(line), fp);
        if (p != NULL)
        {
            trim_bl_line(line);
            //log_debug("the first line in cp file: %s", line);
            int idx = -1, offset = -1;
            parse_bl_list_line(line, &idx, &offset);
            if (idx < 0 || offset < 0)
            {
                log_error("illegal cp(idx offset) in 1st line: %s", line);
                //fclose(fp);
                //zfree(bl);
                //return 0;
            }
            else
            {
                bl->cur_cp_idx = idx;
                bl->cur_cp_offset = offset;
            }
        }
        else
        {
            /* fgets() fail */
            log_error("fgets() ths 1st line fail for cp:%s", strerror(errno));
            //fclose(fp);
            //zfree(bl);
            //return 0;
        }

        /* cp: new_idx new_offset */
        p = fgets(line, sizeof(line), fp);
        fclose(fp);
        if (p != NULL)
        {
            trim_bl_line(line);
            //log_debug("the second line in cp file: %s", line);
            parse_bl_list_line(line, &bl->new_cp_idx, &bl->new_cp_offset);
        }
        else
        {
            /* fgets() fail */
            log_error("fgets() the 2nd line fail for cp:%s", strerror(errno));
        }
    }
    }
#endif

    int cur_bl_idx;
    const int ret = get_bl_idx(bl->path, dbmng_conf.log_prefix, &cur_bl_idx);
    if (ret != 0)
    {
        log_prompt("set cur_bl_idx to default %d", bl->cur_bl_idx);
        //zfree(bl);
        //return 0;
    }
    else
    {
        log_prompt("cur_bl_idx=%d", cur_bl_idx);
        bl->cur_bl_idx = cur_bl_idx;
    }

    return bl;
}

void bl_uninit(void *binlog)
{
    bl_ctx *bl = (bl_ctx *)binlog;

    if (bl->fd_cur != -1)
    {
        close(bl->fd_cur);
    }

    zfree(bl);
}

int bl_load_to_mem(void *binlog, binlog_tab *blt)
{
#ifndef _UPD_DBE_BY_PERIODIC_
    (void)binlog;
    (void)blt;
    return 0;
#else
    if (!(server.has_cache == 1 && server.has_dbe == 1))
    {
        return 0;
    }

    time_t start = time(0);
    log_prompt("begin to load binlog, now=%d", start);

    bl_ctx *bl = (bl_ctx *)binlog;

    size_t cnt = 0;
    size_t total_cnf_cnt = 0;
    int cnf_cnt;
    const int last_idx = GET_BL_NEXT_IDX(bl->cur_bl_idx);
    int offset = bl->cur_cp_offset;
    int idx = bl->cur_cp_idx;
    int cur_cp_offset = offset;
    int cur_cp_idx = idx;
    do
    {
        int end = bl->new_cp_idx == idx ? bl->new_cp_offset : -1;
        const int ret = load_to_mem(blt, bl->path, idx, offset, end, &cnf_cnt);
        if (ret < 0)
        {
            log_error("load_to_mem() fail, idx=%d, offset=%d, end=%d", idx, offset, end);
            return 0;
        }
        cnt += (size_t)ret;
        total_cnf_cnt += cnf_cnt;

        if (idx == bl->new_cp_idx)
        {
            /* there is some unconfirmed binlog, move it to immutable */
            log_prompt("%zd binlog_rec need to confirm, %d:%d -> %d:%d",
                       cnt, bl->cur_cp_idx, bl->cur_cp_offset, bl->new_cp_idx, bl->new_cp_offset);
            cnt = 0;
            total_cnf_cnt = 0;

            bl->cp_taged = 1;
            if (0 != switch_blt_only(0, blt))
            {
            }

            offset = end;
            end = -1;
            cur_cp_idx = idx;
            cur_cp_offset = offset;
            const int ret = load_to_mem(blt, bl->path, idx, offset, end, &cnf_cnt);
            if (ret < 0)
            {
                log_error("load_to_mem() fail, idx=%d, offset=%d, end=%d", idx, offset, end);
                return 0;
            }
            cnt += (size_t)ret;
            total_cnf_cnt += cnf_cnt;
        }

        offset = 0;
        idx = GET_BL_NEXT_IDX(idx);
    } while (idx != last_idx);

    log_prompt("load binlog to binlogtab from (%d,%d): "
               "during=%d(s), bl_rec_cnt=%zd, op_cnt_act=%zd, confirmed_flg_cnt=%zd"
               , cur_cp_idx, cur_cp_offset, time(0) - start, cnt, blt->op_cnt_act, total_cnf_cnt);

    return 0;
#endif
}

int tag_new_cp(void *binlog, int new_idx, int new_offset)
{
    if (binlog == 0)
    {
        return -1;
    }

    bl_ctx *bl = (bl_ctx *)binlog;

    bl->cp_taged = 0;
    bl->new_cp_idx = new_idx == -1 ? bl->cur_bl_idx : new_idx;
    if (new_offset == -1)
    {
        if (bl->fd_cur == -1)
        {
            /* open it at first */
            bl->fd_cur = open_binlog_file(bl->path, dbmng_conf.log_prefix, bl->cur_bl_idx, bl->flags);
        }
        bl->new_cp_offset = get_binlog_file_size(bl->fd_cur);
    }
    else
    {
        bl->new_cp_offset = new_offset;
    }
    if (bl->new_cp_offset == -1)
    {
        log_error("get_binlog_file_size() fail, fd_cur=%d", bl->fd_cur);
        return -2;
    }

    if (bl->cur_cp_idx == bl->new_cp_idx && bl->cur_cp_offset == bl->new_cp_offset)
    {
        return 1;
    }

    log_info("tag_new_cp(): %d,%d ==> %d,%d",
              bl->cur_cp_idx, bl->cur_cp_offset, bl->new_cp_idx, bl->new_cp_offset);

    if (upd_cp_file(bl->path, dbmng_conf.log_prefix, bl->cur_cp_idx, bl->cur_cp_offset, bl->new_cp_idx, bl->new_cp_offset) != 0)
    {
        log_error("tag_new_cp() update bin_log_idx file fail, "
                  "cur_cp_idx=%d, cur_cp_offset=%d, new_cp_idx=%d, new_cp_offset=%d",
                  bl->cur_cp_idx, bl->cur_cp_offset, bl->new_cp_idx, bl->new_cp_offset);
        return -1;
    }

    bl->cp_taged = 1;
    return 0;
}

int confirm_new_cp(void *binlog)
{
    bl_ctx *bl = (bl_ctx *)binlog;
    if (bl->cp_taged == 0)
    {
        return -1;
    }

    bl->cur_cp_idx = bl->new_cp_idx;
    bl->cur_cp_offset = bl->new_cp_offset;
    if (upd_cp_file(bl->path, dbmng_conf.log_prefix, bl->cur_cp_idx, bl->cur_cp_offset, -1, -1) != 0)
    {
        log_error("confirm_new_cp() update bin_log_idx file fail, cur_cp_idx=%d, cur_cp_offset=%d",
                  bl->cur_cp_idx, bl->cur_cp_offset);
        return -1;
    }

    bl->cp_taged = 0;

    return 0;
}

int bl_write_op(void *binlog, unsigned char cmd, const robj *key, int argc, const robj **argv, unsigned long long ds_id, unsigned char db_id, unsigned char type)
{
    bl_ctx *bl = (bl_ctx *)binlog;
/*
    int buf_len;
    char *buf = serialize_op(cmd, key, argc, argv, &buf_len);
    if (buf == 0)
    {
        log_error("serialize_op() fail, cmd=%d, key=%s", cmd, key->ptr);
        return 1;
    }
*/
    const int ret = write_binlog_file(bl, cmd, key, argc, argv, ds_id, db_id, type);
    //const int ret = write_binlog_file(bl, buf, buf_len, ds_id, 1);
    //zfree(buf); /* free after io async */
    if (ret != 0)
    {
        log_error("write_binlog_file() fail, cmd=%d, key=%s", cmd, key->ptr);
        //log_error("write_binlog_file() fail, cmd=%d, key=%s, len=%d", cmd, key->ptr, buf_len);
        return 1;
    }

    server.op_bl_cnt++;
    log_debug("bl_write_op() succ, cmd=%d, key=%s, ds_id=%llu",
              cmd, key->ptr, ds_id);
    //log_debug("bl_write_op() succ, cmd=%d, key=%s, len=%d, ds_id=%llu",
    //          cmd, key->ptr, buf_len, ds_id);
    return 0;
}

int bl_write_confirm(void *binlog, const char *key, unsigned char db_id)
{
    robj k;
    k.type = REDIS_STRING;
    k.encoding = REDIS_ENCODING_RAW;
    k.ptr = (void *)key;
    k.refcount = 1;
    k.lru = server.lruclock;
    k.storage = REDIS_VM_MEMORY;
    k.version = 0;
    k.rsvd_bit = 0;
    k.visited_bit = 0;
    k.reserved = 0;
/*
    int buf_len;
    char *buf = serialize_digsig(&k, &buf_len);
    if (buf == 0)
    {
        log_error("serialize_digsig() fail, key=%s", key);
        return 1;
    }
*/
    bl_ctx *bl = (bl_ctx *)binlog;
    const int ret = write_binlog_file(bl, 0, &k, 0, 0, server.ds_key_num, db_id, 1);
    //const int ret = write_binlog_file(bl, buf, buf_len, server.ds_key_num, 0);
    //zfree(buf); /* free after io async */
    if (ret != 0)
    {
        //log_error("write_binlog_file() fail for confirm, key=%s, len=%d", key, buf_len);
        log_error("write_binlog_file() fail for confirm, key=%s", key);
        return 1;
    }

    //log_debug("bl_write_confirm() succ, key=%s, len=%d", key, buf_len);
    log_debug("bl_write_confirm() succ, key=%s", key);
    return 0;
}

void *bl_get_read_it(const char *path, int idx, int offset)
{
    bl_read_iterator *it = (bl_read_iterator *)zmalloc(sizeof(*it));
    if (it == 0)
    {
        return it;
    }

    it->offset = offset;
    it->fd = open_binlog_file(path, dbmng_conf.log_prefix, idx, O_RDONLY);
    if (it->fd == -1)
    {
        zfree(it);
        return 0;
    }

    return it;
}

int bl_get_next_rec(void *iterator, char **buf, int *buf_len, int *next_offset)
{
    bl_read_iterator *it = (bl_read_iterator *)iterator;

    if (it->fd == -1)
    {
        /* over */
        return 1;
    }

    off_t offset = it->offset;
    uint64_t ds_id = 0;
    const int ret = bin_get_v2(it->fd, (uint8_t **)buf, (uint32_t*)buf_len, &offset, zmalloc, &ds_id);
    if (ret == 0)
    {
        it->offset = offset;
        *next_offset = it->offset;
        return 0;
    }
    else
    {
        /* end of file */
        close(it->fd);
        it->fd = -1;
        return ret;
    }
}

void bl_release_it(void *iterator)
{
    bl_read_iterator *it = (bl_read_iterator *)iterator;

    if (it->fd != -1)
    {
        close(it->fd);
    }

    zfree(it);
}

void bl_release_rec(char *buf)
{
    zfree(buf);
}

static int try_open_bl_file(bl_ctx *bl)
{
    if (bl->fd_cur == -1)
    {
        /* try to open at first */
        bl->fd_cur = open_binlog_file(bl->path, dbmng_conf.log_prefix, bl->cur_bl_idx, bl->flags);
        if (bl->fd_cur == -1)
        {
            log_error("can't open binlog, idx=%d", bl->cur_bl_idx);
            return -1;
        }
        upd_idx_file(bl->path, dbmng_conf.log_prefix, bl->cur_bl_idx);
    }
    return 0;
}

static void try_switch_bl_file(bl_ctx *bl)
{
    int need_create_fd = 0;
    if (bl->fd_cur ==  -1)
    {
        need_create_fd = 1;
    }
    else
    {
        const int file_size = get_binlog_file_size(bl->fd_cur);
        if (file_size >= 0 && file_size > dbmng_conf.binlog_max_size)
        {
            need_create_fd = 1;
            close(bl->fd_cur);
            bl->fd_cur = -1;
            log_debug("binlog_max_size=%d, bl_idx=%d, file_size=%d"
                    , dbmng_conf.binlog_max_size
                    , bl->cur_bl_idx
                    , file_size
                    );
        }
    }
    if (need_create_fd)
    {
        bl->cur_bl_idx = GET_BL_NEXT_IDX(bl->cur_bl_idx);
        bl->flags = O_WRONLY | O_CREAT | O_TRUNC;

        bl->fd_cur = open_binlog_file(bl->path, dbmng_conf.log_prefix, bl->cur_bl_idx, bl->flags);
        if (bl->fd_cur != -1)
        {
            upd_idx_file(bl->path, dbmng_conf.log_prefix, bl->cur_bl_idx);
        }
        else
        {
            log_error("open new binlog fail for idx=%d", bl->cur_bl_idx);
        }
    }
}

/* async mode 2015.05.07 */
//static int write_binlog_file(bl_ctx *bl, char *buf, int len, unsigned long long ds_id, int op_flag)
static int write_binlog_file(bl_ctx *bl, unsigned char cmd, const robj *key, int argc, const robj **argv, unsigned long long ds_id, unsigned char db_id, unsigned char type)
{
#ifdef _UPD_DBE_BY_PERIODIC_
    //log_buffer(buf, len);
    if (try_open_bl_file(bl) != 0)
    {
        return -1;
    }
#endif

    wr_bl_info *info = (wr_bl_info *)zcalloc(sizeof(wr_bl_info));
    info->fd = bl->fd_cur;
    //info->buf = buf;
    //info->buf_len = len;
    info->cmd = cmd;
    info->key = dupStringObject((robj*)key);
    info->argc = argc;
    if (argc > 0)
    {
        info->argv = (robj **)zmalloc(argc * sizeof(robj *));
        int i = 0;
        for (; i < argc; i++)
        {
            info->argv[i] = dupStringObject((robj*)argv[i]);
        }
    }
    else
    {
        info->argv = 0;
    }
    info->ds_id = ds_id;
    info->bl = bl;
    info->op_flag = cmd == 0 ? 0 : 1;
    info->ts = cmd == 0 ? 0 : server.unixtime;
    info->db_id = db_id;
    info->type = type;
#ifdef _UPD_DBE_BY_PERIODIC_
    listAddNodeTail(server.bl_writing, info);
    if (listLength(server.bl_writing) == 1)
    {
        /* start up */
        async_wr_bl_ctx *ctx = &g_wrbl_ctx;
        ctx->cnt = 1;
        ctx->infos[0] = info;
        commit_write_bl_task(ctx);
    }
#else
    if (server.has_cache == 1 && type == 1)
    {
        if (cmd == OP_CMD_DEL)
        {
            /* need to update to dbe */
            dbmng_ctx *dbmngctx = get_entry_ctx(0);
            info->dbe = dbmngctx->db;
        }
        else
        {
            const char cmd_type = get_blcmd_type(cmd);
            if (cmd_type == KEY_TYPE_STRING)
            {
                /* need to update to dbe */
                dbmng_ctx *dbmngctx = get_entry_ctx(0);
                info->dbe = dbmngctx->db;
                get_value_from_rdb(dbmngctx->rdb, key->ptr, &info->buf, &info->buf_len);
            }
            else if (cmd_type != 'k'
                    && cmd_type != '-'
                    && server.dbe_ver == DBE_VER_HIDB2)
            {
                /* need to update to dbe */
                dbmng_ctx *dbmngctx = get_entry_ctx(0);
                info->dbe = dbmngctx->db;
            }
            else
            {
                log_error("ignore: type(%c), cmd=%d(%s), dbe=%d"
                     , cmd_type, cmd, get_cmdstr(cmd), server.dbe_ver);
            }
        }
    }
    lockBlWriteList();
    listAddNodeTail(server.bl_writing, info);
    unlockBlWriteList();
#endif

    return 0;
}

#ifndef _UPD_DBE_BY_PERIODIC_
char gWrblRunning = 1;
extern void wakeupWrbl();
extern void linux_thread_setname(char const* threadName);
#endif

void *do_handle_write_bl(void *ctx)
{
#ifdef _DS_STAT_
    struct timespec ts;
    wr_bl_stat *tmp = &gWrBlTop[0];
#endif
#ifdef _UPD_DBE_BY_PERIODIC_
    async_wr_bl_ctx *wrbl_ctx = (async_wr_bl_ctx *)ctx;
    //log_prompt("wrbl_cnt=%d", wrbl_ctx->cnt);
    int i = 0;
    for (; i < wrbl_ctx->cnt; i++)
    {
        wr_bl_info *info = wrbl_ctx->infos[i];
#else
    (void)ctx;
    linux_thread_setname("handle_bl");
    for (;;)
    {
        lockBlWriteList();
        const unsigned int l_len = listLength(server.bl_writing);
        const unsigned int blockCnt = listLength(server.write_bl_clients);
        if (l_len == 0)
        {
            unlockBlWriteList();
            if (gWrblRunning)
            {
                if (blockCnt > 0)
                {
                    wakeupWrbl();
                }
                msleep(0);
                continue;
            }
            else
            {
                log_prompt("wr_bl thread will exit, bl_list_len=%u", l_len);
                break;
            }
        }
        listNode *ln = listFirst(server.bl_writing);
        wr_bl_info *info = listNodeValue(ln);
        listDelNode(server.bl_writing, ln);
        unlockBlWriteList();
#endif

        bl_ctx *const bl = info->bl;
        const unsigned long long ds_id = info->ds_id;
        const unsigned char op_flag = info->op_flag;

        int buf_len;
        char *buf = 0;
#ifdef _DS_STAT_
        ts = pf_get_time_tick();
#endif
        if (info->cmd == 0)
        {
            buf = serialize_digsig(info->key, &buf_len, info->db_id);
        }
        else
        {
            buf = serialize_op(info->cmd, info->key, info->argc, (const robj**)info->argv, info->ts, info->db_id, info->type, &buf_len);
        }
#ifdef _DS_STAT_
        tmp->serialize_dur = pf_get_time_diff_nsec(ts, pf_get_time_tick()) / 1000;
        tmp->cmd = info->cmd;
        strncpy(tmp->key, info->key->ptr, sizeof(tmp->key) - 1);
        tmp->key[sizeof(tmp->key) - 1] = 0;
        tmp->buf_len = buf_len;
#endif
        if (buf == 0)
        {
            log_error("serialize_xx() fail, cmd=%d, key=%s", info->cmd, info->key->ptr);
        }

#ifndef _UPD_DBE_BY_PERIODIC_
#if 1
        if (buf && info->dbe)
        {
            /* notice wr_dbe_pthread */
            lockWrDbeList();
            listAddNodeTail(server.wr_dbe_list, info);
            unlockWrDbeList();
        }
        else
#endif
#endif
        {
            /* release resource */
            decrRefCount(info->key);
            info->key = 0;
            if (info->argv)
            {
                int j;
                for (j = 0; j < info->argc; j++)
                {
                    decrRefCount(info->argv[j]);
                }
                zfree(info->argv);
                info->argv = 0;
            }
#ifndef _UPD_DBE_BY_PERIODIC_
            if (info->buf)
            {
                zfree(info->buf);
            }
#endif
            zfree(info);
        }

        if (buf == 0)
        {
#ifndef _UPD_DBE_BY_PERIODIC_
            if (l_len <= (unsigned int)server.wr_bl_que_size && blockCnt > 0)
            {
                wakeupWrbl();
            }
#endif
            continue;
        }

#ifndef _UPD_DBE_BY_PERIODIC_
#if 1
        /* push into list */
        wr_bl_arg *arg = (wr_bl_arg*)zmalloc(sizeof(wr_bl_arg));
        arg->op_flag = op_flag;
        arg->ds_id = ds_id;
        arg->bl = bl;
        arg->buf = buf;
        arg->buf_len = buf_len;
        lockWrblList();
        listAddNodeTail(server.wr_bl_list, arg);
        unlockWrblList();
#else
        zfree(buf);
#endif
        if (l_len <= (unsigned int)server.wr_bl_que_size && blockCnt > 0)
        {
            wakeupWrbl();
        }
#else
        write_bl_to_file(buf, buf_len, bl, ds_id, op_flag);
        zfree(buf);
#endif
    }
    return (void*)NULL;
}

static void write_bl_to_file(const char *buf, int buf_len, bl_ctx *bl, unsigned long long ds_id, unsigned char op_flag)
{
#ifndef _UPD_DBE_BY_PERIODIC_
    /* open binlog file */
    if (try_open_bl_file(bl) != 0)
    {
        return;
    }
#endif
    const int fd = bl->fd_cur;
#ifdef _DS_STAT_
    struct timespec ts;
    wr_bl_stat *tmp = &gWrBlTop[0];
    ts = pf_get_time_tick();
#endif
    const int32_t ret = bin_put_v2(fd, (const uint8_t *)buf, buf_len, (uint64_t)ds_id);
#ifdef _DS_STAT_
    tmp->io_dur = pf_get_time_diff_nsec(ts, pf_get_time_tick()) / 1000;
    log_test("write bl file(fd=%d, idx=%d): ret=%d, bl_len=%d, dur=%dms"
        , fd, bl->cur_bl_idx, ret, buf_len, tmp->io_dur);
#endif

    if (ret < 0)
    {
        log_error("bin_put_v2() fail, fd=%d, len=%d, ret=%d, ds_id=%llu, op_flag=%d",
            fd, buf_len, ret, ds_id, op_flag);
    }

#ifndef _UPD_DBE_BY_PERIODIC_
    try_switch_bl_file(bl);
#endif

#ifdef _DS_STAT_
    /* stat */
    int min_idx = 0;
    unsigned int k;
    for (k = 1; k < sizeof(gWrBlTop) / sizeof(gWrBlTop[0]); k++)
    {
        if (gWrBlTop[k].valid == 0)
        {
            min_idx = k;
            break;
        }
        if (gWrBlTop[k].io_dur < gWrBlTop[min_idx].io_dur)
        {
            min_idx = k;
        }
    }
    memcpy(&gWrBlTop[min_idx], &gWrBlTop[0], sizeof(gWrBlTop[0]));
    gWrBlTop[min_idx].valid = 1;
    gWrBlTop[min_idx].time = time(NULL);
    if (tmp->io_dur > gWrBlMax)
    {
        gWrBlMax = tmp->io_dur;
    }
    if (tmp->io_dur < gWrBlMin || gWrBlMin == 0)
    {
        gWrBlMin = tmp->io_dur;
    }
    gWrBlTotal += tmp->io_dur;
    gWrBlCnt++;
#endif
}

#ifndef _UPD_DBE_BY_PERIODIC_
void *do_write_bl(void *ctx)
{
    (void)ctx;
    linux_thread_setname("write_bl");
    for (;;)
    {
        lockWrblList();
        const unsigned int l_len = listLength(server.wr_bl_list);
        if (l_len == 0)
        {
            unlockWrblList();
            if (gWrblRunning)
            {
                msleep(0);
                continue;
            }
            else
            {
                log_prompt("handle_bl thread will exit, handle_bl_list_len=%u", l_len);
                break;
            }
        }
        listNode *ln = listFirst(server.wr_bl_list);
        wr_bl_arg *info = listNodeValue(ln);
        listDelNode(server.wr_bl_list, ln);
        unlockWrblList();

        write_bl_to_file(info->buf, info->buf_len, info->bl, info->ds_id, info->op_flag);
        zfree(info->buf);
        zfree(info);
    }
    return (void*)NULL;
}

void *do_write_dbe(void *ctx)
{
    (void)ctx;
    linux_thread_setname("write_dbe");
    for (;;)
    {
        lockWrDbeList();
        const unsigned int l_len = listLength(server.wr_dbe_list);
        if (l_len == 0)
        {
            unlockWrDbeList();
            if (gWrblRunning)
            {
                msleep(0);
                continue;
            }
            else
            {
                log_prompt("wr_dbe thread will exit, dbe_list_len=%u", l_len);
                break;
            }
        }
        listNode *ln = listFirst(server.wr_dbe_list);
        wr_bl_info *info = listNodeValue(ln);
        listDelNode(server.wr_dbe_list, ln);
        unlockWrDbeList();

        upd_dbe_param param;
        {
            /* succ & need to set to dbe, make param */
            sds key = info->key->ptr;
            memset(&param, 0, sizeof(param));

            if (info->cmd == OP_CMD_DEL)
            {
                if (server.enc_kv == 0)
                {
                    param.pdel_key_len = sdslen(key);
                    param.pdel_key = zmalloc(param.pdel_key_len + 1);
                    strncpy(param.pdel_key, key, param.pdel_key_len);
                    param.pdel_key[param.pdel_key_len] = 0;
                }
                else
                {
                    param.pdel_key = encode_prefix_key((const char*)key, sdslen(key), 0, &param.pdel_key_len);
                }
            }
            else
            {
                param.cmd_type = get_blcmd_type(info->cmd);
                if (param.cmd_type == KEY_TYPE_STRING)
                {
                    if (info->buf == NULL)
                    {
                        log_error("impossible: string cmd=%d, val=%p, val_len=%d, key=%s"
                                , info->cmd, info->buf, info->buf_len, (const char*)key);
                        info->dbe = NULL;
                    }
                    else
                    {
                        if (server.enc_kv == 0)
                        {
                            param.one.ks = sdslen(key);
                            param.one.k = zmalloc(param.one.ks + 1);
                            strncpy(param.one.k, key, param.one.ks);
                            param.one.k[param.one.ks] = 0;
                        }
                        else
                        {
                            param.one.k = encode_string_key((const char*)key, sdslen(key), &(param.one.ks));
                        }
                        param.one.v = info->buf;
                        param.one.vs = info->buf_len;
                        info->buf = 0;
                        info->buf_len = 0;
                    }
                }
                else if (server.dbe_ver == DBE_VER_HIDB)
                {
                    log_error("hidb don't support type(%c), cmd=%d(%s)"
                            , param.cmd_type, info->cmd, get_cmdstr(info->cmd));
                    info->dbe = NULL;
                }
                else
                {
                    if (param.cmd_type == KEY_TYPE_LIST)
                    {
                        make_list_dbe_data(info, &param);
                    }
                    else if (param.cmd_type == KEY_TYPE_SET)
                    {
                        make_set_dbe_data(info, &param);
                    }
                    else if (param.cmd_type == KEY_TYPE_ZSET)
                    {
                        make_zset_dbe_data(info, &param);
                    }
                    else if (param.cmd_type == KEY_TYPE_HASH)
                    {
                        make_hash_dbe_data(info, &param);
                    }
                    else
                    {
                        log_error("wrong cmd_type=%c, cmd=%d, key=%s"
                            , param.cmd_type, info->cmd, (const char*)key);
                        info->dbe = NULL;
                    }
                }
            }
        }

        /* set to dbe */
        log_test("do_write_dbe: list_len=%d, dbe=%p", l_len, info->dbe);
        dbe_set_one_op(info->dbe, &param);

        /* free resource */
        decrRefCount(info->key);
        info->key = 0;
        if (info->argv)
        {
            int j;
            for (j = 0; j < info->argc; j++)
            {
                decrRefCount(info->argv[j]);
            }
            zfree(info->argv);
            info->argv = 0;
        }
        if (info->buf)
        {
            zfree(info->buf);
        }
        zfree(info);
    }
    return (void*)NULL;
}
#endif

void after_write_bl(void *p_ctx)
{
    //log_prompt("%s", "continue after finishing writing bl ...");

    listNode *ln = 0;
#ifdef _UPD_DBE_BY_PERIODIC_
    async_wr_bl_ctx *wrbl_ctx = (async_wr_bl_ctx *)p_ctx;
    wr_bl_info *info = 0;
    bl_ctx *bl = 0;
    int i = 0;
    for (; i < wrbl_ctx->cnt; i++)
    {
        info = wrbl_ctx->infos[i];
        bl = info->bl;

        try_switch_bl_file(bl);

        if (info->op_flag == 1 && server.op_bl_cnt > 0)
        {
            server.op_bl_cnt--;
            if (server.op_bl_cnt == 0)
            {
                dbmng_try_cp(0);
            }
        }

        /* release resource */
        zfree(info);
        info = 0;
        ln = listFirst(server.bl_writing);
        listDelNode(server.bl_writing, ln);
    }
    wrbl_ctx = 0;

    int do_next = 0;
    if (server.op_bl_cnt == 0 || listLength(server.bl_writing) == 0)
    {
        //log_prompt("%s", "finish writing binlog in list");
        const unsigned int old_cnt = listLength(server.bl_writing);
        unsigned int wait_c_cnt;

handle_next_c:
        wait_c_cnt = listLength(server.write_bl_clients);
        if (wait_c_cnt)
        {
            /* go on startup next c */
            redisClient *c = 0;
            ln = listFirst(server.write_bl_clients);
            c = listNodeValue(ln);
            listDelNode(server.write_bl_clients, ln);

            c->wr_bl_block_dur = server.ustime - c->block_start_time;
            //log_prompt("wait in write_bl_que: %lld(us), cmd=%s, waiting_c=%d, waiting_bl=%d"
            //        , c->wr_bl_dur, c->argv[0]->ptr, wait_c_cnt, old_cnt);

            c->flags &= (~REDIS_WR_BL_WAIT);
            aeCreateFileEvent(server.el, c->fd, AE_READABLE, readQueryFromClient, c);
            //struct redisCommand *cmd = lookupCommand(c->argv[0]->ptr);
            call(c);
            resetClient(c);
            if (c->querybuf && sdslen(c->querybuf) > 0)
            {
                log_info("processInputBuff() after_write_bl...");
                processInputBuffer(c);
            }

            const unsigned int new_cnt = listLength(server.bl_writing);
            if (new_cnt == old_cnt)
            {
                /* blocking req don't write bl, go on next blcoked req */
                goto handle_next_c;
            }
            if (old_cnt == 0)
            {
                /* bl_write has startup, need not to startup anymore */
                do_next = 0;
            }
            else
            {
                do_next = 1;
            }
        }
        else
        {
            do_next = listLength(server.bl_writing) > 0 ? 1 : 0;
        }
    }
    else
    {
        do_next = 1;
    }

    if (do_next)
    {
        /* go on startup write binlog in list made from one cmd */
        //log_prompt("%s", "go on write binlog in list");
        wrbl_ctx = &g_wrbl_ctx;
        listIter *it = listGetIterator(server.bl_writing, AL_START_HEAD);
        i = 0;
        while ((ln = listNext(it))
               && (size_t)i < (sizeof(wrbl_ctx->infos) / sizeof(wrbl_ctx->infos[0]))
              )
        {
            info = listNodeValue(ln);
            info->fd = info->bl->fd_cur;
            wrbl_ctx->infos[i++] = info;
        }
        wrbl_ctx->cnt = i;
        listReleaseIterator(it);
        commit_write_bl_task(wrbl_ctx);
    }
#else
    (void)p_ctx;
    lockBlWriteList();
    const unsigned int wait_c_cnt = listLength(server.write_bl_clients);
    if (wait_c_cnt)
    {
        log_test("gono wrbl_blocking_c: %u", wait_c_cnt);
        /* go on startup next c */
        redisClient *c = 0;
        ln = listFirst(server.write_bl_clients);
        c = listNodeValue(ln);
        listDelNode(server.write_bl_clients, ln);
        unlockBlWriteList();

        c->wr_bl_block_dur = server.ustime - c->block_start_time;
        //log_prompt("wait in write_bl_que: %lld(us), cmd=%s, waiting_c=%d, waiting_bl=%d"
        //        , c->wr_bl_dur, c->argv[0]->ptr, wait_c_cnt, old_cnt);

        c->flags &= (~REDIS_WR_BL_WAIT);
        aeCreateFileEvent(server.el, c->fd, AE_READABLE, readQueryFromClient, c);
        //struct redisCommand *cmd = lookupCommand(c->argv[0]->ptr);
        call(c);
        resetClient(c);
        if (c->querybuf && sdslen(c->querybuf) > 0)
        {
            log_info("processInputBuff() after_write_bl...");
            processInputBuffer(c);
        }
    }
    else
    {
        unlockBlWriteList();
    }
#endif
}

static int bl_open_file(const char *f, int flags)
{
    int fd = -1;
    if (flags & O_CREAT)
    {
        fd = open(f, flags, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH);
    }
    else
    {
        fd = open(f, flags);
    }

    if (fd == -1)
    {
        log_error("open() fail, errno=%d, flags=0x%08x, file=%s", errno, flags, f);
    }
    else
    {
        log_debug("open() succ, file=%s, fd=%d, flags=%d", f, fd, flags);
    }

    return fd;
}

static int open_binlog_file(const char *path, const char *prefix, int idx, int flags)
{
    char f[256];
    make_bl_file(f, sizeof(f), path, prefix, idx);

    return bl_open_file(f, flags);
}

/* return:
 * -1 : fail
 *  other value : succ
 */
static int get_binlog_file_size(int fd)
{
    int offset = -1;
    struct stat buf;
    if (fstat(fd, &buf) != -1)
    {
        offset = buf.st_size;
    }
    else
    {
        log_error("fstat() fail, errno=%d, fd=%d", errno, fd);
    }
    return offset;
}

uint64_t bl_get_last_ts(void *binlog, int now_flg)
{
    if (binlog == 0)
    {
        return now_flg ? bin_nowts() : 0;
    }

    char f[256];
    bl_ctx *bl = (bl_ctx *)binlog;

    make_bl_file(f, sizeof(f), bl->path, dbmng_conf.log_prefix, bl->cur_cp_idx);

    const int fd = open(f, O_RDONLY);
    if (fd == -1)
    {
        if (errno != ENOENT)
        {
            log_error("open() fail for O_RDONLY, errno=%d, filename=%s", errno, f);
        }
        return now_flg ? bin_nowts() : 0;
    }

    off_t offset = bl->cur_cp_offset;
    uint64_t ts;
    const int ret = bin_get_ts(fd, &offset, &ts);
    if (ret != 0)
    {
        ts = now_flg ? bin_nowts() : 0;
    }
    close(fd);
    return ts;
}

int bl_get_cp_idx(void *binlog)
{
    if (binlog == 0)
    {
        return -1;
    }

    bl_ctx *bl = (bl_ctx *)binlog;
    return bl->cur_cp_idx;
}

int bl_get_cp_offset(void *binlog)
{
    if (binlog == 0)
    {
        return -1;
    }

    bl_ctx *bl = (bl_ctx *)binlog;
    return bl->cur_cp_offset;
}

static void *bl_get_read_it2(const char *filename)
{
    bl_read_iterator *it = (bl_read_iterator *)zmalloc(sizeof(*it));
    if (it == 0)
    {
        return it;
    }

    it->offset = 0;
    it->fd = bl_open_file(filename, O_RDONLY);
    if (it->fd == -1)
    {
        zfree(it);
        return 0;
    }

    return it;
}

int decode_bl(const char *filename, int to_file_flg)
{
    if (filename == 0)
    {
        return -1;
    }

    void *it = bl_get_read_it2(filename);
    if (it == 0)
    {
        log_error("bl_get_read_it2() fail, filename=%s", filename);
        return -1;
    }

    FILE *pf = stdout;
    if (to_file_flg)
    {
        char txt[256];
        snprintf(txt, sizeof(txt), "%s.txt", filename);
        pf = fopen(txt, "w");
        if (pf == 0)
        {
            log_error("fopen() \"%s\" fail:%s", txt, strerror(errno));
            bl_release_it(it);
            return -1;
        }
    }
    fprintf(pf, "[No.] [bl_len,next_offset] [date] cmd,argc(argv1_len,...)[,db_id,(0:redis-cache;1:redis-persistent)],(k_len)=key\n");

    int cnt = 0;
    int ret;
    char *buf = 0;
    int buf_len;
    int next_offset;
    op_rec rec;
    while ((ret = bl_get_next_rec(it, &buf, &buf_len, &next_offset)) == 0)
    {
        log_debug("bl_get_next_rec(): buf_len=%d, next_offset=%d", buf_len, next_offset);

        ret = parse_op_rec(buf, buf_len, &rec);
        if (ret != 0)
        {
            log_error("parse_op_rec() fail, len=%d, ret=%d, cur_cnt=%d, next_offset=%d",
                      buf_len, ret, cnt, next_offset);
        }
        else
        {
            if (rec.cmd != 0)
            {
                time_t t = rec.time_stamp;
                struct tm tmo;
                localtime_r((const time_t *)&t, &tmo);
                fprintf(pf
                        , "[%d] [%d,%d] [%04d-%02d-%02d %02d:%02d:%02d] %s,%d"
                        , cnt
                        , buf_len
                        , next_offset
                        , tmo.tm_year + 1900
                        , tmo.tm_mon + 1
                        , tmo.tm_mday
                        , tmo.tm_hour
                        , tmo.tm_min
                        , tmo.tm_sec
                        , get_cmdstr(rec.cmd)
                        , rec.argc
                       );
                if (rec.argc > 0)
                {
                    fprintf(pf, "(");
                    uint32_t k = 0;
                    for (k = 0; k < rec.argc; k++)
                    {
                        fprintf(pf, "%s%d", k == 0 ? "" : ",", rec.argv[k].len);
                    }
                    fprintf(pf, ")");

                    zfree(rec.argv);
                }
                if (rec.type == 2)
                {
                    /* mc, use old format(unsupport redis) */
                    fprintf(pf
                        , ",(%d)=%.*s\n"
                        , rec.key.len
                        , rec.key.len
                        , rec.key.ptr
                       );
                }
                else
                {
                    /* redis */
                    fprintf(pf
                        , ",%d,%d,(%d)=%.*s\n"
                        , rec.db_id
                        , rec.type
                        , rec.key.len
                        , rec.key.len
                        , rec.key.ptr
                       );
                }

                cnt++;
            }
        }

        bl_release_rec(buf);
    }

    fclose(pf);
    bl_release_it(it);

    return cnt;
}

static int get_oldest_bl_idx(const char *path, int cur_idx, int load_bl_cnt)
{
    char f[256];
    FILE *fp = 0;
    
    /* 1) check cur_idx */
    make_bl_file(f, sizeof(f), path, dbmng_conf.log_prefix, cur_idx);
    fp = fopen(f, "r");
    if (!fp)
    {
        if (errno == 2)
        {
            /* no exist */
            log_info("file=%s not exist", f);
            return 0;
        }
        else
        {
            log_error("fopen() fail, file=%s, errno=%d", f, errno);
            return 0;
        }
    }
    fclose(fp);
    fp = 0;

    int old_idx;
    if (load_bl_cnt <= 1 + cur_idx)
    {
        old_idx = cur_idx - load_bl_cnt + 1;
    }
    else
    {
        old_idx = cur_idx - load_bl_cnt + 1 + BL_MAX_IDX;
    }

    return old_idx;
}

static int redo_one_bl_file(redisDb *rdb, const char *path, int idx)
{
    const int offset = 0;
    void *it = bl_get_read_it(path, idx, offset);
    if (it == 0)
    {
        log_error("bl_get_read_it() fail, idx=%d, offset=%d", idx, offset);
        return -1;
    }

    int cnt = 0;
    int ret;
    char *buf = 0;
    int buf_len;
    int next_offset;
    while ((ret = bl_get_next_rec(it, &buf, &buf_len, &next_offset)) == 0)
    {
        log_debug("bl_get_next_rec(): buf_len=%d, next_offset=%d", buf_len, next_offset);
        redo_op_rec(rdb, buf, buf_len);
        bl_release_rec(buf);
        cnt++;
    }

    bl_release_it(it);

    log_prompt("redo %d rec in bl_idx=%d file", cnt, idx);

    return cnt;
}

void redo_bl(redisDb *rdb, void *binlog, int load_bl_cnt)
{
    bl_ctx *bl = (bl_ctx *)binlog;
    const long long start = ustime();
    const int max_load_bl_cnt = server.db_max_size / server.binlog_max_size; 

    int load_cnt = load_bl_cnt;
    if (max_load_bl_cnt == 0)
    {
        /* load one at most */
        load_cnt = 1;
    }
    else if (max_load_bl_cnt < load_bl_cnt)
    {
        load_cnt = max_load_bl_cnt;
    }

    int idx = get_oldest_bl_idx(bl->path, bl->cur_bl_idx, load_cnt);
    const int start_idx = idx;

    while (1)
    {
        redo_one_bl_file(rdb, bl->path, idx);
        if (idx == bl->cur_bl_idx)
        {
            break;
        }
        idx = GET_BL_NEXT_IDX(idx);
    }

    log_prompt("redo_bl(): load_cnt=%d, in fact %03d ->%03d, durations=%lldus"
            , load_bl_cnt, start_idx, idx, ustime() - start);
}

void reset_bl(void *binlog)
{
    char f[256];
    FILE *fp = 0;
    bl_ctx *bl = (bl_ctx *)binlog;

    bl->cur_bl_idx = 0;
    if (bl->fd_cur != -1)
    {
        close(bl->fd_cur);
        bl->fd_cur = -1;
    }
    upd_idx_file(bl->path, dbmng_conf.log_prefix, bl->cur_bl_idx);

    int i = 0;
    for (; i < BL_MAX_IDX; i++)
    {
        make_bl_file(f, sizeof(f), bl->path, dbmng_conf.log_prefix, i);
        fp = fopen(f, "r");
        if (fp)
        {
            /* exist, remove it */
            fclose(fp);
            fp = 0;
            remove(f);
            log_prompt("reset_bl: remove %s", f);
        }
    }
}

#ifndef _UPD_DBE_BY_PERIODIC_
static void make_list_dbe_data(wr_bl_info *info, upd_dbe_param *param)
{
    const int rpop_cmd = get_cmd(OP_LRPOP);
    const int lpop_cmd = get_cmd(OP_LLPOP);
    const int rpush_cmd = get_cmd(OP_LRPUSH);
    const int lpush_cmd = get_cmd(OP_LLPUSH);
    const int lset_cmd = get_cmd(OP_LSET);

    sds key = info->key->ptr;

    if ((int)info->cmd == rpush_cmd || (int)info->cmd == lpush_cmd)
    {
        /* push */
        if (info->argc <= 0)
        {
            log_error("argc=%d invalid for list_push_cmd(%d), key=%s"
                    , info->argc, info->cmd, (const char*)key);
            return;
        }
        param->cnt[0] = info->argc;
        param->kv[0] = (kvec_t *)zcalloc(sizeof(kvec_t) * param->cnt[0]);
        int i = 0;
        while (i < info->argc)
        {
            sds item = info->argv[i]->ptr;
            const uint32_t seq = info->argv[i]->reserved;

            param->kv[0][i].k = encode_list_key((const char*)key, sdslen(key), seq, &param->kv[0][i].ks);
            param->kv[0][i].v = encode_list_val((const char*)item, sdslen(item), &param->kv[0][i].vs);

            i++;
        }
    }
    else if ((int)info->cmd == lset_cmd)
    {
        /* set */
        if (info->argc <= 0 || info->argc % 2)
        {
            log_error("argc=%d invalid for list_set_cmd(%d), key=%s"
                    , info->argc, info->cmd, (const char*)key);
            return;
        }
        param->cnt[0] = info->argc / 2;
        param->kv[0] = (kvec_t *)zcalloc(sizeof(kvec_t) * param->cnt[0]);
        int i = 0;
        while ((size_t)i < param->cnt[0])
        {
            sds item = info->argv[i * 2 + 1]->ptr;
            const uint32_t seq = info->argv[i * 2 + 1]->reserved;

            param->kv[0][i].k = encode_list_key((const char*)key, sdslen(key), seq, &param->kv[0][i].ks);
            param->kv[0][i].v = encode_list_val((const char*)item, sdslen(item), &param->kv[0][i].vs);

            i++;
        }
    }
    else if ((int)info->cmd == rpop_cmd || (int)info->cmd == lpop_cmd)
    {
        /* pop */
        if (info->argc <= 0)
        {
            log_error("argc=%d invalid for list_pop_cmd(%d), key=%s"
                    , info->argc, info->cmd, (const char*)key);
            return;
        }
        param->cnt[1] = info->argc;
        param->kv[1] = (kvec_t *)zcalloc(sizeof(kvec_t) * param->cnt[1]);
        int i = 0;
        while (i < info->argc)
        {
            unsigned long long seq;
            getuLongLongFromObject(info->argv[i], &seq);

            param->kv[1][i].k = encode_list_key((const char*)key, sdslen(key), seq, &param->kv[1][i].ks);

            i++;
        }
    }
    else
    {
        log_error("cmd=%d (%s) impossible for set, key=%s"
                , info->cmd, get_cmdstr(info->cmd), (const char*)key);
    }
}

static void make_set_dbe_data(wr_bl_info *info, upd_dbe_param *param)
{
    const int set_add_cmd = get_cmd(OP_SADD);
    const int set_rem_cmd = get_cmd(OP_SREM);
    sds key = info->key->ptr;

    if ((int)info->cmd == set_add_cmd)
    {
        if (info->argc <= 0)
        {
            log_error("argc=%d invalid for set_add_cmd, key=%s"
                    , info->argc, (const char*)key);
            return;
        }
        param->cnt[0] = info->argc;
        param->kv[0] = (kvec_t *)zcalloc(sizeof(kvec_t) * param->cnt[0]);
        int i = 0;
        while (i < info->argc)
        {
            sds member = info->argv[i]->ptr;

            param->kv[0][i].k = encode_set_key((const char*)key, sdslen(key), (const char*)member, sdslen(member), &param->kv[0][i].ks);
            param->kv[0][i].v = encode_set_val((const char*)member, sdslen(member), &param->kv[0][i].vs);

            i++;
        }
    }
    else if ((int)info->cmd == set_rem_cmd)
    {
        if (info->argc <= 0)
        {
            log_error("argc=%d invalid for set_rem_cmd, key=%s"
                    , info->argc, (const char*)key);
            return;
        }
        param->cnt[1] = info->argc;
        param->kv[1] = (kvec_t *)zcalloc(sizeof(kvec_t) * param->cnt[1]);
        int i = 0;
        while (i < info->argc)
        {
            sds member = info->argv[i]->ptr;

            param->kv[1][i].k = encode_set_key((const char*)key, sdslen(key), (const char*)member, sdslen(member), &param->kv[1][i].ks);

            i++;
        }
    }
    else
    {
        log_error("cmd=%d (%s) impossible for set, key=%s"
                , info->cmd, get_cmdstr(info->cmd), (const char*)key);
    }
}

static void make_hash_dbe_data(wr_bl_info *info, upd_dbe_param *param)
{
    const int hash_add_cmd = get_cmd(OP_HMSET);
    const int hash_rem_cmd = get_cmd(OP_HDEL);
    sds key = info->key->ptr;

    if ((int)info->cmd == hash_add_cmd)
    {
        if (info->argc <= 0 || info->argc % 2)
        {
            log_error("argc=%d invalid for hash_add_cmd, key=%s"
                    , info->argc, (const char*)key);
            return;
        }
        param->cnt[0] = info->argc / 2;
        param->kv[0] = (kvec_t *)zcalloc(sizeof(kvec_t) * param->cnt[0]);
        int i = 0;
        int k = 0;
        while (k < info->argc && (size_t)i < param->cnt[0])
        {
            sds field = info->argv[k]->ptr;
            sds val = info->argv[k + 1]->ptr;

            param->kv[0][i].k = encode_hash_key((const char*)key, sdslen(key), (const char*)field, sdslen(field), &param->kv[0][i].ks);
            param->kv[0][i].v = encode_hash_val((const char*)field, sdslen(field), (const char*)val, sdslen(val), &param->kv[0][i].vs);

            k += 2;
            i++;
        }
    }
    else if ((int)info->cmd == hash_rem_cmd)
    {
        if (info->argc <= 0)
        {
            log_error("argc=%d invalid for hash_rem_cmd, key=%s"
                    , info->argc, (const char*)key);
            return;
        }
        param->cnt[1] = info->argc;
        param->kv[1] = (kvec_t *)zcalloc(sizeof(kvec_t) * param->cnt[1]);
        int i = 0;
        while (i < info->argc)
        {
            sds field = info->argv[i]->ptr;

            param->kv[1][i].k = encode_hash_key((const char*)key, sdslen(key), (const char*)field, sdslen(field), &param->kv[1][i].ks);

            i++;
        }
    }
    else
    {
        log_error("cmd=%d (%s) impossible for hash, key=%s"
                , info->cmd, get_cmdstr(info->cmd), (const char*)key);
    }
}

static void make_zset_dbe_data(wr_bl_info *info, upd_dbe_param *param)
{
    const int zset_add_cmd = get_cmd(OP_ZADD);
    const int zset_rem_cmd = get_cmd(OP_ZREM);
    sds key = info->key->ptr;

    if ((int)info->cmd == zset_add_cmd)
    {
        if (info->argc <= 0 || info->argc % 2)
        {
            log_error("argc=%d invalid for zset_add_cmd, key=%s"
                    , info->argc, (const char*)key);
            return;
        }
        param->cnt[0] = info->argc / 2;
        param->kv[0] = (kvec_t *)zcalloc(sizeof(kvec_t) * param->cnt[0]);
        int i = 0;
        int k = 0;
        while (k < info->argc && (size_t)i < param->cnt[0])
        {
            sds member = info->argv[k + 1]->ptr;
            robj *score_obj = info->argv[k];
            double score;
            getDoubleFromObject(score_obj, &score);

            param->kv[0][i].k = encode_zset_key((const char*)key, sdslen(key), (const char*)member, sdslen(member), &param->kv[0][i].ks);
            param->kv[0][i].v = encode_zset_val((const char*)member, sdslen(member), score, &param->kv[0][i].vs);

            k += 2;
            i++;
        }
    }
    else if ((int)info->cmd == zset_rem_cmd)
    {
        if (info->argc <= 0)
        {
            log_error("argc=%d invalid for zset_rem_cmd, key=%s"
                    , info->argc, (const char*)key);
            return;
        }
        param->cnt[1] = info->argc;
        param->kv[1] = (kvec_t *)zcalloc(sizeof(kvec_t) * param->cnt[1]);
        int i = 0;
        while (i < info->argc)
        {
            sds member = info->argv[i]->ptr;

            param->kv[1][i].k = encode_zset_key((const char*)key, sdslen(key), (const char*)member, sdslen(member), &param->kv[1][i].ks);

            i++;
        }
    }
    else
    {
        log_error("cmd=%d (%s) impossible for zset, key=%s"
                , info->cmd, get_cmdstr(info->cmd), (const char*)key);
    }
}
#endif

