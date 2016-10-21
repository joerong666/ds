#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>

#include "file_util.h"
#include "rotate_file.h"
#include "meta_file.h"
#include "zmalloc.h"
#include "pf_crc.h"
#include "pf_file.h"
#include "log.h"
#include "bin_log_impl.h"

static const uint16_t BIN_MAGIC_NUM = 0xfefd;
static const uint16_t BIN_MAGIC_NUM_V2 = 0xfefe;

uint64_t bin_timestamp()
{
    uint64_t time = 0;
    struct timespec cur;
    clock_gettime(CLOCK_REALTIME, &cur);

    time = (uint64_t)(cur.tv_sec * 1000000ULL
                    + cur.tv_nsec / 1000ULL);

    return time;
}

int bin_write_internal(int fd, const uint8_t *buf, uint32_t len, uint64_t *ts)
{
    int          i;
    bin_header_t h;
    struct iovec bin_iov[2];
    pf_crc16_t   *crc_p = NULL;

    crc_p = pf_crc16_start();
    pf_crc16_append(crc_p, buf, len);

    h.magic = BIN_MAGIC_NUM;
    h.crc   = pf_crc16_finish(crc_p);
    h.ts    = bin_timestamp();
    h.len   = len;

    bin_iov[0].iov_base = (void *)&h;
    bin_iov[0].iov_len  = sizeof(bin_header_t);

    bin_iov[1].iov_base = (void *)buf;
    bin_iov[1].iov_len  = len;

    i = writev(fd, bin_iov, 2);
    if (i != (signed)(sizeof(bin_header_t) + len))
    {
        return BL_FILE_SYS_ERR; 
    }

    if (ts) *ts = h.ts;

    return BL_FILE_OK;
}

int bin_write_internal_v2(int fd, const uint8_t *buf, uint32_t len, uint64_t *ts, uint64_t sid)
{
    int          i;
    bin_header_t h;
    struct iovec bin_iov[3];
    pf_crc16_t   *crc_p = NULL;

    crc_p = pf_crc16_start();
    pf_crc16_append(crc_p, buf, len);

    h.magic = BIN_MAGIC_NUM_V2;
    h.crc   = pf_crc16_finish(crc_p);
    h.ts    = bin_timestamp();
    h.len   = len;

    bin_iov[0].iov_base = (void *)&h;
    bin_iov[0].iov_len  = sizeof(bin_header_t);

    bin_iov[1].iov_base = (void *)&sid; 
    bin_iov[1].iov_len  = sizeof(sid);

    bin_iov[2].iov_base = (void *)buf;
    bin_iov[2].iov_len  = len;

    i = writev(fd, bin_iov, 3);
    if (i != (signed)(sizeof(bin_header_t) + len + sizeof(sid)))
    {
        return BL_FILE_SYS_ERR; 
    }

    if (ts) *ts = h.ts;

    return BL_FILE_OK;
}

static int bin_read_internal_v2(int fd, uint8_t **buf, uint32_t *len, off_t offset, uint64_t *ts, uint64_t *sid)
{
    bin_header_t h;
    ssize_t      n = 0;
    int          v2_flag = 0;

    //log_debug("try to read [%u]", offset);
    file_set_pos(fd, offset);
    while ((n = read(fd, &h, sizeof(h))) == sizeof(h))
    {
        /* magic num is matched and len valid */
        if ((h.magic == BIN_MAGIC_NUM 
            || h.magic == BIN_MAGIC_NUM_V2) 
            && IS_RECORD_LEN_VALID(h.len))
        {
            v2_flag = (h.magic == BIN_MAGIC_NUM_V2) ? 1 : 0;
            if (v2_flag)
            {
                n = read(fd, sid, sizeof(uint64_t));
                if (n != sizeof(uint64_t))
                {
                    log_error("read V2 sid error[%s] ret[%d]", strerror(errno), n);
                    return BL_FILE_SYS_ERR; 
                }
            }

            *buf = (uint8_t *)zmalloc(h.len > SIMPLE_BUF_SIZE ? h.len : SIMPLE_BUF_SIZE);

            /* try to read record */
            n = read(fd, *buf, h.len);

            if (n == 0)
            {
                zfree(*buf);
                log_debug("read content, file EOF");
                return BL_FILE_EOF;
            }
            else if (n != h.len)
            {
                zfree(*buf);
                log_error("read content ret [%d] len[%d], file error[%s]", n, h.len, strerror(errno));
                return BL_FILE_SYS_ERR; 
            }

            pf_crc16_t* crc_p = pf_crc16_start();
            pf_crc16_append(crc_p, *buf, h.len);

            /* CRC matched */
            if(h.crc == pf_crc16_finish(crc_p))
            {
                *ts = h.ts;
                *len = h.len;
                return BL_FILE_OK;
            }
            else
            {
                log_error("offset [%d] crc invalid, try next one", offset);
            }

            zfree(*buf);
        }
        else
        {
            log_error("offset [%d] header magic [%04X] len [%u] invalid, try next one", offset, h.magic, h.len);
        }

        /* magic not match, try next bytes */
        offset += 1;
        file_set_pos(fd, offset);
    }

    if (n == 0)
    {
        //log_debug("read header, file EOF");
        return BL_FILE_EOF;
    }

    log_debug("read header, ret [%d], file error[%s]", n, strerror(errno));
    return BL_FILE_SYS_ERR; 
}

int bin_read(int fd, uint8_t **buf, uint32_t *len, off_t *offset, uint64_t *ts, MallocFunc f, uint64_t *sid)
{
    int ret      = 0;
    uint8_t *b   = NULL;
    uint32_t n   = 0;
    MallocFunc func = f ? f : malloc;
    

    ret = bin_read_internal_v2(fd, &b, &n, *offset, ts, sid);

    *offset = file_pos(fd);

    if (*offset < 0)
    {
        log_error("read file error [%s]", strerror(errno));
        return BL_FILE_SYS_ERR;
    }    

    if (ret == BL_FILE_OK)
    {
        *buf = (uint8_t *)func(n);
        *len = n;
        memcpy(*buf, b, *len);
        zfree(b);
    }

    return ret;
}

int bin_read_ts(int fd, off_t *offset, uint64_t *ts)
{
    int ret = 0;
    uint8_t *b  = NULL;
    uint32_t n  = 0;
    uint64_t sid = 0;

    ret = bin_read_internal_v2(fd, &b, &n, *offset, ts, &sid);
    *offset = file_pos(fd);

    if (*offset < 0)
    {
        log_error("read file error [%s]", strerror(errno));
        return BL_FILE_SYS_ERR;
    }    

    if (ret == BL_FILE_OK)
    {
        zfree(b); 
    }

    return ret;
}

int bin_read_by_ts(int fd, uint8_t **buf, uint32_t *len, off_t *offset, uint64_t *ts, MallocFunc f)
{
    int ret = 0;
    uint64_t mt  = 0;
    uint32_t n   = 0;
    off_t    off = *offset;
    uint8_t *b   = NULL;
    uint64_t sid = 0;
    MallocFunc func = f ? f : malloc;

    while (1)
    {
        ret = bin_read_internal_v2(fd, &b, &n, off, &mt, &sid);
        if (ret == BL_FILE_OK)
        {
            if (mt >= *ts)
            {
                log_debug("file ts [%"PRIu64"], param ts [%"PRIu64"]", mt, *ts);
                *buf = (uint8_t *)func(n);
                *len = n;
                memcpy(*buf, b, *len);
                zfree(b); 
                *offset = file_pos(fd);
                *ts = mt;
                return ret;
            }

            off = file_pos(fd);
            zfree(b);
            continue;
        }
        else
            return ret;
    }

    return ret;
}

static int bin_get_offset_by_ts(int fd, off_t *offset, uint64_t ts, uint64_t *last_ts)
{
    int      ret = -1;
    uint64_t mt  = 0;
    off_t    off = 0;
    uint8_t *b   = NULL;
    uint32_t n   = 0;
    uint64_t sid = 0;

    while (1)
    {
        ret = bin_read_internal_v2(fd, &b, &n, off, &mt, &sid);
        if (ret == BL_FILE_OK)
        {
            log_debug("file ts [%"PRIu64"], param ts [%"PRIu64"]", mt, ts);
            //save last_ts
            *last_ts = mt;
            if (mt > ts)
            {
                log_debug("get ts offset [%u]", off); 
                zfree(b); 
                *offset = off; 
                return 0;
            }

            off = file_pos(fd);
            zfree(b);
            continue;
        }
        else
            return -1;
    }

    return -1;
}

static int bin_find_idx_by_ts(bin_meta_t *bm, uint64_t ts)
{
    int      i;
    int      fd;
    uint64_t tc = 0;
    uint64_t th = 0;
    int      suit_idx = -1;
    int      min_ts_idx = -1;
    uint64_t min_ts = UINT64_MAX;
    off_t    offset = 0; 

    rt_file_t  rt;
    memcpy(&rt, &bm->rt, sizeof(rt));

    for (i = 0; i < rt.max_idx; i++)
    {
        rt.idx = i;
        if (!rt_file_exist(&rt))
        {
            continue;
        }

        fd = rt_flags_open(&rt, O_RDONLY);
        if (fd < 0)
        {
            continue;
        }

        offset = 0;
        if (bin_read_ts(fd, &offset, &tc) == BL_FILE_OK)
        {
            /* get very closed ts idx */
            if (tc <= ts && tc > th)
            {
                th = tc;
                suit_idx = i;
            }
    
            /* get min timestamp file idx */
            if (min_ts > tc)
            {
                min_ts = tc;
                min_ts_idx = i;
                log_debug("min_ts[%llu] idx[%d]", min_ts, i);
            }
        }
        close(fd); 
    }        

    /* when return < 0 cant't get idx, >= 0 ok */
    return suit_idx >= 0 ? suit_idx : min_ts_idx; 
}

static int bin_find_last_idx(bin_meta_t *bm)
{
    int      i;
    int      fd;
    uint64_t ts = 0;
    int      max_ts_idx = -1;
    uint64_t max_ts = 0;
    off_t    offset = 0; 

    rt_file_t  rt;
    memcpy(&rt, &bm->rt, sizeof(rt));

    for (i = 0; i < rt.max_idx; i++)
    {
        rt.idx = i;
        if (!rt_file_exist(&rt))
        {
            continue;
        }

        fd = rt_flags_open(&rt, O_RDONLY);
        if (fd < 0)
        {
            continue;
        }

        if (bin_read_ts(fd, &offset, &ts) == BL_FILE_OK)
        {
            /* get max timestamp file idx */
            if (max_ts < ts)
            {
                max_ts = ts;
                max_ts_idx = i;
            }
        }
        close(fd); 
    }        

    /* ATTENTION !!!! set bm timestamp */
    bm->curr_ts = max_ts;

    /* when return < 0 cant't get idx, >= 0 ok */
    return max_ts_idx; 
}

int bin_init(bin_meta_t *bm, uint8_t *path, uint8_t *prefix,
            off_t max_size, int max_idx, uint64_t ts, int meta_persist,
            uint8_t *meta_name, int flags, mode_t mode)
{
    int             idx    = -1;
    off_t           offset = 0;
    rt_file_t      *rt     = &bm->rt;
    int             has_meta;

    /* first we try to get info from meta file */
    has_meta = meta_init(bm, path, prefix, max_size, max_idx,
                        meta_persist, meta_name, flags, mode);

    /* FIXME! has_meta == 0 */
    if (has_meta < 0)
    {
        log_error("init meta file error");
        return -1;
    }

    if (ts == BL_TIMESTAMP_LAST)
    {
        /* we can not get last meta info, try to calc it */
        if (!has_meta)
        {
            idx = bin_find_last_idx(bm);
            if (idx < 0)
            {
                log_debug("we can't get last idx, set to 0");
            }
            else
            {
                log_debug("we got last idx [%d]", idx);

                rt->idx = idx;

                /* set offset to end */
                char buf[256];
                rt_file_name(rt, buf, 256);
                bm->curr_offset = file_size(buf);   
            }
        }
    }
    else if (ts == BL_TIMESTAMP_FIRST)  /* try to find first ts */
    {
        /* here we use 0 to find min timestamp */

        /* FIXME! */
        idx = bin_find_idx_by_ts(bm, 0);
        if (idx < 0)
        {
            log_debug("we can't get first idx, set to 0");
        }
        else
        {
            log_debug("we got first idx [%d]", idx);

            rt->idx = idx;

            /* set offset to head */
            char buf[256];
            rt_file_name(rt, buf, 256);
            bm->curr_offset = 0;   
        }
    }
    else    /* try to find idx and offset by ts */
    {
        idx = bin_find_idx_by_ts(bm, ts);
        if (idx < 0)
        {
            log_debug("we can't get idx by ts, set to 0");
        }
        else
        {
            log_debug("we got ts idx [%d]", idx);

            rt->idx = idx;

            /* set offset to head */
            char buf[256];
            rt_file_name(rt, buf, 256);
            int fd = open(buf, O_RDONLY);
            if (fd < 0)
            {
                log_error("can't open idx [%d] file, error[%s]", idx, strerror(errno));
                meta_close(bm);
                return -1;
            }

            uint64_t last_ts = 0;
            if (bin_get_offset_by_ts(fd, &offset, ts, &last_ts) < 0)
            {
                log_error("can't find ts [%"PRIu64"] offset in idx [%d] file, set to end", ts, idx);
                offset = file_len(fd);
                if (offset < 0)
                {
                    log_error("file len error, error[%s]", strerror(errno));
                    close(fd);
                    return -1;
                }
                /* ATTENTION !!!! set bm timestamp to last ts*/
                if (last_ts != 0)
                    bm->curr_ts = last_ts;
            }
            else
            {
                bm->curr_ts = ts;
            }
            
            close(fd);
            log_debug("we get idx [%d] offset [%"PRIu64"]by ts [%"PRIu64"]", idx, offset, ts);
            bm->curr_offset = offset;   
        }
    }

    /**
     * open the rotate file.
     * when inital open file, we need not use O_TRUNC flag
     */
    if (rt_flags_open(rt, rt->flags & ~O_TRUNC) < 0)
    {
        log_debug("open rotate file error idx[%d] [%s]error[%s]", rt->idx, rt->path, strerror(errno));
        meta_close(bm);
        return -1;
    }

    file_set_pos(rt->fd, bm->curr_offset);
    log_debug("bin init success idx[%d], ts[%"PRIu64"] offset [%"PRIu64"]", bm->rt.idx, bm->curr_ts, bm->curr_offset);

    return 0;
}

void bin_destroy(bin_meta_t *bm)
{
    if (bm)
    {
        if (bm->need_persist && bm->fd > 0)
            close(bm->fd);

        if (bm->rt.fd > 0)
            close(bm->rt.fd);
    }
}

int bin_write(bin_meta_t *bm, const uint8_t *buf, uint32_t len)
{
    int ret; 
    uint64_t  ts  = 0;
    rt_file_t *rt = &bm->rt;

    if (rt_need_rotate(rt))
    {
        rt_switch_file(rt);    
    }

    ret = bin_write_internal(rt->fd, buf, len, &ts);
    log_debug("bin write ret[%d]", ret);

    bm->curr_ts = ts;
    bm->curr_offset = file_pos(rt->fd);  
    if (bm->need_persist)
    {
        meta_update(bm);
    }

    return ret;    
}

int bin_write_v2(bin_meta_t *bm, const uint8_t *buf, uint32_t len, uint64_t sid)
{
    int ret; 
    uint64_t  ts  = 0;
    rt_file_t *rt = &bm->rt;

    if (rt_need_rotate(rt))
    {
        rt_switch_file(rt);    
    }

    ret = bin_write_internal_v2(rt->fd, buf, len, &ts, sid);
    log_debug("bin write ret[%d]", ret);

    bm->curr_ts = ts;
    bm->curr_offset = file_pos(rt->fd);  
    if (bm->need_persist)
    {
        meta_update(bm);
    }

    return ret;    
}

static int get_next_file_ts(rt_file_t *rt, uint64_t *out_ts)
{
    uint64_t    ts = 0;
    off_t       offset;
    char        next[256];

    rt_next_file_name(rt, next, 256);
    int fd = open(next, O_RDONLY);
    if (fd < 0)
    {
        return -1;
    }

    offset = 0;
    /* try to get next file first timestamp */
    if (bin_read_ts(fd, &offset, &ts) != BL_FILE_OK)
    {
        log_info("read ts error [%s] file, error[%s]", next, strerror(errno));
        close(fd);
        return -1;
    }

    *out_ts = ts;
    close(fd);
    return 0;
} 

int bin_read2(bin_meta_t *bm, uint8_t **buf, uint32_t *len, MallocFunc f, uint64_t *sid)
{
    uint64_t    ts  = 0;
    rt_file_t  *rt  = &bm->rt;
    int         ret; 
    off_t       offset;
    int         i;

    offset = bm->curr_offset;
    
    ret = bin_read(rt->fd, buf, len, &offset, &ts, f, sid);
    if (ret == BL_FILE_SYS_ERR)
    {
        return ret;
    }

    bm->curr_offset = offset;  
    //log_debug("read offset[%d]", bm->curr_offset);

    /* FIXME! when file EOF, we must check next file timestamp is larger and switch it, judge by size is not enough */ 
    if (ret == BL_FILE_OK) 
    {
        bm->curr_ts = ts;
    }
    else if (ret == BL_FILE_EOF || ((signed)bm->curr_offset >= rt->max_size && ret == BL_FILE_EOF))
    {
        i = get_next_file_ts(rt, &ts);
        if (i < 0)
        {
            //log_info("bin log [%d] is large [%"PRIu64"], get next file ts ret [%d] error[%s]",
            //        bm->rt.idx, bm->curr_offset, ret, strerror(errno));
            return BL_FILE_EOF;
        }
        /* we need next timestamp larger, so we can switch file */
        if (ts < bm->curr_ts)
        {
            log_info("file large [%"PRIu64"], but next file ts[%"PRIu64"] less than current ts[%"PRIu64"] ", bm->curr_offset, ts, bm->curr_ts);
            return BL_FILE_EOF;
        }

        if (rt_switch_file(rt) < 0)
        {
            log_error("file eof, read switch file error");
            return -1;
        }
        else
        {
            ret = BL_FILE_SWITCH;
        }

        bm->curr_offset = 0;
    }

    if (bm->need_persist)
    {
        meta_update(bm);
    }

    return ret;    
}


