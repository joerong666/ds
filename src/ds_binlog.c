#include "ds_binlog.h"
#include "serialize.h"

#include "ds_log.h"
#include "ds_util.h"

#include <arpa/inet.h>

extern void print_obj(const robj *o);

static void make_cp_file_tmp(char *filename, int len, const char *path, const char *prefix)
{
    snprintf(filename, len, "%s/%s.cp.tmp", path, prefix);
}

static void make_idx_file_tmp(char *filename, int len, const char *path, const char *prefix)
{
    snprintf(filename, len, "%s/%s.index.tmp", path, prefix);
}

static const char *str_to_uint(const char *str, int *i)
{
    int init = 0;
    const char *p = str;
    while (*p == ' ' || *p == '\t') p++;
    while (*p != 0 && *p != ' ' && *p != '\t')
    {
        if (init == 0)
        {
            init = 1;
            *i = 0;
        }

        const char c = *p - '0';
        if (c < 0 || c > 9)
        {
            /* fail */
            *i = -1;
            return p;
        }
        *i = *i * 10 + c;

        p++;
    }

    return p;
}

void trim_bl_line(char *line)
{
    const int len = strlen(line);
    if (line[len - 1] == '\n')
    {
        line[len - 1] = 0;
    }
}

void parse_bl_list_line(const char *line, int *idx, int *offset)
{
    /* format: idx + space + [offset] */

    if (!line || !idx)
    {
        return;
    }

    const char *p = line;
    p = str_to_uint(p, idx);
    if (*idx < 0)
    {
        return;
    }

    if (offset)
    {
        p = str_to_uint(p, offset);
    }

    return;
}

int parse_op_rec(const char *buf, int buf_len, op_rec *op)
{
    if (!buf || buf_len < (1 + 2))
    {
        /* illegal */
        log_error("illegal paramter, buf_len=%d, buf=%p", buf_len, buf);
        return 1;
    }

    int idx = 0;

    /* format
     *
     * old (before 2014.07.07) :
     * cmd(1) + key_len(2) + key [+ seq(8) + ts(4)] [+ argc(1)]
     *
     * new :
     * version(1) + cmd(1) + key_len(2) + key [+ ts(4)] [+ argc(1)] [argv:len(2)+val]
     * (version: from 127)
     *
     * 2014.09.30 :
     * version(1) + cmd(1) + key_len(2) + key [+ ts(4)] [+ argc(1)] [argv:len(4)+val]
     * (version: 128)
     *
     * 2015.10.12 :
     * version(1) + cmd(1) + key_len(2) + key + encode(1) + [+ ts(4)] [+ argc(4)] [argv:len(4)+val]
     * (version: 129)
     * encode: bit7:0-cache;1-persistence; bit6-bit0:db_id
     */

    op->cmd = *(const unsigned char *)(buf + idx);
    idx += sizeof(op->cmd);

    unsigned char prtcl_ver = 0;
    int fmt_len = 0;
    if (op->cmd >= 127)
    {
        prtcl_ver = op->cmd;
        if (prtcl_ver > 129)
        {
            log_error("unknown binlog format, prtcl_ver=%d", prtcl_ver);
            return 1;
        }
        op->cmd = *(const unsigned char *)(buf + idx);
        idx += 1;
        fmt_len = 1 + 1 + 2;
    }
    else
    {
        fmt_len = 1 + 2;
    }

    if (buf_len < fmt_len)
    {
        log_error("parse binlog fail: buf_len(%d) < %d, too small, cmd=%d, prtcl=%d"
                  , buf_len, fmt_len, op->cmd, prtcl_ver);
        return 1;
    }

    op->key.len = ntohs(*(short *)(buf + idx));
    idx += sizeof(short);
    op->key.ptr = buf + idx;
    idx += op->key.len;

    fmt_len += op->key.len + (prtcl_ver == 129 ? 1 : 0);
    if (buf_len < fmt_len)
    {
        log_error("parse binlog fail: buf_len(%d) < %d, too small, cmd=%d, prtcl=%d"
                  , buf_len, fmt_len, op->cmd, prtcl_ver);
        return 1;
    }

    op->argc = 0;
    op->argv = 0;
    op->db_id = prtcl_ver == 129 ? (*(unsigned char *)(buf + idx) & 0x7f) : 0;
    op->type = prtcl_ver == 129 ? ((*(unsigned char *)(buf + idx) & 0x80) ? 1 : 0) : 2;
    if (prtcl_ver == 129)
    {
        idx++;
    }

    if (op->cmd == 0)
    {
        return 0;
    }

    if (prtcl_ver == 0)
    {
        fmt_len += 8 + 4;
        if (buf_len < fmt_len)
        {
            log_error("parse binlog fail: buf_len(%d) < %d, too small, cmd=%d, prtcl=%d"
                      , buf_len, fmt_len, op->cmd, prtcl_ver);
            return 1;
        }
        int64_t seq = (int64_t)ntohll(*(uint64_t *)(buf + idx));
        idx += sizeof(seq);
    }
    else
    {
        fmt_len += 4;
        if (buf_len < fmt_len)
        {
            log_error("parse binlog fail: buf_len(%d) < %d, too small, cmd=%d, prtcl=%d"
                      , buf_len, fmt_len, op->cmd, prtcl_ver);
            return 1;
        }
    }

    op->time_stamp = ntohl(*(uint32_t *)(buf + idx));
    idx += sizeof(uint32_t);

    if (idx == buf_len)
    {
        /* over */
        return 0;
    }

    if (prtcl_ver == 129)
    {
        fmt_len += 4;
        if (buf_len < fmt_len)
        {
            log_error("parse binlog fail: buf_len(%d) < %d, too small, cmd=%d, prtcl=%d"
                      , buf_len, fmt_len, op->cmd, prtcl_ver);
            return 1;
        }
        op->argc = ntohl(*(uint32_t *)(buf + idx));
        idx += sizeof(uint32_t);
    }
    else
    {
        fmt_len += 1;
        op->argc = (unsigned char)*(buf + idx);
        idx += sizeof(char);
    }
    if (op->argc == 0)
    {
        /* over */
        return 0;
    }
    if (idx == buf_len)
    {
        /* illegal */
        log_error("no argv in buffer but argc=%d, buf_len=%d, cmd=%d, prtcl=%d"
                , op->argc, buf_len, op->cmd, prtcl_ver);
        return 1;
    }

    op->argv = (binlog_str *)zcalloc(sizeof(binlog_str) * op->argc);
    if (!op->argv)
    {
        /* fail */
        log_error("zcalloc() fail for argv, argc=%d", op->argc);
        return 1;
    }

    uint32_t i;
    for (i = 0; i < op->argc; i++)
    {
        const int argv_len_size = prtcl_ver < 128 ? sizeof(uint16_t) : sizeof(int32_t);
        if (idx + argv_len_size > buf_len)
        {
            /* fail */
            log_error("parse argv len fail, len=%d, argc=%d, i=%d, idx=%d, cmd=%d, prtcl=%d",
                      buf_len, op->argc, i, idx, op->cmd, prtcl_ver);
            zfree(op->argv);
            op->argv = 0;
            return 1;
        }
        uint32_t argv_len = prtcl_ver < 128 ? ntohs(*(uint16_t *)(buf + idx)) : ntohl(*(uint32_t*)(buf + idx));
        idx += argv_len_size;

        if (idx + argv_len > (unsigned)buf_len)
        {
            /* fail */
            log_error("parse argv fail, len=%d, argc=%d, i=%d, idx=%d, argv_len=%d, cmd=%d, prtcl=%d",
                      buf_len, op->argc, i, idx, argv_len, op->cmd, prtcl_ver);
            zfree(op->argv);
            op->argv = 0;
            return 1;
        }

        op->argv[i].ptr = buf + idx;
        op->argv[i].len = argv_len;

        idx += argv_len;
    }

    //log_debug("finish parse op info: idx=%d, len=%d", idx, buf_len);

    return 0;
}

int parse_op_rec_key(const char *buf, int buf_len, op_rec *op)
{
    if (!buf || buf_len < (1 + 2))
    {
        /* illegal */
        log_error("illegal paramter, buf_len=%d, buf=%p", buf_len, buf);
        return 1;
    }

    int idx = 0;

    /* format
     *
     * old (before 2014.07.07) :
     * cmd(1) + key_len(2) + key [+ seq(8) + ts(4)] [+ argc(1)]
     *
     * new :
     * version(1) + cmd(1) + key_len(2) + key [+ ts(4)] [+ argc(1)] [argv:len(2)+val]
     * (version: from 127)
     *
     * 2014.09.30 :
     * version(1) + cmd(1) + key_len(2) + key [+ ts(4)] [+ argc(1)] [argv:len(4)+val]
     * (version: 128)
     *
     * 2015.10.12 :
     * version(1) + cmd(1) + key_len(2) + key + encode(1) + [+ ts(4)] [+ argc(4)] [argv:len(4)+val]
     * (version: 129)
     * encode: bit7:0-cache;1-persistence; bit6-bit0:db_id
     */

    op->cmd = *(const unsigned char *)(buf + idx);
    idx += sizeof(op->cmd);

    unsigned char prtcl_ver = 0;
    int fmt_len = 0;
    if (op->cmd >= 127)
    {
        prtcl_ver = op->cmd;
        if (prtcl_ver > 129)
        {
            log_error("unknown binlog format, prtcl_ver=%d", prtcl_ver);
            return 1;
        }
        op->cmd = *(const unsigned char *)(buf + idx);
        idx += 1;
        fmt_len = 1 + 1 + 2;
    }
    else
    {
        fmt_len = 1 + 2;
    }

    if (buf_len < fmt_len)
    {
        log_error("parse binlog fail: buf_len(%d) < %d, too small, cmd=%d, prtcl=%d"
                  , buf_len, fmt_len, op->cmd, prtcl_ver);
        return 1;
    }

    op->key.len = ntohs(*(short *)(buf + idx));
    idx += sizeof(short);
    op->key.ptr = buf + idx;
    idx += op->key.len;

    fmt_len += op->key.len + (prtcl_ver == 129 ? 1 : 0);
    if (buf_len < fmt_len)
    {
        log_error("parse binlog fail: buf_len(%d) < %d, too small, cmd=%d, prtcl=%d"
                  , buf_len, fmt_len, op->cmd, prtcl_ver);
        return 1;
    }

    op->argc = 0;
    op->argv = 0;
    op->db_id = prtcl_ver == 129 ? (*(unsigned char *)(buf + idx++) & 0x7f) : 0;
    op->type = prtcl_ver == 129 ? ((*(unsigned char *)(buf + idx++) & 0x80) ? 1 : 0) : 2;

    return 0;
}

/* return:
 * -1 : fail
 *  0 : cache
 *  1 : persistence
 *  2 : illegal
 */
int get_binlog_type(const char *buf, int buf_len)
{
    op_rec op;
    const int ret = parse_op_rec_key(buf, buf_len, &op);
    return ret == 0 ? op.type : -1;
}

int upd_cp_file(const char *path, const char *prefix, int cp_idx, int cp_offset, int cp_idx2, int cp_offset2)
{
    char tmp[256];
    char f[256];
    make_cp_file_tmp(tmp, sizeof(tmp), path, prefix);
    make_cp_file(f, sizeof(f), path, prefix);

    // Truncate  file  to  zero  length  or  create text file for writing
    FILE *fp = fopen(tmp, "w");
    if (!fp)
    {
        log_error("fopen() fail for write cp tmp file, errno=%d, file=%s", errno, tmp);
        return -1;
    }
    const int cnt = fprintf(fp, "%d %d\n%d %d\n", cp_idx, cp_offset, cp_idx2, cp_offset2);
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
        log_error("rename() for cp fail, errno=%d", errno);
        return -1;
    }

    return 0;
}

int upd_idx_file(const char *path, const char *prefix, int idx)
{
    char tmp[256];
    make_idx_file_tmp(tmp, sizeof(tmp), path, prefix);
    char f[256];
    make_idx_file(f, sizeof(f), path, prefix);

    // Truncate  file  to  zero  length  or  create text file for writing
    FILE *fp = fopen(tmp, "w");
    if (!fp)
    {
        log_error("fopen() fail for write index file, errno=%d, file=%s", errno, tmp);
        return -1;
    }
    const int cnt = fprintf(fp, "%d\n", idx);
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
        log_error("rename() for index file fail, errno=%d", errno);
        return -1;
    }

    return 0;
}

char *serialize_op(unsigned char cmd, const robj *key, int argc, const robj **argv, int32_t ts, unsigned char db_id, unsigned char type, int *pLen)
{
    int i;
    char *buf = 0;
    const int key_len = sdslen(key->ptr);
    const unsigned char ver = server.prtcl_redis == 0 ? 128 : 129;

    /* 2015.10.12 */
    /* 
     * mc use old format:
     * version(1) + cmd(1) + key_len(2) + key [+ ts(4)] [+ argc(1)] [argv:len(4)+val]
     * (version: 128)
     * redis use new format:
     * version(1) + cmd(1) + key_len(2) + key(n) + encode(1) + ts(4) + argc(4) 
     * (version: 129)
     */
    if (ver == 128)
    {
        *pLen = 1 + 1 + sizeof(uint16_t) + key_len + sizeof(int32_t) + 1;
    }
    else
    {
        *pLen = 1 + 1 + sizeof(uint16_t) + key_len + 1 + sizeof(int32_t) + 4;
    }
    for (i = 0; i < argc; i++)
    {
        /* argv[i].len(4) + argv[i] */
        *pLen += sizeof(uint32_t) + serializeObjLen(argv[i]);
    }

    buf = (char *)zmalloc(*pLen);
    if (buf)
    {
        int idx = 0;

        *(unsigned char *)(buf + idx++) = ver;

        *(buf + idx++) = cmd;

        *(uint16_t *)(buf + idx) = htons(key_len);
        idx += sizeof(uint16_t);
            
        memcpy(buf + idx, key->ptr, key_len);
        idx += key_len;

        if (ver == 129)
        {
            *(unsigned char *)(buf + idx++) = (db_id & 0x7f) + ((type ? 1 : 0) << 7);
        }

        *(uint32_t *)(buf + idx) = htonl(ts);
        idx += sizeof(uint32_t);

        if (ver == 128)
        {
            *(unsigned char *)(buf + idx++) = (unsigned char)argc;
        }
        else
        {
            *(uint32_t *)(buf + idx) = htonl(argc);
            idx += sizeof(uint32_t);
        }

        for (i = 0; i < argc; i++)
        {
            const int len = serializeObjLen(argv[i]);

            *(uint32_t *)(buf + idx) = htonl(len);
            idx += sizeof(uint32_t);
            
            serializeObj2Buf(argv[i], buf + idx, len, 0);
            idx += len;
        }
    }
    else
    {
        log_error("serialize_op: zmalloc() fail for %d", *pLen);
    }

    return buf;
}

char *serialize_digsig(const robj *key, int *pLen, unsigned char ds_id)
{
    char *buf = 0;
    const int key_len = sdslen(key->ptr);

    if (server.prtcl_redis == 0)
    {
        /* mc use old format */
        /* zero(1) + key_len(2) + key(n) */
        *pLen = 1 + 2 + key_len;
    }
    else
    {
        /* version(1) + zero(1) + key_len(2) + key(n) + encode(1) */
        *pLen = 1 + 1 + 2 + key_len + 1;
    }

    buf = (char *)zmalloc(*pLen);
    if (buf)
    {
        int idx = 0;

        if (server.prtcl_redis)
        {
            *(unsigned char*)(buf + idx++) = 129;
        }

        *(buf + idx++) = 0;

        *(short *)(buf + idx) = htons(key_len);
        idx += sizeof(short);
            
        memcpy(buf + idx, key->ptr, key_len);
        idx += key_len;

        if (server.prtcl_redis)
        {
            *(unsigned char *)(buf + idx++) = (ds_id & 0x7f) + 0x80;
        }
    }
    else
    {
        log_error("serialize_digsig: zmalloc() fail for %d", *pLen);
    }

    return buf;
}

void make_bl_file(char *filename, int len, const char *path, const char *prefix, int idx)
{
    snprintf(filename, len, "%s/%s.%03d", path, prefix, idx);
}

void make_idx_file(char *filename, int len, const char *path, const char *prefix)
{
    snprintf(filename, len, "%s/%s.index", path, prefix);
}

void make_cp_file(char *filename, int len, const char *path, const char *prefix)
{
    snprintf(filename, len, "%s/%s.cp", path, prefix);
}

int get_bl_idx(const char *path, const char *prefix, int *idx)
{
    if (!path || !prefix || !idx)
    {
        return 1;
    }

    char f[256];
    make_idx_file(f, sizeof(f), path, prefix);
    FILE *fp = fopen(f, "r");
    if (!fp)
    {
        if (errno == 2)
        {
            /* no exist */
            log_info("%s not exist", f);
            return -1;
        }
        else
        {
            log_error("fopen() \"%s\" fail:%s", f, strerror(errno));
            return -2;
        }
    }
    else
    {
        /* current binlog: idx */
        char line[64];
        char *p = fgets(line, sizeof(line), fp);
        fclose(fp);
        if (p != NULL)
        {
            trim_bl_line(line);
            log_debug("bin_log_idx: %s", line);
            parse_bl_list_line(line, idx, 0);
            if (*idx < 0)
            {
                log_error("illegal binlog idx:%s", line);
                return 2;
            }
            return 0;
        }
        else
        {
            /* fgets() fail */
            log_error("fgets() fail, can't get binlog idx:%s", strerror(errno));
            return -3;
        }
    }
}

