#ifndef _DS_BINLOG_H_
#define _DS_BINLOG_H_

#include "redis.h"
#include "ds_type.h"

#define BL_MAX_IDX                40
#define GET_BL_NEXT_IDX(idx)      (idx + 1) % BL_MAX_IDX

typedef struct op_rec_st
{
    int32_t time_stamp;

    unsigned char cmd;
    unsigned char db_id;
    unsigned char type; /* 0 - cache; 1 - persistence; 2 - illegal */
    unsigned char rsvd;
    binlog_str key;
    uint32_t argc;
    binlog_str *argv;
} op_rec;

extern void trim_bl_line(char *line);
extern void parse_bl_list_line(const char *line, int *idx, int *offset);
extern int upd_cp_file(const char *path, const char *prefix, int cp_idx, int cp_offset, int cp_idx2, int cp_offset2);
extern int upd_idx_file(const char *path, const char *prefix, int idx);

extern char *serialize_op(unsigned char cmd, const robj *key, int argc, const robj **argv, int32_t ts, unsigned char db_id, unsigned char type, int *pLen);
extern char *serialize_digsig(const robj *key, int *pLen, unsigned char db_id);
extern int parse_op_rec(const char *buf, int buf_len, op_rec *op);
extern int parse_op_rec_key(const char *buf, int buf_len, op_rec *op);
extern int get_binlog_type(const char *buf, int buf_len);

extern void make_bl_file(char *filename, int len, const char *path, const char *prefix, int idx);
extern void make_idx_file(char *filename, int len, const char *path, const char *prefix);
extern void make_cp_file(char *filename, int len, const char *path, const char *prefix);

/*
 * return:
 * -1 : the file not exist
 * -2 : open file fail
 * -3 : read file fail
 *  0 : succ
 *  1 : illegal input parameter
 *  2 : illegal data in file
 */
extern int get_bl_idx(const char *path, const char *prefix, int *idx);

#endif /* _DS_BINLOG_H_ */

