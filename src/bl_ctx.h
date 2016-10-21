#ifndef _BL_CTX_H_
#define _BL_CTX_H_

#include "binlogtab.h"


extern const char *get_bl_path(char *path, int path_len, int binlog_type);
extern const char *make_bl_path(char *path, int path_len, int binlog_type);

/* binlog_type:
 * 0 - local binlog
 * 1 - master-slave sync binlog
 * 2 - transfer binlog
 */
extern void *bl_init(int master, int binlog_type);
extern void bl_uninit(void *binlog);
extern int bl_load_to_mem(void *binlog, binlog_tab *blt);

extern int tag_new_cp(void *binlog, int new_idx, int new_offset);
extern int confirm_new_cp(void *binlog);

extern int bl_write_op(void *binlog, unsigned char cmd, const robj *key, int argc, const robj **argv, unsigned long long ds_id, unsigned char db_id, unsigned char type);
extern int bl_write_confirm(void *binlog, const char *key, unsigned char db_id);

extern void *bl_get_read_it(const char *path, int idx, int offset);
/* return:
 * 0 - succ
 * 1 - over
 */
extern int bl_get_next_rec(void *it, char **buf, int *buf_len, int *next_offset);
extern void bl_release_it(void *it);
extern void bl_release_rec(char *buf);

extern uint64_t bl_get_last_ts(void *binlog, int now_flg);
extern int bl_get_cp_idx(void *binlog);
extern int bl_get_cp_offset(void *binlog);

extern int decode_bl(const char *filename, int to_file_flg);

extern void *do_handle_write_bl(void *ctx);
#ifndef _UPD_DBE_BY_PERIODIC_
extern void *do_write_bl(void *ctx);
extern void *do_write_dbe(void *ctx);
#endif
extern void after_write_bl(void *ctx);

extern void redo_bl(redisDb *rdb, void *binlog, int load_bl_cnt);
extern void reset_bl(void *binlog);

#endif  /* _BL_CTX_H_ */

