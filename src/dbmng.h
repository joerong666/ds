#ifndef _DB_MNG_H_
#define _DB_MNG_H_

#include "redis.h"

struct dbmng_conf_st
{
    char *dbe_root_path;
    char *bl_root_path;
    char *log_prefix;
    int binlog_max_size;
};

typedef struct dbmng_ctx_st
{
    int tag:2;
    int stop:2;
    int giveup:2;
    int pending:2;
    int block_cp:2;
    int hangup_cp:2;

    int cp_status;

    redisDb *rdb;
    void *db;
    void *binlogtab;
    void *binlog;
    void *t;  /* io transaction with dbe thread */
    void *c;  /* redisClient */

    void *db_slave;
    void *binlog_slave;
} dbmng_ctx;

extern const char *get_dbe_path(char *path, int path_len, const char *name);
extern const char *make_dbe_path(char *path, int path_len, const char *name);

extern int dbmng_init(int tag, int slave);
extern int dbmng_uninit(int tag);
extern void dbmng_set_rdb(int tag, redisDb *rdb);
extern void dbmng_check_key(int tag, const robj *key);
extern int dbmng_check_key_io(int tag, const robj *key);
extern int dbmng_save_op(int tag, const char *cmd, const robj *key, int argc, robj **argv, unsigned long long ds_id, unsigned char db_id);
extern void dbmng_try_cp(int tag);
extern void dbmng_start_cp(int tag);
extern void dbmng_print_dbeinfo(int tag);
extern int initDbmngConfig();

extern void *dbmng_get_db(int tag, int master);
extern void *dbmng_get_bl(int tag);
extern int dbmng_cp_is_active(int tag);
extern int dbmng_cp_is_blocked(int tag);
extern int dbmng_block_upd_cp(int tag, uint64_t *ts);
extern int dbmng_unblock_upd_cp(int tag);
extern void dbmng_release_bl(int tag);
extern void dbmng_reset_bl(int tag);

/* return:
 * 0 - not existed
 * 1 - existed
 * 2 - system fail
 */
extern int dbmng_key_exist(int tag, const robj *key, uint64_t *ver);
extern robj *dbmng_lookup_key(int tag, const robj *key, time_t *expire);
extern int dbmng_set_key(int tag, const robj *key, int argc, robj **argv);
extern int dbmng_wr_bl(int tag, const char *cmd, const robj *key, int argc, robj **argv, unsigned long long ds_id, unsigned char db_id);
extern int dbmng_del_key(int tag, const robj *key);

extern struct dbmng_conf_st dbmng_conf;

extern dbmng_ctx *get_entry_ctx(int tag);

#endif  // _DB_MNG_H_

