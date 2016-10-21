#ifndef _BIN_LOG_TAB_H_
#define _BIN_LOG_TAB_H_

#include "redis.h"

#include "ds_type.h"

#include "hidb2/src/db/db_com_def.h"

/* just for test */
typedef struct key_val_t
{
    void *key;
    void *val;
} key_val;

typedef struct binlog_tab_t
{
    dict *active;
    dict *immutable;
    size_t op_cnt_act;

    size_t imm_size;
    size_t imm_scan_cnt;
    size_t imm_do_cnt;
    size_t imm_done_cnt;
    size_t imm_doing_cnt;
    size_t imm_giveup_cnt;
    size_t imm_error_cnt;
    size_t imm_read_rdb_cnt;
    //key_val *ar;
} binlog_tab;

typedef struct upd_dbe_param_st
{
    char cmd_type;
    unsigned char db_id;
    void *key; /* current node: key sds string */
    void *ble; /* current node: binlogEngtry */

    size_t pdel_key_len;
    char *pdel_key;
    size_t cnt[2];
    kvec_t *kv[2]; /* 0 - put; 1 - delete; */
    kvec_t one;
} upd_dbe_param;

#define BLE_STATUS_INIT                0
#define BLE_STATUS_DOING               1
#define BLE_STATUS_DONE                2
typedef struct binlogEntry_t
{
    unsigned char giveup;
    unsigned char status;
    unsigned char db_id;
    unsigned char rsvd;
    int cache_len;
    char *cache_byte; /* the lastest snapshot, for put to db io */
    list *oplist;
} binlogEntry;

typedef struct opAttr_t
{
    unsigned char cmd;
    unsigned char argc;
    robj **argv;
} opAttr;

#define RESTORE_LVL_DBE                0
#define RESTORE_LVL_IMM                1
#define RESTORE_LVL_ACT                2


extern binlog_tab *blt_init();
extern void blt_uninit(binlog_tab *blt);

extern int blt_exist_key(binlog_tab *blt, const robj *key);

extern int blt_add_digsig(binlog_tab *blt, const binlog_str *key, const binlog_str *digest);
//extern int blt_add_digsig2(binlog_tab *blt, const robj *key, uint64_t version);
extern int blt_add_op_generic(binlog_tab *blt, unsigned char cmd, const robj *key, int argc, robj **argv);
extern int blt_add_op(binlog_tab *blt, unsigned char cmd, const binlog_str *key, int argc, const binlog_str *argv);

extern binlogEntry *getbinlogEntryEx(binlog_tab *blt, const char *key, int key_len, int *newflg);
extern int restore_a(redisDb *rdb, binlog_tab *blt, void *db, const robj *key, int lvl, int check);
extern int restore(redisDb *rdb, binlog_tab *blt, void *db, const robj *key, int lvl);

extern int switch_blt_only(redisDb *rdb, binlog_tab *blt);
extern int switch_blt(redisDb *rdb, binlog_tab *blt);
extern void release_blt_immutable(binlog_tab *blt);
extern void reset_blt_active(binlog_tab *blt);
extern dictIterator *scan_immutable(redisDb *rdb, void *db, binlog_tab *blt, dictIterator *dit, upd_dbe_param *param);
extern void finish_ble(void *ble);

extern int get_blt_cnt(binlog_tab *blt);

extern int blt_cp_confirmed(binlog_tab *blt, const binlog_str *key);

extern void redo_blt(redisDb *rdb, binlog_tab *blt, void *db);

extern void save_keys_mirror(redisClient *c);
extern void save_key_mirror_evict(const robj *key);

extern int get_value_from_rdb(redisDb *rdb, sds key, char **ptr, int *len);

extern void free_upd_dbe_param(upd_dbe_param *param);

#endif  // _BIN_LOG_TAB_H_

