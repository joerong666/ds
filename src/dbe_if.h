#ifndef _DBE_IF_H_
#define _DBE_IF_H_

#define DBE_ERR_SUCC              0
#define DBE_ERR_NOT_FOUND         1
#define DBE_ERR_CORRUPTION        2
#define DBE_ERR_IO_FAIL           3
#define DBE_ERR_OTHER_FAIL        4
#define DBE_ERR_ILLEGAL_ARG       5
#define DBE_ERR_RE_INIT           6
#define DBE_ERR_NEVER_INIT        7

#include "hidb2/src/db/db_com_def.h"

typedef void *(*dbeMallocFunc)(size_t size);
typedef void (*dbeFreeFunc)(void *ptr);
typedef int (*dbeFilterFunc)(void *ptr, size_t);
typedef int (*dbeValFilterFunc)(const char *, size_t, int *);

extern int dbe_init(const char *dbname, void **pdb, int read_only, int auto_purge, int dbe_fsize);
extern void dbe_uninit(void *db_ctx);
extern int dbe_startup(void *db_ctx);

extern int dbe_get(void *db_ctx, const char *key, size_t key_len, char **val, int *val_len, dbeMallocFunc f, int64_t *dur);
/* dbe need to release key & val */
extern int dbe_put(void *db_ctx, char *key, int key_len, char *val, int val_len);

/* dbe need to release data */
extern int dbe_mput(void *db_ctx, kvec_t *data, size_t cnt);
/* dbe need to release data */
extern int dbe_mdelete(void *db_ctx, kvec_t *data, size_t cnt);
extern void *dbe_pget(void *db_ctx, const char *key_prefix, size_t len);
extern int dbe_pdelete(void *db_ctx, const char *key_prefix, size_t len);

extern void dbe_free_ptr(void *ptr, dbeFreeFunc f);
extern int dbe_set_filter(void *db_ctx, dbeFilterFunc f);
extern int dbe_set_val_filter(void *db_ctx, dbeValFilterFunc f);
extern int dbe_clean(void *db_ctx);
extern int dbe_clean_file(void *db_ctx, const void *tm);
extern int dbe_freeze(void *db_ctx);
extern int dbe_unfreeze(void *db_ctx);
extern int dbe_merge(void *db_ctx, const char *path);
extern int dbe_purge(void *db_ctx);
extern void *dbe_create_it(void *db_ctx, int level);
extern int dbe_next_key(void *it, char **k, int *k_len, char **v, int *v_len, dbeMallocFunc f, int64_t *dur);
extern void dbe_destroy_it(void *it);
extern void dbe_print_info(void *db_ctx);

extern int dbe_tran_begin(void *db_ctx);
extern int dbe_tran_commit(void *db_ctx);

#endif // _DBE_IF_H_
