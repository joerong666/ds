#ifndef _DB_IO_ENGINE_H_
#define _DB_IO_ENGINE_H_

extern int db_io_init();
extern void db_io_uninit();
extern int commit_dbe_set_task(void *ctx);
extern int commit_dbe_get_task(void *ctx);
extern int commit_write_bl_task(void *ctx);

#endif /* _DB_IO_ENGINE_H_ */

