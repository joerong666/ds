#ifndef __REPLICA_DEF_H__
#define __REPLICA_DEF_H__

#include "replica_thread_mng.h"

extern int repl_bin_master_start(const char *path, const char *prefix,
                off_t max_size, int max_idx, int net_fd, uint64_t ts, void *res, freeres_func func,
                uint64_t master_sid, uint64_t slave_sid, const char *slave_ds_key, filter_func filter);
extern int repl_bin_slave_start(const char *master_ip, int master_port,
                const char *path, uint64_t ts, int redo_port, uint64_t sid, char *ds_key);
#endif
