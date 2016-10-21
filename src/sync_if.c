#include "sync_if.h"

#include <stdio.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <inttypes.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>

#include "ds_log.h"
#include "ds_util.h"
#include "ds_binlog.h"
#include "pf_file.h"
#include "bin_log.h"
#include "tcp_util.h"


int sync_slave_start(const char *master_ip, int master_port, const char *dir, long long bl_tag, int port, unsigned long long ds_id, const char *ds_key)
{
    log_prompt("starting slave(%s:%d): dir=%s, bl_tag=%lld, port=%d, ds_id=%llu, ds_key=%s",
               master_ip, master_port, dir, bl_tag, port, ds_id, ds_key);
    return repl_bin_slave_start(master_ip, master_port, dir, (uint64_t)bl_tag, port, ds_id, ds_key);
}

extern int sync_master_start(const char *bl_dir, const char *prefix, off_t max_size, int max_idx, redisClient *c, freeres_func f, unsigned long long ds_id_m, unsigned long long ds_id_s, const char *ds_key_s, filter_func filter)
{
    log_prompt("starting master: bl_dir=%s, master_ds_id=%llu, slave_ds_id=%llu, slave_ds_key=%s", bl_dir, ds_id_m, ds_id_s, ds_key_s);
    return repl_bin_master_start(bl_dir, prefix, max_size, max_idx, c->fd, (uint64_t)c->bl_tag, (void *)c, f, ds_id_m, ds_id_s, ds_key_s, filter);
}

int sync_slave_stop(const char *master_ip, int master_port)
{
    log_prompt("stopping master(%s:%d)", master_ip, master_port);
    return mng_stop_grp(master_ip, master_port);
}

