#ifndef _SYNC_IF_H_
#define _SYNC_IF_H_

#include "redis.h"
#include "replica.h"
#include "replica_thread_mng.h"

/*
 * return:
 * 0 - binlog belong to ds_key
 * 1 - binlog not belong to ds_key
 */
//typedef int (*filter_func)(const char *bl_ptr, int bl_len, const char *ds_key);

/*
 * protocal between slave and master
 *
 * [command] sync
 * slave --> master (redis format):
 * "*4\r\n$4\r\nsync\r\n$n\r\nbl_tag\r\n$n\r\nds_id\r\n$n\r\nds_key\r\n"
 *
 *
 * [command] sync_status
 * slave_thread --> main_thread (redis format):
 * "*4\r\n$11\r\nsync_status\r\n$n\r\nmaster_ip\r\n$n\r\nmaster_port\r\n$n\r\nstatus\r\n"
 * status: 0-synced, 1-syncing, 2-fail, 3-over
 *
 * main_thread --> slave_thread (redis format):
 * ":result"
 * result: 0-ok, 1-terminate sync, 2-system fail
 *
 *
 * [command] binlog
 * slave_thread --> main_thread (redis format):
 * "*3\r\n$6\r\nbinlog\r\n$n\r\nds_id\r\n$len\r\nbinlog_content\r\n"
 *
 * main_thread --> slave_thread (redis format):
 * ":result"
 * result: 0-ok, 1-error
 */

/* 
 * dir: the path to store something need
 * bl_tag: the binlog timestamp from which the master will send
 * port: the listening port of the main thread
 * return:
 * 0 - startup succ
 * 1 - startup fail
 */
extern int sync_slave_start(const char *master_ip, int master_port, const char *dir, long long bl_tag, int port, unsigned long long ds_id, const char *ds_key);


//typedef void (*FreeClient)(redisClient *c);

/* 
 * bl_dir: the path of binlog root dir
 * prefix: prefix of binlog filename
 * max_size: binlog argument for max size
 * max_idx: binlog argument the max index
 * c: connection info
 * f: the call back function for release resource
 * ds_id_m: ds_id of master
 * ds_id_s: ds_id of slave
 * ds_key_s: ds_key of slave
 * filter: the call back function for filter the binlog
 * return:
 * 0 - succ finish
 * 1 - fail
 */
extern int sync_master_start(const char *bl_dir, const char *prefix, off_t max_size, int max_idx, redisClient *c, freeres_func f, unsigned long long ds_id_m, unsigned long long ds_id_s, const char *ds_key_s, filter_func filter);

extern int sync_slave_stop(const char *master_ip, int master_port);
#endif /* _SYNC_IF_H_ */

