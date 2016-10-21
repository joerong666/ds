#ifndef _OP_CMD_H_
#define _OP_CMD_H_

#include "redis.h"

/* it must stay the same to the definition of cmdmap_tab */
#define OP_CMD_SET               1
#define OP_CMD_SETEX             2
#define OP_CMD_DEL               8
#define OP_CMD_CAS               9

/* expire_time field in binlog is expressed to timestamp form */

/* command in binlog */
/* string category */
#define OP_SET               "set"
/* setex: cmd + key + value + expire */
#define OP_SETEX             "setex"
#define OP_PSETEX            "psetex"
#define OP_SETBIT            "setbit"
#define OP_SETRANGE          "setrange"
#define OP_INCRBY            "incrby"  // unused again
#define OP_APPEND            "append"  // unused again
#define OP_PREPEND           "prepend" // unused again
#define OP_DEL               "del"
#define OP_CAS               "cas"
#define OP_EXPIREAT          "expireat"
#define OP_PERSIST           "persist"
#define OP_FLUSHALL          "flushall"
#define OP_FLUSHDB           "flushdb"
#define OP_RENAME            "rename"
#define OP_MOVE              "move"

/* hash category */
#define OP_HDEL              "hdel"
#define OP_HMSET             "hmset"

/* list category */
#define OP_RPOPLPUSH         "rpoplpush"
#define OP_LRPOP             "rpop"
#define OP_LRPUSH            "rpush"
#define OP_LLPOP             "lpop"
#define OP_LLPUSH            "lpush"
#define OP_LINSERT           "linsert"
#define OP_LREM              "lrem"
#define OP_LSET              "lset"
#define OP_LTRIM             "ltrim"

/* sets category */
#define OP_SADD              "sadd"
#define OP_SREM              "srem"

/* sorted-sets category */
#define OP_ZADD              "zadd"
#define OP_ZREM              "zrem"


extern void initOpCommandTable(void);
extern int get_cmd(const char *cmd);
extern const char *get_cmdstr(unsigned char cmd);
extern int redo_op(redisDb *rdb, const robj *key, unsigned char cmd, int argc, robj **argv);
extern int is_entirety_cmd(int cmd_idx);
extern char get_blcmd_type(int cmd);

#endif /* _OP_CMD_H_ */
