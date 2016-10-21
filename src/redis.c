/*
 * Copyright (c) 2009-2010, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "redis.h"
#include "slowlog.h"
#include "bio.h"

#ifdef HAVE_BACKTRACE
#include <execinfo.h>
#include <ucontext.h>
#endif /* HAVE_BACKTRACE */

#include <time.h>
#include <signal.h>
#include <sys/wait.h>
#include <errno.h>
#include <assert.h>
#include <ctype.h>
#include <stdarg.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/uio.h>
#include <limits.h>
#include <float.h>
#include <math.h>
#include <pthread.h>
#include <sys/resource.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <malloc.h>

#include "dbmng.h"
#include "db_io_engine.h"
#include "op_cmd.h"
#include "heartbeat.h"
#include "dbe_if.h"
#include "pf_log.h"
#include "ds_ctrl.h"
#include "key_filter.h"
#include "ds_zmalloc.h"
#include "ds_util.h"
#include "bl_ctx.h"
#include "sync_if.h"
#include "repl_if.h"
#include "ds_binlog.h"
#include "serialize.h"
#include "dbe_get.h"
#include "write_bl.h"
#include "restore_key.h"
#include "codec_key.h"

#include "convert.h"
#include "db.h"


#define FREE_OBJ_NUM_LRU               5
#define MAXMEMORY_SAMPLES              3

#define CONVERT_THREAD_CNT             6
#define PERSIST_THREAD_CNT             4

static void freeMemoryIfNeededBySelf(void);
static void clean_more(void);
static int upd_cp_cron(struct aeEventLoop *eventLoop, long long id, void *clientData);
static int print_dbeinfo_cron(struct aeEventLoop *eventLoop, long long id, void *clientData);
static int print_info_cron(struct aeEventLoop *eventLoop, long long id, void *clientData);

/* Our shared "common" objects */

struct sharedObjectsStruct shared;
static pthread_t tid = 0;

/* Global vars that are actually used as constants. The following double
 * values are used for double on-disk serialization, and are initialized
 * at runtime to avoid strange compiler optimizations. */

double R_Zero, R_PosInf, R_NegInf, R_Nan;

int master_ctx, slave_ctx;

/* for clean_cache */
static size_t maxMem = 0;
static int db_idx = 0;
static time_t cln_start = 0;

static pthread_mutex_t sc_clean_lock = PTHREAD_MUTEX_INITIALIZER;
static unsigned char sc_clean_c = 0;

static void do_keydump(const char *dbe_path, int fmt);
static void do_getkey(const char *dbe_path, const char *key);
static void do_testkey(const char *dbe_path, const char *key, int max_key, int sample_cnt, int test_type);

/*================================= Globals ================================= */

/* Global vars */
struct redisServer server; /* server global state */
struct redisCommand *commandTable;
struct redisCommand readonlyCommandTable[] = 
{
    {"get",getCommand,2,0,NULL,1,1,1,1,1,1,1,1,1,0, 0, 'K'},
    {"touch",touchCommand,3,REDIS_CMD_DENYOOM,NULL,0,0,0,1,1,1,1,1,1,1, 0, 'K'},
    {"set",setCommand,-3,REDIS_CMD_DENYOOM,NULL,0,0,0,0,0,0,1,1,1,1, 0, 'K'},
    {"setnx",setnxCommand,-3,REDIS_CMD_DENYOOM,NULL,0,0,0,1,1,1,1,1,1,1, 0, 'K'},
    {"setex",setexCommand,4,REDIS_CMD_DENYOOM,NULL,0,0,0,0,0,0,1,1,1,1, 0, 'K'},
    {"setx",setxCommand,5,REDIS_CMD_DENYOOM,NULL,0,0,0,0,0,0,1,1,1,1, 0, 'K'},
    {"add",addCommand,5,REDIS_CMD_DENYOOM,NULL,0,0,0,1,1,1,1,1,1,1, 0, 'K'},
    {"cas",casCommand,6,REDIS_CMD_DENYOOM,NULL,0,0,0,1,1,1,1,1,1,1, 0, 'K'},
    {"replace",replaceCommand,5,REDIS_CMD_DENYOOM,NULL,0,0,0,1,1,1,1,1,1,1, 0, 'K'},
    {"append",appendCommand,3,REDIS_CMD_DENYOOM,NULL,1,1,1,1,1,1,1,1,1,1, 0, 'K'},
    {"appendx",appendxCommand,3,REDIS_CMD_DENYOOM,NULL,1,1,1,1,1,1,1,1,1,1, 0, 'K'},
    {"prepend",prependCommand,3,REDIS_CMD_DENYOOM,NULL,1,1,1,1,1,1,1,1,1,1, 0, 'K'},
    {"strlen",strlenCommand,2,0,NULL,1,1,1,1,1,1,1,1,1,0, 0, 'K'},
    {"del",delCommand,-2,0,NULL,0,0,0,0,0,0,1,-1,1,1, 0, 'k'},
    {"exists",existsCommand,2,0,NULL,1,1,1,1,1,1,1,1,1,0, 0, 'k'},
    {"setbit",setbitCommand,-4,REDIS_CMD_DENYOOM,NULL,1,1,1,1,1,1,1,1,1,1, 0, 'K'},
    {"getbit",getbitCommand,3,0,NULL,1,1,1,1,1,1,1,1,1,0, 0, 'K'},
    {"setrange",setrangeCommand,-4,REDIS_CMD_DENYOOM,NULL,1,1,1,1,1,1,1,1,1,1, 0, 'K'},
    {"getrange",getrangeCommand,4,0,NULL,1,1,1,1,1,1,1,1,1,0, 0, 'K'},
    {"substr",getrangeCommand,4,0,NULL,1,1,1,1,1,1,1,1,1,0, 0, 'K'},
    {"incr",incrCommand,2,REDIS_CMD_DENYOOM,NULL,1,1,1,1,1,1,1,1,1,1, 0, 'K'},
    {"decr",decrCommand,2,REDIS_CMD_DENYOOM,NULL,1,1,1,1,1,1,1,1,1,1, 0, 'K'},
    {"incrx",incrxCommand,3,REDIS_CMD_DENYOOM,NULL,1,1,1,1,1,1,1,1,1,1, 0, 'K'},
    {"decrx",decrxCommand,3,REDIS_CMD_DENYOOM,NULL,1,1,1,1,1,1,1,1,1,1, 0, 'K'},
    {"mget",mgetCommand,-2,0,NULL,1,-1,1,1,-1,1,1,-1,1,0, 0, 'K'},
    {"mgetx",mgetxCommand,-2,0,NULL,1,-1,1,1,-1,1,1,-1,1,0, 0, 'K'},
    {"mgets",mgetsCommand,-2,0,NULL,1,-1,1,1,-1,1,1,-1,1,0, 0, 'K'},
    {"xget",xgetCommand,-2,0,NULL,1,-1,1,1,-1,1,1,-1,1,0, 0, 'K'},
    {"rpush",rpushCommand,-3,REDIS_CMD_DENYOOM,NULL,1,1,1,1,1,1,1,1,1,1, 0, 'L'},
    {"lpush",lpushCommand,-3,REDIS_CMD_DENYOOM,NULL,1,1,1,1,1,1,1,1,1,1, 0, 'L'},
    {"rpushx",rpushxCommand,3,REDIS_CMD_DENYOOM,NULL,1,1,1,1,1,1,1,1,1,1, 0, 'L'},
    {"lpushx",lpushxCommand,3,REDIS_CMD_DENYOOM,NULL,1,1,1,1,1,1,1,1,1,1, 0, 'L'},
    {"linsert",linsertCommand,5,REDIS_CMD_DENYOOM,NULL,1,1,1,1,1,1,1,1,1,1, 0, 'L'},
    {"rpop",rpopCommand,2,0,NULL,1,1,1,1,1,1,1,1,1,1, 0, 'L'},
    {"lpop",lpopCommand,2,0,NULL,1,1,1,1,1,1,1,1,1,1, 0, 'L'},
    {"brpop",brpopCommand,-3,0,NULL,1,1,1,1,1,1,1,-2,1,1, 0, 'L'},
    {"brpoplpush",brpoplpushCommand,4,REDIS_CMD_DENYOOM,NULL,1,2,1,1,2,1,0,0,0,1, 0, 'L'},
    {"blpop",blpopCommand,-3,0,NULL,1,1,1,1,1,1,1,-2,1,1, 0, 'L'},
    {"llen",llenCommand,2,0,NULL,1,1,1,1,1,1,1,1,1,0, 0, 'L'},
    {"lindex",lindexCommand,3,0,NULL,1,1,1,1,1,1,1,1,1,0, 0, 'L'},
    {"lset",lsetCommand,4,REDIS_CMD_DENYOOM,NULL,1,1,1,1,1,1,1,1,1,1, 0, 'L'},
    {"lrange",lrangeCommand,4,0,NULL,1,1,1,1,1,1,1,1,1,0, 0, 'L'},
    {"ltrim",ltrimCommand,4,0,NULL,1,1,1,1,1,1,1,1,1,1, 0, 'L'},
    {"lrem",lremCommand,4,0,NULL,1,1,1,1,1,1,1,1,1,1, 0, 'L'},
    {"rpoplpush",rpoplpushCommand,3,REDIS_CMD_DENYOOM,NULL,1,2,1,1,2,1,0,0,0,1, 0, 'L'},
    {"sadd",saddCommand,-3,REDIS_CMD_DENYOOM,NULL,1,1,1,1,1,1,1,1,1,1, 0, 'S'},
    {"srem",sremCommand,-3,0,NULL,1,1,1,1,1,1,1,1,1,1, 0, 'S'},
    {"smove",smoveCommand,4,0,NULL,1,2,1,1,2,1,0,0,0,1, 0, 'S'},
    {"sismember",sismemberCommand,3,0,NULL,1,1,1,1,1,1,1,1,1,0, 0, 'S'},
    {"scard",scardCommand,2,0,NULL,1,1,1,1,1,1,1,1,1,0, 0, 'S'},
    {"spop",spopCommand,2,0,NULL,1,1,1,1,1,1,1,1,1,1, 0, 'S'},
    {"srandmember",srandmemberCommand,2,0,NULL,1,1,1,1,1,1,1,1,1,0, 0, 'S'},
    {"sinter",sinterCommand,-2,REDIS_CMD_DENYOOM,NULL,1,-1,1,1,-1,1,1,-1,1,0, 0, 'S'},
    {"sinterstore",sinterstoreCommand,-3,REDIS_CMD_DENYOOM,NULL,2,-1,1,2,-1,1,2,-1,1,1, 0, 'S'},
    {"sunion",sunionCommand,-2,REDIS_CMD_DENYOOM,NULL,1,-1,1,1,-1,1,1,-1,1,0, 0, 'S'},
    {"sunionstore",sunionstoreCommand,-3,REDIS_CMD_DENYOOM,NULL,2,-1,1,2,-1,1,2,-1,1,1, 0, 'S'},
    {"sdiff",sdiffCommand,-2,REDIS_CMD_DENYOOM,NULL,1,-1,1,1,-1,1,1,-1,1,0, 0, 'S'},
    {"sdiffstore",sdiffstoreCommand,-3,REDIS_CMD_DENYOOM,NULL,2,-1,1,2,-1,1,2,-1,1,1, 0, 'S'},
    {"smembers",sinterCommand,2,0,NULL,1,1,1,1,1,1,1,1,1,0, 0, 'S'},
    {"zadd",zaddCommand,-4,REDIS_CMD_DENYOOM,NULL,1,1,1,1,1,1,1,1,1,1, 0, 'Z'},
    {"zincrby",zincrbyCommand,4,REDIS_CMD_DENYOOM,NULL,1,1,1,1,1,1,1,1,1,1, 0, 'Z'},
    {"zrem",zremCommand,-3,0,NULL,1,1,1,1,1,1,1,1,1,1, 0, 'Z'},
    {"zremrangebyscore",zremrangebyscoreCommand,4,0,NULL,1,1,1,1,1,1,1,1,1,1, 0, 'Z'},
    {"zremrangebyrank",zremrangebyrankCommand,4,0,NULL,1,1,1,1,1,1,1,1,1,1, 0, 'Z'},
    {"zunionstore",zunionstoreCommand,-4,REDIS_CMD_DENYOOM,zunionInterBlockClientOnSwappedKeys,0,0,0,0,0,0,0,0,0,1, 0, 'Z'},
    {"zinterstore",zinterstoreCommand,-4,REDIS_CMD_DENYOOM,zunionInterBlockClientOnSwappedKeys,0,0,0,0,0,0,0,0,0,1, 0, 'Z'},
    {"zrange",zrangeCommand,-4,0,NULL,1,1,1,1,1,1,1,1,1,0, 0, 'Z'},
    {"zrangebyscore",zrangebyscoreCommand,-4,0,NULL,1,1,1,1,1,1,1,1,1,0, 0, 'Z'},
    {"zrevrangebyscore",zrevrangebyscoreCommand,-4,0,NULL,1,1,1,1,1,1,1,1,1,0, 0, 'Z'},
    {"zcount",zcountCommand,4,0,NULL,1,1,1,1,1,1,1,1,1,0, 0, 'Z'},
    {"zrevrange",zrevrangeCommand,-4,0,NULL,1,1,1,1,1,1,1,1,1,0, 0, 'Z'},
    {"zcard",zcardCommand,2,0,NULL,1,1,1,1,1,1,1,1,1,0, 0, 'Z'},
    {"zscore",zscoreCommand,3,0,NULL,1,1,1,1,1,1,1,1,1,0, 0, 'Z'},
    {"zrank",zrankCommand,3,0,NULL,1,1,1,1,1,1,1,1,1,0, 0, 'Z'},
    {"zrevrank",zrevrankCommand,3,0,NULL,1,1,1,1,1,1,1,1,1,0, 0, 'Z'},
    {"hset",hsetCommand,4,REDIS_CMD_DENYOOM,NULL,1,1,1,1,1,1,1,1,1,1, 0, 'H'},
    {"hsetnx",hsetnxCommand,4,REDIS_CMD_DENYOOM,NULL,1,1,1,1,1,1,1,1,1,1, 0, 'H'},
    {"hget",hgetCommand,3,0,NULL,1,1,1,1,1,1,1,1,1,0, 0, 'H'},
    {"hmset",hmsetCommand,-4,REDIS_CMD_DENYOOM,NULL,1,1,1,1,1,1,1,1,1,1, 0, 'H'},
    {"hmget",hmgetCommand,-3,0,NULL,1,1,1,1,1,1,1,1,1,0, 0, 'H'},
    {"hincrby",hincrbyCommand,4,REDIS_CMD_DENYOOM,NULL,1,1,1,1,1,1,1,1,1,1, 0, 'H'},
    {"hdel",hdelCommand,-3,0,NULL,1,1,1,1,1,1,1,1,1,1, 0, 'H'},
    {"hlen",hlenCommand,2,0,NULL,1,1,1,1,1,1,1,1,1,0, 0, 'H'},
    {"hkeys",hkeysCommand,2,0,NULL,1,1,1,1,1,1,1,1,1,0, 0, 'H'},
    {"hvals",hvalsCommand,2,0,NULL,1,1,1,1,1,1,1,1,1,0, 0, 'H'},
    {"hgetall",hgetallCommand,2,0,NULL,1,1,1,1,1,1,1,1,1,0, 0, 'H'},
    {"hexists",hexistsCommand,3,0,NULL,1,1,1,1,1,1,1,1,1,0, 0, 'H'},
    {"incrby",incrbyCommand,3,REDIS_CMD_DENYOOM,NULL,1,1,1,1,1,1,1,1,1,1, 0, 'K'},
    {"decrby",decrbyCommand,3,REDIS_CMD_DENYOOM,NULL,1,1,1,1,1,1,1,1,1,1, 0, 'K'},
    {"getset",getsetCommand,3,REDIS_CMD_DENYOOM,NULL,1,1,1,1,1,1,1,1,1,1, 0, 'K'},
    {"mset",msetCommand,-3,REDIS_CMD_DENYOOM,NULL,1,-1,2,1,-1,2,1,-1,2,1, 0, 'K'},
    {"msetnx",msetnxCommand,-3,REDIS_CMD_DENYOOM,NULL,1,-1,2,1,-1,2,1,-1,2,1, 0, 'K'},
    {"randomkey",randomkeyCommand,1,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, 'k'},
    {"select",selectCommand,2,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, 'c'},
    {"move",moveCommand,3,0,NULL,1,1,1,1,1,1,1,1,1,1, 0, 'k'},
    {"rename",renameCommand,3,0,NULL,1,1,1,1,1,1,1,1,1,1, 0, 'k'},
    {"renamenx",renamenxCommand,3,0,NULL,1,1,1,1,1,1,1,1,1,1, 0, 'k'},
    {"expire",expireCommand,3,0,NULL,0,0,0,0,0,0,1,1,1,1, 0, 'k'},
    {"expireat",expireatCommand,3,0,NULL,0,0,0,0,0,0,1,1,1,1, 0, 'k'},
    {"keys",keysCommand,2,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, 'k'},
    {"keydump",keydumpCommand,-1,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, '-'},
    {"cachedump",cachedumpCommand,1,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, '-'},
    {"dbsize",dbsizeCommand,1,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, 'c'},
    {"auth",authCommand,2,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, 'c'},
    {"ping",pingCommand,1,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, 'c'},
    {"echo",echoCommand,2,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, 'c'},
    {"save",saveCommand,1,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, 'c'},
    {"bgsave",bgsaveCommand,1,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, 'c'},
    {"bgrewriteaof",bgrewriteaofCommand,1,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, 'c'},
    {"shutdown",shutdownCommand,1,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, 'c'},
    {"lastsave",lastsaveCommand,1,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, 'c'},
    {"type",typeCommand,2,0,NULL,1,1,1,1,1,1,1,1,1,0, 0, 'k'},
    {"multi",multiCommand,1,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, 'c'},
    {"exec",execCommand,1,REDIS_CMD_DENYOOM,execBlockClientOnSwappedKeys,0,0,0,0,0,0,0,0,0,0, 0, 'c'},
    {"discard",discardCommand,1,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, 'c'},

    {"block",blockCommand,1,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, '-'},
    {"unblock",unblockCommand,1,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, '-'},
    {"file",replCommand,1,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, '-'},
    {"sync",syncCommand,-3,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, '-'},
    {"sync_status",syncstatusCommand,-4,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, '-'},
    {"binlog",binlogCommand,3,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, '-'},
    {"start_sync",startsyncCommand,-4,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, '-'},
    {"stop_sync",stopsyncCommand,3,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, '-'},
    {"start_repl",startreplCommand,3,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, '-'},
    {"merge_dbe",mergedbeCommand,2,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, '-'},
    {"clean_cache",cleancacheCommand,2,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, '-'},
    {"clean_dbe",cleandbeCommand,2,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, '-'},
    {"purge_dbe",purgedbeCommand,1,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, '-'},
    {"decode_bl",decodeblCommand,2,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, '-'},
    {"flushcp",flushcpCommand,1,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, '-'},

    {"flushdb",flushdbCommand,1,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, '-'},
    {"flushall",flushallCommand,1,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, '-'},
    {"sort",sortCommand,-2,REDIS_CMD_DENYOOM,NULL,1,1,1,1,1,1,1,1,1,0, 0, 'k'},
    {"info",infoCommand,-1,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, 'c'},
    {"monitor",monitorCommand,1,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, 'c'},
    {"ttl",ttlCommand,2,0,NULL,1,1,1,1,1,1,0,0,0,0, 0, 'k'},
    {"persist",persistCommand,2,0,NULL,1,1,1,1,1,1,0,0,0,1, 0, 'k'},
    {"slaveof",slaveofCommand,3,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, 'c'},
    {"debug",debugCommand,-2,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, 'c'},
    {"config",configCommand,-2,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, 'c'},
    {"subscribe",subscribeCommand,-2,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, 'c'},
    {"unsubscribe",unsubscribeCommand,-1,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, 'c'},
    {"psubscribe",psubscribeCommand,-2,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, 'c'},
    {"punsubscribe",punsubscribeCommand,-1,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, 'c'},
    {"publish",publishCommand,3,REDIS_CMD_FORCE_REPLICATION,NULL,0,0,0,0,0,0,0,0,0,0, 0, 'c'},
    {"watch",watchCommand,-2,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, 'c'},
    {"unwatch",unwatchCommand,1,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, 'c'},
    {"object",objectCommand,-2,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, 'c'},
    {"client",clientCommand,-2,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, 'c'},
    {"slowlog",slowlogCommand,-2,0,NULL,0,0,0,0,0,0,0,0,0,0, 0, 'c'}
};

/*============================ Utility functions ============================ */

void redisLog_(const char *file, int line, int level, const char *fmt, ...)
{
    va_list ap;
    char msg[REDIS_MAX_LOGMSG_LEN];

    if (level < server.verbosity) return;

    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    pf_log_write(0, file, line, level, "%s", msg);

#if 0
    const int syslogLevelMap[] = { LOG_DEBUG, LOG_INFO, LOG_NOTICE, LOG_WARNING };
    const char *c = ".-*#";
    time_t now = time(NULL);
    va_list ap;
    FILE *fp;
    char buf[64];
    char msg[REDIS_MAX_LOGMSG_LEN];

    if (level < server.verbosity) return;

    fp = (server.logfile == NULL) ? stdout : fopen(server.logfile,"a");
    if (!fp) return;

    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    strftime(buf,sizeof(buf),"%d %b %H:%M:%S",localtime(&now));
    fprintf(fp,"[%d] %s %c %s\n",(int)syscall(SYS_gettid),buf,c[level],msg);
    fflush(fp);

    if (server.logfile) fclose(fp);

    if (server.syslog_enabled) syslog(syslogLevelMap[level], "%s", msg);
#endif
}

void redisLogW(const char *file, int line, int tag, size_t id, int level, const char *fmt, ...) 
{
    va_list ap;
    char msg[REDIS_MAX_LOGMSG_LEN];

    if (level < server.verbosity) return;

    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    (void)tag;
    (void)id;
    pf_log_write(0, file, line, level, "%s", msg);

#if 0
    const int syslogLevelMap[] = { LOG_DEBUG, LOG_INFO, LOG_NOTICE, LOG_WARNING };
    const char *c = ".-*#";
    time_t now = time(NULL);
    va_list ap;
    FILE *fp;
    char buf[64];
    char msg[REDIS_MAX_LOGMSG_LEN];

    if (level < server.verbosity) return;

    fp = (server.logfile == NULL) ? stdout : fopen(server.logfile,"a");
    if (!fp) return;

    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    strftime(buf,sizeof(buf),"%d %b %H:%M:%S",localtime(&now));
    fprintf(fp,"[%d] %s %c %s:%d#%d#%d# %s\n",
            (int)syscall(SYS_gettid),buf,c[level],file,line,tag,(int)id,msg);
    fflush(fp);

    if (server.logfile) fclose(fp);

    if (server.syslog_enabled) syslog(syslogLevelMap[level], "%s", msg);
#endif 
}

/* Redis generally does not try to recover from out of memory conditions
 * when allocating objects or strings, it is not clear if it will be possible
 * to report this condition to the client since the networking layer itself
 * is based on heap allocation for send buffers, so we simply abort.
 * At least the code will be simpler to read... */
void oom(const char *msg) {
    redisLog(REDIS_WARNING, "%s: Out of memory\n",msg);
    sleep(1);
    abort();
}

/*====================== Hash table type implementation  ==================== */

/* This is an hash table type that uses the SDS dynamic strings libary as
 * keys and radis objects as values (objects can hold SDS strings,
 * lists, sets). */

void dictVanillaFree(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);
    zfree(val);
}

void dictListDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);
    listRelease((list*)val);
}

int dictSdsKeyCompare(void *privdata, const void *key1,
        const void *key2)
{
    int l1,l2;
    DICT_NOTUSED(privdata);

    l1 = sdslen((sds)key1);
    l2 = sdslen((sds)key2);
    if (l1 != l2) return 0;
    return memcmp(key1, key2, l1) == 0;
}

/* A case insensitive version used for the command lookup table. */
int dictSdsKeyCaseCompare(void *privdata, const void *key1,
        const void *key2)
{
    DICT_NOTUSED(privdata);

    return strcasecmp(key1, key2) == 0;
}

void dictRedisObjectDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);

    if (val == NULL) return; /* Values of swapped out keys as set to NULL */
    decrRefCount(val);
}

void dictSdsDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);

    sdsfree(val);
}

int dictObjKeyCompare(void *privdata, const void *key1,
        const void *key2)
{
    const robj *o1 = key1, *o2 = key2;
    return dictSdsKeyCompare(privdata,o1->ptr,o2->ptr);
}

unsigned int dictObjHash(const void *key) {
    const robj *o = key;
    return dictGenHashFunction(o->ptr, sdslen((sds)o->ptr));
}

unsigned int dictSdsHash(const void *key) {
    return dictGenHashFunction((unsigned char*)key, sdslen((char*)key));
}

unsigned int dictSdsCaseHash(const void *key) {
    return dictGenCaseHashFunction((unsigned char*)key, sdslen((char*)key));
}

int dictEncObjKeyCompare(void *privdata, const void *key1,
        const void *key2)
{
    robj *o1 = (robj*) key1, *o2 = (robj*) key2;
    int cmp;

    if (o1->encoding == REDIS_ENCODING_INT &&
        o2->encoding == REDIS_ENCODING_INT)
            return o1->ptr == o2->ptr;

    o1 = getDecodedObject(o1);
    o2 = getDecodedObject(o2);
    cmp = dictSdsKeyCompare(privdata,o1->ptr,o2->ptr);
    decrRefCount(o1);
    decrRefCount(o2);
    return cmp;
}

unsigned int dictEncObjHash(const void *key) {
    robj *o = (robj*) key;

    if (o->encoding == REDIS_ENCODING_RAW) {
        return dictGenHashFunction(o->ptr, sdslen((sds)o->ptr));
    } else {
        if (o->encoding == REDIS_ENCODING_INT) {
            char buf[32];
            int len;

            len = ll2string(buf,32,(long)o->ptr);
            return dictGenHashFunction((unsigned char*)buf, len);
        } else {
            unsigned int hash;

            o = getDecodedObject(o);
            hash = dictGenHashFunction(o->ptr, sdslen((sds)o->ptr));
            decrRefCount(o);
            return hash;
        }
    }
}

/* Sets type */
dictType setDictType = {
    dictEncObjHash,            /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictEncObjKeyCompare,      /* key compare */
    dictRedisObjectDestructor, /* key destructor */
    NULL                       /* val destructor */
};

/* Sorted sets hash (note: a skiplist is used in addition to the hash table) */
dictType zsetDictType = {
    dictEncObjHash,            /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictEncObjKeyCompare,      /* key compare */
    dictRedisObjectDestructor, /* key destructor */
    NULL                       /* val destructor */
};

/* Db->dict, keys are sds strings, vals are Redis objects. */
dictType dbDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    dictSdsDestructor,          /* key destructor */
    dictRedisObjectDestructor   /* val destructor */
};

/* Db->expires */
dictType keyptrDictType = {
    dictSdsHash,               /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCompare,         /* key compare */
    NULL,                      /* key destructor */
    NULL                       /* val destructor */
};

/* Command table. sds string -> command struct pointer. */
dictType commandTableDictType = {
    dictSdsCaseHash,           /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCaseCompare,     /* key compare */
    dictSdsDestructor,         /* key destructor */
    NULL                       /* val destructor */
};

/* Hash type hash table (note that small hashes are represented with zimpaps) */
dictType hashDictType = {
    dictEncObjHash,             /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictEncObjKeyCompare,       /* key compare */
    dictRedisObjectDestructor,  /* key destructor */
    dictRedisObjectDestructor   /* val destructor */
};

/* Keylist hash table type has unencoded redis objects as keys and
 * lists as values. It's used for blocking operations (BLPOP) and to
 * map swapped keys to a list of clients waiting for this keys to be loaded. */
dictType keylistDictType = {
    dictObjHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictObjKeyCompare,          /* key compare */
    dictRedisObjectDestructor,  /* key destructor */
    dictListDestructor          /* val destructor */
};

int htNeedsResize(dict *dict) {
    long long size, used;

    size = dictSlots(dict);
    used = dictSize(dict);
    return (size && used && size > DICT_HT_INITIAL_SIZE &&
            (used*100/size < REDIS_HT_MINFILL));
}

/* If the percentage of used slots in the HT reaches REDIS_HT_MINFILL
 * we resize the hash table to save memory */
void tryResizeHashTables(void) {
    int j;

    for (j = 0; j < server.dbnum; j++) {
        if (htNeedsResize(server.db[j].dict))
            dictResize(server.db[j].dict);
        if (htNeedsResize(server.db[j].expires))
            dictResize(server.db[j].expires);
    }
}

/* Our hash table implementation performs rehashing incrementally while
 * we write/read from the hash table. Still if the server is idle, the hash
 * table will use two tables for a long time. So we try to use 1 millisecond
 * of CPU time at every serverCron() loop in order to rehash some key. */
void incrementallyRehash(void) {
    int j;

    for (j = 0; j < server.dbnum; j++) {
        if (dictIsRehashing(server.db[j].dict)) {
            dictRehashMilliseconds(server.db[j].dict,1);
            break; /* already used our millisecond for this loop... */
        }
    }
}

/* This function is called once a background process of some kind terminates,
 * as we want to avoid resizing the hash tables when there is a child in order
 * to play well with copy-on-write (otherwise when a resize happens lots of
 * memory pages are copied). The goal of this function is to update the ability
 * for dict.c to resize the hash tables accordingly to the fact we have o not
 * running childs. */
void updateDictResizePolicy(void) {
    if (server.bgsavechildpid == -1 && server.bgrewritechildpid == -1)
        dictEnableResize();
    else
        dictDisableResize();
}

/* ======================= Cron: called every 100 ms ======================== */

void save_stat_info(int init_flg)
{
    if (0 != lock_stat_info())
    {
        return;
    }

    sprintf(gDsStatus.soft_ver, "%s %s %s", DS_VERSION, __DATE__, __TIME__);
    gDsStatus.pid = (int)getpid();
    if (init_flg == 0)
    {
        gDsStatus.block_upd_cp = dbmng_cp_is_blocked(0) ? 1 : 0;
        gDsStatus.upd_cp_status = dbmng_cp_is_active(0) ? 1 : 0;
    }
    gDsStatus.arch_bits = (sizeof(long) == 8) ? 64 : 32;


    gDsStat.uptime = server.unixtime - server.stat_starttime;
    gDsStat.pid = (int)getpid();

    gDsStat.db_used_mem = ds_zmalloc_used_memory();
    gDsStat.used_mem = zmalloc_used_memory();
    gDsStat.peak_mem = server.stat_peak_memory;
    gDsStat.rss_mem = zmalloc_get_rss();

    gDsStat.max_fd = g_max_fd;
    gDsStat.active_clients = listLength(server.clients) - listLength(server.slaves);
    gDsStat.total_connections_received = server.stat_numconnections;
    gDsStat.total_commands_processed = server.stat_numcommands;
    gDsStat.total_commands_processed_dur = server.stat_cmd_total_dur;

    gDsStat.del_lru = server.stat_lru_del_keys;
    gDsStat.expired_keys = server.stat_expiredkeys;
    gDsStat.evicted_keys = server.stat_evictedkeys;

    /* unuse hits & misses & dirty, use it to express bl sync */
    gDsStat.hits = server.bl_sync_delay_total;
    gDsStat.misses = server.bl_sync_cnt;
    gDsStat.dirty = server.stat_misses_in_cache;

    gDsStat.keys = gDsStat.in_expire_keys = 0;
    int j;
    for (j = 0; j < server.dbnum; j++)
    {
        gDsStat.keys += dictSize(server.db[j].dict);
        gDsStat.in_expire_keys += dictSize(server.db[j].expires);
    }

    gDsStat.get_cmds = server.stat_get_cmd;
    gDsStat.set_cmds = server.stat_set_cmd;
    gDsStat.touch_cmds = server.stat_touch_cmd;
    gDsStat.incr_cmds = server.stat_incr_cmd;
    gDsStat.decr_cmds = server.stat_decr_cmd;
    gDsStat.delete_cmds = server.stat_del_cmd;
    gDsStat.auth_cmds = server.stat_auth_cmd;
    gDsStat.get_hits = server.stat_get_hits;
    gDsStat.get_misses = server.stat_get_misses;
    gDsStat.delete_hits = server.stat_del_hits;
    gDsStat.delete_misses = server.stat_del_misses;
    gDsStat.incr_hits = server.stat_incr_hits;
    gDsStat.incr_misses = server.stat_incr_misses;
    gDsStat.decr_hits = server.stat_decr_hits;
    gDsStat.decr_misses = server.stat_decr_misses;
    gDsStat.cas_hits = server.stat_cas_hits;
    gDsStat.cas_misses = server.stat_cas_misses;
    gDsStat.cas_badval = server.stat_cas_badval;
    gDsStat.touch_hits = server.stat_touch_hits;
    gDsStat.touch_misses = server.stat_touch_misses;
    gDsStat.auth_errors = server.stat_auth_errors;

    gDsStat.bytes_read = server.bytes_read;
    gDsStat.bytes_written = server.bytes_written;
    gDsStat.expired_unfetched = server.stat_expired_unfetched;
    gDsStat.evicted_unfetched = server.stat_evicted_unfetched;
    gDsStat.rejected_conns = server.rejected_conns;
    gDsStat.accepting_conns = server.accepting_conns;

    unlock_stat_info();
}

void check_key_validity()
{
#if 1
    static int db_id = 0;
    static dictIterator *it = 0;
    static int fin = 1;
    static int total = 0;
    static int total_del = 0;
    static int total_db = 0;
    static int total_db_del = 0;
    static time_t start = 0;
    static time_t start_db = 0;

    if (server.ip_list_changed == 1)
    {
        if (it)
        {
            // doing, cancel at first and redo it 
            dictReleaseIterator(it);
            it = 0;
        }

        server.ip_list_changed = 0;

        fin = 0;
        start = server.unixtime;
        int i = 0;
        unsigned long total_slot = 0;
        unsigned long total_key = 0;
        for (i = 0; i < server.dbnum; i++)
        {
            total_slot += dictSlots(server.db[i].dict);
            total_key += dictSize(server.db[i].dict);
        }
        redisLog(REDIS_PROMPT, "start key_checking, db_num=%d, slots=%lu, keys=%lu"
                , server.dbnum, total_slot, total_key);
    }
    else
    {
        if (fin == 1)
        {
            return;
        }
    }

    if (it == 0)
    {
        it = dictGetIterator(server.db[db_id].dict);
        if (it == 0)
        {
            redisLog(REDIS_WARNING, "dictGetIterator() fail for db_id=%d", db_id);
            return;
        }
        start_db = server.unixtime;
        total_db = total_db_del = 0;
    }

    dictEntry *de = 0;
    int finish;
    de = dictNextFY(it, &finish);
    if (de != NULL)
    {
        /* handle the whole hash_link */
        int link_size = 0;
        do
        {
            dictEntry *next_de = de->next;
            link_size++;
            total_db++;
            sds key = 0;
            key = (sds)de->key;
            //redisLog(REDIS_PROMPT, "check db_id=%d, key:%s, de=%p", db_id, key, de);
            if (0 != master_filter(key))
            {
                /* the key not belong to here */
                robj k;
                initObject(&k, REDIS_STRING, key);
                dbDelete(&server.db[db_id], &k);

                total_db_del++;

                redisLog(REDIS_DEBUG
                     , "key_checking: db_id=%d, index=%ld, "
                       "table=%d, total=%d, total_del=%d"
                     , db_id, it->index, it->table, total_db, total_db_del);
            }
            de = next_de;
        } while (de);
        if (link_size > 10)
        {
            redisLog(REDIS_NOTICE, "link_size=%d", link_size);
        }
    }
    else if (finish)
    {
        /* db over */
        dictReleaseIterator(it);
        it = 0;

        if (total_db)
        {
            redisLog(REDIS_NOTICE
                , "key_checking over: db_id=%d, total=%d, total_del=%d, dur=%lds"
                , db_id, total_db, total_db_del, server.unixtime - start_db);
        }
        total += total_db;
        total_del += total_db_del;

        db_id = (db_id + 1) % server.dbnum;
        if (db_id == 0)
        {
            /* finish all db */
            redisLog(REDIS_PROMPT
                , "finish key_checking: keys=%d, del=%d, dur=%lds"
                , total, total_del, server.unixtime - start);

            fin = 1;
            total = 0;
            total_del = 0;
        }
    }
#endif
}

void watch_hb_thread()
{
    int status;
    pid_t pid = wait4(tid, &status, WNOHANG, 0);
    if (pid == -1)
    {
        /* error */
        //redisLog(REDIS_WARNING, "waitpid() return -1, fail for tid=%d, errno=%d", tid, errno);
    }
    else if (pid != 0)
    {
        /* hb thread has exist */
        redisLog(REDIS_WARNING, "waitpid() return %d, tid=%d, status=%d", pid, tid, status);
        if (start_heart_beat(&tid))
        {
            redisLog(REDIS_WARNING, "start heart-beat thread fail");
        }
        else
        {
            redisLog(REDIS_DEBUG, "heart-beat thread: %d", tid);
        }
    }
}

/* Try to expire a few timed out keys. The algorithm used is adaptive and
 * will use few CPU cycles if there are few expiring keys, otherwise
 * it will get more aggressive to avoid that too much memory is used by
 * keys that can be removed from the keyspace. */
void activeExpireCycle(void) {
    int j;

    for (j = 0; j < server.dbnum; j++) {
        int expired;
        redisDb *db = server.db+j;

        /* Continue to expire if at the end of the cycle more than 25%
         * of the keys were expired. */
        int cnt = 0;
        do
        {
            cnt++;
            long num = dictSize(db->expires);
            time_t now = time(NULL);

            expired = 0;
            if (num > REDIS_EXPIRELOOKUPS_PER_CRON)
                num = REDIS_EXPIRELOOKUPS_PER_CRON;
            while (num--) {
                dictEntry *de;
                time_t t;

                if ((de = dictGetRandomKey(db->expires)) == NULL) break;
                t = (time_t) dictGetEntryVal(de);
                if (now > t) {
                    sds key = dictGetEntryKey(de);
                    robj *keyobj = createStringObject(key,sdslen(key));

                    robj *const val = (robj *)dictFetchValue(db->dict, key);

                    propagateExpire(db,keyobj);
                    if (dbDelete(db,keyobj))
                    {
                        if (val && val->visited_bit == 0)
                        {
                            server.stat_expired_unfetched++;
                        }
                        if (server.has_dbe == 1)
                        {
                            redisLog(REDIS_PROMPT, "expire %s", keyobj->ptr);
                        }
                        decrRefCount(keyobj);
                        expired++;
                        server.stat_expiredkeys++;
                        redisLog(REDIS_DEBUG, "expiredkeys=%d", server.stat_expiredkeys);
                    }
                }
            }
        } while (expired > REDIS_EXPIRELOOKUPS_PER_CRON/4 && cnt < 100);
    }
}

void updateLRUClock(void) {
    server.lruclock = (server.unixtime/REDIS_LRU_CLOCK_RESOLUTION) &
                                                REDIS_LRU_CLOCK_MAX;
}

void updateCachedTime(void) {
    server.ustime = ustime();
    server.mstime = server.ustime / 1000;
    server.unixtime = server.mstime / 1000;
}

int serverCron(struct aeEventLoop *eventLoop, long long id, void *clientData) {
    int loops = server.cronloops;
    REDIS_NOTUSED(eventLoop);
    REDIS_NOTUSED(id);
    REDIS_NOTUSED(clientData);

    /* We take a cached value of the unix time in the global state because
     * with virtual memory and aging there is to store the current time
     * in objects at every object access, and accuracy is not needed.
     * To access a global var is faster than calling time(NULL) */
    updateCachedTime();

    /* We have just 22 bits per object for LRU information.
     * So we use an (eventually wrapping) LRU clock with 10 seconds resolution.
     * 2^22 bits with 10 seconds resoluton is more or less 1.5 years.
     *
     * Note that even if this will wrap after 1.5 years it's not a problem,
     * everything will still work but just some object will appear younger
     * to Redis. But for this to happen a given object should never be touched
     * for 1.5 years.
     *
     * Note that you can change the resolution altering the
     * REDIS_LRU_CLOCK_RESOLUTION define.
     */
    updateLRUClock();

    check_key_validity();
    clean_more();
    
    if (loops % 5 == 0)
    {
        check_key_validity();
        clean_more();
        freeMemoryIfNeededBySelf();
    }
    if (loops % 10 == 0)
    {
        save_stat_info(0);
        check_key_validity();
        clean_more();
        check_dbe_get_timer();
    }

    if (sc_clean_c && loops % 50 == 0)
    {
        const int ret = pthread_mutex_lock(&sc_clean_lock);
        if (ret == 0)
        {
            listNode *ln;
            unsigned int len = 0;
            redisClient *c = 0;
            int cnt = 0;
            while ((len = listLength(server.cleans)) != 0 && cnt < 10)
            {
                ln = listFirst(server.cleans);
                c = ln->value;
                freeClient(c);
                listDelNode(server.cleans,ln);
                redisLog(REDIS_PROMPT, "clean redisClient, c=%p, list_len=%d", c, len);
                cnt++;
            }
            pthread_mutex_unlock(&sc_clean_lock);
            if (len == 0)
            {
                sc_clean_c = 0;
            }
        }
        else
        {
            redisLog(REDIS_WARNING, "lock fail when scan cleans");
        }
    }
    //if (loops % 1000000) watch_hb_thread();

    /* Record the max memory used since the server was started. */
    const size_t um = zmalloc_used_memory();
    if (um > server.stat_peak_memory)
        server.stat_peak_memory = um;

    /* We received a SIGTERM, shutting down here in a safe way, as it is
     * not ok doing so inside the signal handler. */
    if (server.shutdown_asap) {
        if (prepareForShutdown() == REDIS_OK) exit(0);
        redisLog(REDIS_WARNING,"SIGTERM received but errors trying to shut down the server, check the logs for more information");
    }

#if 0
    /* Show some info about non-empty databases */
    for (j = 0; j < server.dbnum; j++) {
        long long size, used, vkeys;

        size = dictSlots(server.db[j].dict);
        used = dictSize(server.db[j].dict);
        vkeys = dictSize(server.db[j].expires);
        if (!(loops % 50) && (used || vkeys)) {
            redisLog(REDIS_VERBOSE,"DB %d: %lld keys (%lld volatile) in %lld slots HT.",j,used,vkeys,size);
            dictPrintStats(server.dict);
        }
    }

    /* We don't want to resize the hash tables while a bacground saving
     * is in progress: the saving child is created using fork() that is
     * implemented with a copy-on-write semantic in most modern systems, so
     * if we resize the HT while there is the saving child at work actually
     * a lot of memory movements in the parent will cause a lot of pages
     * copied. */
    if (server.bgsavechildpid == -1 && server.bgrewritechildpid == -1) {
        if (!(loops % 10)) tryResizeHashTables();
        if (server.activerehashing) incrementallyRehash();
    }

    /* Show information about connected clients 
    if (!(loops % 50)) {
        redisLog(REDIS_VERBOSE,"%d clients connected (%d slaves), %zu bytes in use",
            listLength(server.clients)-listLength(server.slaves),
            listLength(server.slaves),
            zmalloc_used_memory());
    }
    */
#endif

    /* Close connections of timedout clients */
    if ((server.maxidletime && !(loops % 100)) || server.bpop_blocked_clients)
        closeTimedoutClients();

#if 0
    // disable in FooYun
    /* Start a scheduled AOF rewrite if this was requested by the user while
     * a BGSAVE was in progress. */
    if (server.bgsavechildpid == -1 && server.bgrewritechildpid == -1 &&
        server.aofrewrite_scheduled)
    {
        rewriteAppendOnlyFileBackground();
    }

    /* Check if a background saving or AOF rewrite in progress terminated */
    if (server.bgsavechildpid != -1 || server.bgrewritechildpid != -1) {
        int statloc;
        pid_t pid;

        if ((pid = wait3(&statloc,WNOHANG,NULL)) != 0) {
            if (pid == server.bgsavechildpid) {
                backgroundSaveDoneHandler(statloc);
            } else {
                backgroundRewriteDoneHandler(statloc);
            }
            updateDictResizePolicy();
        }
    } else {
         time_t now = time(NULL);

        /* If there is not a background saving in progress check if
         * we have to save now */
         for (j = 0; j < server.saveparamslen; j++) {
            struct saveparam *sp = server.saveparams+j;

            if (server.dirty >= sp->changes &&
                now-server.lastsave > sp->seconds) {
                redisLog(REDIS_NOTICE,"%d changes in %d seconds. Saving...",
                    sp->changes, sp->seconds);
                rdbSaveBackground(server.dbfilename);
                break;
            }
         }

         /* Trigger an AOF rewrite if needed */
         if (server.bgsavechildpid == -1 &&
             server.bgrewritechildpid == -1 &&
             server.auto_aofrewrite_perc &&
             server.appendonly_current_size > server.auto_aofrewrite_min_size)
         {
            long long base = server.auto_aofrewrite_base_size ?
                            server.auto_aofrewrite_base_size : 1;
            long long growth = (server.appendonly_current_size*100/base) - 100;
            if (growth >= server.auto_aofrewrite_perc) {
                redisLog(REDIS_NOTICE,"Starting automatic rewriting of AOF on %lld%% growth",growth);
                rewriteAppendOnlyFileBackground();
            }
        }
    }


    /* If we postponed an AOF buffer flush, let's try to do it every time the
     * cron function is called. */
    if (server.aof_flush_postponed_start) flushAppendOnlyFile(0);
#endif

    /* Expire a few keys per cycle, only if this is a master.
     * On slaves we wait for DEL operations synthesized by the master
     * in order to guarantee a strict consistency. */
    if (server.masterhost == NULL) activeExpireCycle();

    /* Swap a few keys on disk if we are over the memory limit and VM
     * is enbled. Try to free objects from the free list first. */
    if (vmCanSwapOut()) {
        while (server.vm_enabled && zmalloc_used_memory() >
                server.vm_max_memory)
        {
            int retval = (server.vm_max_threads == 0) ?
                        vmSwapOneObjectBlocking() :
                        vmSwapOneObjectThreaded();
            if (retval == REDIS_ERR && !(loops % 300) &&
                zmalloc_used_memory() >
                (server.vm_max_memory+server.vm_max_memory/10))
            {
                redisLog(REDIS_WARNING,"WARNING: vm-max-memory limit exceeded by more than 10%% but unable to swap more objects out!");
            }
            /* Note that when using threade I/O we free just one object,
             * because anyway when the I/O thread in charge to swap this
             * object out will finish, the handler of completed jobs
             * will try to swap more objects if we are still out of memory. */
            if (retval == REDIS_ERR || server.vm_max_threads > 0) break;
        }
    }

#if 0
    /* Replication cron function -- used to reconnect to master and
     * to detect transfer failures. */
    if (!(loops % 10)) replicationCron();
#endif

    server.cronloops++;
    return 100;
}

/* This function gets called every time Redis is entering the
 * main loop of the event driven library, that is, before to sleep
 * for ready file descriptors. */
void beforeSleep(struct aeEventLoop *eventLoop) {
    REDIS_NOTUSED(eventLoop);
    listNode *ln;
    redisClient *c;

    /* Awake clients that got all the swapped keys they requested */
    if (server.vm_enabled && listLength(server.io_ready_clients)) {
        listIter li;

        listRewind(server.io_ready_clients,&li);
        while((ln = listNext(&li))) {
            c = ln->value;
            struct redisCommand *cmd;

            /* Resume the client. */
            listDelNode(server.io_ready_clients,ln);
            c->flags &= (~REDIS_IO_WAIT);
            server.vm_blocked_clients--;
            aeCreateFileEvent(server.el, c->fd, AE_READABLE,
                readQueryFromClient, c);
            cmd = lookupCommand(c->argv[0]->ptr);
            redisAssert(cmd != NULL);
            call(c);
            resetClient(c);
            /* There may be more data to process in the input buffer. */
            if (c->querybuf && sdslen(c->querybuf) > 0)
                processInputBuffer(c);
        }
    }

    /* Try to process pending commands for clients that were just unblocked. */
    while (listLength(server.unblocked_clients)) {
        ln = listFirst(server.unblocked_clients);
        redisAssert(ln != NULL);
        c = ln->value;
        listDelNode(server.unblocked_clients,ln);
        c->flags &= ~REDIS_UNBLOCKED;

        /* Process remaining data in the input buffer. */
        if (c->querybuf && sdslen(c->querybuf) > 0)
            processInputBuffer(c);
    }

    if (listLength(server.repl_slaves) && dbmng_cp_is_active(0) == 0)
    {
        while (listLength(server.repl_slaves))
        {
            ln = listFirst(server.repl_slaves);
            redisAssert(ln != NULL);
            c = ln->value;
            listDelNode(server.repl_slaves,ln);

            processRepl(c);
        }
    }

#if 0
    // disable in FooYun
    /* Write the AOF buffer on disk */
    flushAppendOnlyFile(0);
#endif
}

/* =========================== Server initialization ======================== */

void createSharedObjects(void) {
    int j;

    shared.crlf = createObject(REDIS_STRING,sdsnew("\r\n"));
    shared.ok = createObject(REDIS_STRING,sdsnew("+OK\r\n"));
    shared.err = createObject(REDIS_STRING,sdsnew("-ERR\r\n"));
    shared.emptybulk = createObject(REDIS_STRING,sdsnew("$0\r\n\r\n"));
    shared.czero = createObject(REDIS_STRING,sdsnew(":0\r\n"));
    shared.cone = createObject(REDIS_STRING,sdsnew(":1\r\n"));
    shared.cnegone = createObject(REDIS_STRING,sdsnew(":-1\r\n"));
    shared.nullbulk = createObject(REDIS_STRING,sdsnew("$-1\r\n"));
    shared.nullmultibulk = createObject(REDIS_STRING,sdsnew("*-1\r\n"));
    shared.emptymultibulk = createObject(REDIS_STRING,sdsnew("*0\r\n"));
    shared.pong = createObject(REDIS_STRING,sdsnew("+PONG\r\n"));
    shared.queued = createObject(REDIS_STRING,sdsnew("+QUEUED\r\n"));
    shared.wrongtypeerr = createObject(REDIS_STRING,sdsnew(
        "-ERR Operation against a key holding the wrong kind of value\r\n"));
    shared.nokeyerr = createObject(REDIS_STRING,sdsnew(
        "-ERR no such key\r\n"));
    shared.nosupport = createObject(REDIS_STRING,sdsnew(
        "-ERR fooyun don't support\r\n"));
    shared.syntaxerr = createObject(REDIS_STRING,sdsnew(
        "-ERR syntax error\r\n"));
    shared.sameobjecterr = createObject(REDIS_STRING,sdsnew(
        "-ERR source and destination objects are the same\r\n"));
    shared.outofrangeerr = createObject(REDIS_STRING,sdsnew(
        "-ERR index out of range\r\n"));
    shared.loadingerr = createObject(REDIS_STRING,sdsnew(
        "-LOADING Redis is loading the dataset in memory\r\n"));
    shared.space = createObject(REDIS_STRING,sdsnew(" "));
    shared.colon = createObject(REDIS_STRING,sdsnew(":"));
    shared.plus = createObject(REDIS_STRING,sdsnew("+"));
    shared.select0 = createStringObject("select 0\r\n",10);
    shared.select1 = createStringObject("select 1\r\n",10);
    shared.select2 = createStringObject("select 2\r\n",10);
    shared.select3 = createStringObject("select 3\r\n",10);
    shared.select4 = createStringObject("select 4\r\n",10);
    shared.select5 = createStringObject("select 5\r\n",10);
    shared.select6 = createStringObject("select 6\r\n",10);
    shared.select7 = createStringObject("select 7\r\n",10);
    shared.select8 = createStringObject("select 8\r\n",10);
    shared.select9 = createStringObject("select 9\r\n",10);
    shared.messagebulk = createStringObject("$7\r\nmessage\r\n",13);
    shared.pmessagebulk = createStringObject("$8\r\npmessage\r\n",14);
    shared.subscribebulk = createStringObject("$9\r\nsubscribe\r\n",15);
    shared.unsubscribebulk = createStringObject("$11\r\nunsubscribe\r\n",18);
    shared.psubscribebulk = createStringObject("$10\r\npsubscribe\r\n",17);
    shared.punsubscribebulk = createStringObject("$12\r\npunsubscribe\r\n",19);
    shared.mbulk3 = createStringObject("*3\r\n",4);
    shared.mbulk4 = createStringObject("*4\r\n",4);
    for (j = 0; j < REDIS_SHARED_INTEGERS; j++) {
        shared.integers[j] = createObject(REDIS_STRING,(void*)(long)j);
        shared.integers[j]->encoding = REDIS_ENCODING_INT;
    }
}

void initServerConfig()
{
    server.port = REDIS_SERVERPORT;
    server.bindaddr = NULL;
    server.unixsocket = NULL;
    server.unixsocketperm = 0;
    server.ipfd = -1;
    server.slave_ipfd = -1;
    server.sofd = -1;
    server.dbnum = REDIS_DEFAULT_DBNUM;
    server.verbosity = REDIS_WARNING;
    //server.verbosity = REDIS_DEBUG;
    server.maxidletime = REDIS_MAXIDLETIME;
    server.client_max_querybuf_len = REDIS_MAX_QUERYBUF_LEN;
    server.saveparams = NULL;
    server.loading = 0;
    server.logfile = NULL; /* NULL = log on standard output */
    server.syslog_enabled = 0;
    server.syslog_ident = zstrdup("redis");
    server.syslog_facility = LOG_LOCAL0;
    server.daemonize = 0;
    server.appendonly = 0;
    server.appendfsync = APPENDFSYNC_EVERYSEC;
    server.no_appendfsync_on_rewrite = 0;
    server.auto_aofrewrite_perc = REDIS_AUTO_AOFREWRITE_PERC;
    server.auto_aofrewrite_min_size = REDIS_AUTO_AOFREWRITE_MIN_SIZE;
    server.auto_aofrewrite_base_size = 0;
    server.aofrewrite_scheduled = 0;
    server.lastfsync = time(NULL);
    server.appendfd = -1;
    server.appendseldb = -1; /* Make sure the first time will not match */
    server.aof_flush_postponed_start = 0;
    server.pidfile = 0;
    server.dbfilename = zstrdup("dump.rdb");
    server.appendfilename = zstrdup("appendonly.aof");
    server.requirepass = NULL;
    server.rdbcompression = 1;
    server.activerehashing = 1;
    server.maxclients = 0;
    server.bpop_blocked_clients = 0;
    server.maxmemory = 0;
    server.maxmemory_policy = REDIS_MAXMEMORY_ALLKEYS_LRU;
    server.maxmemory_samples = 3;
    server.vm_enabled = 0;
    server.vm_swap_file = 0;
    server.vm_page_size = 256;          /* 256 bytes per page */
    server.vm_pages = 1024*1024*100;    /* 104 millions of pages */
    server.vm_max_memory = 1024LL*1024*1024*1; /* 1 GB of RAM */
    server.vm_max_threads = 4;
    server.vm_blocked_clients = 0;
    server.hash_max_zipmap_entries = REDIS_HASH_MAX_ZIPMAP_ENTRIES;
    server.hash_max_zipmap_value = REDIS_HASH_MAX_ZIPMAP_VALUE;
    server.list_max_ziplist_entries = REDIS_LIST_MAX_ZIPLIST_ENTRIES;
    server.list_max_ziplist_value = REDIS_LIST_MAX_ZIPLIST_VALUE;
    server.set_max_intset_entries = REDIS_SET_MAX_INTSET_ENTRIES;
    server.zset_max_ziplist_entries = REDIS_ZSET_MAX_ZIPLIST_ENTRIES;
    server.zset_max_ziplist_value = REDIS_ZSET_MAX_ZIPLIST_VALUE;
    server.shutdown_asap = 0;
    server.repl_ping_slave_period = REDIS_REPL_PING_SLAVE_PERIOD;
    server.repl_timeout = REDIS_REPL_TIMEOUT;

    updateLRUClock();
    resetServerSaveParams();

    appendServerSaveParams(60*60,1);  /* save after 1 hour and 1 change */
    appendServerSaveParams(300,100);  /* save after 5 minutes and 100 changes */
    appendServerSaveParams(60,10000); /* save after 1 minute and 10000 changes */
    /* Replication related */
    server.isslave = 0;
    server.masterauth = NULL;
    server.masterhost = NULL;
    server.masterport = 6379;
    server.master = NULL;
    server.replstate = REDIS_REPL_NONE;
    server.repl_syncio_timeout = REDIS_REPL_SYNCIO_TIMEOUT;
    server.repl_serve_stale_data = 1;
    server.repl_down_since = -1;

    /* Double constants initialization */
    R_Zero = 0.0;
    R_PosInf = 1.0/R_Zero;
    R_NegInf = -1.0/R_Zero;
    R_Nan = R_Zero/R_Zero;

    /* Command table -- we intiialize it here as it is part of the
     * initial configuration, since command names may be changed via
     * redis.conf using the rename-command directive. */
    server.commands = dictCreate(&commandTableDictType,NULL);
    populateCommandTable();
    server.delCommand = lookupCommandByCString("del");
    server.multiCommand = lookupCommandByCString("multi");

    /* Slow log */
    server.slowlog_log_slower_than = REDIS_SLOWLOG_LOG_SLOWER_THAN;
    server.slowlog_max_len = REDIS_SLOWLOG_MAX_LEN;

    /* Assert */
    server.assert_failed = "<no assertion failed>";
    server.assert_file = "<no file>";
    server.assert_line = 0;
    server.bug_report_start = 0;

    server.db_max_size = 1024 * 1024 * 1024L;
    server.db_min_size = 11 * 1024 * 1024L;
    server.app_path = zstrdup("./");
    server.bl_root_path = 0;
    server.dbe_root_path = 0;
    server.binlog_prefix = zstrdup("binlog");
    server.upd_cp_timeout = 86400;
    server.print_dbeinfo_timeout = 10 * 60; // 10 min
    server.binlog_max_size = 200 * 1024 * 1024;
    server.op_max_num = 1024;
    server.period = 2;
    server.dbe_get_que_size = 2000;
    server.wr_bl_que_size = 2000;

    server.log_dir = 0;
    server.log_prefix = zstrdup("ds");
    server.log_file_len = (sizeof(long) == 8) ? 0 : 2000; //32bit platform can't exceed 2G

    strcpy(server.app_id, "ds-debug");
    server.has_cache = 1; // cache
    server.has_dbe = 0;
    server.read_only = 0; // read & write
    server.slave_port = -1; // disable
    server.is_slave = 0;
    server.wr_bl = 0;
    server.load_bl = 0;
    server.load_bl_cnt = 2;
    server.read_dbe = 0;
    server.init_ready = 0;
    server.dbe_hot_level = 15;
    server.slow_log = 0; // disable
    server.log_get_miss = 0;
    server.load_hot_key_max_num = -1;
    server.auto_purge = 1;
    server.ip_list_changed = 0;
    server.dbe_fsize = 4;

    server.querybuf_reuse = 1;
    server.mngr_list = 0;
    server.host = 0;
    server.ds_key_num = 0;

    server.g_key = createStringObject("key", strlen("key"));
    server.prtcl_redis = 0; // mc
    server.dbe_ver = DBE_VER_HIDB2;
    server.enc_kv = 0; // mc: raw; redis: encode kv
}

void initServer() 
{
    int j;

    signal(SIGHUP, SIG_IGN);
    signal(SIGPIPE, SIG_IGN);
    setupSignalHandlers();

    if (server.syslog_enabled) {
        openlog(server.syslog_ident, LOG_PID | LOG_NDELAY | LOG_NOWAIT,
            server.syslog_facility);
    }

    server.mainthread = pthread_self();
    server.clients = listCreate();
    server.slaves = listCreate();
    server.monitors = listCreate();
    server.unblocked_clients = listCreate();
    server.repl_slaves = listCreate();
    server.cleans = listCreate();
    server.dbe_get_clients = listCreate();
    server.write_bl_clients = listCreate();
    server.bl_writing = listCreate();
    server.wr_bl_list = listCreate();
    server.wr_dbe_list = listCreate();
    server.dbe_get_clients_cnt = 0;
    server.op_bl_cnt = 0;
    createSharedObjects();
    server.el = aeCreateEventLoop();
    server.db = zmalloc(sizeof(redisDb)*server.dbnum);

    if (server.port != 0) {
        int i = 0;
        for (i = 0; i < 3; i++)
        {
            server.ipfd = anetTcpServer(server.neterr,server.port,server.bindaddr);
            if (server.ipfd == ANET_ERR)
            {
                redisLog(REDIS_WARNING, "Opening port %d: %s",
                    server.port, server.neterr);
                sleep(2);
            }
            else
            {
                break;
            }
        }
        if (i >= 3)
        {
            redisLog(REDIS_WARNING, "exit after try %d times opening port %d",
                     i, server.port);
            exit(1);
        }
    }
    if (server.bindaddr
        && strcmp(server.bindaddr, "127.0.0.1") != 0
        && strcmp(server.bindaddr, "0.0.0.0") != 0)
    {
        /* slave_port: 127.0.0.1:port */
        server.slave_port = server.port;
        server.slave_ipfd = anetTcpServer(server.neterr,server.slave_port,"127.0.0.1");
        if (server.slave_ipfd == ANET_ERR) {
            redisLog(REDIS_WARNING, "Open port 127.0.0.1:%d fail: %s",
                server.slave_port, server.neterr);
            exit(1);
        }
    }
    if (server.unixsocket != NULL) {
        unlink(server.unixsocket); /* don't care if this fails */
        server.sofd = anetUnixServer(server.neterr,server.unixsocket,server.unixsocketperm);
        if (server.sofd == ANET_ERR) {
            redisLog(REDIS_WARNING, "Opening socket: %s", server.neterr);
            exit(1);
        }
    }
    if (server.ipfd < 0 && server.sofd < 0) {
        redisLog(REDIS_WARNING, "Configured to not listen anywhere, exiting.");
        exit(1);
    }
    for (j = 0; j < server.dbnum; j++) {
        server.db[j].dict = dictCreate(&dbDictType,NULL);
        server.db[j].expires = dictCreate(&keyptrDictType,NULL);
        server.db[j].blocking_keys = dictCreate(&keylistDictType,NULL);
        server.db[j].watched_keys = dictCreate(&keylistDictType,NULL);
        if (server.vm_enabled)
            server.db[j].io_keys = dictCreate(&keylistDictType,NULL);
        server.db[j].id = j;
    }
    server.pubsub_channels = dictCreate(&keylistDictType,NULL);
    server.pubsub_patterns = listCreate();
    listSetFreeMethod(server.pubsub_patterns,freePubsubPattern);
    listSetMatchMethod(server.pubsub_patterns,listMatchPubsubPattern);
    server.cronloops = 0;
    server.bgsavechildpid = -1;
    server.bgrewritechildpid = -1;
    server.bgrewritebuf = sdsempty();
    server.aofbuf = sdsempty();
    server.lastsave = time(NULL);
    server.dirty = 0;
    server.stat_cmd_total_dur = 0;
    server.stat_numcommands = 0;
    server.stat_numconnections = 0;
    server.stat_expiredkeys = 0;
    server.stat_evictedkeys = 0;
    server.stat_lru_del_keys = 0;
    server.stat_keyspace_misses = 0;
    server.stat_keyspace_hits = 0;
    server.stat_peak_memory = 0;
    server.stat_fork_time = 0;
    server.stat_misses_in_cache = 0;
    server.stat_notify_dbe_get_fail = 0;

    server.stat_get_cmd = 0;
    server.stat_set_cmd = 0;
    server.stat_touch_cmd = 0;
    server.stat_incr_cmd = 0;
    server.stat_decr_cmd = 0;
    server.stat_del_cmd = 0;
    server.stat_auth_cmd = 0;
    server.stat_get_hits = 0;
    server.stat_get_misses = 0;
    server.stat_touch_hits = 0;
    server.stat_touch_misses = 0;
    server.stat_incr_hits = 0;
    server.stat_incr_misses = 0;
    server.stat_decr_hits = 0;
    server.stat_decr_misses = 0;
    server.stat_cas_hits = 0;
    server.stat_cas_misses = 0;
    server.stat_cas_badval = 0;
    server.stat_del_hits = 0;
    server.stat_del_misses = 0;
    server.stat_auth_errors = 0;

    server.stat_expired_unfetched = 0;
    server.stat_evicted_unfetched = 0;

    server.stat_dbeio_get = 0;
    server.stat_dbeio_put = 0;
    server.stat_dbeio_del = 0;
    server.stat_dbeio_it = 0;

    server.bl_sync_delay_max = 0;
    server.bl_sync_delay_total = 0;
    server.bl_sync_cnt = 0;

    server.bytes_read = 0;
    server.bytes_written = 0;
    server.rejected_conns = 0;
    server.accepting_conns = 1;

    server.unixtime = time(NULL);
    aeCreateTimeEvent(server.el, 1, serverCron, NULL, NULL);
    /*
    if (server.has_cache == 1 && server.has_dbe == 1)
    {
        aeCreateTimeEvent(server.el, server.upd_cp_timeout * 1000, upd_cp_cron, 0, 0);
    }
    if (server.has_dbe == 1)
    {
        aeCreateTimeEvent(server.el, server.print_dbeinfo_timeout * 1000, print_dbeinfo_cron, 0, 0);
    }
    */
    master_ctx = 0;
    if (server.ipfd > 0 && aeCreateFileEvent(server.el,server.ipfd,AE_READABLE,
        acceptTcpHandler,&master_ctx) == AE_ERR) oom("creating file event for master port");
    slave_ctx = 1;
    if (server.slave_ipfd > 0 && aeCreateFileEvent(server.el,server.slave_ipfd,AE_READABLE,
        acceptTcpHandler,&slave_ctx) == AE_ERR) oom("creating file event for slave port");
    if (server.sofd > 0 && aeCreateFileEvent(server.el,server.sofd,AE_READABLE,
        acceptUnixHandler,NULL) == AE_ERR) oom("creating file event");
    if (db_io_init() != 0)
    {
        oom("db_io_init() fail");
    }

    /* disable in FooYun
    if (server.appendonly) {
        server.appendfd = open(server.appendfilename,O_WRONLY|O_APPEND|O_CREAT,0644);
        if (server.appendfd == -1) {
            redisLog(REDIS_WARNING, "Can't open the append-only file: %s",
                strerror(errno));
            exit(1);
        }
    }
    */

    if (server.vm_enabled) vmInit();
    slowlogInit();
    //bioInit();
    srand(time(NULL)^getpid());
}

/* Populates the Redis Command Table starting from the hard coded list
 * we have on top of redis.c file. */
void populateCommandTable(void) {
    int j;
    int numcommands = sizeof(readonlyCommandTable)/sizeof(struct redisCommand);

    for (j = 0; j < numcommands; j++) {
        struct redisCommand *c = readonlyCommandTable+j;
        int retval;

        retval = dictAdd(server.commands, sdsnew(c->name), c);
        assert(retval == DICT_OK);
    }
}

/* ====================== Commands lookup and execution ===================== */

struct redisCommand *lookupCommand(sds name) {
    return dictFetchValue(server.commands, name);
}

struct redisCommand *lookupCommandByCString(char *s) {
    struct redisCommand *cmd;
    sds name = sdsnew(s);

    cmd = dictFetchValue(server.commands, name);
    sdsfree(name);
    return cmd;
}

/* Call() is the core of Redis execution of a command */
void call(redisClient *c) {
    long long dirty, duration;

    dirty = server.dirty;
    redisLog(REDIS_DEBUG,
             "#%d#%d [%s %s ...] argc=%d",
             g_tag, g_tid, c->cmd->name, c->argc > 1 ? c->argv[1]->ptr : "", c->argc);
    const long long start = server.ustime;
    c->cmd->proc(c);
    //updateCachedTime();
    c->call_dur = server.ustime - start;
    duration = (long long)c->call_dur;
    dirty = server.dirty-dirty;
    slowlogPushEntryIfNeeded(c->argv,c->argc,duration);

#if 0 
    if (server.slow_log && duration >= server.slowlog_log_slower_than)
    {
        redisLog(REDIS_PROMPT,
                 "#%d#%d %s, argc=%d, call()_dur=%lldus",
                 g_tag, g_tid, c->cmd->name, c->argc, duration);
    }

    if (server.appendonly && dirty > 0)
        feedAppendOnlyFile(c->cmd,c->db->id,c->argv,c->argc);
    if ((dirty > 0 || c->cmd->flags & REDIS_CMD_FORCE_REPLICATION) &&
        listLength(server.slaves))
        replicationFeedSlaves(server.slaves,c->db->id,c->argv,c->argc);
#endif
    if (listLength(server.monitors))
        replicationFeedMonitors(server.monitors,c->db->id,c->argv,c->argc);
    server.stat_cmd_total_dur += duration;
    server.stat_numcommands++;
}

/* If this function gets called we already read a whole
 * command, argments are in the client argv/argc fields.
 * processCommand() execute the command or prepare the
 * server for a bulk read from the client.
 *
 * If 1 is returned the client is still alive and valid and
 * and other operations can be performed by the caller. Otherwise
 * if 0 is returned the client was destroied (i.e. after QUIT). */
int processCommand(redisClient *c)
{
    c->recv_dur = server.ustime - c->start_time;
    c->start_time = server.ustime;
    /* The QUIT command is handled separately. Normal command procs will
     * go through checking for replication and QUIT will cause trouble
     * when FORCE_REPLICATION is enabled and would be implemented in
     * a regular command proc. */
    //redisLog(REDIS_PROMPT, "processCommand start_time=%lld", c->start_time);
    if (!strcasecmp(c->argv[0]->ptr,"quit")) {
        //redisLog(REDIS_PROMPT, "quit cmd...fd=%d", c->fd);
        addReply(c,shared.ok);
        c->flags |= REDIS_CLOSE_AFTER_REPLY;
        return REDIS_ERR;
    }

    /* Now lookup the command and check ASAP about trivial error conditions
     * such as wrong arity, bad command name and so forth. */
    c->cmd = c->lastcmd = lookupCommand(c->argv[0]->ptr);
    if (!c->cmd) {
        addReplyErrorFormat(c,"unknown command '%s'",
            (char*)c->argv[0]->ptr);
        return REDIS_OK;
    }

    c->cmd->visit_cnt++;

    if ((c->cmd->arity > 0 && c->cmd->arity != c->argc)
        || (c->argc < -c->cmd->arity))
    {
        addReplyErrorFormat(c,"wrong number of arguments for '%s' command",
            c->cmd->name);
        return REDIS_OK;
    }

    /* Check if the user is authenticated */
    if (server.requirepass && !c->authenticated && c->cmd->proc != authCommand)
    {
        addReplyError(c,"operation not permitted");
        return REDIS_OK;
    }

    /* Handle the maxmemory directive.
     *
     * First we try to free some memory if possible (if there are volatile
     * keys in the dataset). If there are not the only thing we can do
     * is returning an error. */

    check_key_validity();
    clean_more();
    freeMemoryIfNeededBySelf();

    if (server.maxmemory) freeMemoryIfNeeded();
    if (server.maxmemory && (c->cmd->flags & REDIS_CMD_DENYOOM) &&
        zmalloc_used_memory() > server.maxmemory)
    {
        addReplyError(c,"command not allowed when used memory > 'maxmemory'");
        return REDIS_OK;
    }

    /* Only allow SUBSCRIBE and UNSUBSCRIBE in the context of Pub/Sub */
    if ((dictSize(c->pubsub_channels) > 0 || listLength(c->pubsub_patterns) > 0)
        &&
        c->cmd->proc != subscribeCommand &&
        c->cmd->proc != unsubscribeCommand &&
        c->cmd->proc != psubscribeCommand &&
        c->cmd->proc != punsubscribeCommand) {
        addReplyError(c,"only (P)SUBSCRIBE / (P)UNSUBSCRIBE / QUIT allowed in this context");
        return REDIS_OK;
    }

    /* Only allow INFO and SLAVEOF when slave-serve-stale-data is no and
     * we are a slave with a broken link with master. */
    if (server.masterhost && server.replstate != REDIS_REPL_CONNECTED &&
        server.repl_serve_stale_data == 0 &&
        c->cmd->proc != infoCommand && c->cmd->proc != slaveofCommand)
    {
        addReplyError(c,
            "link with MASTER is down and slave-serve-stale-data is set to no");
        return REDIS_OK;
    }

    /* Loading DB? Return an error if the command is not INFO */
    if (server.loading && c->cmd->proc != infoCommand) {
        addReply(c, shared.loadingerr);
        return REDIS_OK;
    }

    /* Exec the command */
    if (c->flags & REDIS_MULTI &&
        c->cmd->proc != execCommand && c->cmd->proc != discardCommand &&
        c->cmd->proc != multiCommand && c->cmd->proc != watchCommand)
    {
        queueMultiCommand(c);
        addReply(c,shared.queued);
    } else {
        if (server.vm_enabled && server.vm_max_threads > 0 &&
            blockClientOnSwappedKeys(c))
        {
            return REDIS_ERR;
        }

        save_keys_mirror(c);
        const int ret = block_client_on_dbe_get(c);
        if (ret == 2)
        {
            /* block_client_list is too long */
            addReplyError(c, "too many clients are blocking at dbe_get");
            return REDIS_OK;
        }
        else if (ret == 1)
        {
            /* need block & blocking succ */
            return REDIS_ERR;
        }

        struct redisCommand *cmd = c->cmd;
        if (cmd->wrbl_flag)
        {
            const int ret = block_client_on_write_bl(c);
            if (ret == 2)
            {
                /* block_client_list is too long */
                addReplyError(c, "too many clients are blocking at write_bl");
                return REDIS_OK;
            }
            else if (ret == 1)
            {
                /* need block & blocking succ */
                return REDIS_ERR;
            }
        }

        call(c);
    }
    return REDIS_OK;
}

/*================================== Shutdown =============================== */

int prepareForShutdown()
{
    redisLog(REDIS_PROMPT, "User requested shutdown...");
#if 0
    // disable in FooYun
    /* Kill the saving child if there is a background saving in progress.
       We want to avoid race conditions, for instance our saving child may
       overwrite the synchronous saving did by SHUTDOWN. */
    if (server.bgsavechildpid != -1) {
        redisLog(REDIS_WARNING,"There is a child saving an .rdb. Killing it!");
        kill(server.bgsavechildpid,SIGKILL);
        rdbRemoveTempFile(server.bgsavechildpid);
    }
    if (server.appendonly) {
        /* Kill the AOF saving child as the AOF we already have may be longer
         * but contains the full dataset anyway. */
        if (server.bgrewritechildpid != -1) {
            redisLog(REDIS_WARNING,
                "There is a child rewriting the AOF. Killing it!");
            kill(server.bgrewritechildpid,SIGKILL);
        }
        /* Append only file: fsync() the AOF and exit */
        redisLog(REDIS_NOTICE,"Calling fsync() on the AOF file.");
        aof_fsync(server.appendfd);
    }
    if (server.saveparamslen > 0) {
        redisLog(REDIS_NOTICE,"Saving the final RDB snapshot before exiting.");
        /* Snapshotting. Perform a SYNC SAVE and exit */
        if (rdbSave(server.dbfilename) != REDIS_OK) {
            /* Ooops.. error saving! The best we can do is to continue
             * operating. Note that if there was a background saving process,
             * in the next cron() Redis will be notified that the background
             * saving aborted, handling special stuff like slaves pending for
             * synchronization... */
            redisLog(REDIS_WARNING,"Error trying to save the DB, can't exit.");
            return REDIS_ERR;
        }
    }
#endif
    if (server.vm_enabled) {
        redisLog(REDIS_NOTICE,"Removing the swap file.");
        unlink(server.vm_swap_file);
    }
    if (server.daemonize) {
        redisLog(REDIS_NOTICE,"Removing the pid file.");
        unlink(server.pidfile);
    }
    /* Close the listening sockets. Apparently this allows faster restarts. */
    if (server.ipfd != -1) close(server.ipfd);
    if (server.sofd != -1) close(server.sofd);
    if (server.unixsocket) {
        redisLog(REDIS_NOTICE,"Removing the unix socket file.");
        unlink(server.unixsocket); /* don't care if this fails */
    }

    db_io_uninit();
    dbmng_uninit(0);

    redisLog(REDIS_PROMPT, "DataServer is now ready to exit, bye bye...");
    return REDIS_OK;
}

/*================================== Commands =============================== */

void authCommand(redisClient *c) {
    server.stat_auth_cmd++;
    if (!server.requirepass) {
        addReplyError(c,"Client sent AUTH, but no password is set");
    } else if (!strcmp(c->argv[1]->ptr, server.requirepass)) {
      c->authenticated = 1;
      addReply(c,shared.ok);
    } else {
      c->authenticated = 0;
      addReplyError(c,"invalid password");
      server.stat_auth_errors++;
    }
}

void pingCommand(redisClient *c) {
    addReply(c,shared.pong);
}

void echoCommand(redisClient *c) {
    addReplyBulk(c,c->argv[1]);
}

/* Convert an amount of bytes into a human readable string in the form
 * of 100B, 2G, 100M, 4K, and so forth. */
void bytesToHuman(char *s, unsigned long long n) {
    double d;

    if (n < 1024) {
        /* Bytes */
        sprintf(s,"%lluB",n);
        return;
    } else if (n < (1024*1024)) {
        d = (double)n/(1024);
        sprintf(s,"%.2fK",d);
    } else if (n < (1024LL*1024*1024)) {
        d = (double)n/(1024*1024);
        sprintf(s,"%.2fM",d);
    } else if (n < (1024LL*1024*1024*1024)) {
        d = (double)n/(1024LL*1024*1024);
        sprintf(s,"%.2fG",d);
    }
}

sds genMCStatInfoString(void) 
{
    sds info;
    info = sdscatprintf(sdsempty(),
        "address: %s\r\n"
        "port: %d\r\n"
        "get_cmds=%lld\n"
        "set_cmds=%lld\n"
        "touch_cmds=%lld\n"
        "incr_cmds=%lld\n"
        "decr_cmds=%lld\n"
        "delete_cmds=%lld\n"
        "get_hits=%lld\n"
        "get_misses=%lld\n"
        "delete_hits=%lld\n"
        "delete_misses=%lld\n"
        "incr_hits=%lld\n"
        "incr_misses=%lld\n"
        "decr_hits=%lld\n"
        "decr_misses=%lld\n"
        "cas_hits=%lld\n"
        "cas_misses=%lld\n"
        "cas_badval=%lld\n"
        "touch_hits=%lld\n"
        "touch_misses=%lld\n"
        "bytes_read=%lld\n"
        "bytes_written=%lld\n"
        "expired_unfetched=%lld\n"
        "evicted_unfetched=%lld\n"
        "rejected_conns=%lld\n"
        "accepting_conns=%d\n"
        "dbeio_get=%llu\n"
        "dbeio_put=%llu\n"
        "dbeio_del=%llu\n"
        "dbeio_it=%llu\n"
        "notify_dbe_get_fail=%llu\n"
        "bl_sync_delay_max=%d\n"
        , server.host
        , server.port
        , server.stat_get_cmd
        , server.stat_set_cmd
        , server.stat_touch_cmd
        , server.stat_incr_cmd
        , server.stat_decr_cmd
        , server.stat_del_cmd
        , server.stat_get_hits
        , server.stat_get_misses
        , server.stat_del_hits
        , server.stat_del_misses
        , server.stat_incr_hits
        , server.stat_incr_misses
        , server.stat_decr_hits
        , server.stat_decr_misses
        , server.stat_cas_hits
        , server.stat_cas_misses
        , server.stat_cas_badval
        , server.stat_touch_hits
        , server.stat_touch_misses
        , server.bytes_read
        , server.bytes_written
        , server.stat_expired_unfetched
        , server.stat_evicted_unfetched
        , server.rejected_conns
        , server.accepting_conns
        , server.stat_dbeio_get
        , server.stat_dbeio_put
        , server.stat_dbeio_del
        , server.stat_dbeio_it
        , server.stat_notify_dbe_get_fail
        , server.bl_sync_delay_max
        );

    return info;
}

sds genRedisStatInfoString(void) 
{
    sds info = sdsempty();
    dictEntry *de;

    info = sdscatprintf(info, "address: %s\r\nport: %d\r\n", server.host, server.port);

    dictIterator *it = dictGetIterator(server.commands);
    while ((de = dictNext(it)) != NULL)
    {
        struct redisCommand *cmd = dictGetEntryVal(de);
        info = sdscatprintf(info, "%s=%zd\n" , cmd->name, cmd->visit_cnt);
    }
    dictReleaseIterator(it);

    return info;
}

/* Create the string returned by the INFO command. This is decoupled
 * by the INFO command itself as we need to report the same information
 * on memory corruption problems. */
sds genRedisInfoString(void) 
{
    sds info;

    const time_t now_t = time(0);
    char now_t_str[24];
    struct tm *p_tm = localtime(&now_t);
    sprintf(now_t_str, "%04d-%02d-%02d %02d:%02d:%02d", p_tm->tm_year + 1900, p_tm->tm_mon + 1, p_tm->tm_mday, p_tm->tm_hour, p_tm->tm_min, p_tm->tm_sec);

    const time_t uptime = now_t - server.stat_starttime;
    int j;
    char ds_hmem[64], hmem[64], peak_hmem[64], rss_hmem[64];
    struct rusage self_ru, c_ru;
    unsigned long lol, bib, buf_len;
    const size_t rss = zmalloc_get_rss();

    getrusage(RUSAGE_SELF, &self_ru);
    getrusage(RUSAGE_CHILDREN, &c_ru);
    getClientsMaxBuffers(&lol,&bib,&buf_len);

    bytesToHuman(ds_hmem,ds_zmalloc_used_memory());
    bytesToHuman(hmem,zmalloc_used_memory());
    bytesToHuman(rss_hmem,rss);
    bytesToHuman(peak_hmem,server.stat_peak_memory);
    info = sdscatprintf(sdsempty(),
        "address: %s\r\n"
        "port: %d\r\n"
        "redis_version:%s\r\n"
        "redis_git_sha1:%s\r\n"
        "redis_git_dirty:%d\r\n"
        "pid: %d\r\n"
        "start_time: %s\r\n"
        "app_id: %s\r\n"
        "ds_key: %s\r\n"
        "ds_key_num: %llu\r\n"
        "version: %s\r\n"
        "Compile time: %s %s\r\n"
        "arch_bits: %s\r\n"
        "multiplexing_api: %s\r\n"
        "now: %ld (%s)\r\n"
        "uptime_in_seconds: %ld\r\n"
        "uptime_in_days: %ld\r\n"
        "lru_clock: %ld\r\n"
        "used_cpu_sys: %.2f\r\n"
        "used_cpu_user: %.2f\r\n"
        "used_cpu_sys_children: %.2f\r\n"
        "used_cpu_user_children: %.2f\r\n"
        "connected_clients: %d\r\n"
        "client_longest_output_list: %lu\r\n"
        "client_biggest_input: %lu\r\n"
        "client_biggest_input_buf: %lu\r\n"
        "blocked_clients: %d\r\n"
        "ds_used_memory: %zu\r\n"
        "ds_used_memory_human: %s\r\n"
        "used_memory: %zu\r\n"
        "used_memory_human: %s\r\n"
        "used_memory_rss: %zu\r\n"
        "used_memory_rss_human: %s\r\n"
        "used_memory_peak: %zu\r\n"
        "used_memory_peak_human: %s\r\n"
        "mem_fragmentation_ratio: %.2f\r\n"
        "mem_allocator: %s\r\n"
        "limit_max_fd: %d\r\n"
        "current_max_fd: %d\r\n"
        "total_connections_received: %lld\r\n"
        "total_commands_processed: %lld\r\n"
        "total_commands_processed_duration: %lld\r\n"
        "expired_keys: %lld\r\n"
        "evicted_keys: %lld\r\n"
        "lru_del_keys: %lld\r\n"
        "keyspace_hits: %lld\r\n"
        "keyspace_misses: %lld\r\n"
        "pubsub_channels: %ld\r\n"
        "pubsub_patterns: %u\r\n"
        "dbe_get_clients: %u(%d)\r\n"
        "write_bl_blocking_clients: %u\r\n"
        "handle_bl_list_len: %d\r\n"
        "wr_bl_list_len: %d\r\n"
        "wr_dbe_list_len: %d\r\n"
        "vm_enabled: %d\r\n"
        , server.host
        , server.port
        , REDIS_VERSION
        , redisGitSHA1()
        , strtol(redisGitDirty(),NULL,10) > 0
        , (int)getpid()
        , server.starttime_str
        , server.app_id
        , server.ds_key ? server.ds_key : ""
        , server.ds_key_num
        , DS_VERSION, __DATE__, __TIME__,
        (sizeof(long) == 8) ? "64" : "32",
        aeGetApiName(),
        now_t, now_t_str,
        uptime,
        uptime/(3600*24),
        (unsigned long) server.lruclock,
        (float)self_ru.ru_stime.tv_sec+(float)self_ru.ru_stime.tv_usec/1000000,
        (float)self_ru.ru_utime.tv_sec+(float)self_ru.ru_utime.tv_usec/1000000,
        (float)c_ru.ru_stime.tv_sec+(float)c_ru.ru_stime.tv_usec/1000000,
        (float)c_ru.ru_utime.tv_sec+(float)c_ru.ru_utime.tv_usec/1000000,
        listLength(server.clients)-listLength(server.slaves),
        lol, bib, buf_len,
        server.bpop_blocked_clients,
        ds_zmalloc_used_memory(),
        ds_hmem,
        zmalloc_used_memory(),
        hmem,
        rss,
        rss_hmem,
        server.stat_peak_memory,
        peak_hmem,
        zmalloc_get_fragmentation_ratio(),
        ZMALLOC_LIB,
        AE_SETSIZE,
        g_max_fd,
        server.stat_numconnections,
        server.stat_numcommands,
        server.stat_cmd_total_dur,
        server.stat_expiredkeys,
        server.stat_evictedkeys,
        server.stat_lru_del_keys,
        server.stat_keyspace_hits,
        server.stat_keyspace_misses,
        dictSize(server.pubsub_channels),
        listLength(server.pubsub_patterns),
        server.dbe_get_clients_cnt,
        listLength(server.dbe_get_clients),
        listLength(server.write_bl_clients),
        listLength(server.bl_writing),
        listLength(server.wr_bl_list),
        listLength(server.wr_dbe_list),
        server.vm_enabled != 0
    );

#if 0
    // disable in FooYun
    if (server.appendonly) {
        info = sdscatprintf(info,
            "aof_current_size:%lld\r\n"
            "aof_base_size:%lld\r\n"
            "aof_pending_rewrite:%d\r\n"
            "aof_buffer_length:%zu\r\n"
            "aof_pending_bio_fsync:%llu\r\n",
            (long long) server.appendonly_current_size,
            (long long) server.auto_aofrewrite_base_size,
            server.aofrewrite_scheduled,
            sdslen(server.aofbuf),
            bioPendingJobsOfType(REDIS_BIO_AOF_FSYNC));
    }

    if (server.masterhost) {
        info = sdscatprintf(info,
            "master_host:%s\r\n"
            "master_port:%d\r\n"
            "master_link_status:%s\r\n"
            "master_last_io_seconds_ago:%d\r\n"
            "master_sync_in_progress:%d\r\n"
            ,server.masterhost,
            server.masterport,
            (server.replstate == REDIS_REPL_CONNECTED) ?
                "up" : "down",
            server.master ? ((int)(time(NULL)-server.master->lastinteraction)) : -1,
            server.replstate == REDIS_REPL_TRANSFER
        );

        if (server.replstate == REDIS_REPL_TRANSFER) {
            info = sdscatprintf(info,
                "master_sync_left_bytes:%ld\r\n"
                "master_sync_last_io_seconds_ago:%d\r\n"
                ,(long)server.repl_transfer_left,
                (int)(time(NULL)-server.repl_transfer_lastio)
            );
        }

        if (server.replstate != REDIS_REPL_CONNECTED) {
            info = sdscatprintf(info,
                "master_link_down_since_seconds:%ld\r\n",
                (long)time(NULL)-server.repl_down_since);
        }
    }
#endif

    if (server.vm_enabled) {
        lockThreadedIO();
        info = sdscatprintf(info,
            "vm_conf_max_memory:%llu\r\n"
            "vm_conf_page_size:%llu\r\n"
            "vm_conf_pages:%llu\r\n"
            "vm_stats_used_pages:%llu\r\n"
            "vm_stats_swapped_objects:%llu\r\n"
            "vm_stats_swappin_count:%llu\r\n"
            "vm_stats_swappout_count:%llu\r\n"
            "vm_stats_io_newjobs_len:%lu\r\n"
            "vm_stats_io_processing_len:%lu\r\n"
            "vm_stats_io_processed_len:%lu\r\n"
            "vm_stats_io_active_threads:%lu\r\n"
            "vm_stats_blocked_clients:%lu\r\n"
            ,(unsigned long long) server.vm_max_memory,
            (unsigned long long) server.vm_page_size,
            (unsigned long long) server.vm_pages,
            (unsigned long long) server.vm_stats_used_pages,
            (unsigned long long) server.vm_stats_swapped_objects,
            (unsigned long long) server.vm_stats_swapins,
            (unsigned long long) server.vm_stats_swapouts,
            (unsigned long) listLength(server.io_newjobs),
            (unsigned long) listLength(server.io_processing),
            (unsigned long) listLength(server.io_processed),
            (unsigned long) server.io_active_threads,
            (unsigned long) server.vm_blocked_clients
        );
        unlockThreadedIO();
    }
#if 0
    // disable in FooYun
    if (server.loading) {
        double perc;
        time_t eta, elapsed;
        off_t remaining_bytes = server.loading_total_bytes-
                                server.loading_loaded_bytes;

        perc = ((double)server.loading_loaded_bytes /
               server.loading_total_bytes) * 100;

        elapsed = time(NULL)-server.loading_start_time;
        if (elapsed == 0) {
            eta = 1; /* A fake 1 second figure if we don't have enough info */
        } else {
            eta = (elapsed*remaining_bytes)/server.loading_loaded_bytes;
        }

        info = sdscatprintf(info,
            "loading_start_time:%ld\r\n"
            "loading_total_bytes:%llu\r\n"
            "loading_loaded_bytes:%llu\r\n"
            "loading_loaded_perc:%.2f\r\n"
            "loading_eta_seconds:%ld\r\n"
            ,(unsigned long) server.loading_start_time,
            (unsigned long long) server.loading_total_bytes,
            (unsigned long long) server.loading_loaded_bytes,
            perc,
            eta
        );
    }
#endif

    for (j = 0; j < server.dbnum; j++) {
        const unsigned long keys = dictSize(server.db[j].dict);
        const unsigned long vkeys = dictSize(server.db[j].expires);
        if (keys || vkeys) {
            const unsigned long slots = dictSlots(server.db[j].dict);
            const unsigned long vslots = dictSlots(server.db[j].expires);
            info = sdscatprintf(info, "db%d:keys=%lu,expires=%lu,slots=%lu,slots_e=%lu\r\n",
                j, keys, vkeys, slots, vslots);
        }
    }
    return info;
}

void infoCommand(redisClient *c) {
    sds info = 0;
    if (c->argc == 1)
    {
        info = genRedisInfoString();
    }
    else
    {
        if (strcmp(c->argv[1]->ptr, "stat") == 0)
        {
            /* info stat */
            info = genMCStatInfoString();
        }
        else if (strcmp(c->argv[1]->ptr, "cmd") == 0)
        {
            /* info cmd */
            info = genRedisStatInfoString();
        }
        else if (strcmp(c->argv[1]->ptr, "config") == 0)
        {
            /* info config */
            info = get_conf_info_str();
            info = sdscatprintf(info, "dbe=%s\n"
                    , server.dbe_ver == DBE_VER_HIDB ? "hidb" : "hidb2");
        }
        else
        {
            info = genRedisInfoString();
        }
    }
    addReplySds(c,sdscatprintf(sdsempty(),"$%lu\r\n", (unsigned long)sdslen(info)));
    addReplySds(c,info);
    addReply(c,shared.crlf);
}

void monitorCommand(redisClient *c) {
    /* ignore MONITOR if aleady slave or in monitor mode */
    if (c->flags & REDIS_SLAVE) return;

    c->flags |= (REDIS_SLAVE|REDIS_MONITOR);
    c->slaveseldb = 0;
    listAddNodeTail(server.monitors,c);
    addReply(c,shared.ok);
}

/* ============================ Maxmemory directive  ======================== */

/* This function gets called when 'maxmemory' is set on the config file to limit
 * the max memory used by the server, and we are out of memory.
 * This function will try to, in order:
 *
 * - Free objects from the free list
 * - Try to remove keys with an EXPIRE set
 *
 * It is not possible to free enough memory to reach used-memory < maxmemory
 * the server will start refusing commands that will enlarge even more the
 * memory usage.
 */
void freeMemoryIfNeeded(void)
{
    /* Remove keys accordingly to the active policy as long as we are
     * over the memory limit. */
    if (server.maxmemory_policy == REDIS_MAXMEMORY_NO_EVICTION) return;

    while (zmalloc_used_memory() > server.maxmemory) {
        int j, k, freed = 0;

        for (j = 0; j < server.dbnum; j++) {
            long bestval = 0; /* just to prevent warning */
            sds bestkey = NULL;
            struct dictEntry *de;
            redisDb *db = server.db+j;
            dict *dict;

            if (server.maxmemory_policy == REDIS_MAXMEMORY_ALLKEYS_LRU ||
                server.maxmemory_policy == REDIS_MAXMEMORY_ALLKEYS_RANDOM)
            {
                dict = server.db[j].dict;
            } else {
                dict = server.db[j].expires;
            }
            if (dictSize(dict) == 0) continue;

            /* volatile-random and allkeys-random policy */
            if (server.maxmemory_policy == REDIS_MAXMEMORY_ALLKEYS_RANDOM ||
                server.maxmemory_policy == REDIS_MAXMEMORY_VOLATILE_RANDOM)
            {
                de = dictGetRandomKey(dict);
                bestkey = dictGetEntryKey(de);
            }

            /* volatile-lru and allkeys-lru policy */
            else if (server.maxmemory_policy == REDIS_MAXMEMORY_ALLKEYS_LRU ||
                server.maxmemory_policy == REDIS_MAXMEMORY_VOLATILE_LRU)
            {
                for (k = 0; k < server.maxmemory_samples; k++) {
                    sds thiskey;
                    long thisval;
                    robj *o;

                    de = dictGetRandomKey(dict);
                    thiskey = dictGetEntryKey(de);
                    /* When policy is volatile-lru we need an additonal lookup
                     * to locate the real key, as dict is set to db->expires. */
                    if (server.maxmemory_policy == REDIS_MAXMEMORY_VOLATILE_LRU)
                        de = dictFind(db->dict, thiskey);
                    o = dictGetEntryVal(de);
                    thisval = estimateObjectIdleTime(o);

                    /* Higher idle time is better candidate for deletion */
                    if (bestkey == NULL || thisval > bestval) {
                        bestkey = thiskey;
                        bestval = thisval;
                    }
                }
            }

            /* volatile-ttl */
            else if (server.maxmemory_policy == REDIS_MAXMEMORY_VOLATILE_TTL) {
                for (k = 0; k < server.maxmemory_samples; k++) {
                    sds thiskey;
                    long thisval;

                    de = dictGetRandomKey(dict);
                    thiskey = dictGetEntryKey(de);
                    thisval = (long) dictGetEntryVal(de);

                    /* Expire sooner (minor expire unix timestamp) is better
                     * candidate for deletion */
                    if (bestkey == NULL || thisval < bestval) {
                        bestkey = thiskey;
                        bestval = thisval;
                    }
                }
            }

            /* Finally remove the selected key. */
            if (bestkey) {
                robj keyobj;
                initObject(&keyobj, REDIS_STRING, bestkey);

                robj *const val = (robj *)dictFetchValue(db->dict, bestkey);
                if (val && val->visited_bit == 0)
                {
                    server.stat_evicted_unfetched++;
                }

                save_key_mirror_evict(&keyobj);

                propagateExpire(db, &keyobj);
                dbDelete(db, &keyobj);
                server.stat_evictedkeys++;
                freed++;
            }
        }
        if (!freed) return; /* nothing to free... */
    }
}

static int removeKeyFromMem(redisDb *db)
{
    dict *dict = db->dict;
    long bestval = 0; /* just to prevent warning */
    sds bestkey = NULL;
    robj *bestvalobj = NULL;
    struct dictEntry *de;

    if (dictSize(dict) == 0)
    {
        return 1;
    }

    int k;
    for (k = 0; k < server.maxmemory_samples; k++)
    {
        sds thiskey;
        long thisval;
        robj *o;

        de = dictGetRandomKey(dict);
        if (de == 0)
        {
            break;
        }
        thiskey = dictGetEntryKey(de);
        /* When policy is volatile-lru we need an additonal lookup
         * to locate the real key, as dict is set to db->expires. */
        o = dictGetEntryVal(de);
        thisval = estimateObjectIdleTime(o);

        /* Higher idle time is better candidate for deletion */
        if (bestkey == NULL || thisval > bestval) {
            bestkey = thiskey;
            bestval = thisval;
            bestvalobj = o;
        }
    }

    /* Finally remove the selected key. */
    if (bestkey)
    {
        redisLog(REDIS_VERBOSE, "\"%s\" evicted", bestkey);
        robj keyobj;
        initObject(&keyobj, REDIS_STRING, bestkey);

        if (bestvalobj && bestvalobj->visited_bit == 0)
        {
            server.stat_evicted_unfetched++;
        }

        save_key_mirror_evict(&keyobj);

        propagateExpire(db, &keyobj);
        dbDelete(db, &keyobj);
        return 0;
    }
    else
    {
        return 1;
    }
}

static void freeMemoryIfNeededBySelf(void)
{
    /* Remove keys accordingly to the active policy as long as we are
     * over the memory limit. */
    const long long start = server.ustime;
    size_t um = ds_zmalloc_used_memory();
    const size_t um_old = um;

    int j = 0;
    int cnt = 0;
    int loop = 0;
    while (um > (size_t)server.db_max_size && cnt <= 5 && loop < server.dbnum)
    {
        //redisLog(REDIS_PROMPT, "db.%d: um=%ld, db_max_size=%ld, db_min_size=%ld",
        //        j, um, server.db_max_size, server.db_min_size);
        redisDb *db = server.db + j;
        if (removeKeyFromMem(db) == 0)
        {
            server.stat_evictedkeys++;
            cnt++;
            um = ds_zmalloc_used_memory();
            loop = 0;
        }
        else
        {
            loop++;
        }
        j = (j + 1) % server.dbnum;
    }

    if (cnt && server.verbosity <= REDIS_VERBOSE)
    {
        updateCachedTime();
        const long long dur = server.ustime - start;
        redisLog(REDIS_VERBOSE,
                 "evicted=%d, %zd -> %zd, diff=%zd, dur=%lld(us), "
                 "db_max_size=%"PRId64", db_min_size=%"PRId64,
                 cnt, um_old, um, um_old - um, dur,
                 server.db_max_size, server.db_min_size);
    }
}

static void cache_clean(int percent)
{
    if (percent <= 0)
    {
        /* do nothing */
    }
    else if (percent < 100)
    {
         cln_start = server.unixtime;
         const size_t curMem = ds_zmalloc_used_memory();
         maxMem = curMem * (100 - percent) / 100;
         redisLog(REDIS_PROMPT
                    , "clean_cache percent=%d, %zd --> %zd"
                    , percent, curMem, maxMem);
    }
    else
    {
        /* empty db */
        const long long cnt = emptyDb();
        redisLog(REDIS_PROMPT, "empty whole db, keys=%lld", cnt);
        maxMem = 0;
        db_idx = 0;
    }
}

static void clean_cache(int percent)
{
    //dbmng_reset_bl(0);
    cache_clean(percent);
}

static void clean_dbe(int somedays)
{
    if (somedays < 0)
    {
        return;
    }
    else if (somedays == 0)
    {
#ifdef _UPD_DBE_BY_PERIODIC_
        /* binlog & binlogtab */
        dbmng_release_bl(0);
#endif

        /* rdb */
        cache_clean(100);

        /* dbe */
        dbe_clean(dbmng_get_db(0, 0));

        return;
    }

    time_t t = somedays * 24 * 60 * 60;
    const time_t now = time(0);
    if (t < now)
    {
        t = now - t;
        struct tm *tm = localtime(&t);
        redisLog(REDIS_PROMPT
                 , "clean_dbe %d, up to %04d-%02d-%02d"
                 , somedays, tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday);
        tm->tm_hour = 23;
        tm->tm_min = tm->tm_sec = 59;
        dbe_clean_file(dbmng_get_db(0, 0), tm);
    }
}

void rsp_hb(aeEventLoop *el, int fd, void *privdata, int mask)
{
    redisLog(REDIS_DEBUG, "rsp_hb()...fd=%d", fd);
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(mask);
    REDIS_NOTUSED(privdata);

    char req[1];
    if (read(fd, req, 1) != 1)
    {
        redisLog(REDIS_NOTICE, "read() fail, fd=%d, hb_recv_fd=%d"
                , fd, server.hb_recv_fd);
        return;
    }

    if (req[0] & LRU_MASK)
    {
        size_t um = ds_zmalloc_used_memory();
        int j = 0;
        int cnt = 0;
        int loop = 0;
        while (um > (size_t)server.db_min_size
               && cnt <= FREE_OBJ_NUM_LRU
               && loop < server.dbnum
              )
        {
            //redisLog(REDIS_PROMPT, "db.%d: um=%ld, server.db_min_size=%ld"
            //        , j, um, server.db_min_size);
            redisDb *db = server.db + j;
            if (removeKeyFromMem(db) == 0)
            {
                server.stat_lru_del_keys++;
                cnt++;
                um = ds_zmalloc_used_memory();
                loop = 0;
            }
            else
            {
                loop++;
            }
            j = (j + 1) % server.dbnum;
        }
        redisLog(REDIS_DEBUG , "mngr lru: freed=%d" , cnt);
    }
    if (req[0] & CLEAN_MASK)
    {
        if (server.has_dbe == 0)
        {
            clean_cache(gDsCtrl.clean_size);
        }
        else if (server.has_dbe == 1)
        {
            /* clean_size mean to some days */
            clean_dbe(gDsCtrl.clean_size);
        }
    }
    if (req[0] & DS_IP_LIST_MASK)
    {
        gen_filter_server();
    }
    if (req[0] & DS_IP_LIST_INCR_MASK)
    {
        gen_filter_server();
        server.ip_list_changed = 1;
    }
}

static void clean_more(void)
{
    if (maxMem > 0)
    {
        /* continue to clean_cache */
        size_t um = ds_zmalloc_used_memory();
        int j = 0;
        while (um > maxMem  && j < server.dbnum)
        {
            //redisLog(REDIS_PROMPT, "db.%d: um=%ld, maxMem=%ld", j, um, maxMem);
            redisDb *db = server.db + db_idx;
            db_idx = (db_idx + 1) % server.dbnum;
            if (removeKeyFromMem(db) == 0)
            {
                um = ds_zmalloc_used_memory();
                break;
            }
            j++;
        }
        /* there was no item to remove in all db or reach the condition */
        if (j >= server.dbnum || maxMem >= um)
        {
            /* finished, stop it */
            redisLog(REDIS_PROMPT
                     , "clean over: loop=%d, to %zd, now %zd, dur=%lds"
                     , j, maxMem, um, server.unixtime - cln_start);
            maxMem = 0;
            db_idx = 0;
        }
    }
}

/* =================================== Main! ================================ */

#ifdef __linux__
int linuxOvercommitMemoryValue(void) {
    FILE *fp = fopen("/proc/sys/vm/overcommit_memory","r");
    char buf[64];

    if (!fp) return -1;
    if (fgets(buf,64,fp) == NULL) {
        fclose(fp);
        return -1;
    }
    fclose(fp);

    return atoi(buf);
}

void linuxOvercommitMemoryWarning(void) {
    if (linuxOvercommitMemoryValue() == 0) {
        redisLog(REDIS_PROMPT,"WARNING overcommit_memory is set to 0! Background save may fail under low memory condition. To fix this issue add 'vm.overcommit_memory = 1' to /etc/sysctl.conf and then reboot or run the command 'sysctl vm.overcommit_memory=1' for this to take effect.");
    }
}
#endif /* __linux__ */

void createPidFile(void) {
    /* Try to write the pid file in a best-effort way. */
    FILE *fp = fopen(server.pidfile,"w");
    if (fp) {
        fprintf(fp,"%d\n",(int)getpid());
        fclose(fp);
    }
}

void daemonize(void) {
    int fd;

    if (fork() != 0) exit(0); /* parent exits */
    setsid(); /* create a new session */

    /* Every output goes to /dev/null. If Redis is daemonized but
     * the 'logfile' is set to 'stdout' in the configuration file
     * it will not log at all. */
    if ((fd = open("/dev/null", O_RDWR, 0)) != -1) {
        dup2(fd, STDIN_FILENO);
        dup2(fd, STDOUT_FILENO);
        dup2(fd, STDERR_FILENO);
        if (fd > STDERR_FILENO) close(fd);
    }
}

void version() {
    printf("FooYun data-server version %s\n", DS_VERSION);
    printf("Compile time %s %s\n", __DATE__, __TIME__);
    //printf("FooYun data-server version %s (%s:%d)\n", DS_VERSION,
    //    redisGitSHA1(), atoi(redisGitDirty()) > 0);
    exit(0);
}

void usage(const char *name)
{
    fprintf(stderr, "Usage: %s [options...]\n", name);
    fprintf(stderr, "Options:\n");
    fprintf(stderr, " -i ip      ip.(hb,bind)\n");
    fprintf(stderr, " -p port    port,default is 6379\n");
    fprintf(stderr, " -s list    mngrserver list as ip:port,ip:port,...\n");
    fprintf(stderr, " -f ini     config file\n");
    fprintf(stderr, " -l         set log level to debug, default is warning\n");
    fprintf(stderr, " -v         version info\n");
    fprintf(stderr, " --version  version info\n");
    fprintf(stderr, " -h         help text\n");
    fprintf(stderr, " -?         help text\n");
    fprintf(stderr, " --help     help text\n");
    fprintf(stderr, " -n ds_id   set ds_id,default is 0.(bl,sync)\n");

    fprintf(stderr, " -b xxxx    decode binlog file xxxx to xxxx.txt at the same dir\n");
    fprintf(stderr, " -B xxxx    decode binlog file xxxx to stdout\n");

    fprintf(stderr, " -c cmd     keydump: dump key from dbe to stdout\n");
    fprintf(stderr, "            keyinfodump: dump key's info from dbe to stdout\n");
    fprintf(stderr, "            get: display k-v from dbe to stdout\n");
    fprintf(stderr, " -k key     use with -c option\n");
    fprintf(stderr, " -e dbe     use with -c option\n");
    fprintf(stderr, " -A app_id  default is \"ds-debug\".(hb,path)\n");
    fprintf(stderr, " -K ds_key  default is ip:port.(sync,filter)\n");
    fprintf(stderr, " -L ds_list default is null.(filter)\n");
    fprintf(stderr, " -N load_level  default is 15\n");
    fprintf(stderr, " -P         persistent,optional,default is cache only\n");
    fprintf(stderr, " -R         redis protocol,optional,default is memcached\n");
    fprintf(stderr, " -W         write_bl,only for cache,default is non\n");
    fprintf(stderr, " -U max     max_size of rdb,unit MB,must >11MB,default is 1GB\n");
    fprintf(stderr, " -V         create dbe with hidb,default hidb2.only for mc\n");
    fprintf(stderr, " -X         slave,optional,default is master\n");
    fprintf(stderr, " -Y         read_dbe when fetch fail in cache,optional,default is without read_dbe\n");
    fprintf(stderr, " -d         daemonize,optional,default is terminal\n");
    fprintf(stderr, " -r         raw mode,optional,default is compress object when serializing\n");
    fprintf(stderr, " -x app_path optional,default is \"./\"\n");
    fprintf(stderr, " -y dbe_root_path optional,default is app_path/app_id/port/dbe\n");
    fprintf(stderr, " -z bl_root_path  optional,default is app_path/app_id/port/bl\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "config set requirepass <passwd>\n");
    fprintf(stderr, "config set slowlog-log-slower-than <xxx>\n");
    fprintf(stderr, "config set slow_log <0|1>\n");
    fprintf(stderr, "config set min_mem <xxx>\n");
    fprintf(stderr, "config set max_mem <xxx>\n");
    fprintf(stderr, "config set read_dbe <0|1>\n");
    fprintf(stderr, "config set wr_bl <0|1>\n");
    fprintf(stderr, "config set compress <0|1>\n");
    fprintf(stderr, "config set log_get_miss <0|1>\n");
    fprintf(stderr, "config set dbe_get_que_size <xxx>\n");
    fprintf(stderr, "config set wr_bl_que_size <xxx>\n");
    fprintf(stderr, "config set loglevel <warning|notice|verbose|debug>\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "how to get parameters:\n");
    fprintf(stderr, "1)fooyun-web,use -s optional.disable -f & other optional\n");
    fprintf(stderr, "2)command line optional\n");
    fprintf(stderr, "3)config file,use -f optional\n");
    fprintf(stderr, "4)default value inside\n");

    /* for db export */
    fprintf(stderr, 
            "For db export/load :\n"
            "\t-D dbpath: db path\n"
            "\t-O dstpath: path export to or load from\n"
            "\t-S split_hash_keys: split db according to this consistent hash keys, splitted by comma\n"
            "\t-H target_hash_index: only keys match this index of consistent hash keys will be exported, start from 0\n"
            "\t-E export_level: default 2, amount of db levels to export\n"
            "\t-M export_time: file modified before this time will not export, format: \"2015-02-04 00:00:00\"\n"
            "\t-m operation type, default 9: 9=>export to, 4=>load from\n"
            "\t-Q : quickly export, just print file names valid to export, without lock db\n"
            "\tExample: \n"
            "\tjust print files whose modify time later after, without lock db:\n"
            "\t./data-server -D src_path -M \"2015-02-04 00:00:00\" -Q\n"
            "\tjust print files in level0~level1, without lock db:\n"
            "\t./data-server -D src_path -E 2 -M \"2015-02-04 00:00:00\" -Q\n"
            "\texport files whose modify time later after, without lock db:\n"
            "\t./data-server -D src_path -O dest_path -M \"2015-02-04 00:00:00\"\n"
            "\texport files in level0~level1, without lock db:\n"
            "\t./data-server -D src_path -O dest_path -E 2 -M \"2015-02-04 00:00:00\"\n"
            "\texport files in level0~level2, key match ds2, will lock db:\n"
            "\t./data-server -D src_path -O dest_path -S ds1,ds2,ds3 -H 1 -E 3\n"
            "\tload data to db_to from db_from:\n"
            "\t./data-server -m 4 -D db_to -O db_from\n"
           );

    fprintf(stderr, "Example:\n");
    fprintf(stderr, " %s -i 127.0.0.1 -p 63790 -s 127.0.0.1:11007\n", name);
    fprintf(stderr, " %s -p 63790 -l\n", name);
    fprintf(stderr, " %s -b ./binlog.002\n", name);
    fprintf(stderr, " %s -c keydump -e ./ds-debug/63790/dbe/local\n", name);
    fprintf(stderr, " %s -c keyinfodump -e ./ds-debug/63790/dbe/local\n", name);
    fprintf(stderr, " %s -c get -k abc -e ./ds-debug/63790/dbe/local\n", name);
    exit(1);
}

static void init_log()
{
    pf_log_config_t t;

    pf_log_init_config(&t);

    strcpy(t.log_path, server.log_dir);
    strcpy(t.log_file, server.log_prefix);
    t.log_level = server.verbosity;
    t.file_len = server.log_file_len;

    pf_log_init(&t);
}

static const char *get_loglevel_str(uint16_t level)
{
    if (level == REDIS_WARNING)
    {
        return "warning";
    }
    if (level == REDIS_NOTICE)
    {
        return "notice";
    }
    if (level == REDIS_VERBOSE)
    {
        return "verbose";
    }
    if (level == REDIS_DEBUG)
    {
        return "debug";
    }
    else
    {
        return "unknown";
    }
}

static const char *get_max_memory_policy_str(int policy)
{
    if (policy == REDIS_MAXMEMORY_VOLATILE_LRU)
    {
        return "volatile-lru";
    }
    if (policy == REDIS_MAXMEMORY_VOLATILE_RANDOM)
    {
        return "volatile-random";
    }
    if (policy == REDIS_MAXMEMORY_VOLATILE_TTL)
    {
        return "volatile-ttl";
    }
    if (policy == REDIS_MAXMEMORY_ALLKEYS_LRU)
    {
        return "allkeys-lru";
    }
    if (policy == REDIS_MAXMEMORY_ALLKEYS_RANDOM)
    {
        return "allkeys-random";
    }
    if (policy == REDIS_MAXMEMORY_NO_EVICTION)
    {
        return "noeviction";
    }
    else
    {
        return "unknown";
    }
}

sds get_conf_info_str()
{
    char *mngr_list = get_mngr_list();
    sds sync_list = get_sync_list();
    sds info;

    info = sdscatprintf(sdsempty(),
                        "host=%s\n"
                        "listen_port=%d\n"
                        "version: %s\n"
                        "Compile time: %s %s\n"
                        "arch_bits: %s\n"
                        "multiplexing_api: %s\n"
                        "mngr_list=%s\n"
                        "sync_list=%s\n"
                        "heart_beat_period=%ds\n"
                        "daemonize=%s\n"
                        "app_id=%s\n"
                        "ds_key=%s\n"
                        "ds_key_num=%llu\n"
                        "unix_socket=%s\n"
                        "dbnum=%d\n"
                        "has_cache=%d\n"
                        "has_dbe=%d\n"
                        "read_only=%d\n"
                        "is_slave=%d\n"
                        "wr_bl=%d\n"
                        "load_bl=%d\n"
                        "load_bl_cnt=%d\n"
                        "dbe_hot_level=%d\n"
                        "read_dbe=%d\n"
                        "auto_purge=%d\n"
                        "db_max_size=%"PRId64"\n"
                        "db_min_size=%"PRId64"\n"
                        "app_path=%s\n"
                        "dbe_root_path=%s\n"
                        "binlog_root_path=%s\n"
                        "binlog_prefix=%s\n"
                        "binlog_max_size=%d\n"
                        "upd_cp_timeout=%ds\n"
                        "op_max_num=%"PRIu64"\n"
                        "pid_file=%s\n"
                        "log_dir=%s\n"
                        "log_prefix=%s\n"
                        "log_max_len=%d\n"
                        "log_level=%s\n"
                        "idle_client_timeout=%d\n"
                        "slowlog_slower_than=%lluus\n"
                        "slowlog_max_len=%llu\n"
                        "vm_enable=%s\n"
                        "vm_swap_file=%s\n"
                        "vm_max_memory=%llu\n"
                        "vm_page_size=%zu\n"
                        "vm_pages=%zu\n"
                        "vm_max_threads=%d\n"
                        "querybuf_reuse=%d\n"
                        "max_clients=%d\n"
                        "max_memory=%llu\n"
                        "max_memory_policy=%s\n"
                        "max_memory_samples=%d\n"
                        "protocol=%s\n"
                        , server.host
                        , server.port
                        , DS_VERSION
                        , __DATE__ , __TIME__
                        , (sizeof(long) == 8) ? "64" : "32"
                        , aeGetApiName()
                        , mngr_list ? mngr_list : ""
                        , sync_list
                        , server.period
                        , server.daemonize ? "yes" : "no"
                        , server.app_id
                        , server.ds_key
                        , server.ds_key_num
                        , server.unixsocket ? server.unixsocket : ""
                        , server.dbnum
                        , server.has_cache
                        , server.has_dbe
                        , server.read_only
                        , server.is_slave
                        , server.wr_bl
                        , server.load_bl
                        , server.load_bl_cnt
                        , server.dbe_hot_level
                        , server.read_dbe
                        , server.auto_purge
                        , server.db_max_size
                        , server.db_min_size
                        , server.app_path ? server.app_path : ""
                        , server.dbe_root_path ? server.dbe_root_path : ""
                        , server.bl_root_path ? server.bl_root_path : ""
                        , server.binlog_prefix
                        , server.binlog_max_size
                        , server.upd_cp_timeout
                        , server.op_max_num
                        , server.pidfile ? server.pidfile : ""
                        , server.log_dir
                        , server.log_prefix
                        , server.log_file_len
                        , get_loglevel_str(server.verbosity)
                        , server.maxidletime
                        , (unsigned long long)server.slowlog_log_slower_than
                        , (unsigned long long)server.slowlog_max_len
                        , server.vm_enabled ? "yes" : "no"
                        , server.vm_swap_file ? server.vm_swap_file : ""
                        , (unsigned long long)server.vm_max_memory
                        , (size_t)server.vm_page_size
                        , (size_t)server.vm_pages
                        , server.vm_max_threads
                        , server.querybuf_reuse
                        , server.maxclients
                        , (unsigned long long)server.maxmemory
                        , get_max_memory_policy_str(server.maxmemory_policy)
                        , server.maxmemory_samples
                        , server.prtcl_redis ? "redis" : "mc"
                       );

    zfree(mngr_list);
    sdsfree(sync_list);
    return info;
}

static void output_run_info(int argc, char **argv)
{
    int i;
    sds info;

    info = sdscatprintf(sdsempty(), "%s", argv[0]);
    for (i = 1; i < argc; i++)
    {
        info = sdscatprintf(info, " %s", argv[i]);
    }

    sds conf = get_conf_info_str();

    redisLog(REDIS_PROMPT, "%s\n%s", info, conf);

    sdsfree(info);
    sdsfree(conf);
}

static int check_path_args()
{
    /* app_path */
    if (server.app_path)
    {
        const int len = strlen(server.app_path);
        if (server.app_path[len - 1] != '/')
        {
            sds tmp = sdscatprintf(sdsempty(), "%s/", server.app_path);
            zfree(server.app_path);
            server.app_path = zstrdup(tmp);
            sdsfree(tmp);
        }
    }

    /* pidfile */
    if (server.daemonize && server.pidfile == 0)
    {
        if (server.app_path == 0)
        {
            redisLog(REDIS_WARNING, "app_path is null, can't construct pidfile");
            return 1;
        }
        sds tmp = sdscatprintf(sdsempty(), "%s%s/%d", server.app_path, server.app_id, server.port);
        tmp = sdscat(tmp, "/ds.pid");
        server.pidfile = zstrdup(tmp);
        sdsfree(tmp);
    }
    if (server.daemonize)
    {
        char *ptr = strrchr(server.pidfile, '/');
        if (ptr && ptr != server.pidfile)
        {
            *ptr = 0;
            if (check_and_make_dir(server.pidfile) != 0)
            {
                redisLog(REDIS_WARNING, "check pidfile fail:%s", server.pidfile);
                return -1;
            }
            *ptr = '/';
        }
    }

    /* log */
    if (server.log_dir == 0)
    {
        if (server.app_path == 0)
        {
            redisLog(REDIS_WARNING, "app_path is null, can't construct log path");
            return 2;
        }
        sds tmp = sdscatprintf(sdsempty(), "%s%s/%d/log/", server.app_path, server.app_id, server.port);
        server.log_dir = zstrdup(tmp);
        sdsfree(tmp);
    }
    else
    {
        const int len = strlen(server.log_dir);
        if (server.log_dir[len - 1] != '/')
        {
            sds tmp = sdscatprintf(sdsempty(), "%s/", server.log_dir);
            zfree(server.log_dir);
            server.log_dir = zstrdup(tmp);
            sdsfree(tmp);
        }
    }
    if (check_and_make_dir(server.log_dir) != 0)
    {
        redisLog(REDIS_WARNING, "check logdir fail:%s", server.log_dir);
        return -1;
    }

    /* binlog */
    if (server.bl_root_path == 0)
    {
        if (server.app_path == 0)
        {
            redisLog(REDIS_WARNING, "app_path is null, can't construct bl path");
            return 3;
        }
        sds tmp = sdscatprintf(sdsempty(), "%s%s/%d/bl/", server.app_path, server.app_id, server.port);
        server.bl_root_path = zstrdup(tmp);
        sdsfree(tmp);
    }
    else
    {
        const int len = strlen(server.bl_root_path);
        if (server.bl_root_path[len - 1] != '/')
        {
            sds tmp = sdscatprintf(sdsempty(), "%s/", server.bl_root_path);
            zfree(server.bl_root_path);
            server.bl_root_path = zstrdup(tmp);
            sdsfree(tmp);
        }
    }
    if (check_and_make_dir(server.bl_root_path) != 0)
    {
        redisLog(REDIS_WARNING, "check bl_root_path fail:%s", server.bl_root_path);
        return -1;
    }

    /* dbe_path */
    const int needdir = (server.has_dbe == 1) ? 1 : 0;
    if (needdir && server.dbe_root_path == 0)
    {
        if (server.app_path == 0)
        {
            redisLog(REDIS_WARNING, "app_path is null, can't construct dbe path");
            return 4;
        }
        sds tmp = sdscatprintf(sdsempty(), "%s%s/%d/dbe/", server.app_path, server.app_id, server.port);
        server.dbe_root_path = zstrdup(tmp);
        sdsfree(tmp);
    }
    else if (server.dbe_root_path)
    {
        const int len = strlen(server.dbe_root_path);
        if (server.dbe_root_path[len - 1] != '/')
        {
            sds tmp = sdscatprintf(sdsempty(), "%s/", server.dbe_root_path);
            zfree(server.dbe_root_path);
            server.dbe_root_path = zstrdup(tmp);
            sdsfree(tmp);
        }
    }
    if (needdir && check_and_make_dir(server.dbe_root_path) != 0)
    {
        redisLog(REDIS_WARNING, "check dbe_root_path fail:%s", server.dbe_root_path);
        return -1;
    }

#if 0
    /* vm-swap-file */
    if (server.vm_enabled && server.vm_swap_file == 0)
    {
        if (server.app_path == 0)
        {
            return 1;
        }
        sds tmp = sdscatprintf(sdsempty(), "%s%s/%d", server.app_path, server.app_id, server.port);
        tmp = sdscat(tmp, "/vm.swap");
        server.vm_swap_file = zstrdup(tmp);
        sdsfree(tmp);
    }
    if (server.vm_enabled)
    {
        char *ptr = strrchr(server.vm_swap_file, '/');
        if (ptr && ptr != server.vm_swap_file)
        {
            *ptr = 0;
            if (check_and_make_dir(server.vm_swap_file) != 0)
            {
                return -1;
            }
            *ptr = '/';
        }
    }
#endif
    return 0;
}

int main(int argc, char **argv)
{
    char link[64];
    sprintf(link, "/proc/%d/exe", getpid());
    const int rslt = readlink(link, server.path, sizeof(server.path));
    if (rslt < 0 || rslt > (int)sizeof(server.path))
    {
        printf("error, rslt=%d\n", rslt);
        return -1;
    }
    server.path[rslt] = 0;
    char *ptr = strrchr(server.path, '/');
    if (ptr)
    {
        *ptr = 0;
    }
    //printf("running path: %s\n", server.path);

    if (argc == 2)
    {
        if (strcmp(argv[1], "-v") == 0 ||
            strcmp(argv[1], "--version") == 0)
        {
            version();
        }
        if (strcmp(argv[1], "-?") == 0 ||
            strcmp(argv[1], "-h") == 0 ||
            strcmp(argv[1], "--help") == 0)
        {
            usage(argv[0]);
        }
    }

    int argval;
    char *bl_file = 0;
    char *conf = 0;
    const char *mngr_list = 0;
    const char *port = 0;
    const char *host = 0;
    int raw = 0;
    int log_debug = 0;
    long ds_id = -1;
    int to_file = 0;
    char *cmd_arg = 0;
    char *key_arg = 0;
    char *dbe_arg = 0;
    char daemon_flag = 0;
    char persistent_flag = 0;
    char slave_flag = 0;
    char wrbl_flag = 0;
    char read_dbe_flag = 0;
    char redis_flag = 0;
    int load_level = -1;
    char *app_id = NULL;
    char *ds_ip_list = NULL;
    char *ds_key = NULL;
    char *app_path = NULL;
    char *dbe_path = NULL;
    char *bl_path = NULL;
    int max_size = 0;
    int hidb_flag = 0;

    int test_max_key_num = 1000000;
    int test_sample_num = 500000;
    int test_type = 0;

#if 0
    struct hidba_s dba = HIDBA_INITIALIZER;
#endif

    while ((argval = getopt(argc, argv, "dlrPRVWXYi:p:s:f:n:b:B:c:k:e:D:O:S:H:E:M:m:QA:L:K:N:U:x:y:z:")) != EOF)
    {
        switch (argval)
        {
            case 'b':
                bl_file = optarg;
                to_file = 1;
                break;

            case 'B':
                bl_file = optarg;
                to_file = 0;
                break;

            case 'f':
                conf = optarg;
                break;
            case 's':
                mngr_list = optarg;
                break;
            case 'p':
                port = optarg;
                break;
            case 'i':
                host = optarg;
                break;
            case 'l':
                log_debug = 1;
                break;
            case 'r':
                raw = 1;
                break;
            case 'A':
                app_id = zstrdup(optarg);
                break;
            case 'K':
                ds_key = zstrdup(optarg);
                break;
            case 'L':
                ds_ip_list = zstrdup(optarg);
                break;
            case 'N':
                load_level = atol(optarg);
                break;
            case 'P':
                persistent_flag = 1;
                break;
            case 'R':
                redis_flag = 1;
                break;
            case 'U':
                max_size = atol(optarg);
                break;
            case 'V':
                hidb_flag = 1;
                break;
            case 'W':
                wrbl_flag = 1;
                break;
            case 'X':
                slave_flag = 1;
                break;
            case 'Y':
                read_dbe_flag = 1;
                break;
            case 'd':
                daemon_flag = 1;
                break;
            case 'x':
                app_path = zstrdup(optarg);
                break;
            case 'y':
                dbe_path = zstrdup(optarg);
                break;
            case 'z':
                bl_path = zstrdup(optarg);
                break;

            case 'n':
                ds_id = atol(optarg);
                break;

            case 'c':
                cmd_arg = zstrdup(optarg);
                break;
            case 'k':
                key_arg = zstrdup(optarg);
                break;
            case 'e':
                dbe_arg = zstrdup(optarg);
                break;
#if 0
            /* for dbe */
            case 'D':
                dba.dbpath = strdup(optarg);
                break;
            case 'O':
                dba.dstpath = strdup(optarg);
                break;
            case 'S':
                dba.hs_ids = strdup(optarg);
                break;
            case 'H':
                dba.hs_idx = atoi(optarg);
                break;
            case 'E':
                dba.exp_lvl = atoi(optarg);
                break;
            case 'M':
                dba.exp_time = strdup(optarg);
                break;
            case 'm':
                dba.tc_idx = atoi(optarg);
                break;
            case 'Q':
                dba.do_quick = 1;
                break;
#else
            case 'S':
                test_sample_num = atoi(optarg);
                break;
            case 'M':
                test_max_key_num = atoi(optarg);
                break;
            case 'Q':
                test_type = 1;
                break;
#endif
            default:
                usage(argv[0]);
        }
    }

    /* -b/-B parameter */
    if (bl_file)
    {
        pf_log_config_t t;
        pf_log_init_config(&t);
        t.log_level = REDIS_WARNING;
        t.file_len = (sizeof(long) == 8) ? 0 : 2000;
        pf_log_init(&t);

        const int cnt = decode_bl(bl_file, to_file);
        redisLog(REDIS_PROMPT, "decode binlog records: %d", cnt);
        return 0;
    }

    /* -c paramter */
    if (cmd_arg)
    {
        pf_log_config_t t;
        pf_log_init_config(&t);
        t.log_level = REDIS_WARNING;
        t.file_len = (sizeof(long) == 8) ? 0 : 2000;
        pf_log_init(&t);

        if (strcmp(cmd_arg, "keydump") == 0)
        {
            if (dbe_arg)
            {
                do_keydump(dbe_arg, 0);
            }
            else
            {
                goto cmd_fail;
            }
        }
        else if (strcmp(cmd_arg, "keyinfodump") == 0)
        {
            if (dbe_arg)
            {
                do_keydump(dbe_arg, 1);
            }
            else
            {
                goto cmd_fail;
            }
        }
        else if (strcmp(cmd_arg, "get") == 0)
        {
            if (dbe_arg && key_arg)
            {
                do_getkey(dbe_arg, key_arg);
            }
            else
            {
                goto cmd_fail;
            }
        }
        else if (strcmp(cmd_arg, "test") == 0)
        {
            if (dbe_arg)
            {
                if (key_arg == NULL)
                {
                    key_arg = "abc";
                }
                do_testkey(dbe_arg, key_arg, test_max_key_num, test_sample_num, test_type);
            }
            else
            {
                goto cmd_fail;
            }
        }
        else
        {
cmd_fail:
            zfree(cmd_arg);
            usage(argv[0]);
        }
        return 0;
    }

#if 0
    /* -D paramter */
    if(dba.dbpath) {
        dba.vfilter = value_filter;
        return hidba_export(&dba);
    } 
#endif
    mallopt(M_MMAP_THRESHOLD, 64 * 1024);

    initOpCommandTable();
    initServerConfig();
    zmalloc_enable_thread_safeness();

    int local = 1;
    if (mngr_list)
    {
        //printf("mngr_list = %s\n", mngr_list);
        //printf("port = %s\n", port);
        server.mngr_list = zstrdup(mngr_list);
        set_mngr_list(mngr_list);
        local = 0;
    }
    else if (conf)
    {
        //printf("config_file = %s\n", conf);
        loadServerConfig(conf);
    }

    if (host)
    {
        server.host = zstrdup(host);
#if 1
        server.bindaddr = zstrdup(host);
#endif
    }
    if (port)
    {
        server.port = atoi(port);
    }

    if (local == 1)
    {
        if (raw)
        {
            server.rdbcompression = 0;
        }
        if (ds_id >= 0)
        {
            server.ds_key_num = ds_id;
        }
        if (daemon_flag == 1)
        {
            server.daemonize = 1;
        }
        if (persistent_flag == 1)
        {
            server.has_dbe = 1;
        }
        if (wrbl_flag == 1)
        {
            server.wr_bl = 1;
        }
        if (slave_flag == 1)
        {
            server.is_slave = 1;
        }
        if (redis_flag == 1)
        {
            server.prtcl_redis = 1;
            server.enc_kv = 1;
        }
        if (server.has_dbe)
        {
            if (read_dbe_flag == 1)
            {
                server.read_dbe = 1;
            }
            if (load_level >= 0)
            {
                server.dbe_hot_level = load_level;
            }
        }
        if (app_id != NULL)
        {
            strncpy(server.app_id, app_id, sizeof(server.app_id));
            zfree(app_id);
        }
        if (ds_key != NULL)
        {
            if (server.ds_key)
            {
                zfree(server.ds_key);
                server.ds_key = 0;
            }
            server.ds_key = ds_key;
        }
        if (ds_ip_list != NULL)
        {
            set_ds_list(ds_ip_list);
            zfree(ds_ip_list);
        }
        if (log_debug)
        {
            server.verbosity = REDIS_DEBUG;
        }
        if (app_path)
        {
            if (server.app_path)
            {
                zfree(server.app_path);
                server.app_path = NULL;
            }
            server.app_path = app_path;
        }
        if (dbe_path)
        {
            if (server.dbe_root_path)
            {
                zfree(server.dbe_root_path);
                server.dbe_root_path = NULL;
            }
            server.dbe_root_path = dbe_path;
        }
        if (bl_path)
        {
            if (server.bl_root_path)
            {
                zfree(server.bl_root_path);
                server.bl_root_path = NULL;
            }
            server.bl_root_path = bl_path;
        }
        if (max_size > 11)
        {
            server.db_max_size = max_size * 1024 * 1024;
        }
        if (hidb_flag == 1 && redis_flag == 0)
        {
            server.dbe_ver = DBE_VER_HIDB;
        }
    }

    if (0 != get_my_addr())
    {
        redisLog(REDIS_WARNING, "Warning: can't get ip_address, can't startup");
        return 1;
    }

    init_ctrl_info();
    init_filter();

    updateCachedTime();
    server.stat_starttime = server.unixtime;
    struct tm start_tm;
    struct tm *p_tm = &start_tm;
    localtime_r(&server.stat_starttime, p_tm);
    sprintf(server.starttime_str, "%04d-%02d-%02d %02d:%02d:%02d", p_tm->tm_year + 1900, p_tm->tm_mon + 1, p_tm->tm_mday, p_tm->tm_hour, p_tm->tm_min, p_tm->tm_sec);

    //while (local == 0 && init_with_mngr(load_conf_local) && load_conf_local == 0)
    if (local == 0 && init_with_mngr(0))
    {
        redisLog(REDIS_WARNING, "Warning: can't interact with mngr-server");
        //sleep(2);
        return 1;
    }

    if (check_path_args() != 0)
    {
        redisLog(REDIS_WARNING, "Warning: illegal parameters, can't startup");
        return 1;
    }

    redisLog(REDIS_PROMPT, "host:%s, log_dir: %s", server.host, server.log_dir);
    if (server.daemonize) daemonize();
    init_log();

    /* ready to startup data-server */

    if (0 != initDbmngConfig())
    {
        return 1;
    }

    initServer();

    redisLog(REDIS_PROMPT, "----start data-server----");
    output_run_info(argc, argv);

    if (0 != dbmng_init(0, server.slave_port > 0 ? 1 : 0))
    {
        redisLog(REDIS_WARNING, "Warning: dbmng_init() fail for tag=%d", 0);
        return 1;
    }
    server.init_ready = 1;

    save_stat_info(1);
    if (local == 0 && start_heart_beat(&tid) != 0)
    {
        redisLog(REDIS_WARNING, "Warning: heart-beat with mngr-server fail");
    }

    /* start some timer */
    if (server.has_cache == 1 && server.has_dbe == 1)
    {
        aeCreateTimeEvent(server.el, server.upd_cp_timeout * 1000, upd_cp_cron, 0, 0);
    }
    if (server.has_dbe == 1)
    {
        aeCreateTimeEvent(server.el, server.print_dbeinfo_timeout * 1000, print_dbeinfo_cron, 0, 0);
    }
    aeCreateTimeEvent(server.el, 600000, print_info_cron, 0, 0);

#ifdef __linux__
    linuxOvercommitMemoryWarning();
#endif
    /*
     * disable in FooYun
    time_t start;
    start = time(NULL);
    if (server.appendonly) {
        if (loadAppendOnlyFile(server.appendfilename) == REDIS_OK)
            redisLog(REDIS_NOTICE,"DB loaded from append only file: %ld seconds",time(NULL)-start);
    } else {
        if (rdbLoad(server.dbfilename) == REDIS_OK) {
            redisLog(REDIS_NOTICE,"DB loaded from disk: %ld seconds",
                time(NULL)-start);
        } else if (errno != ENOENT) {
            redisLog(REDIS_WARNING,"Fatal error loading the DB. Exiting.");
            exit(1);
        }
    }
    */

    if (server.daemonize) createPidFile();
    redisLog(REDIS_PROMPT
             , "DataServer started, version: %s, pid: %d, hb-thread: %lu"
             , DS_VERSION, (int)getpid(), tid);

    if (server.ipfd > 0)
        redisLog(REDIS_PROMPT,"ready on %s:%d"
                , server.bindaddr ? server.bindaddr : "0.0.0.0", server.port);
    if (server.slave_ipfd > 0)
        redisLog(REDIS_PROMPT,"ready on 127.0.0.1:%d", server.slave_port);
    if (server.sofd > 0)
        redisLog(REDIS_PROMPT,"ready at unixsocket: %s", server.unixsocket);

    aeSetBeforeSleepProc(server.el,beforeSleep);
    aeMain(server.el);
    aeDeleteEventLoop(server.el);
    return 0;
}

#ifdef HAVE_BACKTRACE
static void *getMcontextEip(ucontext_t *uc) {
#if defined(__FreeBSD__)
    return (void*) uc->uc_mcontext.mc_eip;
#elif defined(__dietlibc__)
    return (void*) uc->uc_mcontext.eip;
#elif defined(__APPLE__) && !defined(MAC_OS_X_VERSION_10_6)
  #if __x86_64__
    return (void*) uc->uc_mcontext->__ss.__rip;
  #elif __i386__
    return (void*) uc->uc_mcontext->__ss.__eip;
  #else
    return (void*) uc->uc_mcontext->__ss.__srr0;
  #endif
#elif defined(__APPLE__) && defined(MAC_OS_X_VERSION_10_6)
  #if defined(_STRUCT_X86_THREAD_STATE64) && !defined(__i386__)
    return (void*) uc->uc_mcontext->__ss.__rip;
  #else
    return (void*) uc->uc_mcontext->__ss.__eip;
  #endif
#elif defined(__i386__)
    return (void*) uc->uc_mcontext.gregs[14]; /* Linux 32 */
#elif defined(__X86_64__) || defined(__x86_64__)
    return (void*) uc->uc_mcontext.gregs[16]; /* Linux 64 */
#elif defined(__ia64__) /* Linux IA64 */
    return (void*) uc->uc_mcontext.sc_ip;
#else
    return NULL;
#endif
}

void bugReportStart(void) {
    if (server.bug_report_start == 0) {
        redisLog(REDIS_WARNING,
            "=== REDIS BUG REPORT START: Cut & paste starting from here ===");
        server.bug_report_start = 1;
    }
}

static void sigsegvHandler(int sig, siginfo_t *info, void *secret) {
    void *trace[100];
    char **messages = NULL;
    int i, trace_size = 0;
    ucontext_t *uc = (ucontext_t*) secret;
    sds infostring, clients;
    struct sigaction act;
    REDIS_NOTUSED(info);

    bugReportStart();
    redisLog(REDIS_WARNING,
        "    data-server %s crashed by signal: %d", DS_VERSION, sig);
    redisLog(REDIS_WARNING,
        "    Failed assertion: %s (%s:%d)", server.assert_failed,
                        server.assert_file, server.assert_line);

    /* Generate the stack trace */
    trace_size = backtrace(trace, 100);

    /* overwrite sigaction with caller's address */
    if (getMcontextEip(uc) != NULL) {
        trace[1] = getMcontextEip(uc);
    }
    messages = backtrace_symbols(trace, trace_size);
    redisLog(REDIS_WARNING, "--- STACK TRACE");
    for (i=1; i<trace_size; ++i)
        redisLog(REDIS_WARNING,"%s", messages[i]);

    /* Log INFO and CLIENT LIST */
    redisLog(REDIS_WARNING, "--- INFO OUTPUT");
    infostring = genRedisInfoString();
    redisLog(REDIS_WARNING, infostring);
    sdsfree(infostring);
    redisLog(REDIS_WARNING, "--- CLIENT LIST OUTPUT");
    clients = getAllClientsInfoString();
    redisLog(REDIS_WARNING, clients);
    sdsfree(clients);
    /* Don't sdsfree() strings to avoid a crash. Memory may be corrupted. */

    redisLog(REDIS_WARNING,
"=== REDIS BUG REPORT END. Make sure to include from START to END. ===\n\n"
"    Please report the crash opening an issue on github:\n\n"
"        http://github.com/antirez/redis/issues\n\n"
);
    /* free(messages); Don't call free() with possibly corrupted memory. */
    if (server.daemonize) unlink(server.pidfile);

    /* Make sure we exit with the right signal at the end. So for instance
     * the core will be dumped if enabled. */
    sigemptyset (&act.sa_mask);
    /* When the SA_SIGINFO flag is set in sa_flags then sa_sigaction
     * is used. Otherwise, sa_handler is used */
    act.sa_flags = SA_NODEFER | SA_ONSTACK | SA_RESETHAND;
    act.sa_handler = SIG_DFL;
    sigaction (sig, &act, NULL);
    kill(getpid(),sig);
}
#endif /* HAVE_BACKTRACE */

static void sigtermHandler(int sig) {
    REDIS_NOTUSED(sig);

    redisLog(REDIS_WARNING,"Received SIGTERM, scheduling shutdown...");
    server.shutdown_asap = 1;
}

void setupSignalHandlers(void) {
    struct sigaction act;

    /* When the SA_SIGINFO flag is set in sa_flags then sa_sigaction is used.
     * Otherwise, sa_handler is used. */
    sigemptyset(&act.sa_mask);
    act.sa_flags = SA_NODEFER | SA_ONSTACK | SA_RESETHAND;
    act.sa_handler = sigtermHandler;
    sigaction(SIGTERM, &act, NULL);

#ifdef HAVE_BACKTRACE
    sigemptyset(&act.sa_mask);
    act.sa_flags = SA_NODEFER | SA_ONSTACK | SA_RESETHAND | SA_SIGINFO;
    act.sa_sigaction = sigsegvHandler;
    sigaction(SIGSEGV, &act, NULL);
    sigaction(SIGBUS, &act, NULL);
    sigaction(SIGFPE, &act, NULL);
    sigaction(SIGILL, &act, NULL);
#endif
    return;
}

static int upd_cp_cron(struct aeEventLoop *eventLoop, long long id, void *clientData)
{
    REDIS_NOTUSED(eventLoop);
    REDIS_NOTUSED(id);
    REDIS_NOTUSED(clientData);

    dbmng_start_cp(0);

    return server.upd_cp_timeout * 1000;
}

static int print_dbeinfo_cron(struct aeEventLoop *eventLoop, long long id, void *clientData)
{
    REDIS_NOTUSED(eventLoop);
    REDIS_NOTUSED(id);
    REDIS_NOTUSED(clientData);

    dbmng_print_dbeinfo(0);

    return server.print_dbeinfo_timeout * 1000;
}

static int print_info_cron(struct aeEventLoop *eventLoop, long long id, void *clientData)
{
    REDIS_NOTUSED(eventLoop);
    REDIS_NOTUSED(id);
    REDIS_NOTUSED(clientData);

    int out = 0;
    unsigned int i;

    for (i = 1; i < sizeof(gWrBlTop) / sizeof(gWrBlTop[0]); i++)
    {
        if (gWrBlTop[i].valid == 0)
        {
            break;
        }
        if (out == 0)
        {
            redisLog(REDIS_PROMPT
                    , "WrBl(us): cnt[%d] max[%lld] min[%lld] avg[%lld] max_wait[%d]"
                      " (io, s, len, time, cmd, key)"
                    , gWrBlCnt, gWrBlMax, gWrBlMin
                    , gWrBlCnt ? (gWrBlTotal / gWrBlCnt) : 0, gWaitBlClntMax
                    );
            out = 1;
            gWrBlTotal = gWrBlCnt = gWrBlMax = gWrBlMin = 0;
            gWaitBlClntMax = 0;
        }
        redisLog(REDIS_PROMPT, "No.%d: %"PRId64", %"PRId64", %d, %ld, %d, %s"
                , i, gWrBlTop[i].io_dur, gWrBlTop[i].serialize_dur
                , gWrBlTop[i].buf_len, gWrBlTop[i].time
                , gWrBlTop[i].cmd, gWrBlTop[i].key
                );
        gWrBlTop[i].valid = 0;
    }

    out = 0;
    for (i = 1; i < sizeof(gDbeGetTop) / sizeof(gDbeGetTop[0]); i++)
    {
        if (gDbeGetTop[i].valid == 0)
        {
            break;
        }
        if (out == 0)
        {
            redisLog(REDIS_PROMPT
                    , "dbe_get(us): cnt[%d] max[%lld] min[%lld] avg[%lld]"
                    , gDbeGetCnt, gDbeGetMax, gDbeGetMin, gDbeGetCnt ? (gDbeGetTotal / gDbeGetCnt) : 0
                    );
            out = 1;
            gDbeGetTotal = gDbeGetCnt = gDbeGetMax = gDbeGetMin = 0;
        }
        redisLog(REDIS_PROMPT, "No.%d: %"PRId64", %"PRId64", %d, %ld"
                , i, gDbeGetTop[i].io_dur, gDbeGetTop[i].unserialize_dur
                , gDbeGetTop[i].buf_len, gDbeGetTop[i].time
                );
        gDbeGetTop[i].valid = 0;
    }

    out = 0;
    for (i = 1; i < sizeof(gDbeSetTop) / sizeof(gDbeSetTop[0]); i++)
    {
        if (gDbeSetTop[i].valid == 0)
        {
            break;
        }
        if (out == 0)
        {
            redisLog(REDIS_PROMPT
                    , "dbe_set(us): cnt[%d] max[%lld] min[%lld] avg[%lld]"
                    , gDbeSetCnt, gDbeSetMax, gDbeSetMin, gDbeSetCnt ? (gDbeSetTotal / gDbeSetCnt) : 0
                    );
            out = 1;
            gDbeSetTotal = gDbeSetCnt = gDbeSetMax = gDbeSetMin = 0;
        }
        redisLog(REDIS_PROMPT, "No.%d: %"PRId64", %d, %ld"
                , i, gDbeSetTop[i].io_dur
                , gDbeSetTop[i].buf_len, gDbeSetTop[i].time
                );
        gDbeSetTop[i].valid = 0;
    }

    return 600000;
}

/* 
 * cmd format: sync_status master_ip master_port status [bl_tag]
 */
void syncstatusCommand(redisClient *c)
{
    redisLog(REDIS_DEBUG, "sync_status %s %s %s %s",
             c->argv[1]->ptr, c->argv[2]->ptr, c->argv[3]->ptr,
             c->argc == 5 ? c->argv[4]->ptr : "");

    const int master_port = atoi(c->argv[2]->ptr);
    const int status = atoi(c->argv[3]->ptr);

    if (lock_sync_status() != 0)
    {
        addReplyLongLong(c, 2);
        return;
    }

    int result;
    sync_status_node *node = find_sync_status(c->argv[1]->ptr, master_port);
    if (node)
    {
        /* status value according to sync_if.h */
        if (status == 0)
        {
            node->status = SYNC_STATUS_SYNCED;
            if (c->argc == 5)
            {
                node->bl_tag = strtoll(c->argv[4]->ptr, 0, 10);
            }
        }
        else if (status == 1)
        {
            node->status = SYNC_STATUS_DOING;
            if (c->argc == 5)
            {
                node->bl_tag = strtoll(c->argv[4]->ptr, 0, 10);
            }
        }
        else if (status == 2)
        {
            node->status = SYNC_STATUS_FAILED;
        }
        else if (status == 3)
        {
            node->status = SYNC_STATUS_STOPED;
        }

        if (node->stop_flag == 1)
        {
            node->stop_flag = 0;
            result = 1;
        }
        else
        {
            result = 0;
        }
    }
    else
    {
        redisLog(REDIS_WARNING, "can't find %s:%d in sync_status",
                 c->argv[1]->ptr, master_port);
        result = 2;
    }

    unlock_sync_status();
    addReplyLongLong(c, result);
}

/* 
 * cmd format: binlog ds_id binlog_content
 */
void binlogCommand(redisClient *c)
{
    const int bl_len = sdslen(c->argv[2]->ptr);
    unsigned long long ds_id = 0;
    getuLongLongFromObject(c->argv[1], &ds_id);
    redisLog(REDIS_VERBOSE, "binlogCommand()...bl_len=%d, ds_id=%llu", bl_len, ds_id);

    op_rec rec;
    int ret = 0;

    ret = parse_op_rec(c->argv[2]->ptr, bl_len, &rec);
    if (ret != 0)
    {
        redisLog(REDIS_WARNING, "parse_op_rec() fail, bl_len=%d", bl_len);
        addReplyError(c, "parse_op_rec() fail");
        return;
    }

    if (rec.cmd == 0)
    {
        /* ignore */
        addReply(c, shared.ok);
        return;
    }
    const char *cmd_str = get_cmdstr(rec.cmd);
    if (cmd_str == 0)
    {
        /* unknown cmd */
        redisLog(REDIS_WARNING, "unknown cmd=%d in binlog", rec.cmd);
        addReplyError(c, "unknown cmd");
        if (rec.argc > 0)
        {
            zfree(rec.argv);
        }
        return;
    }
    if (rec.db_id >= server.dbnum)
    {
        redisLog(REDIS_WARNING, "db_id(%d) >= server.dbnum(%d)", rec.db_id, server.dbnum);
        addReplyError(c, "db_id too large");
        if (rec.argc > 0)
        {
            zfree(rec.argv);
        }
        return;
    }
#if 0
    if (server.is_slave == 0 && master_filter_keylen(rec.key.ptr, rec.key.len) != 0)
    {
        /* the key do not belong to this ds */
        redisLog(REDIS_VERBOSE, "the key \"%.*s\" do not belong to this DS, ignore it",
                 rec.key.len, rec.key.ptr);
        addReplyError(c, "ignore the key");
        return;
    }
#endif

    if (c->db->id != rec.db_id)
    {
        selectDb(c, rec.db_id);
    }

    robj *o = 0;
    long expire = -1;
    int exp_idx = -1;
    const time_t now = time(0);
    if (strcmp(cmd_str, OP_SETEX) == 0)
    {
        exp_idx = 1;
    }
    else if (strcmp(cmd_str, OP_CAS) == 0)
    {
        exp_idx = 2;
    }

    if (exp_idx != -1)
    {
        o = unserializeObj((char *)rec.argv[exp_idx].ptr, rec.argv[exp_idx].len, 0);
        robj *dec_o = getDecodedObject_unsigned(o);
        decrRefCount(o);
        char buf[64];
        snprintf(buf, sizeof(buf), "%.*s", (int)sdslen(dec_o->ptr), (char *)dec_o->ptr);
        decrRefCount(dec_o);
        expire = atol(buf);
        if (expire > 0)
        {
            if (expire <= now)
            {
                /* ignore */
                redisLog(REDIS_PROMPT
                        , "binlogCommand: key expired in %s, expire=%d, now=%d"
                        , cmd_str, expire, now);
                addReply(c, shared.ok);
                if (rec.argc > 0)
                {
                    zfree(rec.argv);
                }
                return;
            }
            expire = expire - now;
        }
    }
    if (now >= rec.time_stamp)
    {
        const int diff = now - rec.time_stamp;
        redisLog(REDIS_DEBUG, "ts=%d, now=%d, diff=%ds", rec.time_stamp, now, diff);
        if (server.bl_sync_delay_max < diff)
        {
            redisLog(REDIS_PROMPT, "max_delay(s): %d -> %d, total=%lld, cnt=%lld"
                    , server.bl_sync_delay_max, diff, server.bl_sync_delay_total, server.bl_sync_cnt);
            server.bl_sync_delay_max = diff;
        }
        server.bl_sync_delay_total += diff;
    }
    else
    {
        redisLog(REDIS_VERBOSE, "os's timer do not sync: ts=%d, now=%d, cnt=%lld"
                , rec.time_stamp, now, server.bl_sync_cnt);
    }
    server.bl_sync_cnt++;

    c->read_only = 0;
    c->ds_id = ds_id;

    robj *key = createStringObject((char *)rec.key.ptr, rec.key.len);
    redisLog(REDIS_VERBOSE, "op info in binlog: cmd=%d(%s), argc=%d, key=%s"
            , rec.cmd, cmd_str, rec.argc, key->ptr);
    robj **old_argv = c->argv;
    const uint32_t old_argc = c->argc;
    c->argc = rec.argc + 2;
    c->argv = zmalloc(sizeof(robj*) * c->argc);

    uint32_t j;
    c->argc = 0;
    if (strcmp(cmd_str, OP_SETEX) == 0)
    {
        c->argv[c->argc++] = createStringObject((char *)cmd_str, strlen(cmd_str));
        c->argv[c->argc++] = key;

        /* format: cmd key expire value */
        char buf[64];
        snprintf(buf, sizeof(buf), "%ld", expire);
        c->argv[c->argc++] = createStringObject(buf, strlen(buf));
        o = unserializeObj((char *)rec.argv[0].ptr, rec.argv[0].len, 0);
        c->argv[c->argc++] = getDecodedObject(o);
        decrRefCount(o);
    }
    else if (strcmp(cmd_str, OP_CAS) == 0)
    {
        /* cas -> setx */
        const char *setx_cmd = "setx";
        c->argv[c->argc++] = createStringObject((char *)setx_cmd, strlen(setx_cmd));
        c->argv[c->argc++] = key;

        /* format: cmd key value flags expire */
        for (j = 0; j < rec.argc - 1; j++)  /* ignore cas_unique */
        {
            if (j == 2)
            {
                /* expire parameter */
                char buf[64];
                snprintf(buf, sizeof(buf), "%ld", expire);
                c->argv[c->argc++] = createStringObject(buf, strlen(buf));
            }
            else
            {
                o = unserializeObj((char *)rec.argv[j].ptr, rec.argv[j].len, 0);
                c->argv[c->argc++] = getDecodedObject(o);
                decrRefCount(o);
            }
        }
    }
    else
    {
        c->argv[c->argc++] = createStringObject((char *)cmd_str, strlen(cmd_str));
        c->argv[c->argc++] = key;

        for (j = 0; j < rec.argc; j++)
        {
            o = unserializeObj((char *)rec.argv[j].ptr, rec.argv[j].len, 0);
            c->argv[c->argc++] = getDecodedObject(o);
            decrRefCount(o);
        }
    }

    if (rec.argc > 0)
    {
        zfree(rec.argv);
    }

    /* free old_argv */
    for (j = 0; j < old_argc; j++)
    {
        decrRefCount(old_argv[j]);
    }
    zfree(old_argv);

    c->cmd = c->lastcmd = lookupCommand(c->argv[0]->ptr);
    if (!c->cmd)
    {
        redisLog(REDIS_WARNING, "lookupCommand() fail for %s", (char*)c->argv[0]->ptr);
        addReplyErrorFormat(c, "unknown command '%s'", (char*)c->argv[0]->ptr);
    }
    else
    {
        if (server.has_dbe == 0
            && (rec.cmd == OP_CMD_SET || rec.cmd == OP_CMD_SETEX || rec.cmd == OP_CMD_CAS)
           )
        {
            // check if allowing to handle this binlog (set setex cas) only for cache
            robj *new_val = c->argv[2];
            if (rec.cmd == OP_CMD_SETEX)
            {
                new_val = c->argv[3];
            }
#if 0
            redisLog(REDIS_PROMPT, "%s: bit=%d, ts=%"PRIu64
                    , (char*)c->argv[1]->ptr
                    , new_val->ts_bit
                    , new_val->timestamp);
#endif
            if (new_val->ts_bit)
            {
                robj *old_val = lookupKeyRead(c->db, c->argv[1]);
                if (old_val && old_val->ts_bit && old_val->timestamp > new_val->timestamp)
                {
                    // value in binlog is more early,ignore
                    redisLog(REDIS_WARNING, "sync ignore %s, ts=%"PRIu64", cache.ts=%"PRIu64
                            , (char*)c->argv[1]->ptr
                            , new_val->timestamp
                            , old_val->timestamp);
                    addReplyErrorFormat(c, "ignore '%s'", (char*)c->argv[1]->ptr);
                    return;
                }
            }
#if 0
            else
            {
                // ignore the binlog from earlier version ds
                redisLog(REDIS_WARNING, "ignore %s, ts_bit=%d, earlier version"
                    , (char*)c->argv[1]->ptr
                    , new_val->ts_bit);
                addReplyErrorFormat(c, "ignore '%s'", (char*)c->argv[1]->ptr);
                return;
            }
#endif
        }
        call(c);
    }
}

int start_syncs(sync_addr *masters, int cnt)
{
    if (lock_sync_status() != 0)
    {
        redisLog(REDIS_WARNING, "lock_sync_status() fail, cnt=%d", cnt);
        return -1;
    }

    sync_status_node *node = 0;

    /* stop some master */
    node = gSyncStatusMngr.head;
    while (node)
    {
        if (node->status != SYNC_STATUS_STOPED && node->mode == 0)
        {
            int i;
            for (i = 0; i < cnt; i++)
            {
                if (node->master_port == (uint32_t)masters[i].port
                    && strcmp(node->master_ip, masters[i].ip) == 0)
                {
                    break;
                }
            }
            if (i >= cnt)
            {
                /* not found */
                sync_slave_stop(node->master_ip, node->master_port);
            }
        }
        node = node->next;
    }

    /* start some master */
    int i;
    for (i = 0; i < cnt; i++)
    {
        const char *master_ip = masters[i].ip;
        const int master_port = masters[i].port;
        node = find_sync_status(master_ip, master_port);
        if (node)
        {
            if (node->status == SYNC_STATUS_STOPED)
            {
                node->status = SYNC_STATUS_INIT;
            }
            else
            {
                redisLog(REDIS_DEBUG, "ignore master(%s:%d) sync_status=%d",
                         master_ip, master_port, node->status);
                continue;
            }
        }
        else
        {
            /* add a new node into sync_status_mngr */
            node = (sync_status_node *)zmalloc(sizeof(*node));
            strcpy(node->master_ip, master_ip);
            node->master_port = master_port;
            node->status = SYNC_STATUS_INIT;
            node->conf = 1;
            node->stop_flag = 0;
            node->mode = 0;
            node->next = 0;
            if (gSyncStatusMngr.head)
            {
                node->next = gSyncStatusMngr.head;
            }
            gSyncStatusMngr.head = node;
        }

        char path[256];
        const char *dir = make_bl_path(path, sizeof(path), 1);
        const long long bl_tag = 1;//bl_get_last_ts(dbmng_get_bl(0), 0);
        const char *ds_key = server.is_slave ? "" : server.ds_key;
        redisLog(REDIS_PROMPT
             , "auto sync dir(slave): %s from %s:%d, bl_tag=%lld, ds_key=%s, ds_key_num=%d"
             , dir, master_ip, master_port, bl_tag, ds_key, server.ds_key_num);

        const int ret = sync_slave_start(master_ip, master_port, dir, bl_tag, server.port,
                                         server.ds_key_num, ds_key);
        if (ret != 0)
        {
            redisLog(REDIS_WARNING, "sync_slave_start() fail, ret=%d", ret);
            node->status = SYNC_STATUS_FAILED;
        }
    }

    unlock_sync_status();
    return 0;
}

/*
 * return:
 * mode: 0-auto, 1-manual, 2-manual & filter
 * 0: succ
 * -1: lock fail
 * -2: not allow
 * -3: start fail
 */
int start_sync(const char *master_ip, int master_port, long long bl_tag, int conf, int ignore, int mode)
{
    if (lock_sync_status() != 0)
    {
        redisLog(REDIS_WARNING, "lock_sync_status() fail, %s:%d, bl_tag=%lld",
                 master_ip, master_port, bl_tag);
        return -1;
    }

    sync_status_node *node = find_sync_status(master_ip, master_port);
    if (node)
    {
        if (node->status != SYNC_STATUS_STOPED)
        {
            /* not allow to start a new one */
            if (ignore == 0)
            {
                redisLog(REDIS_WARNING, "%s:%d status=%d, forbidden, bl_tag=%lld",
                         master_ip, master_port, node->status, bl_tag);
            }
            unlock_sync_status();
            return -2;
        }
        else
        {
            node->status = SYNC_STATUS_INIT;
            node->conf = conf;
            node->stop_flag = 0;
            node->mode = (mode <= 2) ? mode : 1;
            mode = node->mode;

            unlock_sync_status();
        }
    }
    else
    {
        /* add a new node into sync_status_mngr */
        node = (sync_status_node *)zmalloc(sizeof(*node));
        strcpy(node->master_ip, master_ip);
        node->master_port = master_port;
        node->status = SYNC_STATUS_INIT;
        node->conf = conf;
        node->stop_flag = 0;
        node->mode = (mode <= 2) ? mode : 1;
        mode = node->mode;
        node->next = 0;
        if (gSyncStatusMngr.head)
        {
            node->next = gSyncStatusMngr.head;
        }
        gSyncStatusMngr.head = node;

        unlock_sync_status();
    }

    char path[256];
    const char *dir = make_bl_path(path, sizeof(path), 1);
    const char *ds_key = (mode == 1 || server.is_slave) ? "" : server.ds_key;
    redisLog(REDIS_PROMPT
             , "sync dir(slave): %s from %s:%d, bl_tag=%lld, ds_key=%s, ds_key_num=%d, mode=%d"
             , dir, master_ip, master_port, bl_tag, ds_key, server.ds_key_num, mode);

    const int ret = sync_slave_start(master_ip, master_port, dir, bl_tag, server.port,
                                     server.ds_key_num, ds_key);
    if (ret == 0)
    {
        return 0;
    }
    else
    {
        redisLog(REDIS_WARNING, "sync_slave_start() fail, ret=%d", ret);
        node->status = SYNC_STATUS_FAILED;
        return -3;
    }
}

/* 
 * cmd format: start_sync master_ip master_port bl_tag mode(optional)
 * bl_tag: 0-begin, 1-current position, other-specified position(us)
 */
void startsyncCommand(redisClient *c)
{
    redisLog(REDIS_PROMPT, "start_sync %s %s %s %s",
             c->argv[1]->ptr, c->argv[2]->ptr, c->argv[3]->ptr,
             c->argc > 4 ? c->argv[4]->ptr : "");

    const int mode = c->argc > 4 ? atoi(c->argv[4]->ptr) : 1;
    const int master_port = atoi(c->argv[2]->ptr);
    unsigned long long bl_tag;
    if (getuLongLongFromObjectOrReply(c, c->argv[3], &bl_tag, 0) != REDIS_OK)
    {
        redisLog(REDIS_WARNING, "bl_tag=%s invalid", c->argv[3]->ptr);
        return;
    }

    //if (bl_tag == 1)
    //{
    //    bl_tag = bl_get_last_ts(dbmng_get_bl(c->tag), 0);
    //    redisLog(REDIS_DEBUG, "bl_tag=%llu to start_sync()", bl_tag);
    //}
    const int ret = start_sync(c->argv[1]->ptr, master_port, bl_tag, 0, 0, mode);
    if (ret == -1)
    {
        addReplyError(c, "system fail");
    }
    else if (ret == -2)
    {
        addReplyError(c, "not allow because another one doing");
    }
    else if (ret == -3)
    {
        addReplyError(c, "sync_slave_start() fail");
    }
    else
    {
        addReply(c, shared.ok);
    }
}

/* 
 * cmd format: stop_sync master_ip master_port
 */
void stopsyncCommand(redisClient *c)
{
    redisLog(REDIS_PROMPT, "stop_sync %s %s", c->argv[1]->ptr, c->argv[2]->ptr);

    const int master_port = atoi(c->argv[2]->ptr);

    if (sync_slave_stop(c->argv[1]->ptr, master_port) == 0)
    {
        addReply(c, shared.ok);
    }
    else
    {
        redisLog(REDIS_WARNING, "stop_sync %s:%d fail",
                 c->argv[1]->ptr, master_port);
        addReplyError(c, "stop sync fail");
    }
}

void NoticeReplStatus(const char *master_ip, int master_port, int status, const char *info)
{
    redisLog(REDIS_DEBUG,
             "NoticeReplStatus(): master_ip=%s, master_port=%d, status=%d, info=%s",
             master_ip, master_port, status, info ? info : "");

    if (lock_repl_status() != 0)
    {
        return;
    }

    repl_status_node *node = find_repl_status(master_ip, master_port);
    if (node)
    {
        /* status value according to repl_if.h */
        if (status == 0)
        {
            node->status = REPL_STATUS_FAILED;

            redisLog(REDIS_WARNING, "%s:%d NoticeReplStatus fail",
                     master_ip, master_port);
        }
        else if (status == 1)
        {
            node->status = REPL_STATUS_DOING;
        }
        else if (status == 2)
        {
            node->status = REPL_STATUS_STOPED;
            node->bl_tag = strtoll(info, 0, 10);

            redisLog(REDIS_VERBOSE, "%s:%d NoticeReplStatus finish, bl_tag=%lld",
                     master_ip, master_port, node->bl_tag);

            /* send merge_dbe cmd to main_thread */
            char err[256];
            const int fd = anetTcpConnect(err, "127.0.0.1", server.port);
            if (fd == ANET_ERR)
            {
                redisLog(REDIS_WARNING, "anetTcpConnect() to 127.0.0.1:%d fail: %s",
                         server.port, err);
            }
            else
            {
                snprintf(err, sizeof(err), "*2\r\n$9\r\nmerge_dbe\r\n$%d\r\n%s\r\n",
                         (int)strlen(node->path), node->path);
                anetWrite(fd, err, strlen(err));
                close(fd);
            }

            unlock_repl_status();
            return;
        }
    }
    else
    {
        redisLog(REDIS_WARNING, "can't find %s:%d in NoticeReplStatus()",
                 master_ip, master_port);
    }

    unlock_repl_status();
}

/*
 * cmd format: start_repl master_ip master_port
 */
void startreplCommand(redisClient *c)
{
    redisLog(REDIS_VERBOSE, "start_repl %s %s", c->argv[1]->ptr, c->argv[2]->ptr);

    const int master_port = atoi(c->argv[2]->ptr);

    if (lock_repl_status() != 0)
    {
        addReplyError(c, "system fail");
        return;
    }

    const char *master_ip = c->argv[1]->ptr;
    repl_status_node *node = find_repl_status(master_ip, master_port);
    if (node)
    {
        if (node->status == REPL_STATUS_INIT || node->status == REPL_STATUS_DOING)
        {
            /* not allow to start a new one */
            unlock_repl_status();
            addReplyError(c, "not allow because another one doing");
            return;
        }
        else
        {
            node->status = REPL_STATUS_INIT;
            unlock_repl_status();
        }
    }
    else
    {
        /* add a new node into sync_status_mngr */
        node = (repl_status_node *)zmalloc(sizeof(*node));
        strcpy(node->master_ip, master_ip);
        node->master_port = master_port;
        node->status = REPL_STATUS_INIT;
        node->bl_tag = 0;
        node->next = 0;
        if (gReplStatusMngr.head)
        {
            node->next = gReplStatusMngr.head;
        }
        gReplStatusMngr.head = node;

        unlock_repl_status();
    }

    char name[256];
    snprintf(name, sizeof(name), "repl/%s:%d/", (char *)c->argv[1]->ptr, master_port);
    char path[256];
    const char *dir = make_dbe_path(path, sizeof(path), name);

    const int ret = repl_slave_start(c->argv[1]->ptr, master_port, dir, NoticeReplStatus);
    if (ret == 0)
    {
        if (dir[0] != '/')
        {
            snprintf(name, sizeof(name), "%s/%s", server.path, dir);
        }
        else
        {
            snprintf(name, sizeof(name), "%s", dir);
        }
        strncpy(node->path, name, sizeof(node->path));
        redisLog(REDIS_DEBUG, "start repl to \'%s\' from %s:%d",
                 name, c->argv[1]->ptr, master_port);
        addReplyBulkCString(c, name);
    }
    else
    {
        node->status = REPL_STATUS_FAILED;
        redisLog(REDIS_WARNING, "repl_slave_start() fail, from %s:%d to \'%s\'",
                 c->argv[1]->ptr, master_port, dir);
        addReplyError(c, "repl_slave_start() fail");
    }
}

/*
 * cmd format: merge_dbe path
 */
void mergedbeCommand(redisClient *c)
{
    redisLog(REDIS_PROMPT, "merge_dbe %s", c->argv[1]->ptr);

    if (server.has_dbe == 0)
    {
        redisLog(REDIS_WARNING, "not allow to merge dbe in only cache mode");
        addReplyErrorFormat(c, "not allow to merge dbe in only cache mode");
        return;
    }

    const int ret = dbe_merge(dbmng_get_db(c->tag, 0), c->argv[1]->ptr);
    if (ret != DBE_ERR_SUCC)
    {
        addReplyErrorFormat(c, "dbe_merge() fail from %s, ret=%d", (char *)c->argv[1]->ptr, ret);
    }
    else
    {
        addReply(c, shared.ok);
    }
}

/*
 * cmd format: clean_cache percent
 */
void cleancacheCommand(redisClient *c)
{
    redisLog(REDIS_PROMPT, "clean_cache %s", c->argv[1]->ptr);

    const int percent = atoi(c->argv[1]->ptr);

    if (server.has_cache == 0)
    {
        redisLog(REDIS_WARNING, "not need to clean cache in pure persistence mode");
        addReplyErrorFormat(c, "not need to clean cache in pure persistence mode");
        return;
    }

    if (percent < 0)
    {
        redisLog(REDIS_WARNING, "illegal parameter");
        addReplyErrorFormat(c, "illegal parameter");
        return;
    }

    clean_cache(percent);
    addReply(c, shared.ok);
}

/*
 * cmd format: clean_dbe somedays_before
 */
void cleandbeCommand(redisClient *c)
{
    redisLog(REDIS_PROMPT, "clean_dbe %s", c->argv[1]->ptr);

    const int somedays = atoi(c->argv[1]->ptr);

    if (server.has_dbe == 0)
    {
        redisLog(REDIS_WARNING, "can not clean dbe in pure cache mode");
        addReplyErrorFormat(c, "can not clean dbe in pure cache mode");
        return;
    }

    if (somedays < 0)
    {
        redisLog(REDIS_WARNING, "illegal parameter");
        addReplyErrorFormat(c, "illegal parameter");
        return;
    }

    clean_dbe(somedays);
    addReply(c, shared.ok);
}

/*
 * cmd format: purge_dbe
 */
void purgedbeCommand(redisClient *c)
{
    redisLog(REDIS_PROMPT, "purge_dbe");

    if (server.has_dbe == 0)
    {
        redisLog(REDIS_WARNING, "not allow to purge dbe in only cache mode");
        addReplyErrorFormat(c, "not allow to purge dbe in only cache mode");
        return;
    }

    const int ret = dbe_purge(dbmng_get_db(c->tag, 0));
    if (ret != DBE_ERR_SUCC)
    {
        addReplyErrorFormat(c, "dbe_purge() fail, ret=%d", ret);
    }
    else
    {
        addReply(c, shared.ok);
    }
}

void push_back_to_clean_list(redisClient *c)
{
    const int ret = pthread_mutex_lock(&sc_clean_lock);
    if (ret == 0)
    {
        listAddNodeTail(server.cleans, c);
        pthread_mutex_unlock(&sc_clean_lock);
        sc_clean_c = 1;
    }
    else
    {
        redisLog(REDIS_WARNING, "push_back_to_clean_list() lock fail, c=%p", c);
    }
}

/*
 * cmd format: decode_bl binlogfile
 */
void decodeblCommand(redisClient *c)
{
    redisLog(REDIS_DEBUG, "decode_bl %s", c->argv[1]->ptr);

    decode_bl(c->argv[1]->ptr, 1);
    addReply(c, shared.ok);
}

static int dump_key(redisDb *db, int fmt, FILE *fp)
{
    dictIterator *di;
    dictEntry *de;
    unsigned long numkeys = 0;
    unsigned int const memlimit = 8 * 1024 * 1024;
    char *buffer = 0;
    unsigned int bufcurr = 0;
    unsigned int len = 0;
    char temp[512];
    size_t wr_ret;
    const long long start_tm = ustime();

    /* 1) malloc buffer */
    buffer = zmalloc(memlimit);
    if (buffer == 0)
    {
        return 1;
    }

    /* 2) scan dict */
    di = dictGetIterator(db->dict);
    while ((de = dictNext(di)) != NULL)
    {
        sds key = dictGetEntryKey(de);
        robj k;
        initObject(&k, REDIS_STRING, 0);
        k.ptr = key;

        if (expireIfNeeded(db, &k) == 0)
        {
            numkeys++;
            if (fmt == 0)
            {
                len = snprintf(temp, sizeof(temp), "%s\r\n", key);
            }
            else
            {
                size_t val_len = 0;
                robj *const val = (robj *)dictFetchValue(db->dict, key);
                if (val && val->type == REDIS_STRING)
                {
                    if (val->encoding == REDIS_ENCODING_RAW)
                    {
                        val_len = sdslen(val->ptr);
                    }
                    else if (val->encoding == REDIS_ENCODING_INT)
                    {
                        char tmp[128];
                        sprintf(tmp, "%ld", (long)val->ptr);
                        val_len = strlen(tmp);
                    }
                }
                const time_t expire = getExpire(db, &k);
                len = snprintf(temp, sizeof(temp), "ITEM %s [%zd b; %ld s]\r\n",
                               key, val_len, expire == -1 ? 0 : expire);
            }
            if (bufcurr + len > memlimit)
            {
                /* write(append) current buffer into file at first */
                wr_ret = fwrite(buffer, 1, bufcurr, fp);
                if (wr_ret != (size_t)bufcurr)
                {
                    redisLog(REDIS_WARNING, "fwrite() fail: %lu,ret=%zd", bufcurr, wr_ret);
                }

                bufcurr = 0;
            }
            memcpy(buffer + bufcurr, temp, len);
            bufcurr += len;
        }
    }
    dictReleaseIterator(di);

    /* 3) write(append) buffer into file & close file */
    if (bufcurr > 0)
    {
        wr_ret = fwrite(buffer, 1, bufcurr, fp);
        if (wr_ret != (size_t)bufcurr)
        {
            redisLog(REDIS_WARNING, "fwrite() fail: %lu,ret=%zd", bufcurr, wr_ret);
        }
    }
    zfree(buffer);
    buffer = 0;

    redisLog(REDIS_PROMPT, "keydump: %lu, during: %llu us", numkeys, ustime() - start_tm);

    return 0;
}

/* cmd format: keydump [fmt]
 * fmt: 0 - key only; other value - compatible for mc "stats cachedump" cmd as follow
 * ITEM key [val_len b; expire s]
 * val_len = 0 means can't get its real len.
 * expire = 0 means no expire time. otherwise it means timestamp at expire.
 */
void keydumpCommand(redisClient *c)
{
    if (server.has_cache == 0)
    {
        addReplyError(c, "not support without cache");
        return;
    }

    /* 1) create output file */
    sds fname = sdscatprintf(sdsempty(), "%s%s/%d/key.txt", server.app_path, server.app_id, server.port);
    FILE *fp = fopen(fname, "w");
    if (fp == NULL)
    {
        addReplyError(c, "fopen fail");
        redisLog(REDIS_WARNING, "fopen() fail(errno=%d): %s", errno, fname);
        sdsfree(fname);
        return;
    }
    sdsfree(fname);

    /* 2) dump */
    const int fmt = c->argc > 1 ? atoi(c->argv[1]->ptr) : 0;
    if (dump_key(c->db, fmt, fp) == 1)
    {
        addReplyError(c, "zmalloc fail");
    }
    else
    {
        addReply(c, shared.ok);
    }

    fclose(fp);
}

void cachedumpCommand(redisClient *c)
{
    if (server.has_dbe == 1)
    {
        addReplyError(c, "not support under dbe mode");
        return;
    }

    dictIterator *di;
    dictEntry *de;
    unsigned long numkeys = 0;
    const long long start_tm = ustime();
    int ret;
    void *dbe = 0;

    /* 1) open dbe */
    sds dbe_path = sdscatprintf(sdsempty(), "%s%s/%d/dbe/dump/", server.app_path, server.app_id, server.port);
    check_and_make_dir(dbe_path);
    //redisLog(REDIS_PROMPT, "dbe_path:%s", dbe_path);
    ret = dbe_init(dbe_path, &dbe, 0, 0, 4);
    if (ret != 0)
    {
        redisLog(REDIS_WARNING, "dbe_init() fail: ret=%d, path=%s", ret, dbe_path);
        addReplyError(c, "dbe_init fail");
        sdsfree(dbe_path);
        return;
    }
    sdsfree(dbe_path);

    /* 2) scan dict */
    di = dictGetIterator(c->db->dict);
    while ((de = dictNext(di)) != NULL)
    {
        sds key = dictGetEntryKey(de);
        robj k;
        initObject(&k, REDIS_STRING, 0);
        k.ptr = key;

        if (expireIfNeeded(c->db, &k) == 0)
        {
            numkeys++;

            robj *const val = (robj *)dictFetchValue(c->db->dict, key);
            if (val->type != REDIS_STRING)
            {
                redisLog(REDIS_WARNING, "ignore key=%s, type=%d, not string",
                        key, val->type);
                continue;
            }

            const time_t expire = getExpire(c->db, &k);
            kvec_t data;
            int vs;
            data.k = encode_string_key(key, strlen(key), &data.ks);
            data.v = serializeObjExp(val, &vs, expire == -1 ? 0 : expire);
            data.vs = vs;
            ret = dbe_put(dbe, data.k, data.ks, data.v, data.vs);
            if (ret != 0)
            {
                redisLog(REDIS_WARNING, "dbe_put() fail: ret=%d, val_len=%d, dbe_key=%s",
                         ret, data.vs, data.k);
            }
            zfree(data.v);
            zfree(data.k);
        }
        else
        {
            redisLog(REDIS_VERBOSE, "key=%s expired", key);
        }
    }
    dictReleaseIterator(di);

    /* 3) close dbe */
    dbe_uninit(dbe);

    addReply(c, shared.ok);

    redisLog(REDIS_PROMPT,
             "cachedump: %lu, during: %llu us",
             numkeys, ustime() - start_tm);
}

static int dump_key_from_dbe(void *db, int fmt, FILE *fp)
{
    unsigned long numkeys = 0;
    unsigned int const memlimit = 8 * 1024 * 1024;
    char *buffer = 0;
    unsigned int bufcurr = 0;
    unsigned int len = 0;
    char temp[512];
    size_t wr_ret;
    const long long start_tm = ustime();

    char *k = 0;
    int k_len;
    char *v = 0;
    int v_len;
    robj *key_obj = 0;
    robj *val = 0;
    time_t expire;

    /* 1) malloc buffer */
    buffer = zmalloc(memlimit);
    if (buffer == 0)
    {
        redisLog(REDIS_WARNING, "zmalloc() fail");
        return 1;
    }

    /* 2) scan dict */
    void *it = dbe_create_it(db, 15);
    if (it == 0)
    {
        zfree(buffer);
        redisLog(REDIS_WARNING, "dbe_create_it() fail");
        return 1;
    }
    while (dbe_next_key(it, &k, &k_len, &v, &v_len, zmalloc, NULL) == 0)
    {
        key_obj = createStringObject(k, strlen(k));
        val = unserializeObj(v, v_len, &expire);
        const time_t now = time(0);
        sds key = key_obj->ptr;

        if (val && expire > 0 && expire < now)
        {
            redisLog(REDIS_WARNING, "\"%s\" expired, ignore it, expire=%d, now=%d",
                     key, expire, now);
            decrRefCount(val);
        }
        else if (val)
        {
            numkeys++;

            if (fmt == 0)
            {
                len = snprintf(temp, sizeof(temp), "%s\r\n", key);
            }
            else
            {
                size_t val_len = 0;
                if (val->type == REDIS_STRING)
                {
                    if (val->encoding == REDIS_ENCODING_RAW)
                    {
                        val_len = sdslen(val->ptr);
                    }
                    else if (val->encoding == REDIS_ENCODING_INT)
                    {
                        char tmp[128];
                        sprintf(tmp, "%ld", (long)val->ptr);
                        val_len = strlen(tmp);
                    }
                }
                len = snprintf(temp, sizeof(temp), "ITEM %s [%zd b; %ld s]\r\n",
                               key, val_len, expire > 0 ? expire : 0);
            }

            if (bufcurr + len > memlimit)
            {
                /* write(append) current buffer into file at first */
                wr_ret = fwrite(buffer, 1, bufcurr, fp);
                if (wr_ret != (size_t)bufcurr)
                {
                    redisLog(REDIS_WARNING, "fwrite() fail: %lu,ret=%zd", bufcurr, wr_ret);
                }

                bufcurr = 0;
            }
            memcpy(buffer + bufcurr, temp, len);
            bufcurr += len;
        }
        else
        {
            redisLog(REDIS_WARNING, "load from dbe fail, key=%s", key);
        }

        decrRefCount(key_obj);
        zfree(k);
        zfree(v);
    }
    dbe_destroy_it(it);

    /* 3) write(append) buffer into file & close file */
    if (bufcurr > 0)
    {
        wr_ret = fwrite(buffer, 1, bufcurr, fp);
        if (wr_ret != (size_t)bufcurr)
        {
            redisLog(REDIS_WARNING, "fwrite() fail: %lu,ret=%zd", bufcurr, wr_ret);
        }
    }
    zfree(buffer);
    buffer = 0;

    redisLog(REDIS_PROMPT, "keys: %lu, during: %llu us", numkeys, ustime() - start_tm);

    return 0;
}

static void do_keydump(const char *dbe_path, int fmt)
{
    void *dbe = 0;
    int ret = dbe_init(dbe_path, &dbe, 0, 0, 4);
    if (ret != 0)
    {
        redisLog(REDIS_WARNING, "dbe_init() fail: ret=%d, path=%s", ret, dbe_path);
        return;
    }

    dbe_set_val_filter(dbe, value_filter);
    dump_key_from_dbe(dbe, fmt, stdout);

    dbe_uninit(dbe);
}

static void do_getkey(const char *dbe_path, const char *key)
{
    void *dbe = 0;
    int ret = dbe_init(dbe_path, &dbe, 0, 0, 4);
    if (ret != 0)
    {
        redisLog(REDIS_WARNING, "dbe_init() fail: ret=%d, path=%s", ret, dbe_path);
        return;
    }

    val_attr rslt;
    ret = restore_key_from_dbe(dbe, key, strlen(key), 'K', &rslt);
    if (ret != 0)
    {
        dbe_uninit(dbe);
        redisLog(REDIS_WARNING, "dbe_get() fail: ret=%d, key=%s", ret, key);
        return;
    }

    /* unserialize */
    time_t expire = rslt.expire_ms;
    robj *v = rslt.val;
    dbe_uninit(dbe);
    {
        const time_t now = time(0);
        if (expire > 0 && expire <= now)
        {
            redisLog(REDIS_WARNING, "expired, key=%s, expire=%d, now=%d", key, expire, now);
        }
        else
        {
            if (v->type == REDIS_STRING)
            {
                if (v->encoding == REDIS_ENCODING_RAW)
                {
                    fprintf(stdout, "ITEM %s [%zd b; %d s]\n%s\n",
                            key, sdslen(v->ptr), expire > 0 ? (int)expire : 0, (char*)v->ptr);
                }
                else if (v->encoding == REDIS_ENCODING_INT)
                {
                    char tmp[128];
                    sprintf(tmp, "%ld", (long)v->ptr);
                    fprintf(stdout, "ITEM %s [%zd b; %d s]\n%s\n",
                            key, strlen(tmp), expire > 0 ? (int)expire : 0, tmp);
                }
                else
                {
                    redisLog(REDIS_WARNING, "unsupport value's encoding: %d", v->encoding);
                }
            }
            else
            {
                redisLog(REDIS_WARNING, "unsupport value's type: %d", v->type);
            }
        }

        decrRefCount(v);
    }
}

void flushcpCommand(redisClient *c)
{
    dbmng_start_cp(0);
    addReply(c, shared.ok);
}

#ifdef _TEST_DBE_IF_ 
typedef struct rand_ctx_st
{
    unsigned char *mark;
    int size;
    int max;
} rand_ctx;

void *init_rand(int max)
{
    const int size = (max + 8 - 1) / 8;
    char *const ptr = zcalloc(sizeof(rand_ctx) + size);
    rand_ctx *ctx = (rand_ctx*)ptr;
    ctx->max = max;
    ctx->size = size;
    ctx->mark = (unsigned char *)(ptr + sizeof(rand_ctx));
    return ctx;
}

int get_rand(void *it)
{
    rand_ctx *ctx = (rand_ctx*)it;
    int const tmp = rand() % ctx->max;
    int r = tmp;
    do
    {
        const int byte = r / 8;
        const int bit = r % 8;
        if ((ctx->mark[byte] & (1<<bit)) == 0)
        {
            ctx->mark[byte] |= 1 << bit;
            break;
        }
        r = (r + 1) % ctx->max;
    } while (r != tmp);
    return r;
}

void uninit_rand(void *it)
{
    zfree(it);
}
#endif

static void do_testkey(const char *dbe_path, const char *key, int max_key, int sample_cnt, int test_type)
{
#ifndef _TEST_DBE_IF_ 
    (void)dbe_path;
    (void)key;
    (void)max_key;
    (void)sample_cnt;
    (void)test_type;
#else
#include "restore_key.h"
    void *dbe = 0;
    int ret = dbe_init(dbe_path, &dbe, 0, 0, 4);
    if (ret != 0)
    {
        redisLog(REDIS_WARNING, "dbe_init() fail: ret=%d, path=%s", ret, dbe_path);
        return;
    }
    dbe_set_val_filter(dbe, value_filter);
    //dbe_startup(dbe);

    int k = 1;
    char key_buf[256];

    void *rand_it = init_rand(max_key);
    int *seq = zmalloc(sizeof(int) * sample_cnt);
    for (k = 0; k < sample_cnt; k++)
    {
        seq[k] = get_rand(rand_it); // random mode
        //seq[k] = k;  // sequence mode
    }
    uninit_rand(rand_it);

    struct timespec stat_start = pf_get_time_tick();
    for (k = 0; k < sample_cnt; k++)
    {
        sprintf(key_buf, "%s-%d", key, seq[k]);
        if (test_type == 1)
        {
            // test restore_key_from_dbe()
            val_attr rslt;
            const int ret = restore_key_from_dbe(dbe, key_buf, strlen(key_buf), 'K', &rslt);
            if (ret == 0 && rslt.val)
            {
                decrRefCount(rslt.val);
            }
            else
            {
                redisLog(REDIS_WARNING, "restore_key_from_dbe fail, ret=%d, key=%s", ret, key_buf);
            }
        }
        else
        {
            // test dbe_get()
            int len;
            char *ptr = 0;
#if 1
            const int ret = dbe_get(dbe, key_buf, strlen(key_buf), &ptr, &len, zmalloc, NULL);
#else
            char *val = zmalloc(256);
            int k_len = strlen(key_buf);
            char *k = zmalloc(k_len);
            strcpy(k, key_buf);
            const int ret = dbe_put(dbe, k, k_len, val, 256);
#endif
            if (ret == DBE_ERR_SUCC)
            {
                zfree(ptr);
            }
            else
            {
                redisLog(REDIS_WARNING, "dbe_get fail, ret=%d, key=%s", ret, key_buf);
            }
        }
    }
    struct timespec stat_end = pf_get_time_tick();
    const time_t stat_diff_ns = pf_get_time_diff_nsec(stat_start, stat_end);
    time_t stat_diff_ms = stat_diff_ns / 1000000;
    if (stat_diff_ms < 1)
    {
        stat_diff_ms = 1;
    }
    redisLog(REDIS_PROMPT, "%s: dur=%dms, max=%d, sample=%d, tps=%d"
            , test_type == 0 ? "dbe_get" : "restore", stat_diff_ms, max_key, sample_cnt, (unsigned long)sample_cnt * 1000 / stat_diff_ms);
    zfree(seq);

    //dbe_uninit(dbe); // hidb2 will hungup here without calling dbe_startup,so ...
#endif
}

/* The End */
