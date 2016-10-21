#include "op_cmd.h"

#include "uthash.h"
#include "ds_log.h"

#include "codec_key.h"

#include <string.h>
#include <ctype.h>


/* string command */
extern void op_setCommand(redisClient *c);
extern void op_setexCommand(redisClient *c);
extern void op_setbitCommand(redisClient *c);
extern void op_setrangeCommand(redisClient *c);
extern void op_incrbyCommand(redisClient *c);
extern void op_appendCommand(redisClient *c);
extern void op_prependCommand(redisClient *c);
extern void op_casCommand(redisClient *c);
//extern void op_psetexCommand(redisClient *c);

/* hash command */
extern void op_hdelCommand(redisClient *c);
extern void op_hmsetCommand(redisClient *c);

/* list command */
extern void op_rpoplpushCommand(redisClient *c);
extern void op_rpopCommand(redisClient *c);
extern void op_rpushCommand(redisClient *c);
extern void op_lpopCommand(redisClient *c);
extern void op_lpushCommand(redisClient *c);
extern void op_linsertCommand(redisClient *c);
extern void op_lremCommand(redisClient *c);
extern void op_lsetCommand(redisClient *c);
extern void op_ltrimCommand(redisClient *c);

/* sets command */
extern void op_saddCommand(redisClient *c);
extern void op_sremCommand(redisClient *c);

/* sorted-sets command */
extern void op_zaddCommand(redisClient *c);
extern void op_zremCommand(redisClient *c);

/* key command */
static void op_delCommand(redisClient *c)
{
    dbDelete(c->db, c->argv[1]);
}

static void op_expireatCommand(redisClient *c)
{
    dictEntry *de;
    long when;
    robj *key = c->argv[1];

    if (getLongFromObject(c->argv[2], &when) != REDIS_OK)
    {
        return;
    }

    de = dictFind(c->db->dict, key->ptr);
    if (de == NULL)
    {
        return;
    }

    const time_t now = time(0);
    if (when < now)
    {
        when = now + 1;
        //dbDelete(c->db, key);
        //return;
    }

    setExpire(c->db, key, when);
    signalModifiedKey(c->db, key);
    server.dirty++;
    return;
}

static void op_persistCommand(redisClient *c)
{
    dictEntry *de = dictFind(c->db->dict, c->argv[1]->ptr);
    if (de)
    {
        if (removeExpire(c->db, c->argv[1]))
        {
            server.dirty++;
        }
    }
}

static void op_flushdbCommand(redisClient *c)
{
    server.dirty += dictSize(c->db->dict);
    signalFlushedDb(c->db->id);
    dictEmpty(c->db->dict);
    dictEmpty(c->db->expires);
}

static void op_flushallCommand(redisClient *c)
{
    REDIS_NOTUSED(c);
    server.dirty += emptyDb(NULL);
    signalFlushedDb(-1);
}

static void op_renameCommand(redisClient *c)
{
    robj *o;
    time_t expire;

    if ((o = lookupKeyWrite(c->db, c->argv[1])) == NULL)
        return;

    incrRefCount(o);
    expire = getExpire(c->db,c->argv[1]);
    if (lookupKeyWrite(c->db,c->argv[2]) != NULL)
    {
        dbDelete(c->db,c->argv[2]);
    }
    dbAdd(c->db,c->argv[2],o);
    if (expire != -1) setExpire(c->db,c->argv[2],expire);
    dbDelete(c->db,c->argv[1]);
    signalModifiedKey(c->db,c->argv[1]);
    signalModifiedKey(c->db,c->argv[2]);
    server.dirty++;
}

static void op_moveCommand(redisClient *c)
{
    robj *o;
    redisDb *src, *dst;
    int srcid;

    /* Obtain source and target DB pointers */
    src = c->db;
    srcid = c->db->id;
    if (selectDb(c,atoi(c->argv[2]->ptr)) == REDIS_ERR)
    {
        return;
    }
    dst = c->db;
    selectDb(c,srcid); /* Back to the source DB */

    /* If the user is moving using as target the same
     * DB as the source DB it is probably an error. */
    if (src == dst) 
    {
        return;
    }

    /* Check if the element exists and get a reference */
    o = lookupKeyWrite(c->db,c->argv[1]);
    if (!o) 
    {
        return;
    }

    /* Return zero if the key already exists in the target DB */
    if (lookupKeyWrite(dst,c->argv[1]) != NULL) 
    {
        return;
    }
    dbAdd(dst,c->argv[1],o);
    incrRefCount(o);

    /* OK! key moved, free the entry in the source DB */
    dbDelete(src,c->argv[1]);
    server.dirty++;
}


typedef void opCommandProc(redisClient *c);
typedef struct opCommand_st
{
    char name[32];
    opCommandProc *proc;
    char type;
    unsigned char cmd;
    UT_hash_handle hh;
} opCommand;

typedef struct opCommandMap_st
{
    char *name;
    opCommandProc *proc;
    char type;
} opCommandMap;

/* allow to append the array when need to add some cmd, can't change the sequence */
static opCommand *hash_head = 0;
static opCommandMap cmdmap_tab[] =
{
    {"digsig", 0, '-'}
    , {OP_SET, op_setCommand, KEY_TYPE_STRING}                   // 1
    , {OP_SETEX, op_setexCommand, KEY_TYPE_STRING}               // 2
    , {OP_SETBIT, op_setbitCommand, KEY_TYPE_STRING}             // 3
    , {OP_SETRANGE, op_setrangeCommand, KEY_TYPE_STRING}         // 4
    , {OP_INCRBY, op_incrbyCommand, KEY_TYPE_STRING}             // 5
    , {OP_APPEND, op_appendCommand, KEY_TYPE_STRING}             // 6
    , {OP_PREPEND, op_prependCommand, KEY_TYPE_STRING}           // 7
    , {OP_DEL, op_delCommand, 'k'}                               // 8
    , {OP_CAS, op_casCommand, KEY_TYPE_STRING}                   // 9
    , {OP_HDEL, op_hdelCommand, KEY_TYPE_HASH}                   // 10
    , {OP_HMSET, op_hmsetCommand, KEY_TYPE_HASH}                 // 11
    , {OP_SADD, op_saddCommand, KEY_TYPE_SET}                    // 12
    , {OP_SREM, op_sremCommand, KEY_TYPE_SET}                    // 13
    , {OP_ZADD, op_zaddCommand, KEY_TYPE_ZSET}                   // 14
    , {OP_ZREM, op_zremCommand, KEY_TYPE_ZSET}                   // 15
    , {OP_EXPIREAT, op_expireatCommand, 'k'}                     // 16
    , {OP_PERSIST, op_persistCommand, 'k'}                       // 17
    , {OP_FLUSHDB, op_flushdbCommand, 'k'}                       // 18
    , {OP_FLUSHALL, op_flushallCommand, 'k'}                     // 19
    , {OP_RENAME, op_renameCommand, 'k'}                         // 20
    , {OP_RPOPLPUSH, op_rpoplpushCommand, KEY_TYPE_LIST}         // 21
    , {OP_LRPOP, op_rpopCommand, KEY_TYPE_LIST}                  // 22
    , {OP_LRPUSH, op_rpushCommand, KEY_TYPE_LIST}                // 23
    , {OP_LLPOP, op_lpopCommand, KEY_TYPE_LIST}                  // 24
    , {OP_LLPUSH, op_lpushCommand, KEY_TYPE_LIST}                // 25
    , {OP_LINSERT, op_linsertCommand, KEY_TYPE_LIST}             // 26
    , {OP_LREM, op_lremCommand, KEY_TYPE_LIST}                   // 27
    , {OP_LSET, op_lsetCommand, KEY_TYPE_LIST}                   // 28
    , {OP_LTRIM, op_ltrimCommand, KEY_TYPE_LIST}                 // 29
    , {OP_MOVE, op_moveCommand, 'k'}                             // 30
//    , {OP_PSETEX, op_psetexCommand, '-'}                         // 31
};

const int ci_num = sizeof(cmdmap_tab) / sizeof(cmdmap_tab[0]);

static opCommand *cmd_tab = 0;


void initOpCommandTable(void)
{
    if (cmd_tab)
    {
        return;
    }

    cmd_tab = (opCommand *)zmalloc(sizeof(opCommand) * ci_num);
    if (cmd_tab == 0)
    {
        log_error("zmalloc() fail, initOpCommandTable() fail, tab_size=%d", ci_num);
        return;
    }

    int i;
    for (i = 0; i < ci_num; i++)
    {
        opCommand *c = cmd_tab + i;
        strcpy(c->name, cmdmap_tab[i].name);
        c->proc = cmdmap_tab[i].proc;
        c->type = cmdmap_tab[i].type;
        c->cmd = i;

        HASH_ADD_STR(hash_head, name, c);
    }
}

int get_cmd(const char *cmd)
{
    char cmd_lower[128];
    int i = 0;
    do
    {
        cmd_lower[i] = tolower(cmd[i]);
    } while (cmd[i++]);

    opCommand *s = 0;
    HASH_FIND_STR(hash_head, cmd_lower, s);
    const int ret = s ? s->cmd : -1;
    return ret;
}

const char *get_cmdstr(unsigned char cmd)
{
    if (cmd < ci_num)
    {
        return cmdmap_tab[cmd].name;
    }
    else
    {
        return 0;
    }
}

int redo_op(redisDb *rdb, const robj *key, unsigned char cmd, int argc, robj **argv)
{
    log_debug("redo_op(): rdb=%p, key=%s, cmd=%d, argc=%d"
            , (void*)rdb, (const char*)key->ptr, cmd, argc);

    if (cmd >= ci_num)
    {
        log_error("illegal cmd=%d, cmd_tab_max_num=%d", cmd, ci_num);
        return -1;
    }

    redisClient c;
    c.db = rdb;
    c.argc = 2 + argc;
    c.argv = (robj **)zcalloc(sizeof(robj *) * c.argc);
    if (c.argv == 0)
    {
        log_error("zcalloc() fail for redisClient.argv, argc=%d", c.argc);
        return -1;
    }
    c.argv[1] = (robj *)key;
    int i;
    for (i = 0; i < argc; i++)
    {
        c.argv[i + 2] = argv[i];
    }

    opCommandProc *proc = cmd_tab[cmd].proc;
    if (proc)
    {
        proc(&c);
    }
    else
    {
        log_error("proc is null, cmd=%d", cmd);
    }

    zfree(c.argv);
    c.argv = 0;

    return 0;
}

int is_entirety_cmd(int cmd)
{
    if (cmd == OP_CMD_SET || cmd == OP_CMD_SETEX || cmd == OP_CMD_CAS || cmd == OP_CMD_DEL)
    {
        return 1;
    }
    return 0;
}

char get_blcmd_type(int cmd)
{
    char type = '-';
    if (cmd < ci_num)
    {
        type = cmd_tab[cmd].type;
    }

    return type;
}

