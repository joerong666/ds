#ifndef _DS_CTRL_H_
#define _DS_CTRL_H_

#define APP_STATUS_NORMAL              0
#define APP_STATUS_PENDING             1
#define APP_STATUS_DISABLE             2
#define APP_STATUS_UNUSE               3

#define LRU_DISABLE                    0
#define LRU_ENABLE                     1

#define TRAN_STATUS_NULL               0
#define TRAN_STATUS_INIT               1
#define TRAN_STATUS_FIN                2
#define TRAN_STATUS_READY              3
#define TRAN_STATUS_FAIL               4

#define SYNC_STATUS_DISABLE            0
#define SYNC_STATUS_ENABLE             1

#define SYNC_STATUS_INIT               0
#define SYNC_STATUS_DOING              1
#define SYNC_STATUS_FAILED             2
#define SYNC_STATUS_SYNCED             3
#define SYNC_STATUS_STOPED             4

#define REPL_STATUS_INIT               0
#define REPL_STATUS_DOING              1
#define REPL_STATUS_FAILED             2
#define REPL_STATUS_STOPED             3

#define LRU_BIT                        0
#define CLEAN_BIT                      1
#define DS_IP_LIST_BIT                 2
#define DS_IP_LIST_INCR_BIT            3

#define LRU_MASK                       (1 << LRU_BIT)
#define CLEAN_MASK                     (1 << CLEAN_BIT)
#define DS_IP_LIST_MASK                (1 << DS_IP_LIST_BIT)
#define DS_IP_LIST_INCR_MASK           (1 << DS_IP_LIST_INCR_BIT)

typedef struct sync_status_node_t
{
    char master_ds_id[128];
    char master_ip[128];
    uint32_t master_port;
    uint32_t status:4;
    uint32_t conf:2;
    uint32_t stop_flag:2;
    uint32_t mode:2;
    long long bl_tag;
    struct sync_status_node_t *next;
} sync_status_node;

typedef struct sync_status_mngr_t
{
    sync_status_node *head;
    int cnt;
} sync_status_mngr;

typedef struct repl_status_node_t
{
    char master_ip[128];
    uint32_t master_port;
    uint32_t status:4;
    char path[256];
    long long bl_tag;
    struct repl_status_node_t *next;
} repl_status_node;

typedef struct repl_status_mngr_t
{
    repl_status_node *head;
    int cnt;
} repl_status_mngr;

typedef struct ds_ip_node_tag
{
    int ds_seq;
    char ds_ip[128];
    struct ds_ip_node_tag *next;
} ds_ip_node;

typedef struct ds_ctrl_info_tag
{
    unsigned char flag;

    char *dsip_list;
    ds_ip_node *ds_ip_list;
    ds_ip_node *ds_ip_list_tmp;
    int ip_cnt;
    int set_ip_list;
    int LRU_flag;
    int clean_size;

    int transfer_status;
    int sync_status;

    int app_status;
    int64_t max_mem;
    int64_t min_mem;
    int upd_cp_timeout;
    int64_t op_max_num;
    int read_dbe;
    char *log_level;
    int period;
    int wr_bl;
    int dbe_get_que_size;
    int wr_bl_que_size;
    long long slowlog_log_slower_than;
    int slow_log;
    int rdb_compression;
    int log_get_miss;
} ds_ctrl_info;

typedef struct ds_stat_info_tag
{
    time_t uptime;
    int pid;

    uint64_t db_used_mem;
    uint64_t used_mem;
    uint64_t peak_mem;
    uint64_t rss_mem;

    int max_fd;
    int active_clients;
    uint64_t total_connections_received;
    uint64_t total_commands_processed;
    uint64_t total_commands_processed_dur;

    uint64_t keys;
    uint64_t in_expire_keys;
    uint64_t dirty;
    uint64_t hits;
    uint64_t misses;
    uint64_t del_lru;
    uint64_t expired_keys;
    uint64_t evicted_keys;

    uint64_t get_cmds;
    uint64_t set_cmds;
    uint64_t touch_cmds;
    uint64_t incr_cmds;
    uint64_t decr_cmds;
    uint64_t delete_cmds;
    uint64_t auth_cmds;
    uint64_t get_hits;
    uint64_t get_misses;
    uint64_t touch_hits;
    uint64_t touch_misses;
    uint64_t incr_hits;
    uint64_t incr_misses;
    uint64_t decr_hits;
    uint64_t decr_misses;
    uint64_t cas_hits;
    uint64_t cas_misses;
    uint64_t cas_badval;
    uint64_t delete_hits;
    uint64_t delete_misses;
    uint64_t auth_errors;

    uint64_t expired_unfetched;
    uint64_t evicted_unfetched;
    uint64_t bytes_read;
    uint64_t bytes_written;
    uint64_t rejected_conns;
    unsigned char accepting_conns;
} ds_stat_info;

typedef struct ds_status_info_tag
{
    char soft_ver[128];
    int pid;
    unsigned char block_upd_cp;
    unsigned char upd_cp_status;
    unsigned char arch_bits;
} ds_status_info;

typedef struct transfer_cmd_tag
{
    int master_slave;
    char peer_address[256];
    int peer_port;
    int peer_master_slave;
    int operate;
    int delete_flag;

    int status[2];
} transfer_cmd;

typedef struct wr_bl_stat_tag
{
    unsigned char valid;
    unsigned char cmd;
    int buf_len;
    time_t time;
    char key[256];
    int64_t serialize_dur;
    int64_t io_dur;
} wr_bl_stat;

typedef struct dbe_stat_tag
{
    unsigned char valid;
    int buf_len;
    time_t time;
    int64_t unserialize_dur;
    int64_t io_dur;
} dbe_stat;

extern ds_ctrl_info gDsCtrl; /* for ds ctrl */
extern transfer_cmd gTran; /* for transfer command */
extern ds_stat_info gDsStat; /* for ds stat */
extern ds_status_info gDsStatus; /* for ds status */
extern sync_status_mngr gSyncStatusMngr;
extern repl_status_mngr gReplStatusMngr;

extern unsigned int gWaitBlClntMax;

extern wr_bl_stat gWrBlTop[11];
extern long long gWrBlMax, gWrBlMin, gWrBlTotal;
extern int gWrBlCnt;

extern dbe_stat gDbeGetTop[11];
extern long long gDbeGetMax, gDbeGetMin, gDbeGetTotal;
extern int gDbeGetCnt;

extern dbe_stat gDbeSetTop[11];
extern long long gDbeSetMax, gDbeSetMin, gDbeSetTotal;
extern int gDbeSetCnt;

extern void init_ctrl_info();
extern int lock_ctrl_info_rd();
extern int lock_ctrl_info_wr();
extern int unlock_ctrl_info();

extern int lock_stat_info();
extern int unlock_stat_info();

extern int lock_sync_status();
extern int unlock_sync_status();

extern int lock_repl_status();
extern int unlock_repl_status();

extern int lock_bl_filter();
extern int unlock_bl_filter();
extern int lock_sync_filter();
extern int unlock_sync_filter();
extern int lock_cache_filter();
extern int unlock_cache_filter();

#ifndef _UPD_DBE_BY_PERIODIC_
extern int lockBlWriteList();
extern int unlockBlWriteList();
extern int lockWrblList();
extern int unlockWrblList();
extern int lockWrDbeList();
extern int unlockWrDbeList();
#endif

extern int lockDbeGet();
extern int unlockDbeGet();

/* caller need to lock */
extern sync_status_node *find_sync_status(const char *master_ip, int master_port);
extern repl_status_node *find_repl_status(const char *master_ip, int master_port);
extern sds get_sync_list();
extern char *get_mngr_list();

#endif /* _DS_CTRL_H_ */

