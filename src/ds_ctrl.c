#include "redis.h"
#include "ds_ctrl.h"
#include "ds_log.h"

#include <pthread.h>
#include <sys/types.h>
#include <unistd.h>

ds_ctrl_info gDsCtrl;
transfer_cmd gTran;
ds_stat_info gDsStat;
ds_status_info gDsStatus;
sync_status_mngr gSyncStatusMngr;
repl_status_mngr gReplStatusMngr;

unsigned int gWaitBlClntMax;

wr_bl_stat gWrBlTop[11];
long long gWrBlMax, gWrBlMin, gWrBlTotal;
int gWrBlCnt;

dbe_stat gDbeGetTop[11];
long long gDbeGetMax, gDbeGetMin, gDbeGetTotal;
int gDbeGetCnt;

dbe_stat gDbeSetTop[11];
long long gDbeSetMax, gDbeSetMin, gDbeSetTotal;
int gDbeSetCnt;

static pthread_rwlock_t g_rwlock;
//static pthread_mutex_t sc_stat_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t sc_sync_status_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t sc_repl_status_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t sc_bl_filter_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t sc_sync_filter_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t sc_cache_filter_lock = PTHREAD_MUTEX_INITIALIZER;

#ifndef _UPD_DBE_BY_PERIODIC_
static pthread_mutex_t sc_bl_write_list_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t sc_wr_bl_list_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t sc_wr_dbe_list_lock = PTHREAD_MUTEX_INITIALIZER;
#endif
static pthread_mutex_t sc_dbe_get_lock = PTHREAD_MUTEX_INITIALIZER;

void init_ctrl_info()
{
    pthread_rwlock_init(&g_rwlock, 0);

    gDsCtrl.LRU_flag = LRU_DISABLE;
    gDsCtrl.clean_size = -1;

    gDsCtrl.flag = 0;
    gDsCtrl.dsip_list = 0;
    gDsCtrl.ds_ip_list = 0;
    gDsCtrl.ds_ip_list_tmp = 0;
    gDsCtrl.set_ip_list = 0;
    gDsCtrl.log_level = 0;
    gDsCtrl.app_status = APP_STATUS_NORMAL;
    gDsCtrl.transfer_status = TRAN_STATUS_NULL;
    gDsCtrl.sync_status = SYNC_STATUS_DISABLE;
    gDsCtrl.wr_bl = 0;
    gDsCtrl.upd_cp_timeout = -1;
    gDsCtrl.op_max_num = -1;
    gDsCtrl.dbe_get_que_size = 2000;
    gDsCtrl.wr_bl_que_size = 2000;

    memset(&gDsStat, 0, sizeof(gDsStat));
    gDsStat.pid = (int)getpid();

    memset(&gDsStatus, 0, sizeof(gDsStatus));
    sprintf(gDsStatus.soft_ver, "%s %s %s", DS_VERSION, __DATE__, __TIME__);
    gDsStatus.pid = (int)getpid();
    gDsStatus.arch_bits = (sizeof(long) == 8) ? 64 : 32;

    memset(&gSyncStatusMngr, 0, sizeof(gSyncStatusMngr));
    memset(&gReplStatusMngr, 0, sizeof(gReplStatusMngr));
}

int lock_ctrl_info_rd()
{
    const int ret = pthread_rwlock_rdlock(&g_rwlock);
    if (ret != 0)
    {
        log_fatal("pthread_rwlock_rdlock() fail: %d", ret);
    }
    return ret;
}

int lock_ctrl_info_wr()
{
    const int ret = pthread_rwlock_wrlock(&g_rwlock);
    if (ret != 0)
    {
        log_fatal("pthread_rwlock_wrlock() fail: %d", ret);
    }
    return ret;
}

int unlock_ctrl_info()
{
    const int ret = pthread_rwlock_unlock(&g_rwlock);
    if (ret != 0)
    {
        log_fatal("pthread_mutex_unlock() fail: %d", ret);
    }
    return ret;
}

int lock_stat_info()
{
    const int ret = 0;//pthread_mutex_lock(&sc_stat_lock);
    if (ret != 0)
    {
        log_fatal("pthread_mutex_lock() fail: %d", ret);
    }
    return ret;
}

int unlock_stat_info()
{
    const int ret = 0;//pthread_mutex_unlock(&sc_stat_lock);
    if (ret != 0)
    {
        log_fatal("pthread_mutex_unlock() fail: %d", ret);
    }
    return ret;
}

int lock_sync_status()
{
    const int ret = pthread_mutex_lock(&sc_sync_status_lock);
    if (ret != 0)
    {
        log_fatal("pthread_mutex_lock() fail: %d", ret);
    }
    return ret;
}

int unlock_sync_status()
{
    const int ret = pthread_mutex_unlock(&sc_sync_status_lock);
    if (ret != 0)
    {
        log_fatal("pthread_mutex_unlock() fail: %d", ret);
    }
    return ret;
}

int lock_repl_status()
{
    const int ret = pthread_mutex_lock(&sc_repl_status_lock);
    if (ret != 0)
    {
        log_fatal("pthread_mutex_lock() fail: %d", ret);
    }
    return ret;
}

int unlock_repl_status()
{
    const int ret = pthread_mutex_unlock(&sc_repl_status_lock);
    if (ret != 0)
    {
        log_fatal("pthread_mutex_unlock() fail: %d", ret);
    }
    return ret;
}

int lock_bl_filter()
{
    const int ret = pthread_mutex_lock(&sc_bl_filter_lock);
    if (ret != 0)
    {
        log_fatal("pthread_mutex_lock() fail: %d", ret);
    }
    return ret;
}

int unlock_bl_filter()
{
    const int ret = pthread_mutex_unlock(&sc_bl_filter_lock);
    if (ret != 0)
    {
        log_fatal("pthread_mutex_unlock() fail: %d", ret);
    }
    return ret;
}

int lock_sync_filter()
{
    const int ret = pthread_mutex_lock(&sc_sync_filter_lock);
    if (ret != 0)
    {
        log_fatal("pthread_mutex_lock() fail: %d", ret);
    }
    return ret;
}

int unlock_sync_filter()
{
    const int ret = pthread_mutex_unlock(&sc_sync_filter_lock);
    if (ret != 0)
    {
        log_fatal("pthread_mutex_unlock() fail: %d", ret);
    }
    return ret;
}

int lock_cache_filter()
{
    const int ret = pthread_mutex_lock(&sc_cache_filter_lock);
    if (ret != 0)
    {
        log_fatal("pthread_mutex_lock() fail: %d", ret);
    }
    return ret;
}

int unlock_cache_filter()
{
    const int ret = pthread_mutex_unlock(&sc_cache_filter_lock);
    if (ret != 0)
    {
        log_fatal("pthread_mutex_unlock() fail: %d", ret);
    }
    return ret;
}

sync_status_node *find_sync_status(const char *master_ip, int master_port)
{
    sync_status_node *node = gSyncStatusMngr.head;
    while (node)
    {
        if (master_port >= 0
            && (uint32_t)master_port == node->master_port
            && strcmp(master_ip, node->master_ip) == 0)
        {
            break;
        }
        node = node->next;
    }
    return node;
}

repl_status_node *find_repl_status(const char *master_ip, int master_port)
{
    repl_status_node *node = gReplStatusMngr.head;
    while (node)
    {
        if (master_port >= 0
            && (uint32_t)master_port == node->master_port
            && strcmp(master_ip, node->master_ip) == 0)
        {
            break;
        }
        node = node->next;
    }
    return node;
}

sds get_sync_list()
{
    sds sync_list = sdsempty();
    if (lock_sync_status() == 0)
    {
        sync_status_node *node = gSyncStatusMngr.head;
        while (node)
        {
            if (sdslen(sync_list) != 0)
            {
                sync_list = sdscatprintf(sync_list, ",%s:%d", node->master_ip, node->master_port);
            }
            else
            {
                sync_list = sdscatprintf(sync_list, "%s:%d", node->master_ip, node->master_port);
            }
            node = node->next;
        }

        unlock_sync_status();
    }

    return sync_list;
}

char *get_mngr_list()
{
    char *mngr_list = 0;
    lock_ctrl_info_rd();
    if (server.mngr_list)
    {
        mngr_list = zstrdup(server.mngr_list);
    }
    unlock_ctrl_info();
    return mngr_list;
}

#ifndef _UPD_DBE_BY_PERIODIC_
int lockBlWriteList()
{
    const int ret = pthread_mutex_lock(&sc_bl_write_list_lock);
    if (ret != 0)
    {
        log_fatal("pthread_mutex_lock() fail: %d", ret);
    }
    return ret;
}

int unlockBlWriteList()
{
    const int ret = pthread_mutex_unlock(&sc_bl_write_list_lock);
    if (ret != 0)
    {
        log_fatal("pthread_mutex_unlock() fail: %d", ret);
    }
    return ret;
}

int lockWrblList()
{
    const int ret = pthread_mutex_lock(&sc_wr_bl_list_lock);
    if (ret != 0)
    {
        log_fatal("pthread_mutex_lock() fail: %d", ret);
    }
    return ret;
}

int unlockWrblList()
{
    const int ret = pthread_mutex_unlock(&sc_wr_bl_list_lock);
    if (ret != 0)
    {
        log_fatal("pthread_mutex_unlock() fail: %d", ret);
    }
    return ret;
}

int lockWrDbeList()
{
    const int ret = pthread_mutex_lock(&sc_wr_dbe_list_lock);
    if (ret != 0)
    {
        log_fatal("pthread_mutex_lock() fail: %d", ret);
    }
    return ret;
}

int unlockWrDbeList()
{
    const int ret = pthread_mutex_unlock(&sc_wr_dbe_list_lock);
    if (ret != 0)
    {
        log_fatal("pthread_mutex_unlock() fail: %d", ret);
    }
    return ret;
}
#endif

int lockDbeGet()
{
    const int ret = pthread_mutex_lock(&sc_dbe_get_lock);
    if (ret != 0)
    {
        log_fatal("pthread_mutex_lock() fail: %d", ret);
    }
    return ret;
}

int unlockDbeGet()
{
    const int ret = pthread_mutex_unlock(&sc_dbe_get_lock);
    if (ret != 0)
    {
        log_fatal("pthread_mutex_unlock() fail: %d", ret);
    }
    return ret;
}

