#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <inttypes.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "pf_file.h"
#include "replica_proto.h"
#include "bin_log.h"
#include "meta_file.h"
#include "relay_log.h"
#include "tcp_util.h"
#include "zmalloc.h"
#include "log.h"
#include "util.h"
#include "replica_thread_mng.h"
#include "replica.h"

typedef struct ladder_timer
{
    uint32_t   *usec_arr;
    int         count;
    int         idx;         
}ladder_timer_t;

void ladder_timer_init(ladder_timer_t *t, uint32_t *usec_arr, int count)
{
    memset(t, 0x00, sizeof(ladder_timer_t));    
    t->usec_arr = usec_arr;
    t->count    = count; 
    t->idx      = 0;
}

void ladder_timer_sleep(ladder_timer_t *t)
{
    uint32_t usec;
    
    if (t->idx >= t->count)
        return;

    usec = t->usec_arr[t->idx];
    if ( usec <= 0)
    {
        return;    
    }
    else if (usec >= 1000)
    {
        sleep(usec / 1000);
    }
    else
    {
        usleep(usec * 1000);
    }

    if (t->idx + 1 < t->count)
        t->idx++;
}

void ladder_timer_reset(ladder_timer_t *t)
{
    t->idx = 0;
}

static void destroy_arg(repl_thread_arg_t *arg)
{
    if (arg)
    {
        if (arg->ip)
        {
            zfree(arg->ip);
        }
        if (arg->path)
        {
            zfree(arg->path);
        }
        if (arg->prefix)
        {
            zfree(arg->prefix);
        }
        if (arg->slave_ds_key)
        {
            zfree(arg->slave_ds_key);
        }
        if (arg->res && arg->free_res)
        {
            arg->free_res(arg->res);
        }
        zfree(arg);
    }
}

static void *master_worker(void *param)
{
    int                     i;
    bin_meta_t              bm;
    char                   *buf;
    uint32_t                len;
    int                     ret;
    uint64_t                curr_ts = 0;

    /* timer */
    ladder_timer_t          ti;
    uint32_t                usec_arr[] = {10, 100, 500, 1000};

    repl_thread_grp_t      *grp   = (repl_thread_grp_t *)param;
    repl_thread_t          *my_th = get_repl_thread(grp, REPL_MASTER_TH);  
    repl_thread_arg_t      *arg   = my_th->arg;

    uint64_t                sid = 0;
    time_t                  heartbeat_inter;
    time_t                  curr_time;


    memset(&bm, 0x00, sizeof(bin_meta_t));
    ladder_timer_init(&ti, usec_arr, 4);

    log_debug("master start!");
    /* first, we send master sid to slave */
    if (repl_send_slave(arg->fd, (uint8_t *)&arg->master_sid, REPL_FLAG_SID, sizeof(arg->master_sid), curr_ts) < 0)
    {
        log_error("repl send master sid to slave error, peer[%s:%d], error[%s]",
                arg->ip, arg->port, strerror(errno));
        goto master_error; 
    }

    log_debug("master get path[%s] ts[%"PRIu64"] msid[%"PRIu64"] ssid[%"PRIu64"]",
                arg->path, arg->ts, arg->master_sid, arg->slave_sid); 

    while (1)
    { 
        /* master do not need persist meta file */
        ret = bin_init(&bm, (uint8_t *)arg->path, (uint8_t *)arg->prefix,
                    arg->max_size, arg->max_idx, arg->ts,
                    0, NULL, O_RDONLY, 0600);
        
        if (ret >= 0)
            break;

        /* here is no bin log, wait for create it */
        if (ret < 0 && errno == ENOENT)
        {
            if (repl_send_slave(arg->fd, NULL, REPL_FLAG_FIN, 0, curr_ts) < 0)
            {
                log_error("repl send to slave error, peer[%s:%d], error[%s]",
                        arg->ip, arg->port, strerror(errno));
                goto master_error; 
            }
            log_debug("bin init no binlog found, sleep and continue, we set ts to BL_TIMESTAMP_FIRST");
            arg->ts = BL_TIMESTAMP_FIRST;

            set_repl_status(my_th, RTS_WAIT_BIN_INIT); 
            ladder_timer_sleep(&ti);
            continue;
        }
        else
        {
            log_error("repl bin init error, peer[%s:%d]", arg->ip, arg->port);
            goto master_error; 
        }
    }

    log_debug("master get binlog, try to sync");

    ladder_timer_reset(&ti);

    heartbeat_inter = time(0);
    while (1)
    {
        /* FIXME */
        //mng_print_thread_status();

        /* send heartbeat */
        curr_time = time(0);
        if (curr_time - heartbeat_inter > 3)
        {
            heartbeat_inter = curr_time;
            if (repl_send_slave(arg->fd, NULL, REPL_FLAG_HEARTBEAT, 0, 0) < 0)
            {
                log_error("repl send to slave heartbeat error, peer[%s:%d], error[%s]",
                        arg->ip, arg->port, strerror(errno));
                goto master_error; 
            }
        }

        i = bin_read2(&bm, &buf, &len, zmalloc, &sid);
        if (i == BL_FILE_OK)
        {
            /* do not send same sid log */
            if (sid == arg->slave_sid)
            {
                log_debug("get same slave sid [%"PRIu64"] log, drop it", sid);
                ladder_timer_reset(&ti);
                zfree(buf);
                continue;
            }

            /* do not send filter log */ 
            if (arg->filter(buf, len, arg->slave_ds_key))
            {
                log_debug("filter record");
                ladder_timer_reset(&ti);
                zfree(buf);
                continue;

            }
            
            curr_ts = bm.curr_ts;
            ret = repl_send_slave(arg->fd, (uint8_t *)buf, REPL_FLAG_DATA, len, curr_ts);
            //log_debug("send to slave len [%d] data:\n%s",len, hex_dump(buf, len));
            log_debug("send to slave len [%d]",len);
            ladder_timer_reset(&ti);
            zfree(buf);
        }
        else if (i == BL_FILE_EOF)
        {
            ret = repl_send_slave(arg->fd, NULL, REPL_FLAG_FIN, 0, 0);
            ladder_timer_sleep(&ti);
        }
        else if (i == BL_FILE_SWITCH)
        {
            /* do not sleep */
            log_debug("binlog file switched");
        }
        else
        {
            ret = repl_send_slave(arg->fd, NULL, REPL_FLAG_ERR, 0, 0);
            ladder_timer_sleep(&ti);
        }
       
        if (ret < 0)
        {
            log_error("repl send to slave error, peer[%s:%d], error[%s]", arg->ip, arg->port, strerror(errno));
            goto master_error; 
        } 
    }
    
master_error:
    /* clean mng */
    memset(grp, 0x00, sizeof(repl_thread_grp_t));
    destroy_arg(arg);

    /* don't close , let free res do it */
    /* close(arg->fd);*/
    bin_destroy(&bm);
    return NULL;
}

int repl_bin_master_start(const char *path, const char *prefix,
                off_t max_size, int max_idx, int net_fd, uint64_t ts, void *res, freeres_func func,
                uint64_t master_sid, uint64_t slave_sid, const char *slave_ds_key, filter_func filter)
{
    pthread_t               th;
    repl_thread_arg_t      *arg;
    char                    ip[64];
    int                     port;

    if (slave_ds_key == NULL)
    {
        log_error("get NULL slave_ds_key");
        return -1;
    }

    if (get_peer_ip(net_fd, ip, &port) < 0)
    {
        log_error("error get peer ip, error[%s][%d]", strerror(errno), net_fd);
        return -1;  
    }
    
    arg = zcalloc(sizeof(repl_thread_arg_t)); 

    arg->ip          = zstrdup(ip);
    arg->port        = port;
    arg->path        = zstrdup(path);
    arg->prefix      = zstrdup(prefix);
    arg->ts          = ts;
    arg->fd          = net_fd;
    arg->max_size    = max_size;
    arg->max_idx     = max_idx;
    arg->res         = res;
    arg->free_res    = func; 
    arg->slave_sid   = slave_sid; 
    arg->master_sid  = master_sid; 
    arg->slave_ds_key= zstrdup(slave_ds_key); 
    arg->filter      = filter; 

    if (mng_thread_create(REPL_MASTER_TH, &th, NULL, master_worker, arg) != 0)
    {
        /* FIXME! close arg fd ? */
        log_error("master start error [%s]", strerror(errno));
        destroy_arg(arg);
        return -1;
    }

    return 0;
}


static void *slave_recv_worker(void *param)
{
    int                     fd = -1;
    int                     redo_fd = -1;
    uint32_t                len;
    int                     flag;
    uint8_t                *rec;
    uint64_t                remote_ts = 0;
    uint64_t                tmp_ts;
    bin_meta_t              bm;
    int                     ret = 0;
    time_t                  status_time = time(0);
    char                    rpath[MAX_FNAME];

    /* timer */
    ladder_timer_t          ti;
    uint32_t                usec_arr[] = {10, 100, 500, 1000};

    repl_thread_grp_t      *grp   = (repl_thread_grp_t *)param;
    repl_thread_t          *my_th = get_repl_thread(grp, REPL_SLAVE_RECV_TH);  
    repl_thread_arg_t      *arg   = my_th->arg;

    uint64_t               master_sid = 0;

    /* get or create relay path */
    if (relay_path(rpath, sizeof(rpath), (uint8_t *)arg->path, (uint8_t *)arg->ip, arg->port) < 0)
    {
        log_error("create relay path error [%s]", strerror(errno));
        goto slave_recv_error; 
    }

    /* try to connect localhost redo_port */
    redo_fd = do_connect("127.0.0.1", arg->redo_port);
        
    if (redo_fd < 0)
    {
        log_error("connect self redo port [127.0.0.1:%d] error[%s]", arg->redo_port, strerror(errno));
        goto slave_recv_error; 
    }

    /* ATTENTION: redo socket is nodelay and nonblock */
    set_tcp_nodelay(redo_fd);
    set_tcp_nonblock(redo_fd);

    /* init meta file */
    memset(&bm, 0x00, sizeof(bm));
    if (meta_init(&bm, (uint8_t *)rpath, (uint8_t *)RELAY_LOG_PREFIX, 0, 0,
                  1, (uint8_t *)RELAY_WRITE_INFO_FILE, O_CREAT | O_TRUNC | O_WRONLY, 0666) < 0)
    {
        log_error("meta file init error [%s]", strerror(errno));
        goto slave_recv_error; 
    }
    
    /* init timer */
    ladder_timer_init(&ti, usec_arr, 4);

    /* init ts */
    remote_ts = arg->ts; 

    /* if specific ts, we record it */
    if (remote_ts != BL_TIMESTAMP_FIRST 
        && remote_ts != BL_TIMESTAMP_LAST)
    {
        bm.remote_ts = remote_ts;
        meta_update(&bm);
    }
    else if (remote_ts == BL_TIMESTAMP_LAST
            && bm.remote_ts != 0)
    {
        /* we has slave sync status info */
        remote_ts = bm.remote_ts; 
    }

    /* recv bin log from master */ 
    while (1)
    {
        /* FIXME */
        //mng_print_thread_status();

        if (is_thread_should_stop(my_th))
        {
            set_repl_status(my_th, RTS_DEAD);
            log_debug("recv worker exit");
            goto slave_recv_error; 
        }

        if (fd < 0)
        {
            /* try to re-connect master */
            while (1)
            {
                if (is_thread_should_stop(my_th))
                {
                    set_repl_status(my_th, RTS_DEAD);
                    log_debug("recv worker exit");
                    goto slave_recv_error; 
                }

                log_debug("try to connect [%s:%d]", arg->ip, arg->port);
                fd = do_connect(arg->ip, arg->port);
                    
                if (fd < 0)
                {
                    log_error("connect master [%s:%d] error[%s]", arg->ip, arg->port, strerror(errno));
                }
                else
                {
                    /* ATTENTION: repl socket is nodelay and nonblock */
                    set_tcp_nodelay(fd);
                    set_tcp_nonblock(fd);

                    /* send sync init command */
                    if (repl_slave_init_send(fd, remote_ts, arg->slave_sid, arg->slave_ds_key) >= 0)
                    {
                        /* first, we try to get master sid */
                        flag = repl_recv_master(fd, &rec, &len, &tmp_ts);
                        log_debug("recv init record len[%d]", len)
                        if (flag == REPL_FLAG_SID && len <= sizeof(master_sid))
                        {
                            memcpy(&master_sid, rec, len);              
                            zfree(rec);
                            log_debug("send sync init success, get master id [%"PRIu64"]", master_sid);
                            ladder_timer_reset(&ti);
                            break;
                        }
                    }
                    log_error("send sync init error");
                    close(fd);
                    fd = -1;
                }
                ladder_timer_sleep(&ti);
            }
        }

        rec = NULL;
        flag = repl_recv_master(fd, &rec, &len, &tmp_ts);
        if (flag < 0)
        {
            log_error("recv from master error");
            close(fd);  
            fd = -1;
            continue;
        }

        log_debug("get master flag [%d]", flag);
        ret = 0;
        switch (flag)
        {
            case REPL_FLAG_DATA:
                remote_ts = tmp_ts;
                ret = repl_slave_redo_send_data(redo_fd, rec, len, master_sid);
                //log_debug("redo send len [%d] data:\n%s",len, hex_dump(rec, len));
                log_debug("redo send len [%d]",len);
                ladder_timer_reset(&ti);
                if (time(0) - status_time > 10)
                {
                    ret = repl_slave_redo_send_stat(redo_fd, (uint8_t *)arg->ip, arg->port, REPL_STATUS_SYNCING, remote_ts);
                    status_time = time(0);
                }
                bm.remote_ts = remote_ts;
                meta_update(&bm);

                break;
            case REPL_FLAG_FIN:
                ret = repl_slave_redo_send_stat(redo_fd, (uint8_t *)arg->ip, arg->port, REPL_STATUS_SYNCED, remote_ts);
                break;
            case REPL_FLAG_ERR:
                /*FIXME! */
                ret = repl_slave_redo_send_stat(redo_fd, (uint8_t *)arg->ip, arg->port, REPL_STATUS_FAIL, remote_ts);
                break;
            case REPL_FLAG_SID:
                log_debug("get peer sid, why?");
                break;
            case REPL_FLAG_HEARTBEAT:
                log_debug("get heartbeat");
                break;
            default:
                log_error("get unknown replicate flag [%d]", flag);
                break;
        }

        if (ret < 0)
        {
            log_error("repl redo send error, peer[%s:%d], error[%s]", arg->ip, arg->port, strerror(errno));
            goto slave_recv_error; 
        } 

        if (rec)
            zfree(rec);
        
        log_debug("redo send end, try to recv");
        repl_slave_redo_recv(redo_fd);
        log_debug("redo recv end");
    }
    return NULL;

slave_recv_error:
    /* report exit status */
    if (redo_fd > 0)
    {
        repl_slave_redo_send_stat(redo_fd, (uint8_t *)arg->ip, arg->port, REPL_STATUS_OVER, remote_ts);
        repl_slave_redo_recv(redo_fd);
        close(redo_fd);
    }

    /* clean mng */
    log_debug("clean repl mng");
    memset(grp, 0x00, sizeof(repl_thread_grp_t));
    destroy_arg(arg);
    if (fd > 0)
        close(fd);
    bin_destroy(&bm);
    return NULL;
}

int repl_bin_slave_start(const char *master_ip, int master_port,
                const char *path, uint64_t ts, int redo_port, uint64_t sid, char *ds_key)
{

    pthread_t               th;
    repl_thread_arg_t      *arg;

    if (ds_key == NULL)
    {
        log_error("get NULL ds_key");
        return -1;
    }

    arg = zcalloc(sizeof(repl_thread_arg_t)); 

    arg->ip           = zstrdup(master_ip);
    arg->port         = master_port;
    arg->redo_port    = redo_port;
    arg->path         = zstrdup(path);
    arg->ts           = ts;
    arg->slave_sid    = sid;
    arg->slave_ds_key = zstrdup(ds_key);

    if (mng_thread_create(REPL_SLAVE_RECV_TH, &th, NULL, slave_recv_worker, arg) != 0)
    {
        destroy_arg(arg);
        return -1;
    }

    return 0;
}



