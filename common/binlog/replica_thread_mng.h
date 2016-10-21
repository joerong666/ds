#ifndef __REPL_THREAD_MANAGER_DEF_H__
#define __REPL_THREAD_MANAGER_DEF_H__

#include <stdlib.h>
#include <pthread.h>

#define REPL_MAX_GRP_NUM  256 

typedef enum 
{
    RTS_INIT = 1,
    RTS_WAIT,  
    RTS_WAIT_BIN_INIT,  
    RTS_WAIT_RELAY_INIT,  
    RTS_RUN,
    RTS_DEAD,
    RTS_UNKNOWN 
}repl_thread_status_e;

#define REPL_MASTER_TH       0 
#define REPL_SLAVE_RECV_TH   1 
#define REPL_SLAVE_REDO_TH   2

typedef void (*freeres_func)(void *c);
typedef int (*filter_func)(const char *bl_ptr, int bl_len, const char *ds_key); 

typedef struct
{
    char                   *ip;
    int                     port;
    int                     redo_port;
    char                   *path; 
    char                   *prefix; 
    int                     fd; 
    int                     max_idx; 
    off_t                   max_size; 
    uint64_t                ts; 
    uint64_t                slave_sid; 
    uint64_t                master_sid; 
    char                   *slave_ds_key; 
    void                   *res;
    freeres_func            free_res;
    filter_func             filter; 
}repl_thread_arg_t;

typedef struct
{
    time_t                  start_time;
    char                    stop_flag;
    pid_t                   tid;
    repl_thread_status_e    status;
    repl_thread_arg_t      *arg;
}repl_thread_t;

typedef struct 
{
    char                    net_mark[64];
    repl_thread_t           ths[3]; 
    uint64_t                remote_ts;
}repl_thread_grp_t;

typedef struct
{
    repl_thread_grp_t       grp_arr[REPL_MAX_GRP_NUM];
    int                     grp_num;
}repl_thread_mng_t;

extern repl_thread_t *get_repl_thread(repl_thread_grp_t *grp, int type);
extern int mng_stop_grp(const char *ip, int port);
extern void set_repl_status(repl_thread_t *th, repl_thread_status_e s);
extern void  mng_print_thread_status();
extern int mng_thread_create(int type, pthread_t *thread, const pthread_attr_t *attr, void *(*start_routine)(void*), repl_thread_arg_t *arg);
extern int is_thread_should_stop(repl_thread_t *rt);
extern int is_grp_all_dead(repl_thread_grp_t *grp);


#endif



