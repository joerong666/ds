#include <stdlib.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <signal.h>

#include "log.h"
#include "zmalloc.h"
#include "replica_thread_mng.h"

static repl_thread_mng_t   manager;
static pthread_mutex_t     manager_latch = PTHREAD_MUTEX_INITIALIZER;

repl_thread_grp_t *mng_get_free_grp()
{
    int i;
    repl_thread_grp_t *grp = manager.grp_arr;
    
    for (i = 0; i < REPL_MAX_GRP_NUM; i++)
    {
        if (grp->net_mark[0] == 0)
        {
            return grp;    
        }    
        grp++;
    }
    return NULL;
}

repl_thread_grp_t *mng_find_grp(char *ip, int port)
{
    char buf[64];
    int i;
    repl_thread_grp_t *grp = manager.grp_arr;

    snprintf(buf, 64, "%s:%d", ip, port);
    
    for (i = 0; i < REPL_MAX_GRP_NUM; i++)
    {
        if (grp->net_mark[0] != 0
            && strncmp(buf, grp->net_mark, 64) == 0)
        {
            return grp;    
        }    
        grp++;
    }
    return NULL;
}

int mng_stop_grp(const char *ip, int port)
{
    char buf[64];
    int i;
    repl_thread_grp_t *grp = manager.grp_arr;

    snprintf(buf, 64, "%s:%d", ip, port);
    
    for (i = 0; i < REPL_MAX_GRP_NUM; i++)
    {
        if (grp->net_mark[0] != 0
            && strncmp(buf, grp->net_mark, 64) == 0)
        {
            grp->ths[0].stop_flag = 1;
            grp->ths[1].stop_flag = 1;
            grp->ths[2].stop_flag = 1;
            return 0;
        }    
        grp++;
    }
    return -1;
}

int is_grp_all_dead(repl_thread_grp_t *grp)
{
    if ((grp->ths[0].status == RTS_DEAD || grp->ths[0].status == 0)
       && (grp->ths[1].status == RTS_DEAD || grp->ths[1].status == 0)
       && (grp->ths[2].status == RTS_DEAD || grp->ths[2].status == 0))
    {
        return 1;
    }    
    return 0;
}

int mng_clean_grp(char *ip, int port)
{
    char buf[64];
    int i;
    repl_thread_grp_t *grp = manager.grp_arr;

    snprintf(buf, 64, "%s:%d", ip, port);
    
    for (i = 0; i < REPL_MAX_GRP_NUM; i++)
    {
        if (grp->net_mark[0] != 0
            && strncmp(buf, grp->net_mark, 64) == 0)
        {
            memset(grp, 0x00, sizeof(repl_thread_grp_t));
            return 0;
        }    
        grp++;
    }
    return -1;
}

int is_thread_should_stop(repl_thread_t *rt)
{
    return (rt->stop_flag == 1);
}

repl_thread_t *get_repl_thread(repl_thread_grp_t *grp, int type)
{
    if (type >=3 || type < 0)
        return NULL;
    return &grp->ths[type]; 
}

void set_repl_status(repl_thread_t *th, repl_thread_status_e s)
{
    th->status = s; 
}

repl_thread_grp_t *mng_get_grp(char *ip, int port)
{
    repl_thread_grp_t *grp;
 
    if ((grp = mng_find_grp(ip, port)) == NULL)
    {
        grp = mng_get_free_grp(); 
    }

    return grp;
}

void mng_set_status(repl_thread_t *th, repl_thread_status_e stat)
{
    /* ATTENTION: here when thread not run and reload flag been set, thread exit */
    /*if (stat != TS_RUN && g_server.sys_reload_flag == 1)
    {
        pthread_exit(NULL);    
    }*/
    th->status = stat;    
} 

int mng_thread_create(int type, pthread_t *thread,
                    const pthread_attr_t *attr,
                    void *(*start_routine)(void*), repl_thread_arg_t *arg) 
{
    int ret;

    if (type >= 3 || type < 0)
    {
        log_error("invalid repl thread type [%d]", type);
        return -1;
    }

    /* avoid valgrind report error */
    pthread_mutex_lock(&(manager_latch));
    repl_thread_grp_t *grp = mng_get_grp(arg->ip, arg->port);

    if (grp == NULL)
    {
        pthread_mutex_unlock(&(manager_latch));
        log_error("can't get replication thread info [%s:%d]", arg->ip, arg->port);
        return -1;
    }

    grp->ths[type].arg = arg;
    set_repl_status(&grp->ths[type], RTS_INIT);
    snprintf(grp->net_mark, sizeof(grp->net_mark), "%s:%d", arg->ip, arg->port);

    ret = pthread_create(thread, attr, start_routine, grp); 
    pthread_mutex_unlock(&(manager_latch));

    return ret;
}

void  mng_print_thread_status()
{
    int i;
    repl_thread_grp_t *grp = manager.grp_arr;

    for (i = 0; i < REPL_MAX_GRP_NUM; i++)
    {
        if (grp->net_mark[0] != 0)
        {
            log_debug("repl thread group [%s] master[%d] recv[%d] redo[%d]",
                    grp->net_mark,
                    grp->ths[0].status, 
                    grp->ths[1].status,
                    grp->ths[2].status);
        }    
        grp++;
    }
}


