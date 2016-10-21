#include <stdlib.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <signal.h>
extern int pthread_kill (pthread_t __threadid, int __signo);

#include "thread_manager.h"
#include "log.h"
#include "zmalloc.h"

static thread_manager_t thread_mng;
static pthread_mutex_t thread_mng_latch = PTHREAD_MUTEX_INITIALIZER;

void init_thread_mng()
{
    memset(&thread_mng, 0x00, sizeof(thread_mng));
}

thread_info_t *get_free_thread_info()
{
    int i;
    thread_info_t *info = thread_mng.info_arr;
    
    for (i = 0; i < THREAD_MAX_NUM; i++)
    {
        if (info->ppid == 0x00)
        {
            return info;    
        }    
        info++;
    }
    return NULL;
}

void set_thread_status(thread_info_t *info, thread_status_e stat)
{
    /* ATTENTION: here when thread not run and reload flag been set, thread exit */
    info->status = stat;    
} 

int add_thread_info(thread_info_t *info, const char *name, pthread_t ppid)
{
    if (info == NULL)
    {
        log_error("%d", "thread manager is full, maybe we create too many threads");
        return -1;
    }
    else
    {
        info->name = zstrdup(name);
        info->ppid = ppid;    

        info->start_time = time(NULL);
        thread_mng.thread_num++;
    }

    return 0;
} 

void check_threads_status()
{
    int i;
    int status;
    thread_info_t *info = thread_mng.info_arr;
    
    for (i = 0; i < THREAD_MAX_NUM; i++)
    {
        if (info->ppid != 0x00)
        {
            status = pthread_kill(info->ppid, 0);
            if (status == ESRCH)
            {
                if (info->name != NULL)
                {
                    log_error("thread [%s] ppid [%d] is dead!", info->name, (int)info->ppid);    
                }
                info->status = TS_DEAD;
                thread_mng.thread_num--;
                memset(info, 0x00, sizeof(thread_info_t));
            }
        }    
        
        info++;
    }
}

int pthread_create_warp(const char *name, pthread_t *thread, const pthread_attr_t *attr, void *(*start_routine)(void*), void *arg) 
{
    int ret;

    /* avoid valgrind report error */
    pthread_mutex_lock(&(thread_mng_latch));
    thread_info_t *info = get_free_thread_info();

    if (info == NULL)
    {
        pthread_mutex_unlock(&(thread_mng_latch));
        log_error("%s", "can't get free thread info");
        return -1;
    }

    info->status = TS_INIT;
    info->arg = arg;
    ret = pthread_create(thread, attr, start_routine, info); 
    add_thread_info(info, name, *thread);
    pthread_mutex_unlock(&(thread_mng_latch));

    return ret;
}

static char *status2str(thread_status_e s)
{
    char *ret;

    switch (s)
    {
        case TS_INIT:
            ret = "init";
            break;
        case TS_WAIT:
            ret = "wait";
            break;
        case TS_RUN:
            ret = "run";
            break;
        case TS_DEAD:
            ret = "dead";
            break;
        case TS_UNKNOWN:
            ret = "unknown";
            break;
        default:
            ret = "error status";
            break;
    }
    return ret;    
}

int get_threads_info(char *out, unsigned int outlen)
{
    int i;
    int pos = 0;    
    thread_info_t *info = thread_mng.info_arr;
    
    /* not tight */
    if (outlen < 128) 
        return -1;

    //pos += snprintf(out + pos , outlen - pos, "total threads is: [%d]\n", thread_mng.thread_num);

    for (i = 0; i < THREAD_MAX_NUM; i++)
    {
        if (info->ppid != 0x00)
        {
            pos += snprintf(out + pos , outlen - pos, "name=%s ppid=%u status=%s start_time=%d \n", info->name == NULL ? "unknown" : info->name, (int)info->ppid, status2str(info->status), (int)info->start_time);

            /* not tight */
            if (pos > (signed)outlen - 128)
                break; 
        }
        
        info++;
    }

    return pos;
} 


