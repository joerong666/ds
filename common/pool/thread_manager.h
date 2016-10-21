#ifndef THREAD_MANAGER_DEF_H
#define THREAD_MANAGER_DEF_H

#include <stdlib.h>
#include <pthread.h>

#define THREAD_MAX_NUM  512 

typedef enum 
{
    TS_INIT = 0,
    TS_WAIT,  
    TS_RUN,
    TS_DEAD,
    TS_UNKNOWN 
}thread_status_e;

typedef struct 
{
    char            *name;
    pthread_t       ppid;
    thread_status_e status;
    time_t          start_time;
    void            *arg;
}thread_info_t;

typedef struct
{
    thread_info_t info_arr[THREAD_MAX_NUM];
    int thread_num;
}thread_manager_t;

extern int pthread_create_warp(const char *name, pthread_t *thread, const pthread_attr_t *attr, void *(*start_routine)(void*), void *arg);

extern void check_threads_status();
extern thread_info_t *get_free_thread_info();
extern int add_thread_info(thread_info_t *info, const char *name, pthread_t ppid);
extern void init_thread_mng();
extern void set_thread_status(thread_info_t *info, thread_status_e stat);
extern int get_threads_info(char *out, unsigned int outlen);

#endif



