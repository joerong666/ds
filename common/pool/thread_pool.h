#ifndef THREAD_POOL_DEF_H                                         
#define THREAD_POOL_DEF_H 

#include <stdint.h>
#include "queue.h" 
#include "list.h"

typedef enum queue_flag                                           
{
    TOW_QUEUE = 0,
    ONLY_REQ_QUEUE = 1                                            
}queue_flag_e;

typedef struct thread_pool 
{
    char                 *name;

    job_queue_t          *req_queue;
    job_queue_t          *resp_queue;
    uint32_t             queue_size;
    queue_flag_e         queue_flag;

    struct list_head     worker_list;
    uint16_t             num_of_worker;

    volatile uint16_t    stop_flag;
}thread_pool_t;

typedef struct queue_data
{
    time_t timestamp;

    /*worker should run this function*/
    void *routine_cb;
    void *data;
}queue_data_t;

typedef int  (*init_func)(void **private);
typedef int  (*action_func)(void **private, queue_data_t *task);
typedef void (*cleanup_func)(void **private); 

typedef struct thread_pool_worker
{
    struct list_head list;

    pthread_t        id; 

    init_func        init; 
    action_func      action; 
    cleanup_func     cleanup; 

    thread_pool_t    *pool;

}thread_pool_worker_t;

extern int put_req(thread_pool_t *pool, queue_data_t *task, int blocking);
extern int put_req_no_touch(thread_pool_t *pool, queue_data_t *task, int blocking);
extern int put_resp(thread_pool_t *pool, queue_data_t *task, int blocking);
extern int put_resp_no_touch(thread_pool_t *pool, queue_data_t *task, int blocking);
extern queue_data_t *get_resp(thread_pool_t *pool, int blocking);
extern queue_data_t *get_req(thread_pool_t *pool, int blocking);
extern void check_threads_alive(thread_pool_t* pool);
extern int get_thread_pool_req_num(thread_pool_t* pool);
extern int get_thread_pool_resp_num(thread_pool_t* pool);
extern void destroy_thread_pool(thread_pool_t* pool);
extern int add_thread_pool_worker(thread_pool_t *pool, char *worker_name, init_func init, action_func action, cleanup_func cleanup, uint16_t worker_num);
extern thread_pool_t* create_thread_pool(char *name, uint32_t queue_size, queue_flag_e queue_flag);

#endif


