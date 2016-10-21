#include <stdlib.h>
#include <pthread.h>
#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <string.h>
#include <sys/syscall.h>
#include <signal.h>
extern int pthread_kill (pthread_t __threadid, int __signo);

#include "log.h"
#include "zmalloc.h"
#include "thread_pool.h"
#include "thread_manager.h"

int put_req(thread_pool_t *pool,
            queue_data_t *task,
            int blocking)
{
    task->timestamp = time(0);
    return put_job(pool->req_queue, (void *)task, blocking);
}

int put_req_no_touch(thread_pool_t *pool,
                     queue_data_t *task,
                     int blocking)
{
    return put_job(pool->req_queue, (void *)task, blocking);
}

int put_resp(thread_pool_t *pool,
             queue_data_t *task,
             int blocking)
{
    task->timestamp = time(0);
    return put_job(pool->resp_queue, (void *)task, blocking);
}

int put_resp_no_touch(thread_pool_t *pool,
                      queue_data_t *task,
                      int blocking)
{
    return put_job(pool->resp_queue, (void *)task, blocking);
}

queue_data_t *get_resp(thread_pool_t *pool,
                       int blocking)
{
    return get_job(pool->resp_queue, blocking);
}

queue_data_t *get_req(thread_pool_t *pool,
                      int blocking)
{
    return get_job(pool->req_queue, blocking);
}

/**
 * This is the routine the worker threads do during their life.
 *
 */

#include <sys/prctl.h>
void linux_thread_setname (char const* threadName)
{
        prctl (PR_SET_NAME, threadName, 0, 0, 0);
}

static void *run_worker(void *arg)
{
    thread_info_t *th_info = NULL;
    thread_pool_worker_t *w = NULL;
    thread_pool_t *pool = NULL;
	queue_data_t *task = NULL;
    void *private = NULL;

    if (arg == NULL)
    {
        log_fatal("%s", "thread can't get own manager");
        return NULL; 
    }

    th_info = (thread_info_t *)arg;
    w = (thread_pool_worker_t *)th_info->arg;
    if (w == NULL)
    {
        log_fatal("%s", "thread can't get own worker control");
        return NULL; 
    }
    pool = (thread_pool_t *)w->pool;

    if (w->init)
    {
        if (w->init(&private) < 0)
        {
            log_fatal("%s", "thread init error");
            return NULL; 
        }
    }

    linux_thread_setname(th_info->name);

    //log_debug("%s", "worker running...");
	while (1) 
    {
        if (pool->stop_flag) 
        {
            log_error("%s", "The worker thread stop.");
            break;
        }

        //log_debug("worker get_req() from pool=%p...", pool);
        set_thread_status(th_info, TS_WAIT);
		task = (queue_data_t *)get_req(pool, QUEUE_BLOCKING);
        set_thread_status(th_info, TS_RUN);
        //log_debug("worker get_req() succ, pool=%p, task=%p", pool, task);
		if (task == NULL)
        {
            /* An error has occurred. */
            log_fatal("%s", "get a NULL task when trying to obtain a worker task.");
            log_fatal("%s", "The worker thread has exited.");
            break;
		}

        if (pool->stop_flag) 
        {
            log_error("%s", "The worker thread stop.");
            break;
        }

        if (w->action)
        {
            if (w->action(&private, task) < 0)
            {
                log_fatal("%s", "thread action run error");    
                break;
            }    
        }
        else
        {
            log_fatal("%s", "thread action is NULL");    
            break;
        }
    }

    if (w->cleanup)
        w->cleanup(&private);    

    return NULL;
}

static void cancel_all_threads(thread_pool_t* pool)
{
    pthread_t sid;
    int status;
    void *result;
    int i = 0;
    
    thread_pool_worker_t *w;
    struct list_head *pos, *q;

    list_for_each_safe(pos, q, &(pool->worker_list))
    {
        w = list_entry(pos, thread_pool_worker_t, list);
        sid = w->id;
        status = pthread_cancel(sid);
        if (status != 0)
        {
            log_error("cancel thread [%d] error, ret [%d].", (int)sid, status);
        }

        status = pthread_join(sid, &result);
        if (status != 0)
        {
            log_error("join thread [%d] error, ret [%d].", (int)sid, status);
        }

        if (result == PTHREAD_CANCELED)
        {
            log_info("[%d] thread [%d] cancelled.", i, (int)sid);
        }
        else
        {
            log_error("[%d] thread [%d] was not cancelled", i, (int)sid);
        }

        list_del(&(w->list));
    
        /* FIXME! need more clean */
        zfree(w);
        i++;
    } 
}

void check_threads_alive(thread_pool_t* pool)
{
    pthread_t sid;
    int status;
    
    thread_pool_worker_t *w;
    struct list_head *pos, *q;

    list_for_each_safe(pos, q, &(pool->worker_list))
    {
        w = list_entry(pos, thread_pool_worker_t, list);
        sid = w->id;
        status = pthread_kill(sid, 0);
        if (status == ESRCH)
        {
            log_error("thread [%d] is not alive!!!", (int)sid);
        }
    }
}

int get_thread_pool_req_num(thread_pool_t* pool)
{
    if (pool && pool->req_queue)
        return job_queue_getsize(pool->req_queue);
    return 0;
}

int get_thread_pool_resp_num(thread_pool_t* pool)
{
    if (pool && pool->resp_queue)
        return job_queue_getsize(pool->resp_queue);
    return 0;
}

void destroy_thread_pool(thread_pool_t* pool)
{
    if (pool == NULL)
        return;

    /* cancel threads */
    cancel_all_threads(pool);
    destroy_queue(pool->req_queue);
    destroy_queue(pool->resp_queue);
    if (pool->name)
    {
        zfree(pool->name);
        pool->name = NULL;
    }
    zfree(pool);    
}

int add_thread_pool_worker(thread_pool_t *pool,
                           char *worker_name, 
                           init_func init,
                           action_func action,
                           cleanup_func cleanup, 
                           uint16_t worker_num)
{
    int i = 0;
    thread_pool_worker_t *w = NULL;

    if (worker_num <= 0)
    {
        log_error("worker number is invalid [%d]", worker_num);
        return -1;
    }

    for (i = 0; i < worker_num; i++)
    { 
        if ((w = (thread_pool_worker_t *)zmalloc(sizeof(thread_pool_worker_t))) == NULL)
        {
            log_error("%s", "malloc OOM: thread worker");
            return -1;
        }
        w->init = init;
        w->action = action;
        w->cleanup = cleanup;
        w->pool = pool;

        if (pthread_create_warp(worker_name, &(w->id), NULL, run_worker, w)) 
        {
            log_error("%s", "pthread_create worker");
            zfree(w);
            return -1;
        }

        list_add(&(w->list), &(pool->worker_list)); 
        pool->num_of_worker++;
    }

    return 0;
}

thread_pool_t* create_thread_pool(char *name,
                                  uint32_t queue_size,
                                  queue_flag_e queue_flag)
{
	thread_pool_t *pool;

	/* Create the thread pool struct. */
	if ((pool = (thread_pool_t *)zmalloc(sizeof(thread_pool_t))) == NULL) 
    {
		log_error("malloc OOM: thread_pool_t [%s]", name);
		return NULL;
	}

	pool->stop_flag = 0;
    INIT_LIST_HEAD(&(pool->worker_list));
    pool->num_of_worker = 0;
    pool->queue_flag = queue_flag;

	/* Init the jobs queue. */
    pool->queue_size = queue_size;

    log_debug("pool size is [%d]", pool->queue_size);
	pool->req_queue = create_queue(pool->queue_size);
    if (pool->req_queue == NULL)
    {
        zfree(pool);    
        return NULL;
    }
    
    if (pool->queue_flag == TOW_QUEUE)
    {
        pool->resp_queue = create_queue(pool->queue_size);
        if (pool->resp_queue == NULL)
        {
            destroy_queue(pool->req_queue);
            zfree(pool);    
            return NULL;
        }
    }
    else
    {
        pool->resp_queue = NULL;
    }

    pool->name = zstrdup(name); 
	return pool;
}


#ifdef THREAD_POOL_UNIT_TEST

#define ARR_SIZE 1000000
static pthread_mutex_t count_mutex = PTHREAD_MUTEX_INITIALIZER;
static int count;

static void fast_task(void *ptr)
{
    int *pval = (int*)ptr;
    int i;

    //printf("fast task: count value is %d.\n",count);
    for (i = 0; i < 1000; i++) {
        (*pval)++;
    }

    pthread_mutex_lock(&count_mutex);
    count++;
    pthread_mutex_unlock(&count_mutex);
    return;
}

static void slow_task(void *ptr)
{
    printf("slow task: count value is %d.\n",count);

    pthread_mutex_lock(&count_mutex);
    count++;
    pthread_mutex_unlock(&count_mutex);
    return;
}

queue_data_t *gen_test_task(routine_func rf, void *data)
{
    queue_data_t *task =(queue_data_t *)zmalloc(sizeof( queue_data_t));    
    if (task == NULL)
        return NULL;

    task->routine_cb = rf;
    task->data = data;

    return task;
}

int main(int argc, char **argv)
{
    int arr[ARR_SIZE], i, ret, failed_count = 0;

    for (i = 0; i < ARR_SIZE; i++) {
        arr[i] = i;
    }

    thread_pool_t* pool;  // make a new thread pool structure

    /* Create a pool of 10 thread workers. */
    if ((pool = create_thread_pool(10, 100)) == NULL) {
        printf("Error! Failed to create a thread pool struct.\n");
        exit(-1);
    }

    queue_data_t *task;
    for (i = 0; i < ARR_SIZE; i++) {
        if (i % 10000 == 0) {
            task = gen_test_task(slow_task, arr + i);
        }
        else {
            task = gen_test_task(fast_task, arr + i);
        }
        if (task == NULL)
            printf("task is NULL");

        //printf("task->routine[%d]data[%d]\n", task->routine_cb, task->data);
        put_req(pool, task, QUEUE_BLOCKING);

        if (ret == -1) {
            printf("An error had occurred while adding a task.");
            exit(-1);
        }

        if (ret == -2) {
            failed_count++;
        }
    }

    /* Stop the pool. */
    sleep(1);
    //pool_free(pool,1);
    //thpool_destroy(pool);

    printf("Example ended.\n");
    printf("%d tasks out of %d have been executed.\n",count,ARR_SIZE);
    printf("%d tasks out of %d did not execute since the pool was overloaded.\n",failed_count,ARR_SIZE);
    printf("All other tasks had not executed yet.");

    return 0;
}

#endif




