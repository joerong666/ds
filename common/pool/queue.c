#include <stdlib.h>
#include <pthread.h>
#include <stdio.h>
#include <unistd.h>

#include "queue.h"
#include "zmalloc.h"
#include "log.h"


/**
 * adds data to the tail of the queue.
 */
static int job_queue_enqueue(job_queue_t *queue, void *data)
{
	if (queue->num_of_job >= queue->max_jobs) 
    {
		log_error("%s", "The queue is full, unable to add data to it.");
		return -1;
	}

	if (queue->jobs[queue->tail] != (void *)NULL) 
    {
		log_error("%s", "queue expected NULL, but found a different value.");
		return -2;
	}

	queue->jobs[queue->tail] = data;

	queue->num_of_job++;
	queue->tail++;

	if (queue->tail >= queue->max_jobs) 
    {
		queue->tail = 0;
	}

	return 0;
}

/**
 * removes and returns the head data element in the queue.
 */

static void *job_queue_dequeue(job_queue_t *queue)
{
	void *data;

	if (queue->num_of_job == 0) 
    {
        log_error("%s", "Try to dequeue from an empty queue.");
        return NULL;
	}

	data = queue->jobs[queue->head];

	queue->jobs[queue->head] = NULL;
	queue->num_of_job--;

	if (queue->num_of_job == 0) 
    {
		queue->head = 0;
		queue->tail = 0;
	}
	else 
    {
		queue->head++;
		if (queue->head >= queue->max_jobs) 
        {
			queue->head = 0;
		}
	}

	return data;
}

/**
 * checks if a given queue is empty.
 */
static int job_queue_is_empty(job_queue_t *queue)
{
	if (queue->num_of_job == 0) 
    {
		return 1;
	}

	return 0;
}

static int job_queue_is_full(job_queue_t *queue)
{
	if (queue->num_of_job >= queue->max_jobs) 
    {
		return 1;
	}

	return 0;
}


/**
 * inits a threadpool queue.
 */
job_queue_t *create_queue(unsigned int size)
{
    job_queue_t *queue = zmalloc(sizeof(job_queue_t));
    if (queue == NULL)
    {
		log_error("%s", "create error.");
        return NULL;
    }

    queue->max_jobs = size;
    queue->jobs = zcalloc(sizeof(void *) * size);

    if (queue->jobs == NULL)
    {
        log_error("%s", "create queue jobs error.");
        zfree(queue);
        return NULL;
    }
 
    log_debug("sizeof queue is [%d]", sizeof(void *) * size);

	queue->head = 0;
	queue->tail = 0;
	queue->num_of_job = 0;

	pthread_mutex_init(&queue->wait_mutex, NULL); 
	pthread_mutex_init(&queue->mutex, NULL); 
	pthread_cond_init(&queue->not_empty, NULL);
	pthread_cond_init(&queue->not_full, NULL);

    return queue;
}

void destroy_queue(job_queue_t *queue)
{
    if (queue != NULL)
    {
        if (queue->jobs != NULL)
        {
            zfree(queue->jobs);
        }
        zfree(queue);
    }
}

inline void lock(job_queue_t *queue)                              
{                                                                 
    pthread_mutex_lock(&(queue->wait_mutex));                 
}                                                                 
inline void unlock(job_queue_t *queue)                            
{                                                                 
    pthread_mutex_unlock(&(queue->wait_mutex));               
}      

void* get_job(job_queue_t *queue, int blocking)
{
    //log_debug("%s", "get_job()...");
    void *job;

	if (queue == NULL) 
    {
		log_error("%s", "The threadpool received as argument is NULL.");
		return NULL;
	}

    /**
     * The linux pthread_cond_signal may cause thundering herd
     * on multiprocessor, so we let thread in one by one.
     */
    if (blocking == QUEUE_BLOCKING)                           
    {
        lock(queue);                                      
    }       
    //log_debug("%s", "starting to get from queue");
        
	/* Obtain a task */
	pthread_mutex_lock(&(queue->mutex));

	while (job_queue_is_empty(queue)) 
    {
        if (blocking == QUEUE_BLOCKING)
        {    
            /* Block until a new task arrives. */
            //log_debug("%s", "waiting for queue...");
            if (pthread_cond_wait(&queue->not_empty,&queue->mutex)) 
            {
                log_error("%s", "pthread_cond_wait error!");
                pthread_mutex_unlock(&(queue->mutex));
                unlock(queue);
                return NULL;
            }
        }
        else
        {
            log_debug("%s", "is empty, nonblocking return.");
            pthread_mutex_unlock(&(queue->mutex));
            return NULL;
        }
            
    }

    //log_debug("%s", "dequeue...");
	if ((job = job_queue_dequeue(queue)) == NULL) 
    {
		/* Since task is NULL returning task will return NULL as required. */
		log_error("%s", "Failed to obtain a task from the jobs queue.");
	}

    pthread_cond_signal(&queue->not_full); 
	pthread_mutex_unlock(&(queue->mutex));

    
    if (blocking == QUEUE_BLOCKING)
    {
        unlock(queue);
    }


	return job;
}

int put_job(job_queue_t *queue, void *data, int blocking)
{
    //log_debug("put_job(): queue=%p, data=%p", queue, data);

    int ret;

	if (queue == NULL) 
    {
		log_error("%s", "The threadpool received as argument is NULL.");
		return -1;
	}

	/* obtain a task */
	pthread_mutex_lock(&queue->mutex);

	while (job_queue_is_full(queue)) 
    {
        if (blocking == QUEUE_BLOCKING)
        {    
            /* block until a new task arrives. */
            if (pthread_cond_wait(&(queue->not_full),&(queue->mutex))) 
            {
                log_error("%s", "pthread_cond_wait: ");
                pthread_mutex_unlock(&queue->mutex);
                return -1;
            }
        }
        else
        {
            //log_error("%s", "is full, nonblocking return .");
            pthread_mutex_unlock(&queue->mutex);
            return -2;
        }
    }

	if ((ret = job_queue_enqueue(queue, data)) != 0) 
    {
		/*FIXME!! Since task is NULL returning task will return NULL as required. */
		log_error("%s", "Failed to put a task to the jobs queue.");
	}

    pthread_cond_signal(&queue->not_empty); 
	pthread_mutex_unlock(&queue->mutex);

    //log_debug("%s", "enqueue succ and wake up");

    if (ret != 0)
        return -3;
	return 0;
}

/**
 * This function queries for the size of the given queue argument.
 */
int job_queue_getsize(job_queue_t *queue)
{
	return queue->num_of_job;
}

int job_queue_getmax(job_queue_t *queue)
{
	return queue->max_jobs;
}


#ifdef QUEUE_UNIT_TEST

job_queue_t *queue;

void* consumer()
{

    int *p;
    int i;

    while (1) {
        p = get_job(queue, 1);
        if (p == NULL)
            printf("get error\n");
        else
        {
            printf("[%d] get[%d]\n", pthread_self(), *p);
            zfree(p);
        }
    }
}

void* producer()
{
    int i;
    int *p; 

    sleep(1);
    for (i = 0 ; i < 100000; i++)
    {
        p = zmalloc(sizeof(int));
        *p = i;
        if (put_job(queue, (void *)p, 1) < 0)
            printf("put error\n");
    }

    printf("[%d] exit\n", pthread_self());
}


int main()
{
    int j;

    queue = create_queue(10);

    pthread_t ppid;
    int m;
    for (m = 0; m < 8; m++)
        pthread_create(&ppid, NULL, producer, NULL);

    for (m = 0; m < 15; m++)
        pthread_create(&ppid, NULL, consumer, NULL);

    getchar();
    return 0;
}

#endif


