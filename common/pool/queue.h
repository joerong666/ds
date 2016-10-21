
#ifndef QUEUE_DEF_H
#define QUEUE_DEF_H

#include <pthread.h>

#define QUEUE_BLOCKING 1
#define QUEUE_NONBLOCK 0

typedef struct job_queue
{
	unsigned int head;
	unsigned int tail;

	unsigned int num_of_job;
    unsigned int max_jobs;

	void **jobs;

    pthread_mutex_t wait_mutex;
	pthread_mutex_t mutex;
	pthread_cond_t not_empty;
	pthread_cond_t not_full;
}job_queue_t;

extern job_queue_t *create_queue(unsigned int size);
extern void* get_job(job_queue_t *queue, int blocking);
extern int put_job(job_queue_t *queue, void *data, int blocking);
extern int job_queue_getsize(job_queue_t *queue);
extern int job_queue_getmax(job_queue_t *queue);
extern void destroy_queue(job_queue_t *queue);

#endif


