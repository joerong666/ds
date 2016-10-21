#include "redis.h"
#include "zmalloc.h"
#include "ds_zmalloc.h"
#include "log.h"

#if 0
#define update_zmalloc_stat_alloc(_n) do { \
    if (zmalloc_thread_safe) { \
        pthread_mutex_lock(&used_memory_mutex);  \
        used_memory += _n; \
        pthread_mutex_unlock(&used_memory_mutex); \
    } else { \
        used_memory += _n; \
    } \
} while(0)
static int zmalloc_thread_safe = 0;
static pthread_mutex_t used_memory_mutex = PTHREAD_MUTEX_INITIALIZER;
#endif
static size_t used_memory = 0;

void *ds_zmalloc(size_t size)
{
    int len;
    void *ptr = zmalloc_ex(size, &len);
    if (ptr)
    {
        //log_debug("ds_zmalloc(): size=%d, len=%d", size, len);
        //update_zmalloc_stat_alloc(len);
    }
    return ptr;
}

void *ds_zcalloc_m(size_t size)
{
    int len;
    void *ptr = zcalloc_m_ex(size, &len);
    if (ptr)
    {
        //log_debug("ds_zcalloc_m(): size=%d, len=%d", size, len);
        //update_zmalloc_stat_alloc(len);
    }
    return ptr;
}

void *ds_zrealloc(void *ptr, size_t size)
{
    int len;
    void *newptr = zrealloc_ex(ptr, size, &len);
    if (newptr)
    {
        //log_debug("ds_zrealloc(): size=%d, len=%d", size, len);
        //update_zmalloc_stat_alloc(len);
    }
    return newptr;
}

void ds_zfree(void *ptr)
{
    int len;
    zfree_ex(ptr, &len);
    //update_zmalloc_stat_alloc(-len);
    //log_debug("ds_zfree(): len=%d", len);
}

char *ds_zstrdup(const char *s)
{
    int len;
    char *ptr = zstrdup_ex(s, &len);
    if (ptr)
    {
        //log_debug("ds_zstrdup(): len=%d", len);
        //update_zmalloc_stat_alloc(len);
    }
    return ptr;
}

size_t ds_zmalloc_used_memory(void)
{
    size_t um;
    //if (zmalloc_thread_safe) pthread_mutex_lock(&used_memory_mutex);
    um = server.prtcl_redis ? zmalloc_used_memory() : used_memory;
    //if (zmalloc_thread_safe) pthread_mutex_unlock(&used_memory_mutex);
    return um;
}

void ds_zmalloc_enable_thread_safeness(void)
{
    zmalloc_enable_thread_safeness();
    //zmalloc_thread_safe = 1;
}

/* Fragmentation = RSS / allocated-bytes */
float ds_zmalloc_get_fragmentation_ratio(void)
{
    return (float)zmalloc_get_rss()/ds_zmalloc_used_memory();
}

void ds_update_mem_stat(int n)
{
    if (n) used_memory += n;
}

void ds_set_mem_stat(size_t n)
{
    used_memory = n;
}

