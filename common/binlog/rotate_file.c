#define  _XOPEN_SOURCE 500 
#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>

#include "file_util.h"
#include "rotate_file.h"
#include "log.h"

int rt_init(rt_file_t *rt, uint8_t *path, uint8_t *prefix, off_t max_size, int max_idx, int idx, int flags, mode_t mode)
{
    if (max_idx <= 0 || path == NULL || prefix == NULL)
        return -1;

    rt->fd       = -1;    
    rt->idx      = idx;
    rt->max_idx  = max_idx;
    rt->max_size = max_size;
    rt->flags    = flags;
    rt->mode     = mode;
    strncpy((char *)rt->path, (char *)path, sizeof(rt->path) - 1);
    strncpy((char *)rt->prefix, (char *)prefix, sizeof(rt->prefix) - 1);
    
    return 0;
}

void rt_file_name(rt_file_t *rt, char *name, int len)
{
    snprintf(name, len, "%s/%s.%03d", rt->path, rt->prefix, rt->idx);
    log_debug("----------------- [%s] [%s] [%d]\n", name, rt->prefix, rt->idx);
}

void rt_next_file_name(rt_file_t *rt, char *name, int len)
{
    snprintf(name, len, "%s/%s.%03d", rt->path, rt->prefix, rt_next_idx(rt));
    //log_debug("next---------------- [%s] [%s] [%d]\n", name, rt->prefix, rt_next_idx(rt));
}

int rt_next_idx(rt_file_t *rt)
{
    return (rt->idx + 1) % rt->max_idx;    
}

int rt_prev_idx(rt_file_t *rt)
{
    return (rt->idx - 1 + rt->max_idx) % rt->max_idx; 
}

int rt_need_rotate(rt_file_t *rt)
{
    return (file_len(rt->fd) >= rt->max_size);
}

int rt_switch_file(rt_file_t *rt)
{
    int idx;
    int fd;

    idx = rt->idx;
    fd  = rt->fd;

    rt->idx = rt_next_idx(rt);
    if (rt_open(rt) < 0)
    {
        rt->idx = idx;
        rt->fd  = fd;
        return -1;    
    }
    else
    {
        if (fd >= 0)
        {
            close(fd);
        }
    }
    return 0;
}

int rt_file_exist(rt_file_t *rt)
{
    char fn[MAX_FNAME];

    rt_file_name(rt, fn, sizeof(fn));

    return file_exist(fn);
}

int rt_open(rt_file_t *rt)
{
    int  n;
    char fn[MAX_FNAME];
    
    rt_file_name(rt, fn, sizeof(fn));
    n = open(fn, rt->flags, rt->mode);

    if (n >= 0)
        rt->fd = n;

    return n;
}

int rt_flags_open(rt_file_t *rt, int flags)
{
    int  n;
    char fn[MAX_FNAME];
    
    rt_file_name(rt, fn, sizeof(fn));
    n = open(fn, flags, rt->mode);

    if (n >= 0)
        rt->fd = n;

    return n;
}

ssize_t rt_switch_write(rt_file_t *rt, const void *buf, size_t nbyte)
{
    if (rt_need_rotate(rt))
    {
        rt_switch_file(rt);    
    }
    return write(rt->fd, buf, nbyte);
}

ssize_t rt_pwrite(rt_file_t *rt, const void *buf, size_t nbyte, off_t offset)
{
    return pwrite(rt->fd, buf, nbyte, offset);
}

ssize_t rt_write(rt_file_t *rt, const void *buf, size_t nbyte)
{
    return write(rt->fd, buf, nbyte);
}

ssize_t rt_read(rt_file_t *rt, void *buf, size_t nbyte)
{
    return read(rt->fd, buf, nbyte);
}

ssize_t rt_pread(rt_file_t *rt, void *buf, size_t nbyte, off_t offset)
{
    return pread(rt->fd, buf, nbyte, offset);
}

ssize_t rt_switch_read(rt_file_t *rt, void *buf, size_t nbyte)
{
    if (file_pos(rt->fd) >= rt->max_size)
    {
        rt_switch_file(rt);    
    }
    return read(rt->fd, buf, nbyte);
}


