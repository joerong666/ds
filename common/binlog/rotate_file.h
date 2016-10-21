#ifndef __ROTATE_FILE_H_DEF__
#define __ROTATE_FILE_H_DEF__

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#define MAX_FNAME 256 

typedef struct rt_file
{
    int     fd;    
    int     flags;
    mode_t  mode;
    int     idx;
    int     max_idx;
    off_t   max_size;     
    uint8_t path[MAX_FNAME];
    uint8_t prefix[MAX_FNAME];
}rt_file_t;

extern int rt_init(rt_file_t *rt, uint8_t *path, uint8_t *prefix, off_t max_size, int max_idx, int idx, int flags, mode_t mode);

extern void rt_file_name(rt_file_t *rt, char *name, int len);

extern int rt_next_idx(rt_file_t *rt);

extern int rt_prev_idx(rt_file_t *rt);

extern int rt_need_rotate(rt_file_t *rt);

extern int rt_switch_file(rt_file_t *rt);

extern int rt_open(rt_file_t *rt);

extern int rt_flags_open(rt_file_t *rt, int flags);

extern ssize_t rt_switch_write(rt_file_t *rt, const void *buf, size_t nbyte);

extern ssize_t rt_pwrite(rt_file_t *rt, const void *buf, size_t nbyte, off_t offset);

extern ssize_t rt_write(rt_file_t *rt, const void *buf, size_t nbyte);

extern ssize_t rt_read(rt_file_t *rt, void *buf, size_t nbyte);

extern ssize_t rt_pread(rt_file_t *rt, void *buf, size_t nbyte, off_t offset);

extern ssize_t rt_switch_read(rt_file_t *rt, void *buf, size_t nbyte);

extern int rt_file_exist(rt_file_t *rt);

extern void rt_next_file_name(rt_file_t *rt, char *name, int len);


#endif
