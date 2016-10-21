#ifndef __META_FILE_H_DEF
#define __META_FILE_H_DEF

#include "rotate_file.h"

typedef struct bin_meta
{
    uint8_t   name[MAX_FNAME];
    int       fd;
    int       need_persist;
    uint64_t  curr_ts;
    uint64_t  curr_offset;
    uint64_t  remote_ts; 
    rt_file_t rt;     
}bin_meta_t;

extern int meta_update(bin_meta_t *bm);
extern int meta_get(bin_meta_t *bm);
extern void meta_set(bin_meta_t *bm, int idx, off_t offset, uint64_t ts);
extern int meta_init(bin_meta_t *bm, uint8_t *path, uint8_t *prefix, off_t max_size, int max_idx, int meta_persist_flag, uint8_t *meta_name, int flags, mode_t mode);

extern void meta_close(bin_meta_t *bm);
#endif
