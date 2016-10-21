#ifndef _DS_ZMALLOC_H
#define _DS_ZMALLOC_H

#define ds_zcalloc ds_zcalloc_m

void *ds_zmalloc(size_t size);
void *ds_zcalloc_m(size_t size);
void *ds_zrealloc(void *ptr, size_t size);
void ds_zfree(void *ptr);
char *ds_zstrdup(const char *s);
size_t ds_zmalloc_used_memory(void);
void ds_zmalloc_enable_thread_safeness(void);
float ds_zmalloc_get_fragmentation_ratio(void);
void ds_update_mem_stat(int n);
void ds_set_mem_stat(size_t n);

#endif /* _DS_ZMALLOC_H */

