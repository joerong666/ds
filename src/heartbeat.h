#ifndef _HEART_BEAT_H_
#define _HEART_BEAT_H_

#include <pthread.h>

extern int get_my_addr();
extern int init_with_mngr(int local_conf);
extern int start_heart_beat(pthread_t *tid);

extern void set_mngr_list(const char *mngr_list);
extern void set_ds_list(const char *ds_list);

#endif /* _HEART_BEAT_H_ */

