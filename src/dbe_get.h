#ifndef _DBE_GET_H_
#define _DBE_GET_H_

#include "redis.h"
#include "dbmng.h"

extern int block_client_on_dbe_get(redisClient *c);
extern void do_dbe_get(void *);
extern void after_dbe_get(void *);
extern void check_dbe_get_timer();


#endif /* _DBE_GET_H_ */

