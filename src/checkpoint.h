#ifndef _CHECK_POINT_H_
#define _CHECK_POINT_H_

#include "dbmng.h"

extern void update_checkpoint(dbmng_ctx *ctx, int switch_flg);
extern void dbe_set_one_op(void *dbe, void *param);
extern void do_dbe_set(dbmng_ctx *ctx);
extern void after_dbe_set(dbmng_ctx *ctx);

extern void hangup_cp(dbmng_ctx *ctx);
extern void wakeup_cp(dbmng_ctx *ctx);


#endif /* _CHECK_POINT_H_ */

