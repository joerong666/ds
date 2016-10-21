#ifndef _RESTORE_KEY_H_
#define _RESTORE_KEY_H_

#include "redis.h"

typedef struct val_attr_t
{
    robj *val;
    uint64_t expire_ms; // in ms, 0 means for ever
} val_attr;

/* ret:
 * 0 - succ
 * 1 - not found
 * 2 - dbe fail
 * 3 - process fail
 */
extern int restore_key_from_dbe(void *db, const char *key, size_t key_len, char key_type, val_attr *rslt);

#endif /* _RESTORE_KEY_H_ */
