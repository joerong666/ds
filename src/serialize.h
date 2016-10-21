#ifndef _SERIALIZE_H_
#define _SERIALIZE_H_

#include "redis.h"

extern int serializeObjLen(const robj *o);
extern int serializeObj2Buf(const robj *o, char *buf, int len, int expire);
extern char *serializeObj(const robj *o, int *pLen);
extern char *serializeObjExp(const robj *o, int *pLen, int expire);
extern robj *unserializeObj(const char *s, int len, time_t *expire);
extern int getVersion(const char *s, int len, uint64_t *version, time_t *expire);

extern char *serializeStrExp(const char *buf, int buf_len, int *pLen, int expire);
extern void freeStr(char *str);

/*return :
 * 0 - valid value
 * 1 - illegal value(expired)
 */
extern int32_t value_filter(const char *val, size_t len, int *exp_time);

#endif // _SERIALIZE_H_

