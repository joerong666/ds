#ifndef _T_ZSET_H_
#define _T_ZSET_H_

#include "redis.h"

extern unsigned char *zzlFind(unsigned char *zl, robj *ele, double *score);
extern unsigned char *zzlDelete(unsigned char *zl, unsigned char *eptr);
extern unsigned char *zzlInsert(unsigned char *zl, robj *ele, double score);
extern unsigned int zzlLength(unsigned char *zl);
extern void zsetConvert(robj *zobj, int encoding);
extern zskiplistNode *zslInsert(zskiplist *zsl, double score, robj *obj);
extern int zslDelete(zskiplist *zsl, double score, robj *obj);

extern int zaddMember(robj *zobj, const char *member, size_t m_len, double score);

#endif /* _T_ZSET_H_ */
