#ifndef _LOG_H_
#define _LOG_H_

#ifdef FOR_UNIT_TEST
#include <stdio.h>

#define log_debug(format, ...)   printf(format, ##__VA_ARGS__);
#define log_info(format, ...)    printf(format, ##__VA_ARGS__); 
#define log_warn(format, ...)    printf(format, ##__VA_ARGS__); 
#define log_error(format, ...)   printf(format, ##__VA_ARGS__);
#define log_fatal(format, ...)   printf(format, ##__VA_ARGS__);
#define log_prompt(format, ...)  printf(format, ##__VA_ARGS__);
#define log_test(format, ...)    printf(format, ##__VA_ARGS__);

#else

#include "redis.h"
#include "pf_log.h"

#define log_debug(format, ...)   redisLogW(__FILE__, __LINE__, g_tag, g_tid, PF_LOG_DEBUG, format, ##__VA_ARGS__);
#define log_info(format, ...)    redisLogW(__FILE__, __LINE__, g_tag, g_tid, PF_LOG_INFO, format, ##__VA_ARGS__); 
#define log_warn(format, ...)    redisLogW(__FILE__, __LINE__, g_tag, g_tid, PF_LOG_WARN, format, ##__VA_ARGS__); 
#define log_error(format, ...)   redisLogW(__FILE__, __LINE__, g_tag, g_tid, PF_LOG_ERROR, format, ##__VA_ARGS__);
#define log_fatal(format, ...)   redisLogW(__FILE__, __LINE__, g_tag, g_tid, PF_LOG_FATAL, format, ##__VA_ARGS__);
#define log_prompt(format, ...)  redisLogW(__FILE__, __LINE__, g_tag, g_tid, PF_LOG_PROMPT, format, ##__VA_ARGS__);

#define log_debug2(tag, format, ...)   redisLogW(__FILE__, __LINE__, tag, -2, PF_LOG_DEBUG, format, ##__VA_ARGS__);
#define log_info2(tag, format, ...)    redisLogW(__FILE__, __LINE__, tag, -2, PF_LOG_INFO, format, ##__VA_ARGS__); 
#define log_warn2(tag, format, ...)    redisLogW(__FILE__, __LINE__, tag, -2, PF_LOG_WARN, format, ##__VA_ARGS__); 
#define log_error2(tag, format, ...)   redisLogW(__FILE__, __LINE__, tag, -2, PF_LOG_ERROR, format, ##__VA_ARGS__);
#define log_fatal2(tag, format, ...)   redisLogW(__FILE__, __LINE__, tag, -2, PF_LOG_FATAL, format, ##__VA_ARGS__);
#define log_prompt2(tag, format, ...)  redisLogW(__FILE__, __LINE__, tag, -2, PF_LOG_PROMPT, format, ##__VA_ARGS__);

#ifdef _DS_TEST_DEBUG_
#define log_test(format, ...)          redisLogW(__FILE__, __LINE__, g_tag, g_tid, PF_LOG_PROMPT, format, ##__VA_ARGS__);
#else
#define log_test(format, ...)
#endif

#endif

extern void log_buffer(const char *buf, int len);
extern void log_string(const char *buf, int len);

extern void log_buffer2(const char *buf, int len, int tag);
extern void log_string2(const char *buf, int len, int tag);

extern void print_obj(const robj *o);

#endif /* _LOG_H_ */

