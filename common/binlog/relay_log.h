#ifndef __RELAY_LOG_H_DEF__
#define __RELAY_LOG_H_DEF__

#include "rotate_file.h"
#include "meta_file.h"
#include "bin_log.h"

#define MAX_RELAY_LOG_NUM     20
#define MAX_RELAY_LOG_LEN     256000000 
//#define MAX_RELAY_LOG_LEN     2560000 
#define RELAY_LOG_PREFIX      "relay_log"
#define RELAY_READ_INFO_FILE  "relay_read.info"
#define RELAY_WRITE_INFO_FILE "relay_write.info"

#define RELAY_WRITE_FLAG 0
#define RELAY_READ_FLAG  1

typedef struct relay_info
{
    char ip[64];
    int  port;
    char path[MAX_FNAME];
    bin_meta_t bm;
}relay_info_t;
extern int relay_init(relay_info_t *ri, uint8_t *path, uint8_t *ip, uint64_t ts, int port, int flag);
extern int relay_write(relay_info_t *ri, const uint8_t *buf, uint32_t len, uint64_t remote_ts, uint64_t sid);
extern int relay_read(relay_info_t *ri, uint8_t **buf, uint32_t *len, MallocFunc f, uint64_t *sid);
extern int relay_path(char *out_path, int len, uint8_t *path, uint8_t *ip, int port);

#endif
