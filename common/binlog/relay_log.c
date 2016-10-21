/**
 * We use bin log impl relay log
 */

#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>

#include "file_util.h"
#include "bin_log_impl.h"
#include "relay_log.h"
#include "log.h"

int relay_path(char *out_path, int len, uint8_t *path, uint8_t *ip, int port) 
{
    snprintf(out_path, len, "%s/%s_%d", path, ip, port);

    if (mkpath(out_path, 0777) != 0)
    {
        log_error("can't create replay log path [%s] error[%s]", out_path, strerror(errno));
        return -1;
    }
    return 0;
}

int relay_init(relay_info_t *ri, uint8_t *path, uint8_t *ip, uint64_t ts, int port, int flag)
{
    int ret;

    snprintf(ri->path, sizeof(ri->path), "%s/%s_%d", path, ip, port);
    strncpy(ri->ip, (char *)ip, sizeof(ri->ip) - 1);
    ri->port = port;
    
    if (mkpath(ri->path, 0777) != 0)
    {
        log_error("can't create replay log path [%s] error[%s]", ri->path, strerror(errno));
        return -1;
    }

    if (flag == RELAY_WRITE_FLAG)
    {
        ret = bin_init(&ri->bm, (uint8_t *)ri->path, (uint8_t *)RELAY_LOG_PREFIX,
                     MAX_RELAY_LOG_LEN, MAX_RELAY_LOG_NUM,
                     ts, 1, (uint8_t *)RELAY_WRITE_INFO_FILE,
                     O_CREAT | O_TRUNC | O_WRONLY, 0666);
    }
    else
    {
        ret = bin_init(&ri->bm, (uint8_t *)ri->path, (uint8_t *)RELAY_LOG_PREFIX,
                     MAX_RELAY_LOG_LEN, MAX_RELAY_LOG_NUM,
                     ts, 1, (uint8_t *)RELAY_READ_INFO_FILE,
                     O_RDONLY, 0600);

    }

    return ret;
}

int relay_write(relay_info_t *ri, const uint8_t *buf, uint32_t len, uint64_t remote_ts, uint64_t sid)
{
    ri->bm.remote_ts = remote_ts; 
    return bin_write_v2(&ri->bm, buf, len, sid);
}

int relay_read(relay_info_t *ri, uint8_t **buf, uint32_t *len, MallocFunc f, uint64_t *sid)
{
    return bin_read2(&ri->bm, buf, len, f, sid);
}


