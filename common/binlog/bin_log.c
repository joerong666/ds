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

#include "bin_log.h"

/**
 * for interface compatible
 */

uint64_t bin_nowts()
{
    return bin_timestamp();
}

int bin_get(int fd, uint8_t **buf, uint32_t *len, off_t *offset, MallocFunc f)
{
    uint64_t ts = 0;
    uint64_t sid = 0;

    return bin_read(fd, buf, len, offset, &ts, f, &sid);
}

int bin_get_v2(int fd, uint8_t **buf, uint32_t *len, off_t *offset, MallocFunc f, uint64_t *sid)
{
    uint64_t ts = 0;

    return bin_read(fd, buf, len, offset, &ts, f, sid);
}

int bin_put(int fd, const uint8_t *buf, uint32_t len)
{
    uint64_t ts = 0;
    return bin_write_internal(fd, buf, len, &ts);
}

int bin_put_v2(int fd, const uint8_t *buf, uint32_t len, uint64_t sid)
{
    uint64_t ts = 0;
    return bin_write_internal_v2(fd, buf, len, &ts, sid);
}

int bin_get_ts(int fd, off_t *offset, uint64_t *timestamp)
{
    return bin_read_ts(fd, offset, timestamp);
}
