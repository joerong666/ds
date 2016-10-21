#ifndef __BIN_LOG_IMPL_H_DEF__
#define __BIN_LOG_IMPL_H_DEF__

/**
 * exclude the size of HEADER
 * fortmat like below
 *      
 * /-------HEADER------------------\
 * --------------------------------------
 * |MAGIC|CRC|content size|TIMESTAMP|content|
 * --------------------------------------
 */
#include <inttypes.h>
#include "meta_file.h"

typedef void *(*MallocFunc)(size_t); 
    
#define BL_FILE_EOF       1
#define BL_FILE_OK        0
#define BL_FILE_SWITCH    2
#define BL_FILE_SYS_ERR  -1
#define BL_FILE_DATA_ERR -2

#define RECORD_MAX_SIZE (1024 * 1024 * 128)

#define SIMPLE_BUF_SIZE (1024 * 4)

#define IS_RECORD_LEN_VALID(len) (len <= RECORD_MAX_SIZE)

#define BL_TIMESTAMP_FIRST 0
#define BL_TIMESTAMP_LAST  1

/* no alignment problem */
typedef struct bin_header
{
    uint16_t magic;
    uint16_t crc;
    uint32_t len;
    uint64_t ts;
}bin_header_t;

uint64_t bin_timestamp();
int bin_read(int fd, uint8_t **buf, uint32_t *len, off_t *offset, uint64_t *ts, MallocFunc f, uint64_t *sid);
int bin_write_internal(int fd, const uint8_t *buf, uint32_t len, uint64_t *ts);
int bin_write_internal_v2(int fd, const uint8_t *buf, uint32_t len, uint64_t *ts, uint64_t sid);
int bin_read_ts(int fd, off_t *offset, uint64_t *ts);
int bin_init(bin_meta_t *bm, uint8_t *path, uint8_t *prefix, off_t max_size, int max_idx, uint64_t ts, int meta_persist, uint8_t *meta_name, int flags, mode_t mode);
int bin_write_v2(bin_meta_t *bm, const uint8_t *buf, uint32_t len, uint64_t sid);
int bin_read2(bin_meta_t *bm, uint8_t **buf, uint32_t *len, MallocFunc f, uint64_t *sid);
void bin_destroy(bin_meta_t *bm);

#endif
