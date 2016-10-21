#ifndef __BIN_LOG_H_DEF__
#define __BIN_LOG_H_DEF__

#include "bin_log_impl.h"

/**
* RETURN CODES
*   BL_FILE_EOF: read file EOF
*   BL_FILE_OK: write ok
*   BL_FILE_SYS_ERR: create crc fail 
*   BL_FILE_DATA_ERR: write file fail
*/

extern uint64_t bin_nowts();
extern int bin_get(int fd, uint8_t **buf, uint32_t *len, off_t *offset, MallocFunc f);
extern int bin_get_v2(int fd, uint8_t **buf, uint32_t *len, off_t *offset, MallocFunc f, uint64_t *sid);
extern int bin_put(int fd, const uint8_t *buf, uint32_t len);
extern int bin_put_v2(int fd, const uint8_t *buf, uint32_t len, uint64_t sid);
extern int bin_get_ts(int fd, off_t *offset, uint64_t *timestamp);

#endif

