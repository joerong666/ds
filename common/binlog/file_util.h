#ifndef __FILE_UTIL_H_DEF__
#define __FILE_UTIL_H_DEF__

extern int file_exist(char *fn);
extern int file_clean(int fd);
extern int file_trunc(int fd, off_t off);
extern off_t file_size(char *fn);
extern off_t file_len(int fd);
extern off_t file_pos(int fd);
extern off_t file_set_end(int fd);
extern off_t file_set_pos(int fd, off_t pos);
extern int mkpath(const char *path, mode_t mode);

#endif
