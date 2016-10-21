#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>

#include "file_util.h"

int file_exist(char *fn)
{
    return (access(fn, F_OK) == 0);
}

int file_clean(int fd)
{
    return (ftruncate(fd, 0) == 0);
}

int file_trunc(int fd, off_t off)
{
    return (ftruncate(fd, off) == 0);
}

off_t file_size(char *fn)
{
    struct stat st;
    if (stat(fn, &st) < 0)
        return -1;
    return st.st_size;
}

off_t file_len(int fd)
{
    struct stat stbuf;
    if (fstat(fd, &stbuf) != -1)
    {
        return stbuf.st_size;
    }
    return -1;
}

off_t file_pos(int fd)
{
    return lseek(fd, 0, SEEK_CUR);
}

off_t file_set_end(int fd)
{
    return lseek(fd, 0, SEEK_END);
}

off_t file_set_pos(int fd, off_t pos)
{
    return lseek(fd, pos, SEEK_SET);
}

static int do_mkdir(const char *path, mode_t mode)
{
    struct stat     st;
    int             status = 0;

    if (stat(path, &st) != 0)
    {
        /* Directory does not exist. EEXIST for race condition */
        if (mkdir(path, mode) != 0 && errno != EEXIST)
            status = -1;
    }
    else if (!S_ISDIR(st.st_mode))
    {
        errno = ENOTDIR;
        status = -1;
    }

    return(status);
}

/**
** mkpath - ensure all directories in path exist
** Algorithm takes the pessimistic view and works top-down to ensure
** each directory in path exists, rather than optimistically creating
** the last element and working backwards.
*/
int mkpath(const char *path, mode_t mode)
{
    char           *pp;
    char           *sp;
    int             status;
    char           *copypath = strdup(path);

    status = 0;
    pp = copypath;
    while (status == 0 && (sp = strchr(pp, '/')) != 0)
    {
        if (sp != pp)
        {
            /* Neither root nor double slash in path */
            *sp = '\0';
            status = do_mkdir(copypath, mode);
            *sp = '/';
        }
        pp = sp + 1;
    }
    if (status == 0)
        status = do_mkdir(path, mode);
    free(copypath);
    return (status);
}


