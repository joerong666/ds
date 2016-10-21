#define  _XOPEN_SOURCE 500 
#include <stdio.h>                                                    
#include <stdlib.h>                                                   
#include <inttypes.h>                                                 
#include <time.h>                                                     
#include <sys/types.h>                                                
#include <sys/stat.h>                                                 
#include <fcntl.h>                                                    
#include <unistd.h>                                                   
#include <string.h>                                                   
                                                                      
#include "file_util.h"                                                
#include "rotate_file.h"
#include "meta_file.h"
#include "zmalloc.h"                                                  
#include "log.h"

int meta_update(bin_meta_t *bm)
{
    int i;
    char buf[256];

   /**
    * the meta file format:
    * "current index\r\ncurrent timestamp\r\ncurrent offset\r\n"
    */
    memset(buf, 0x00, sizeof(buf));

    i = sprintf(buf, "idx:%03d\r\ncurr ts:%016"PRIu64"\r\ncurr offset:%016"PRIu64"\r\nremote ts:%016"PRIu64"\r\n",
                bm->rt.idx, bm->curr_ts, bm->curr_offset, bm->remote_ts);

    //if (file_clean(bm->fd))
    if (bm->fd >= 0)
    {

        i = pwrite(bm->fd, buf, i, 0);
        return i;
    }
    return -1;
}
    
int meta_get(bin_meta_t *bm)
{
    char buf[256];

    if (file_len(bm->fd) > 0)
    {
        memset(buf, 0x00, sizeof(buf));
        pread(bm->fd, buf, 256, 0);

        /* not strict */
        sscanf(buf, "idx:%d\r\ncurr ts:%"PRIu64"\r\ncurr offset:%"PRIu64"\r\nremote ts:%"PRIu64"\r\n",
                &bm->rt.idx, &bm->curr_ts, &bm->curr_offset, &bm->remote_ts);

        log_debug("get relay info idx[%d] ts[%"PRIu64"] offset[%"PRIu64"] remote_ts[%"PRIu64"]", bm->rt.idx, bm->curr_ts, bm->curr_offset, bm->remote_ts);

        return 0;
    }
    return -1;
}

void meta_set(bin_meta_t *bm, int idx, off_t offset, uint64_t ts)
{
    bm->rt.idx      = idx;
    bm->curr_offset = offset;
    bm->curr_ts     = ts;
}

void meta_close(bin_meta_t *bm)
{
    if (bm->need_persist)
    {
        if (bm->fd >= 0)
            close(bm->fd);
    }
}

int meta_init(bin_meta_t *bm, uint8_t *path, uint8_t *prefix,
            off_t max_size, int max_idx, int meta_persist_flag,
            uint8_t *meta_name, int flags, mode_t mode)
{
    rt_file_t  *rt = &bm->rt;

    memset(bm, 0x00, sizeof(bin_meta_t));
    bm->need_persist = meta_persist_flag;
    bm->fd = -1;

    /* set initial idx 0 */
    rt_init(rt, path, prefix, max_size, max_idx, 0, flags, mode);

    /* try to get initial info from meta */
    if (bm->need_persist)
    {
        snprintf((char *)bm->name, sizeof(bm->name), "%s/%s", path, meta_name); 
        bm->fd = open((char *)bm->name, O_CREAT | O_RDWR, 0666);
        if (bm->fd < 0)
            return -1;

        if (meta_get(bm) < 0)
            return 0;
        else
            return 1;
    }

    return 0;
}


