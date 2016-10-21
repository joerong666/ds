#include "write_bl.h"
#include "db_io_engine.h"
#include "ds_log.h"
#include "ds_ctrl.h"
#include "key_filter.h"

#include <sys/timeb.h>

#include "dict.h"

/* return:
 * 0 - not need block
 * 1 - need block & block succ
 * 2 - need block but too many clients
 */
int block_client_on_write_bl(redisClient *c)
{
    if (server.has_dbe == 0)
    {
        /* cache need not block */
        return 0;
    }
    if (c->argc > 1 && cache_filter(c->argv[1]->ptr, sdslen(c->argv[1]->ptr)) == 1)
    {
        /* the key belong to cache, need not block */
        //log_prompt("%s belong to cache, need not block", c->argv[1]->ptr);
        return 0;
    }

#ifndef _UPD_DBE_BY_PERIODIC_
#if 0
    lockWrblList();
    const unsigned int bl_l_len = listLength(server.wr_bl_list);
    unlockWrblList();
    lockWrDbeList();
    const unsigned int dbe_l_len = listLength(server.wr_dbe_list);
    unlockWrDbeList();
    lockBlWriteList();
    const unsigned int listLen = listLength(server.bl_writing);
    if (listLen > (unsigned int)server.wr_bl_que_size
        || bl_l_len > (unsigned int)server.wr_bl_que_size
        || dbe_l_len > (unsigned int)server.wr_bl_que_size
       )
    {
        listAddNodeTail(server.write_bl_clients, c);
        const unsigned int blockCnt = listLength(server.write_bl_clients);
        unlockBlWriteList();

        log_error("handle_len=%u, blList=%u, dbeList=%u, write_bl.max=%d, blocking=%u"
                , listLen, bl_l_len, dbe_l_len, server.wr_bl_que_size, blockCnt);
        c->block_start_time = server.ustime;
        c->flags |= REDIS_WR_BL_WAIT;
        aeDeleteFileEvent(server.el, c->fd, AE_READABLE);

        return 1;
    }

    /* do not block any more, write bl & update to dbe right now. 2016.06.27 */
    unlockBlWriteList();
#endif
    return 0;
#else

    const unsigned int clients = listLength(server.write_bl_clients);
    const unsigned int bl_writing_cnt = listLength(server.bl_writing);
    if (clients > gWaitBlClntMax)
    {
        gWaitBlClntMax = clients;
    }
    if (clients > (unsigned int)server.wr_bl_que_size)
    {
        log_error("too many clients(%d) are queue at write_bl", clients);
        return 2;
    }

    if (clients > 0 || bl_writing_cnt > 0)
    {
        log_debug("need to block the client, blocked_client=%d, bl_writing_cnt=%d"
                  , clients, bl_writing_cnt);

        c->block_start_time = server.ustime;
        c->flags |= REDIS_WR_BL_WAIT;
        aeDeleteFileEvent(server.el, c->fd, AE_READABLE);

        listAddNodeTail(server.write_bl_clients, c);
        return 1;
    }
    else
    {
        return 0;
    }
#endif
}

