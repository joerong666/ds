#ifndef _REPLICATION_H_
#define _REPLICATION_H_

#include "redis.h"

/* transaction of transfer between data-servers */
#define REPL_STATUS_NULL               0
#define REPL_STATUS_CONNECTING         1
#define REPL_STATUS_SENDING            2
#define REPL_STATUS_RECVING            3
#define REPL_STATUS_FIN                4
#define REPL_STATUS_FAIL               5

typedef struct repl_t_tag
{
    int sync;
    int master_slave;
    int peer_master_slave;

    int fd;
    int status;
    int transfer_lastio;
    int transfer_left;
    void *bl_ctx;
} repl_t;

#define REPL_MASTER                   0
#define REPL_SLAVE                    1

#define REPL_TRANSFER                 0
#define REPL_SYNC                     1

#endif /* _REPLICATION_H_ */

