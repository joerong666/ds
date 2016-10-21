#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <inttypes.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "pf_file.h"
#include "tcp_util.h"
#include "replica_proto.h"
#include "zmalloc.h"
#include "log.h"

int repl_send_slave(int net_fd, uint8_t *rec, uint8_t flag, uint32_t len, uint64_t ts)
{
    int i;
    uint32_t net_len;
    uint64_t net_ts;
    struct iovec repl_iov[2];
    repl_header_t header;

    if (rec == NULL && len != 0 )
    {
        log_error("get NULL rec");
        return -1;
    }

    header.magic[0] = REPL_MAGIC_1;
    header.magic[1] = REPL_MAGIC_2;

    header.flag = flag;

    net_len = htonl(len);
    memcpy(header.len, &net_len, sizeof(net_len));

    net_ts = hton64(&ts);
    memcpy(header.ts, &net_ts, sizeof(net_ts));

    repl_iov[0].iov_base = (void *)&header;
    repl_iov[0].iov_len  = sizeof(repl_header_t);

    repl_iov[1].iov_base = (void *)rec;
    repl_iov[1].iov_len  = len; 

    i = pf_writev_n(net_fd, PF_FD_NONBLOCK, repl_iov, 2, -1);
    if (i != (int)(sizeof(repl_header_t) + len))
    {
        log_error("send bin record to slave error, error[%s]", strerror(errno));
        return -1;
    }
    return 0;
}

/* slave will consider master is down if not recv data in MAX_RECV_MASTER_TIMEOUT */ 
#define MAX_RECV_MASTER_TIMEOUT (3 * 60 * 1000)
//#define MAX_RECV_MASTER_TIMEOUT (5000)
int repl_recv_master(int net_fd, uint8_t **rec, uint32_t *len, uint64_t *ts)
{
    uint32_t net_len;
    uint32_t h_len;
    uint64_t net_ts;
    repl_header_t header;

    if (pf_read_n(net_fd, PF_FD_NONBLOCK, &header, sizeof(header), MAX_RECV_MASTER_TIMEOUT) != sizeof(header))
    {
        log_error("recv bin record from server error, errorno[%d]", errno);
        return -1;
    }

    if ( header.magic[0] != REPL_MAGIC_1 
      || header.magic[1] != REPL_MAGIC_2 )
    {
        log_error("protocol error, magic number invalid");
        return -1;
    }

    if (header.flag != REPL_FLAG_DATA
        && header.flag != REPL_FLAG_SID)
    {
        return header.flag;
    }

    memcpy(&net_ts, header.ts, sizeof(net_ts));
    *ts = ntoh64(&net_ts);

    memcpy(&net_len, header.len, sizeof(net_len));
    h_len = ntohl(net_len);     
    
    if (h_len > 1024 * 1024 * 128)
    {
        log_error("repl len [%u] is too large", h_len);
        return -1;
    }

    log_debug("get repl len [%u]", h_len);

    *rec = zmalloc(h_len);
    if (pf_read_n(net_fd, PF_FD_NONBLOCK, *rec, h_len, MAX_RECV_MASTER_TIMEOUT) != (int)h_len)
    {
        log_error("recv bin record content from server error, errorno[%d]", errno);
        zfree(*rec);
        return -1;
    }

    *len = h_len;
    return (int)header.flag;
}

/*
 * INIT: slave --> master (redis format):
 * "*4\r\n$4\r\nsync\r\n$n\r\nbl_tag\r\n$n\r\nds_id\r\n$n\r\nds_key\r\n"
 */
int repl_slave_init_send(int net_fd, uint64_t ts, uint64_t sid, const char *ds_key)
{
    uint8_t     buf[128];    /* It's enough */
    uint8_t     tmp[32];    
    uint8_t     tmp_sid[32];    
    int         len;
    int         i;
    int         sid_len;
    int         ds_key_len;

    ds_key_len = strlen(ds_key);
    sid_len    = snprintf((char *)tmp_sid, sizeof(tmp_sid), "%"PRIu64"", sid);
    len        = snprintf((char *)tmp, sizeof(tmp), "%"PRIu64"", ts);
    len        = snprintf((char *)buf, sizeof(buf), "*4\r\n$4\r\nsync\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
                        len, tmp, sid_len, tmp_sid, ds_key_len, ds_key);

    if (len <= 0)
        return -1;

    log_debug("get init string[%s]", buf);

    i = pf_write_n(net_fd, PF_FD_NONBLOCK, buf, len, -1);
    if (i != len)
    {
        log_error("send init tag to master error, ret [%d], error[%s]", i, strerror(errno));
        return -1;
    }

    return 0;
}

/**
 * [command] binlog
 * slave_thread --> main_thread (redis format):
 * "*3\r\n$6\r\nbinlog\r\n$n\r\nds_id\r\n$len\r\nbinlog_content\r\n" 
 */
int repl_slave_redo_send_data(int net_fd, uint8_t *rec, uint32_t len, uint64_t sid)
{
    uint8_t             head[128];    /* It's enough */
    int                 hlen;
    uint8_t             tmp_sid[32];    
    int                 sid_len;
    int                 i;
    struct iovec        iov[3];
    uint8_t            *crlf = "\r\n"; 

    sid_len        = snprintf((char *)tmp_sid, sizeof(tmp_sid), "%"PRIu64"", sid);
    hlen           = snprintf((char *)head, sizeof(head), "*3\r\n$6\r\nbinlog\r\n$%d\r\n%s\r\n$%d\r\n", sid_len, tmp_sid, len);

    if (hlen <= 0)
        return -1;

    log_debug("get data head string[%s]", head);

    iov[0].iov_base = (void *)head;
    iov[0].iov_len  = hlen; 

    iov[1].iov_base = (void *)rec;
    iov[1].iov_len  = len; 

    iov[2].iov_base = (void *)crlf;
    iov[2].iov_len  = 2; 

    i = pf_writev_n(net_fd, PF_FD_NONBLOCK, iov, 3, -1);
    if (i != (signed)(hlen + len + 2))
    {
        log_error("send init tag to master error, ret [%d], error[%s]", i, strerror(errno));
        return -1;
    }
    return 0;
}

/**
 *
 * [command] sync_status
 * slave_thread --> main_thread (redis format):
 * "*5\r\n$11\r\nsync_status\r\n$n\r\nmaster_ip\r\n$n\r\nmaster_port\r\n$n\r\nstatus\r\n$n\r\ntimestamp\r\n"
 * status: 0-synced, 1-syncing, 2-fail, 3-over
 *
 */
int repl_slave_redo_send_stat(int net_fd, uint8_t *ip, int port, int status, uint64_t ts)
{
    uint8_t     buf[128];    /* It's enough */
    uint8_t     tmp[10];    
    int         port_len;
    uint8_t     tmp_ts[20];    
    int         ts_len;
    int         ip_len;
    int         len;
    int         i;

    ip_len = strlen((char *)ip);
    port_len = snprintf((char *)tmp, sizeof(tmp), "%d", port);
    ts_len = snprintf((char *)tmp_ts, sizeof(tmp_ts), "%"PRIu64, ts);
    len = snprintf((char *)buf, sizeof(buf), "*5\r\n$11\r\nsync_status\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$1\r\n%d\r\n$%d\r\n%s\r\n",
                                     ip_len, ip, port_len, tmp, status, ts_len, tmp_ts);

    if (len <= 0)
        return -1;

    log_debug("get sync status string[%s]", buf);

    i = pf_write_n(net_fd, PF_FD_NONBLOCK, buf, len, -1);
    if (i != len)
    {
        log_error("send init tag to master error, ret [%d], error[%s]", i, strerror(errno));
        return -1;
    }

    return 0;
}

/**
 * for redo opt, discard all recv data
 */
int repl_slave_redo_recv(int net_fd)
{
    uint8_t     buf[1024]; 
    int         i;

    i = read(net_fd, buf, 1024);
    if (i < 0)
    {
        if (errno != EAGAIN && errno != EINTR)
        {
            log_error("redo recv error, ret [%d], error[%s]", i, strerror(errno));
            return -1;
        }
    }
    else if (i == 0)
        log_debug("redo recv closed");
        

    /*
    if (can_read(net_fd, 10))
    {
        i = read(net_fd, buf, 1024);
        if (i <= 0)
        {
            log_error("redo recv error, ret [%d], error[%s]", i, strerror(errno));
            return -1;
        }
    }
    */

    /*
    else
    {
        log_error("redo recv timeout, error[%s]", strerror(errno));
    }
    */

    return 0;
}


