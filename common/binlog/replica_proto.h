#ifndef __REPLICA_PROTO_H_DEF__
#define __REPLICA_PROTO_H_DEF__

#define REPL_MAGIC_1   0xFA
#define REPL_MAGIC_2   0xFB

#define REPL_FLAG_DATA 0x00
#define REPL_FLAG_FIN  0x01
#define REPL_FLAG_ERR  0x02
#define REPL_FLAG_SID  0x03
#define REPL_FLAG_HEARTBEAT  0x04

#define REPL_STATUS_SYNCED    0
#define REPL_STATUS_SYNCING   1
#define REPL_STATUS_FAIL      2
#define REPL_STATUS_OVER      3

typedef struct repl_header
{
    uint8_t     magic[2];
    uint8_t     flag;
    uint8_t     len[4];
    uint64_t    ts[8]; 
}repl_header_t;

int repl_send_slave(int net_fd, uint8_t *rec, uint8_t flag, uint32_t len, uint64_t ts);
int repl_recv_master(int net_fd, uint8_t **rec, uint32_t *len, uint64_t *ts);
int repl_slave_init_send(int net_fd, uint64_t ts, uint64_t sid, const char *ds_key);
int repl_slave_redo_send_data(int net_fd, uint8_t *rec, uint32_t len, uint64_t sid);
int repl_slave_redo_send_stat(int net_fd, uint8_t *ip, int port, int status, uint64_t ts);
int repl_slave_redo_recv(int net_fd);

#endif
