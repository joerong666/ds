#ifndef _REPL_IF_H_
#define _REPL_IF_H_

/*
 * protocal between slave and master
 *
 * slave --> master (redis format):
 * "*1\r\n$4\r\nfile\r\n"
 */

/*
 * status:
 * 0 - fail
 * 1 - doing
 * 2 - succ finished
 */
typedef void (*NoticeFunc)(const char *master_ip, int master_port, int status, const char *info);

/* 
 * dir: the path to store files receiving from master
 * f: the callback function to notice status to the main thread
 * return:
 * 0 - startup succ
 * 1 - startup fail
 */
extern int repl_slave_start(const char *master_ip, int master_port, const char *dir, NoticeFunc f);

/* 
 * dir: the dbe's path whose files will be sent to slave
 * fd: the connection fd with slave, use to send files
 * bl_tag: the earliest binlog timestamp un-update to dbe's files
 * return:
 * 0 - succ finish
 * 1 - fail
 */
extern int repl_master_start(const char *dir, int fd, long long bl_tag);

#endif /* _REPL_IF_H_ */

