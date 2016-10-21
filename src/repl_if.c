#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/sendfile.h>
#include <arpa/inet.h>
#include <dirent.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <libgen.h>

#include "repl_if.h"

#ifdef _test_
    #define log_wrap(...) \
        fprintf(stdout, "file:%s,line:%d,", __FILE__, __LINE__), \
        fprintf(stdout, __VA_ARGS__), \
        fprintf(stdout, "\n")
    
    #define log_debug log_wrap
    #define log_info log_wrap
    #define log_warn log_wrap
    #define log_error log_wrap
    #define log_prompt log_wrap

    #ifdef _client_
        int child_finish = 0;
    #endif

    #define Malloc(size) malloc(size)
    #define Realloc(ptr, size) realloc(ptr, size)
    #define Free(ptr) do { \
        free(ptr); \
        ptr = NULL; \
    }while(0)

    #define Strdup strdup
#else
    #include "ds_log.h"
    #include "zmalloc.h"

    #define Malloc(size) zmalloc(size)
    #define Realloc(ptr, size) zrealloc(ptr, size)
    #define Free(ptr) do { \
        zfree(ptr); \
        ptr = NULL; \
    }while(0)

    #define Strdup zstrdup
#endif

#define MAXSLEEP 32
#define BIG_BUF_SIZE 4096
#define MID_BUF_SIZE 1024
#define SML_BUF_SIZE 256
#define TIMEOUT 5

#define REDIS_FILE  "*1\r\n$4\r\nfile\r\n"
#define REDIS_FILE_LEN  14  /* strlen(REDIS_FILE) */

#define BEGIN_FMT_PRINTF "repl:begin,file:%s,ori_size:%ld,new_size:%ld,chksum:%s"
#define BEGIN_FMT_SCANF "repl:begin,file:%[^,],ori_size:%ld,new_size:%ld,chksum:%s"

#define FINISH_FMT "repl:finish:%lld"

#define REPL_FILE_EXIST "repl:exist"
#define REPL_FILE_EXIST_LEN 10

#define REPL_OK "repl:ok"
#define REPL_OK_LEN 7

#define REPL_FAIL "repl:fail"
#define REPL_FAIL_LEN 9

#define REPL_ERR "repl:err"
#define REPL_ERR_LEN 8


typedef struct {
    char *master_ip;
    int master_port;
    char *dir;
    NoticeFunc notice;
    long long bl_tag;
    int cnnfd;
} repl_args_t;

#define GO_OUT(er_no, ...)  ({ \
    char buf[MID_BUF_SIZE], msg[MID_BUF_SIZE]; \
    snprintf(msg, sizeof(msg), __VA_ARGS__); \
    if(er_no == 0){ \
        snprintf(buf, sizeof(buf), "msg:%s", msg); \
    } else { \
        snprintf(buf, sizeof(buf), "msg:%s,err(%d):%s", msg, er_no, strerror(er_no)); \
    } \
    log_error("%s", buf); \
    rc = -1; \
    goto _out; \
})

#define GO_OUT_IF_FAIL(expr, er_no, ...) ({ \
    if(!(expr)) { \
        GO_OUT(er_no, __VA_ARGS__); \
    } \
})

#define TRIGGER_NOTICE(er_no, notice_func, status, ...)  do { \
    char buf[MID_BUF_SIZE], msg[MID_BUF_SIZE]; \
    snprintf(msg, sizeof(msg), __VA_ARGS__); \
    if(status == 2) { \
        snprintf(buf, sizeof(buf), "%s", msg); \
    } else {\
        if(er_no == 0){ \
            snprintf(buf, sizeof(buf), "msg:%s", msg); \
        } else { \
            snprintf(buf, sizeof(buf), "msg:%s,err(%d):%s", msg, er_no, strerror(er_no)); \
        } \
    } \
\
    notice_func(master_ip, master_port, status, buf); \
} while(0)

#define Recv(fd, buf, buf_size) ({ \
    ssize_t ret; \
    errno = 0; \
    while(1) { \
        ret = recv(fd, buf, buf_size, 0); \
        if(-1 == ret) { if(EINTR == errno) continue; } \
        break; \
    } \
    ret; \
})
        
#define Send(sockfd, buf, buf_size)  ({ \
    ssize_t ret, size_sent = 0, size=(buf_size); \
    while(1) { \
        if((ret = send(sockfd, buf, size - size_sent, 0)) < 0) { \
            if (EINTR == errno) continue; \
            break; \
        } \
\
        size_sent += ret; \
        if(size == size_sent) break; \
    } \
    size_sent; \
})

void
free_file_list(char **flist, int cnt)
{
    int i;
    
    for(i = 0; i < cnt; i++) {
        Free(flist[i]);
    }

    Free(flist);
}

static int
get_file_list(const char *path, char ***flist, int *cnt)
{
    int c = 0, m = SML_BUF_SIZE, rc = 0;
    struct dirent *dirptr;
    DIR *dir = NULL;
    char **files = NULL, **p = NULL;

    *flist = NULL;
    *cnt = 0;

    dir = opendir(path);
    GO_OUT_IF_FAIL(dir != NULL, errno, "Open %s fail", path);

    files = p = Malloc(sizeof(char *) * m);
    GO_OUT_IF_FAIL(p != NULL, errno, "Malloc fail");

    memset(files, 0, sizeof(char *) * m);

    while((dirptr = readdir(dir)) != NULL) {
        if(dirptr->d_name[0] == '.' || strcmp(dirptr->d_name, "..") == 0 ) continue;

        struct stat info;
        char f_name[SML_BUF_SIZE];
        snprintf(f_name, SML_BUF_SIZE, "%s/%s", path, dirptr->d_name);

        stat(f_name, &info);
        if(S_ISDIR(info.st_mode)) continue;

        if(c == m) {
            m += SML_BUF_SIZE;
            p = Realloc(files, sizeof(char *) * m);
            GO_OUT_IF_FAIL(p != NULL, errno, "Realloc fail");

            files = p;
            memset(files + c, 0, sizeof(char *) * SML_BUF_SIZE);
        }

        files[c++] = Strdup(f_name);
    }

    *flist = files;
    *cnt = c;
    
_out:
    if(rc < 0) free_file_list(files, c);
    closedir(dir);
    return 0;
}

int
connect_retry(int sockfd, const struct sockaddr *addr, socklen_t alen)
{
    int numsec;
    
    for (numsec = 1; numsec <= MAXSLEEP; numsec <<= 1) {
        if (connect(sockfd, addr, alen) == 0) return 0;

        log_debug("Sleep and then re-connect again");
        /* Delay before trying again.*/
        if (numsec <= MAXSLEEP/2) sleep(numsec);
    }

    return -1;
}

static int 
slave_process(const char *master_ip, int master_port, const char *dir, NoticeFunc notice_func)
{
    ssize_t ret;
    int clifd, rc = 0, ffd = -1;
    long int ori_size, new_size;
    long long bl_tag = -1;
    char rbuf[BIG_BUF_SIZE * 10], f_name[SML_BUF_SIZE], chksum[SML_BUF_SIZE], tmpbuf[SML_BUF_SIZE];
    struct sockaddr_in svraddr;
    struct stat fst;
    fd_set rfds;

    memset(&svraddr, 0, sizeof(svraddr));
    svraddr.sin_family = AF_INET;
    svraddr.sin_port = htons((uint16_t)master_port);

    #undef GO_OUT_NOTICE_IF_FAIL
    #define GO_OUT_NOTICE_IF_FAIL(expr, er_no, ...)  ({ \
        if(!(expr)) { \
            TRIGGER_NOTICE(er_no, notice_func, 0, __VA_ARGS__); \
            GO_OUT(er_no, __VA_ARGS__); \
        } \
    })

    #undef RECEIVE_LOOP_BEGIN
    #define RECEIVE_LOOP_BEGIN  while(1) { \
        FD_ZERO(&rfds); \
        FD_SET(clifd, &rfds); \
\
        struct timeval tv = {.tv_sec = TIMEOUT, .tv_usec = 0}; \
        ret = select(clifd + 1, &rfds, NULL, NULL, &tv); \
        GO_OUT_NOTICE_IF_FAIL(ret != 0, 0, "Read from master timeout"); \
\
        if(ret == -1) { \
            if(EINTR == errno) continue; \
\
            GO_OUT_NOTICE_IF_FAIL(0, errno, "Select fail"); \
        } \
\
        if(FD_ISSET(clifd, &rfds)) { 
    
    #undef RECEIVE_LOOP_END
    #define RECEIVE_LOOP_END }}

    #undef GO_REDO_IF_NETWORK_ERR
    #define GO_REDO_IF_NETWORK_ERR(sockfd, status, er_no) do{ \
        int er_ori = er_no;   \
        if(status == 0 || (status == -1 &&  \
            (ECONNRESET == er_ori ||  ENOTCONN == er_ori ||  EPIPE == er_ori))) { \
                log_warn("Network error, reconnect and redo again,err(%d):%s", \
                    er_ori, er_ori ? strerror(er_ori) : ""); \
                close(sockfd); \
                if(ffd >= 0) close(ffd); \
                errno = er_ori; \
                goto _redo; \
        } \
    }while(0)

    #undef Recv_r
    #define Recv_r(sockfd, buf, buf_size) ({ \
        int ret = Recv(sockfd, buf, buf_size); \
        GO_REDO_IF_NETWORK_ERR(sockfd, ret, errno); \
        ret; \
    })

    #undef Send_r
    #define Send_r(sockfd, buf, buf_size)  ({ \
        int ret = Send(sockfd, buf, buf_size); \
        GO_REDO_IF_NETWORK_ERR(sockfd, ret, errno); \
        ret; \
    })

_redo:
    clifd = socket(PF_INET, SOCK_STREAM, 0);
    GO_OUT_NOTICE_IF_FAIL(clifd != -1, errno, "Create socket fail");

    ret = inet_pton(AF_INET, master_ip, &svraddr.sin_addr);
    GO_OUT_NOTICE_IF_FAIL(ret > 0, errno, "Parse master ip fail");

    log_debug("Connect to %s:%d", master_ip, master_port);
    ret = connect_retry(clifd, (struct sockaddr *)&svraddr, sizeof(svraddr));
    GO_OUT_NOTICE_IF_FAIL(ret == 0, errno, "Connect to %s:%d fail", master_ip, master_port);

    log_info("Replication from master begin");
    TRIGGER_NOTICE(0, notice_func, 1, "Replication from master begin");

#ifndef _test_
    log_debug("Send redis file protocol for replication");
    ret = Send_r(clifd, REDIS_FILE, REDIS_FILE_LEN);
    GO_OUT_NOTICE_IF_FAIL(ret == REDIS_FILE_LEN, errno, 
        "Send redis file request fail,ret(expt:%d,act:%ld)", REDIS_FILE_LEN, ret);
#endif

    RECEIVE_LOOP_BEGIN

    /* receive file meta */
    log_debug("Wait for receiving file meta");
    ret = Recv_r(clifd, rbuf, sizeof(rbuf));
    GO_OUT_NOTICE_IF_FAIL(ret > 0, errno, "Receive file meta fail,ret(%ld)", ret);

    rbuf[ret] = '\0';
    ret = sscanf(rbuf, FINISH_FMT, &bl_tag);
    if(1 == ret) {
        log_info("Replication from master finished");
        TRIGGER_NOTICE(0, notice_func, 2, "%lld", bl_tag);
        break;
    }

    ret = sscanf(rbuf, BEGIN_FMT_SCANF, f_name, &ori_size, &new_size, chksum);
    GO_OUT_NOTICE_IF_FAIL(ret == 4, errno, "Parse file meta fail:%s", rbuf);

    log_debug("Replicate %s from master", f_name);
    snprintf(tmpbuf, sizeof(tmpbuf), "%s/%s", dir, f_name);

    /* file existed */
    if(!stat(tmpbuf, &fst) && fst.st_size == new_size) {
        log_debug("%s existed in slave", f_name);
        ret = Send_r(clifd, REPL_FILE_EXIST, REPL_FILE_EXIST_LEN);
        GO_OUT_NOTICE_IF_FAIL(ret == REPL_FILE_EXIST_LEN, errno, "Send exist fail");

        continue;
    }

    ffd = open(tmpbuf, O_CREAT|O_WRONLY|O_TRUNC, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH);
    GO_OUT_NOTICE_IF_FAIL(ffd >= 0, errno, "Open %s fail", tmpbuf);

    log_debug("Send %s after file meta received", REPL_OK);
    ret = Send_r(clifd, REPL_OK, REPL_OK_LEN);
    GO_OUT_NOTICE_IF_FAIL(ret == REPL_OK_LEN, errno, "Send %s fail,ret(expt:%d,act:%ld)", REPL_OK, REPL_OK_LEN, ret);

    ssize_t content_size = 0;
    /* receive file content */
    RECEIVE_LOOP_BEGIN

    log_debug("Wait for receiving content,bufsize(%lu)", sizeof(rbuf));
    ret = Recv_r(clifd, rbuf, sizeof(rbuf));
    GO_OUT_NOTICE_IF_FAIL(ret > 0, errno, "Receive file content fail,ret(%ld)", ret);
    
    log_debug("Write buffer received to file");
    ssize_t w_size = ret;
    while(1) {
        if((ret = write(ffd, rbuf, w_size)) == -1) {
            if(EINTR == errno) continue;
            
            GO_OUT_NOTICE_IF_FAIL(0, errno, "Write content to %s fail,ret(%ld)", tmpbuf, ret);
        }

        GO_OUT_NOTICE_IF_FAIL(ret == w_size, 0, "Write file content fail,ret(expt:%ld,act:%ld)", w_size, ret);

        break;
    }

    content_size += w_size; 
    if(content_size == new_size) break;

    RECEIVE_LOOP_END        

    log_debug("Send %s after file content received", REPL_OK);
    ret = Send_r(clifd, REPL_OK, REPL_OK_LEN);
    GO_OUT_NOTICE_IF_FAIL(ret == REPL_OK_LEN, errno, "Send %s fail,ret(expt:%d,act:%ld)", REPL_OK, REPL_OK_LEN, ret);

    close(ffd);
    ffd = -1;

    RECEIVE_LOOP_END        

_out:

    if(ffd >= 0) close(ffd);
    log_debug("Close connection");
    close(clifd);

    return rc;
}

static int 
master_process(const char *dir, int fd, long long bl_tag)
{
    ssize_t ret;
    char wbuf[BIG_BUF_SIZE], rbuf[SML_BUF_SIZE];
    char **flist = NULL;
    int fcnt, i, ffd = -1, rc = 0;
    struct stat fst;

    ret = get_file_list(dir, &flist, &fcnt);
    GO_OUT_IF_FAIL(ret == 0, errno, "Get file list fail");

    for(i = 0; i < fcnt; ) {
        log_debug("Replicate %s to slave", basename(flist[i]));

        if(-1 == ffd) {
            ffd = open(flist[i], O_RDONLY);
            GO_OUT_IF_FAIL(ffd != -1, errno, "Open file fail: flist[%d]:%s\n", i, flist[i]);

            fstat(ffd, &fst);
            if(0 == fst.st_size) {
                log_debug("Size of %s is 0, will not replicate", flist[i]);
                goto _next;
            }
        }

        snprintf(wbuf, sizeof(wbuf), BEGIN_FMT_PRINTF, basename(flist[i]), fst.st_size, fst.st_size, "NULL");

        log_debug("Send file meta:%s", wbuf);
        int len = strlen(wbuf);
        ret = Send(fd, wbuf, len);
        GO_OUT_IF_FAIL(ret == len, errno, "Send file meta fail,ret(expt:%d,act:%ld)", len, ret);

        log_debug("Wait response after file meta sent");
        ret = Recv(fd, rbuf, sizeof(rbuf));
        GO_OUT_IF_FAIL(ret > 0, errno, "Receive fail,ret(%ld)", ret);

        rbuf[ret] = '\0';
        
        /* exist, no need to replicate again */
        if(!strncmp(REPL_FILE_EXIST, rbuf, 
            REPL_FILE_EXIST_LEN < strlen(rbuf) ? REPL_FILE_EXIST_LEN : strlen(rbuf))) {
            log_debug("%s existed in slave, will not replicate", basename(flist[i]));
            goto _next; 
        }

        /* not ok, replicate again */
        ret = strncmp(REPL_OK, rbuf, REPL_OK_LEN < strlen(rbuf) ? REPL_OK_LEN : strlen(rbuf));
        GO_OUT_IF_FAIL(ret == 0, errno, "Response received should be %s or %s", REPL_OK, REPL_FILE_EXIST);

        /* send file content */
        log_debug("Send file content");
        ret = sendfile(fd, ffd, 0, fst.st_size);
        GO_OUT_IF_FAIL(ret == fst.st_size, errno, "Send file content fail,ret(expt:%ld,act:%ld)", fst.st_size, ret);
            
        log_debug("Wait response after file content sent");
        ret = Recv(fd, rbuf, sizeof(rbuf));
        GO_OUT_IF_FAIL(ret > 0, errno, "Receive fail,ret(%ld)", ret);

        /* not ok, replicate again */
        rbuf[ret] = '\0';
        if(strncmp(REPL_OK, rbuf, REPL_OK_LEN < strlen(rbuf) ? REPL_OK_LEN : strlen(rbuf))) {
            log_debug("Response received is %s, not %s, re-send file", rbuf, REPL_OK);
            continue;
        }

_next:
        close(ffd);
        ffd = -1;
        i++;
    }

    log_debug("Send replication finish flag");
    snprintf(wbuf, sizeof(wbuf), FINISH_FMT, bl_tag);
    int len = strlen(wbuf);
    ret = Send(fd, wbuf, len);
    GO_OUT_IF_FAIL(ret == len, errno, "Send replication finish flag fail,ret(expt:%d,act:%ld)", len, ret);

_out:
    if(ffd >= 0) close(ffd);
    free_file_list(flist, fcnt);
    return rc;
}

static void
slave_process_wrap(repl_args_t *args)
{
    slave_process(args->master_ip, args->master_port, args->dir, args->notice);
    Free(args->master_ip);
    Free(args->dir);
    Free(args);

#ifdef _test_
    #ifdef _client_
        child_finish = 1;
    #endif
#endif
}

int 
repl_slave_start(const char *master_ip, int master_port, const char *dir, NoticeFunc f)
{
    pthread_t tid;
    int ret, rc = 0;
    repl_args_t *args = NULL;

    if(!master_ip || master_ip[0] == '\0' || !dir || dir[0] == '\0') {
        log_error("Invalid %s args:%s,%d,%s", __func__, master_ip, master_port, dir);
        rc = -1;
        goto _out;
    }

    args = Malloc(sizeof(repl_args_t));
    GO_OUT_IF_FAIL(args != NULL, errno, "Malloc fail");

    args->master_ip = Strdup(master_ip);
    args->master_port = master_port;
    args->dir = Strdup(dir);
    args->notice = f;

    if(!args->master_ip || args->master_ip[0] == '\0' || !args->dir || args->dir[0] == '\0') {
        log_error("Strdup fail, args:%s,%d,%s", args->master_ip, args->master_port, args->dir);
        rc = -1;
        goto _out;
    }

    if((ret = pthread_create(&tid, NULL, (void * (*)(void *))slave_process_wrap, args)) != 0) {
        int er_no = errno;
        TRIGGER_NOTICE(ret, f, 1, "Create slave replaction thread fail");
        GO_OUT_IF_FAIL(0, er_no, "Create slave replaction thread fail");
    }

    pthread_detach(tid);

_out:
    if(rc < 0 && args != NULL) {
        Free(args->master_ip);
        Free(args->dir);
        Free(args);
    }

    return rc;
}

int 
repl_master_start(const char *dir, int fd, long long bl_tag)
{
    log_debug("%s args:%s,%d,%lld", __func__, dir, fd, bl_tag);

    char *dir_ = Strdup(dir);
    int ret = master_process(dir_, fd, bl_tag);

    Free(dir_);

    return ret;
}

/*
* Test as server: gcc -Wall -D_test_ repl_if.c -o repl_svr, ./repl_svr <dir> <port>
* Test as client: gcc -Wall -D_test_ -D_client_ repl_if.c  -o repl_cli, ./repl_cli <dir> <host> <port> 
* Memcheck with valgrind: gcc -D_test_ -D_mc_ repl_if.c -o repl_svr
*/
#ifdef _test_

void
notice(const char *master_ip, int master_port, int status, const char *info)
{
    log_debug("Notice:%d----%s", status, info);
}

int
main(int argc, char *argv[])
{
#ifdef _client_
    //slave_process(argv[2], atoi(argv[3]), argv[1], notice);
    repl_slave_start(argv[2], atoi(argv[3]), argv[1], notice);
    log_debug("Sleep for waiting for child thread finish");
    while(!child_finish) { 
        sleep(1);
    }
#else
    int sockfd, connfd, ret;
    char buf[SML_BUF_SIZE];
    socklen_t addr_len;
    struct sockaddr_in svraddr, cliaddr;
    
    if((sockfd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        log_error("create socket,err(%d):%s", errno, strerror(errno));
        exit(1);
    }

    memset(&cliaddr, 0, sizeof(cliaddr));
    memset(&svraddr, 0, sizeof(svraddr));

    svraddr.sin_family = PF_INET;
    svraddr.sin_port = htons(atoi(argv[2]));
    svraddr.sin_addr.s_addr = INADDR_ANY;

    if(bind(sockfd, (struct sockaddr *)&svraddr, sizeof(struct sockaddr)) == -1) {
        log_error("bind,err(%d):%s", errno, strerror(errno));
        exit(1);
    }

    if(listen(sockfd, 5) == -1) {
        log_error("listen,err(%d):%s", errno, strerror(errno));
        exit(1);
    }

#ifndef _mc_
    while(1) {
#endif
        log_debug("Wait for new connection\n");

        addr_len = sizeof(struct sockaddr);
        if((connfd = accept(sockfd,(struct sockaddr *)&cliaddr, &addr_len)) == -1) {
            log_error("accept,err(%d):%s", errno, strerror(errno));
            exit(1);
        }

        log_debug("server: got connection from %s, port %d, socket %d",
               inet_ntop(AF_INET, &cliaddr.sin_addr, buf, sizeof(buf)), ntohs(cliaddr.sin_port), connfd);
//        ret = master_process(argv[1], connfd, 10);
        ret = repl_master_start(argv[1], connfd, 10);
        close(connfd);
#ifndef _mc_
    }
#endif

#endif

    return 0;
}

#endif
