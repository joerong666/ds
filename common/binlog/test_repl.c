#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <signal.h>
#include <sys/wait.h>
#include <errno.h>
#include <assert.h>
#include <ctype.h>
#include <stdarg.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/uio.h>
#include <limits.h>
#include <float.h>
#include <math.h>
#include <pthread.h>
#include <sys/resource.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <malloc.h>
#include <errno.h>
#include <string.h>

#include "pf_log.h"
#include "tcp_util.h"
#include "bin_log.h"
#include "log.h"

int do_master()
{
    int fd;
    int cfd;

    fd = gen_tcp_server(65531, NULL); 

    if (fd < 0)
    {
        log_debug("error [%s]", strerror(errno)); 
        log_debug("11111111111"); 
        return -1;
    } 

    log_debug("try to accept..."); 
    cfd = server_tcp_accept(fd, NULL, NULL);
    if (cfd < 0)
    {
        log_debug("error [%s]\n", strerror(errno)); 
        return -1;
    }
    log_debug("accept slave");

    repl_bin_master_start("./", "bintest", 1000000, 5, cfd, BL_TIMESTAMP_FIRST);

    sleep(1000000); 

    return 0;
}

int do_slave()
{
    repl_bin_slave_start("127.0.0.1", 65531, "./", BL_TIMESTAMP_FIRST, 123);

    while(1)
        sleep(5);
    return 0;
}

int main(int argc, char *argv[])
{
    int argval;
    if (argc <= 1)
    {
        log_debug("Usage: test_binlog -[gs]\n");
        return -1;
    }
    while ((argval = getopt(argc, argv, "ms")) != EOF)
    {
        switch (argval)
        {
            case 'm':
                log_debug("try to start master:\n---------------\n");
                do_master();
                break;
            case 's':
                log_debug("try to start slave:\n---------------\n");
                do_slave();
                break;
            default:
                log_debug("Usage: test_repl -[ms]\n");
                break;
        }
    }

    return 0;
}





