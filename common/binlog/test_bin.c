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
#include "bin_log.h"
#include "zmalloc.h"

void read_test()
{
    int i;
    bin_meta_t bm;
    char *buf;
    uint32_t len;

    bin_init(&bm, "./", "bintest", 1000000, 5, BL_TIMESTAMP_FIRST, 1, "binread.info", O_CREAT | O_RDONLY, 0666);

    while (1)
    {
        i = bin_read2(&bm, &buf, &len, zmalloc);

        if (i == BL_FILE_OK)
        {
            printf("read [%d][%.*s]", len, len, buf);
            zfree(buf);
        }
    }

}

void write_test()
{
    int i;
    bin_meta_t bm;
    char *buf;
    uint32_t len;
    char b[128];

    bin_init(&bm, "./", "bintest", 1000000, 5, BL_TIMESTAMP_LAST, 1, "binwrite.info", O_CREAT | O_TRUNC | O_WRONLY, 0666);

    for (i = 0; i < 10000; i++)
    {
        printf("[%d]\n", i);
        sprintf(b, "asdfafasfasdfasfdasf test [%d]", i);
        bin_write(&bm, b, strlen(b));    
    }

}

void write_test_v2()
{
    int i;
    bin_meta_t bm;
    char *buf;
    uint32_t len;
    char b[128];

    bin_init(&bm, "./", "bintest", 1000000, 5, BL_TIMESTAMP_LAST, 1, "binwrite.info", O_CREAT | O_TRUNC | O_WRONLY, 0666);

    for (i = 0; i < 10000; i++)
    {
        printf("[%d]\n", i);
        sprintf(b, "asdfafasfasdfasfdasf test [%d]", i);
        bin_write_v2(&bm, b, strlen(b), 9999998);    
    }

}

int main(int argc, char *argv[])
{
    int i;
    bin_meta_t bm;
    char *buf;
    uint32_t len;
    
    int argval;
    if (argc <= 1)
    {
        printf("Usage: test_binlog -[rw]\n");
        return -1;
    }

    while ((argval = getopt(argc, argv, "rwmn")) != EOF)
    {
        switch (argval)
        {
            case 'r':
                printf("try to read:\n---------------\n");
                read_test();
                break;
            case 'w':
                printf("try to write:\n---------------\n");
                write_test();
                break;
            case 'm':
                printf("try to write:\n---------------\n");
                write_test_v2();
                break;
            default:
                printf("Usage: test_binlog -[rw]\n");
                break;
        }
    }
    
    return 0;
}

