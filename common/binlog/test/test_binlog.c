#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>

#include "zmalloc.h"

#include "include/binlog.h"

void test_bin_put(void)
{
	int fd = open("binlog-000001.log", O_CREAT | O_APPEND | O_RDWR, S_IRUSR | S_IWUSR);

	uint8_t buf[100];
	int i = 0;
	while(i < 100)
	{
		sprintf(buf, "content%d", i++);
		bin_put(fd, buf, strlen(buf) + 1);		
	}
	close(fd);
}

void test_bin_get(void)
{
	int32_t ret = 0;

	int fd = open("binlog-000001.log", O_CREAT | O_APPEND | O_RDWR, S_IRUSR | S_IWUSR);

	uint8_t *buf = NULL;
	uint32_t size = 0;
	off_t offset = 0;

	while(1)
	{
		ret = bin_get(fd, &buf, &size, &offset, zmalloc);	

		if(ret) break;

		printf("ret:%d, content:%s--size:%d--offset:%d\n", ret, buf, size, offset);
		if(buf) zfree(buf);
	}


	close(fd);
}

void test_bin_first_ts(void)
{
	int32_t ret = 0;

	int fd = open("binlog-000001.log", O_CREAT | O_APPEND | O_RDWR, S_IRUSR | S_IWUSR);

    uint64_t ts = 0;
    off_t offset = 0;
    ret = bin_get_ts(fd, &offset, &ts);	
    printf("get ts [%llu] ret [%d] offset [%lu]", ts, ret, offset);

	close(fd);
}

void test_bin_get_by_ts(void)
{
	int32_t ret = 0;

	int fd = open("binlog-000001.log", O_CREAT | O_APPEND | O_RDWR, S_IRUSR | S_IWUSR);

	uint8_t *buf = NULL;
	uint32_t size = 0;
	off_t offset = 0;
    uint64_t ts = bin_nowts();

    usleep(1);
	uint8_t tbuf[100];
    sprintf(tbuf, "content get ts[%llu] test", ts);
    bin_put(fd, tbuf, strlen(tbuf));		

    ret = bin_get_by_ts(fd, &buf, &size, &offset, ts, zmalloc);	
    printf("ts [%llu] ret:%d, content:%s--size:%d--offset:%d\n", ts, ret, buf, size, offset);
    zfree(buf);

	close(fd);
}

int main(int argc, char *argv[])
{
    int argval;
    if (argc <= 1)
    {
        printf("Usage: test_binlog -[gs]\n");
        return -1;
    }

    while ((argval = getopt(argc, argv, "gsto")) != EOF)
    {
        switch (argval)
        {
            case 'g':
                printf("try to get binlog:\n---------------\n");
                test_bin_get();
                break;
            case 's': 
                printf("try to put binlog:\n---------------\n");
                test_bin_put();
                printf("put end\n");
                break;
            case 't': 
                printf("try to get first timestamp:\n---------------\n");
                test_bin_first_ts();
                break;
            case 'o': 
                printf("try to get binlog by timestamp:\n---------------\n");
                test_bin_get_by_ts();
                break;
            default:
                printf("Usage: test_binlog -[gs]\n");
                break;
        }
    }
	return 0;
}

