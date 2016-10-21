#include "fmacros.h"

#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>
#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "ds_log.h"
#include "dynarray.h"
#include "zmalloc.h"

static int get_ipaddr_by_name(const char *pcHostName, char *pcIp, int iSize);

int check_and_make_dir(const char *path)
{
    char dir[2048];
    strcpy(dir, path);
    int len = strlen(dir);
    if (len <= 0)
    {
        return -1;
    }
    if (dir[len - 1] != '/')
    {
        dir[len] = '/';
        len++;
        dir[len] = '\0';
    }
    int i;
    for (i = 1; i < len; i++)
    {
        if (dir[i] == '/')
        {
            dir[i] = 0;
            if (access(dir, 0) != 0)
            {
                if (mkdir(dir, 0755) != 0)
                {
                    log_error("mkdir() fail, errno=%d, dir=%s", errno, dir);
                    return -1;
                }
            }
            dir[i] = '/';
        }
    }

    return 0;
}

/**
    \brief get local host ip address
    \param pcIp: to store host ip address
    \param iSize: buffer size
    \return 0=ok, <0 means FAIL and error code
*/
int get_host_ipaddr(char *pcIp, int iSize)
{
    if (!pcIp)
    {
        return -1;
    }

    char szHostName[128];
    if (0 == gethostname(szHostName, sizeof(szHostName)))
    {
        return get_ipaddr_by_name(szHostName, pcIp, iSize);
    }
    else
    {
        return -2;
    }
}

/**
    \brief get the first ip address of the host by name
    \param pcHostName: host name
    \param pcIp: to store host ip address
    \param iSize: buffer size
    \return 0=ok, <0 means FAIL and error code
*/
static int get_ipaddr_by_name(const char *pcHostName, char *pcIp, int iSize)
{
    if (!pcHostName || !pcIp)
    {
        return -1;
    }

    struct hostent *pHostInfo = 0;
    pHostInfo = gethostbyname(pcHostName);
    if (pHostInfo)
    {
        /*// just for test
        char **ptr = pHostInfo->h_aliases;
        while (*ptr)
        {
            TRACE("aliase:%s\n", *ptr);
        }*/
        if (AF_INET == pHostInfo->h_addrtype)
        {
            // Ipv4,return the first ip from ip lists
            struct in_addr addr;
            addr.s_addr = *(int *)pHostInfo->h_addr_list[0];
            if ((int)strlen(inet_ntoa(addr)) >= iSize)
            {
                return -2;
            }
            strcpy(pcIp, inet_ntoa(addr));
            return 0;
        }
        else
        {
            return -2;
        }
    }
    else
    {
        return -2;
    }
}

/*
 * x86, 6502, Z80, VAX,和 PDP-11都是采用小端字节序
 * Motorola 6800、68k, IBM POWER, 和 System/360则采用大端字节序
 * tcp/ip和java都是采用大端字节序
 */

#define BigEndian 1
#define LittleEndian 0
static int BigEndianTest()
{
    const int n = 1;
    if (*(char *)&n)
    {
        return LittleEndian;
    }
    return BigEndian;
}

uint64_t ntohll(uint64_t val)
{
    if (BigEndianTest() == LittleEndian)
    {
        return (((uint64_t )ntohl((int)((val << 32) >> 32))) << 32) | (unsigned int)ntohl((int)(val >> 32));
    }
    else
    {
        return val;
    }
}

uint64_t htonll(uint64_t val)
{
    if (BigEndianTest() == LittleEndian)
    {
        return (((uint64_t )htonl((int)((val << 32) >> 32))) << 32) | (unsigned int)htonl((int)(val >> 32));
    }
    else
    {
        return val;
    }
}

char** parse_list_str(const char *list_str, int *list_cnt, const char *delim)
{
    char *rest;         /* to point to the rest of the string.*/
    char *token;        /* to point to the actual token returned.*/
    char *const s = zstrdup(list_str);
    char *ptr = s;    /* make point to start*/

    DynArray *ar = create_dyn_array();

    while ((token = strtok_r(ptr, delim, &rest)))
    {
        ptr = rest;
        if (token != NULL)
        {
            push_dyn_array(ar, token);
            //log_prompt("No.%d: %s\n", ar->cnt, token);
        }
    }

    char **list = (char **)zmalloc(ar->cnt * sizeof(char *));
    int i;
    for (i = 0; i < ar->cnt; i++)
    {
        list[i] = zstrdup(ar->array[i]);
    }
    *list_cnt = ar->cnt;

    destroy_dyn_array(ar);
    zfree(s);

    return list;
}


#if 0
int main(int argc, char **argv)
{
    char *path = "/home/wujian/svn/fooyun/redis/src/db";
    check_and_make_dir(path);
    return 0;
}
#endif

