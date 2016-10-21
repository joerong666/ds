#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include "ch.h"

typedef struct ip_node_tag
{
    char ip[128];
    struct ip_node_tag *next;
} ip_node;

static void trim_line(char *line)
{
    const int len = strlen(line);
    if (len > 1 && (line[len - 1] == '\n') || (line[len - 1] == '\r'))
    {
        line[len - 1] = 0;
    }
    if (len > 2 && (line[len - 2] == '\n') || (line[len - 2] == '\r'))
    {
        line[len - 2] = 0;
    }
}

static void get_randon_key(char *key, int key_len)
{
    static char dict[] = "zyxwvutsrqponmlkjihgfedcba1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    static const int dict_size = sizeof(dict) - 1;
    const int len = (rand() + 1) % (key_len - 1);
    int i;
    for (i = 0; i < len; i++)
    {
        const int idx_rand = rand() % dict_size;
        key[i] = dict[idx_rand];
    }
    key[len] = 0;
}

static int get_cont(serv_node_info_t **cont, const char *conf)
{
    /* read from file */
    ip_node *ip_list = 0;
    ip_node **pnode = &ip_list;
    ip_node *node = *pnode;

    FILE *fp = fopen(conf, "r");
    if (fp == 0)
    {
        printf("fopen() fail for read, errno=%d, file=%s\n", errno, conf);
        return 1;
    }

    char line[128];
    char *p = 0;
    int cnt = 0;
    while ((p = fgets(line, sizeof(line), fp)) != 0)
    {
        trim_line(line);
        node = (ip_node *)malloc(sizeof(ip_node));
        node->next = 0;
        *pnode = node;

        strcpy(node->ip, line);

        pnode = &node->next;
        node = *pnode;
        cnt++;
    }
    fclose(fp);
    printf("these is %d ip in the file\n", cnt);


    char **server_keys = 0;
    int *sk_lens = 0;
    server_keys = (char **)malloc(cnt * sizeof(char *));
    sk_lens = (int *)malloc(cnt * sizeof(int));
    int i;
    node = ip_list;
    for (i = 0; i < cnt; i++)
    {
        server_keys[i] = node->ip;
        sk_lens[i] = strlen(server_keys[i]);
        node = node->next;
    }
    int ret = 0;
    *cont = consistent_hash_init((const char *const*)server_keys, cnt);
    if (0 == *cont)
    {
        ret = 2;
    }

    /* release */
    node = ip_list;
    while (node)
    {
        ip_node *tmp = node;
        node = node->next;
        free(tmp);
    }
    free(server_keys);
    free(sk_lens);

    return ret;
}

int main(int argc, char **argv)
{
    char key[128];
    const char *conf = 0;
    int num = 0;
    if (argc == 2)
    {
        num = atoi(argv[1]);
        conf = "./ip_list.txt";
    }
    else if (argc == 3)
    {
        num = atoi(argv[1]);
        conf = argv[2];
    }
    else
    {
        printf("Usage: %s num [ip_list_config_file]\n", argv[0]);
        return 1;
    }

    serv_node_info_t *cont = 0;
    int ret = get_cont(&cont, conf);
    if (ret != 0)
    {
        return 1;
    }

    srand(time(0));

    serv_node_t *serv = 0;
    int i;
    printf("==== servers(%d)====\n", cont->numpoints);
    
    for (i = 0; i < cont->numpoints; i++)
    {
        printf("%d %s\n",
               cont->servers[i].point, cont->servers[i].server_key);
    }
    printf("====\n\n");
    

    int *cnt = (int *)calloc(cont->numpoints, sizeof(int));
    if (cnt == 0)
    {
        printf("calloc() fail for %d * sizeof(int)\n", cont->numpoints);
        consistent_hash_destory(cont);
        return 0;
    }

    for (i = 0; i < num; i++)
    {
        get_randon_key(key, sizeof(key));
        const int key_len = strlen(key);
        serv = ch_get_server(cont, key, key_len);
        if (serv)
        {
            //printf("the key=\"%s\" (hash:%"PRIu64") on %d, %s\n",
            //       key, ch_hash(key, key_len), serv->point, serv->server_key);
            cnt[serv->point]++;
        }
        else
        {
            printf("ch_get_server() fail for key=%s\n", key);
        }
    }

    printf("%d randon keys hash to:\n", num);
    for (i = 0; i < cont->numpoints; i++)
    {
        serv = cont->servers + i;
        printf("No.%d (%s) : %d, %4.2f\%\n"
               , serv->point, serv->server_key, cnt[serv->point], cnt[serv->point] * 100.0 / num);
    }
    free(cnt);
    cnt = 0;

    consistent_hash_destory(cont);

    return 0;
}

