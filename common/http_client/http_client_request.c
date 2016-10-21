#include <string.h>

#include "http_client_request.h"
#include "zmalloc.h"


HttpClientRequest *create_http_request()
{
    HttpClientRequest *r = (HttpClientRequest *)zcalloc_m(sizeof(HttpClientRequest));
    if (r)
    {
        r->last = &(r->rsp_body);
    }
    return r;
}

void free_http_request(HttpClientRequest *r)
{
    if (r == 0)
    {
        return;
    }

    chain_t *c = r->rsp_body;

    while (c)
    {
        chain_t *tmp = c;
        c = c->next;
        zfree(tmp);
    }

    zfree(r);
}

void append_rsp_data(HttpClientRequest *r, const char *data, int data_len)
{
    const int size = sizeof(chain_t) + data_len;
    char *buf = (char *)zmalloc(size);
    if (buf == 0)
    {
        return;
    }

    chain_t *c = (chain_t *)buf;
    c->next = 0;

    c->buf.ptr = buf + sizeof(*c);
    c->buf.size = data_len;

    memcpy(c->buf.ptr, data, data_len);

    *r->last = c;
    r->last = &(c->next);
}

int get_rsp(HttpClientRequest *r, char **rsp, int *rsp_len)
{
    int total = 0;
    chain_t *c = r->rsp_body;

    while (c)
    {
        total += c->buf.size;
        c = c->next;
    }

    *rsp = 0;
    if (total)
    {
        *rsp = (char *)zmalloc(total + 1);
        if (*rsp)
        {
            *rsp_len = total;
            char *ptr = *rsp;
            c = r->rsp_body;
            while (c)
            {
                memcpy(ptr, c->buf.ptr, c->buf.size);
                ptr += c->buf.size;

                c = c->next;
            }
            (*rsp)[total] = 0;
        }
    }

    if (*rsp)
    {
        //printf("%s", *rsp);
        return 0;
    }
    else
    {
        return 1;
    }
}


