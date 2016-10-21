#ifndef _HTTP_CLIENT_REQUEST_H_
#define _HTTP_CLIENT_REQUEST_H_

typedef struct buf_tag
{
    char *ptr;
    int size;
} buf_t;

typedef struct chain_tag
{
    buf_t buf;
    struct chain_tag *next;
} chain_t;

typedef struct HttpClientRequest_t
{
    chain_t *rsp_body;
    chain_t **last;
} HttpClientRequest;

extern HttpClientRequest *create_http_request();
extern void free_http_request(HttpClientRequest *r);
extern void append_rsp_data(HttpClientRequest *r, const char *data, int data_len);
extern int get_rsp(HttpClientRequest *r, char **rsp, int *rsp_len);

#endif /* _HTTP_CLIENT_REQUEST_H_ */

