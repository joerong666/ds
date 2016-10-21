#ifndef _CONSISTENCE_HASH_H_
#define _CONSISTENCE_HASH_H_

#include <inttypes.h>
#include <stddef.h>

#define CH_SERVER_KEY_MAX_LENGT        64
#define CH_PER_SERVER_VNODE            256 

typedef struct serv_node
{
    unsigned int point;
	char         server_key[CH_SERVER_KEY_MAX_LENGT]; 
}serv_node_t;

typedef struct serv_vnode
{
    serv_node_t     *sn;
    uint64_t        end;
}serv_vnode_t;

typedef struct serv_node_info
{
	int			    numpoints; 
    serv_node_t     *servers;
    int             num_vpoints;
	serv_vnode_t	*v_servers;
}serv_node_info_t;

extern uint64_t ch_hash(const void *buf, size_t len);
extern serv_node_info_t* consistent_hash_init(const char *const nodes[], int count);
extern serv_node_t* ch_get_server(serv_node_info_t *sni, const char *key, int key_len);
extern void consistent_hash_destory(serv_node_info_t *sni);

#endif /* _CONSISTENCE_HASH_H_ */

