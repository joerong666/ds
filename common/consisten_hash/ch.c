#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>

#include "ch.h"
//#include "md5.h"

/* 64 bit FNV-1a non-zero initial basis */
#define FNV1A_64_INIT ((uint64_t) 0xcbf29ce484222325ULL)
#define FNV_64_PRIME ((uint64_t) 0x100000001b3ULL)

/* 64 bit Fowler/Noll/Vo FNV-1a hash code */
static inline uint64_t fnv_64a_buf(const void *buf, size_t len, uint64_t hval)
{
	const unsigned char *p = (const unsigned char *) buf;
    int i;

	for (i = 0; i < len; i++)
    {
		hval ^= (uint64_t) p[i];
		hval *= FNV_64_PRIME;
	}

	return hval;
}

/*
 * The result is same as fnv_64a_buf(&v, sizeof(v), hval) but this function
 * is a bit faster.
 */
static inline uint64_t fnv_64a_64(uint64_t v, uint64_t hval)
{
	hval ^= v & 0xff;
	hval *= FNV_64_PRIME;
	hval ^= v >> 8 & 0xff;
	hval *= FNV_64_PRIME;
	hval ^= v >> 16 & 0xff;
	hval *= FNV_64_PRIME;
	hval ^= v >> 24 & 0xff;
	hval *= FNV_64_PRIME;
	hval ^= v >> 32 & 0xff;
	hval *= FNV_64_PRIME;
	hval ^= v >> 40 & 0xff;
	hval *= FNV_64_PRIME;
	hval ^= v >> 48 & 0xff;
	hval *= FNV_64_PRIME;
	hval ^= v >> 56 & 0xff;
	hval *= FNV_64_PRIME;

	return hval;
}

uint64_t ch_hash(const void *buf, size_t len)
{
	uint64_t hval = fnv_64a_buf(buf, len, FNV1A_64_INIT);

	return fnv_64a_64(hval, hval);
}

static inline uint64_t ch_hash_64(uint64_t v)
{
	uint64_t hval = fnv_64a_64(v, FNV1A_64_INIT);

	return fnv_64a_64(hval, hval);
}

static inline uint64_t ch_hash_next(uint64_t hval)
{
	return fnv_64a_64(hval, hval);
}

static inline void ch_create_vnode(serv_node_t *sn, serv_vnode_t *v)
{
    int i;
    uint64_t hval = ch_hash(sn->server_key, strlen(sn->server_key));

    for (i = 0; i < CH_PER_SERVER_VNODE; i++)
    {
        hval = ch_hash_next(hval);
        v->end = hval;
        v->sn = sn;
        v++;
    }
}

static inline int vnode_cmp(const void *p1, const void *p2)
{
    serv_vnode_t *s1 = (serv_vnode_t *)p1;
    serv_vnode_t *s2 = (serv_vnode_t *)p2;
    return (s1->end < s2->end ? -1 : s1->end > s2->end ? 1 : 0);
}

serv_node_info_t* consistent_hash_init(const char *const nodes[], int count)
{
	serv_node_t         *sn;
    serv_vnode_t        *v;
	serv_node_info_t    *sni;
	int                  i;

	if (nodes == NULL || count <= 0)
	{
		return NULL;
	}
	
	sni = malloc(sizeof(serv_node_info_t));
	sni->numpoints = count;
    sni->num_vpoints = count * CH_PER_SERVER_VNODE;
	sni->servers = calloc(sni->numpoints, sizeof(serv_node_t));
	sni->v_servers = calloc(sni->num_vpoints, sizeof(serv_vnode_t));

	sn = sni->servers;
    v  = sni->v_servers;
	for (i = 0; i < sni->numpoints; i++, sn++)
	{
        sn->point = i;
		strncpy(sn->server_key, nodes[i], sizeof(sn->server_key) - 1);
        ch_create_vnode(sn, v);
        v += CH_PER_SERVER_VNODE;  
	}

	qsort(sni->v_servers, sni->num_vpoints, sizeof(serv_vnode_t), vnode_cmp); 

    /*
    v  = sni->v_servers;
	for (i = 0; i < sni->numpoints; i++, v++)
	{
		printf("[%d] [%s] [%llu]\n", v->sn->point, v->sn->server_key, v->end);
	}
    */

	return sni;	
}

/*
static inline void
ch_md5_digest(unsigned char md5pword[16], const char *str, int str_len)
{
    md5_state_t md5state;

    md5_init(&md5state);
    md5_append(&md5state, (const md5_byte_t *)str, str_len);
    md5_finish(&md5state, md5pword);
}


uint64_t ch_hash(const void* buf, size_t len)
{
    unsigned char digest[16];
    ch_md5_digest(digest, (const char *)buf, (int)len);
    uint64_t ret = ( (uint64_t)digest[7] << 56 )
                     | ( (uint64_t)digest[6] << 48 )
                     | ( (uint64_t)digest[5] << 40 )
                     | ( (uint64_t)digest[4] << 32 )
                     | ( (uint64_t)digest[3] << 24 )
                     | ( (uint64_t)digest[2] << 16 )
                     | ( (uint64_t)digest[1] <<  8 )
                     |   (uint64_t)digest[0];

    return ret;
}
*/

serv_node_t* ch_get_server(serv_node_info_t *sni, const char *key, int key_len)
{
	serv_vnode_t *v;
	int 		count;
	uint64_t 	hcode;
	int 		bidx;
	int 		start, end, pos;

	if (sni == NULL || key == NULL)
	{
		return NULL;
	}

	count = sni->num_vpoints;
	v = sni->v_servers;

	//printf("key is [%s]\n", key);
	if (count <= 0)
	{
		return NULL;
	}

	if (count == 1)
	{
		return v[0].sn;
	}
	
	hcode = ch_hash(key, key_len);
	//printf("get hcode is [%llu]\n", hcode);

	if (hcode <= v[0].end
		|| hcode > v[count - 1].end)
	{
		return v[0].sn;
	}

	start = 0;
    end = count - 1;

    /* 折半查找 前 < id, 后 >= id 的节点，返回后 */
    for (;;)
	{
        pos = (end - start) / 2 + start;
        if (v[pos].end < hcode)
		{
            if (v[pos + 1].end >= hcode)
			{
				return v[pos + 1].sn;
			}
            start = pos;
        }
		else
		{
            end = pos;
		}
    }
	return NULL;	
}

void consistent_hash_destory(serv_node_info_t *sni)
{
	if (sni)
	{
        if (sni->v_servers)
            free(sni->v_servers);
		if (sni->servers)
			free(sni->servers);
		free(sni);
	}
}

#if 0
void gen_random(char *s, const int len)
{
	int i;
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

    for (i = 0; i < len; ++i) {
        s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }

    s[len] = 0;
}

int main()
{
	char* a[] = {
			"192.168.3.151",
			"19asddfasvb.3.16",
			"19adf.168.3.7adasd",
			"202.1.3.121"
		};

    int  b[4] = {0};
	char key[64];
	serv_node_info_t* sni;
    serv_node_t *sn;

	sni = consistent_hash_init((const char**)a, 4);

	int i;
    int j;
	srandom(time(0));
	for (i = 0; i < 10; i++)
	{
		gen_random(key, 63);
        sn = ch_get_server(sni, key, 63);
		//printf("get node [%s]\n", sn->server_key);
        for (j = 0; j < 4; j++)
        {
            if (strcmp(sn->server_key, a[j]) == 0)
            {
                b[j]++;
            }
        }
	}
    for (j = 0; j < 4; j++)
    {
        printf("[%d] count [%d]\n", j, b[j]);
    }
	consistent_hash_destory(sni);
	return 0;
}
#endif



