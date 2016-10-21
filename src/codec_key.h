#ifndef _CODEC_KEY_H_
#define _CODEC_KEY_H_

#include "redis.h"

#define KEY_TYPE_STRING      'K'
#define KEY_TYPE_LIST        'L'
#define KEY_TYPE_SET         'S'
#define KEY_TYPE_ZSET        'Z'
#define KEY_TYPE_HASH        'H'

typedef struct dbe_key_attr_t
{
    char *key;
    size_t key_len;
    char type;
    char *sub_key;
    size_t sub_key_len;
    int64_t seq;
} dbe_key_attr;

typedef struct zset_val_attr_t
{
    char *member;
    size_t m_len;
    double score;
} zset_val_attr;

typedef struct hash_val_attr_t
{
    char *field;
    size_t f_len;
    char* val;
    size_t v_len;
} hash_val_attr;

/* format of key stored in dbe:
 * key_len(2 bytes) + key(n bytes) + key_type(1 byte) + sub_key(optional)
 * [key_len] string format in hex
 * [sub_key] null for string, seq for list, hash_val for hash/set/zset, 16 bytes
 */

extern char *encode_prefix_key(const char* key, size_t k_len, char key_type, size_t *enc_len);
extern char *encode_string_key(const char* key, size_t k_len, size_t *enc_len);
extern char *encode_list_key(const char* key, size_t k_len, int64_t seq, size_t *enc_len);
extern char *encode_set_key(const char* key, size_t k_len, const char* member, size_t m_len, size_t *enc_len);
extern char *encode_zset_key(const char* key, size_t k_len, const char* member, size_t m_len, size_t *enc_len);
extern char *encode_hash_key(const char* key, size_t k_len, const char* field, size_t f_len, size_t *enc_len);

extern int decode_dbe_key(const char* dbe_key, size_t dbe_k_len, dbe_key_attr *attr);

/* format of the value stored in dbe:
 * 1) string:
 * value only(serialized stream)
 * 2) list:
 * type(128,1byte) + value(n bytes)
 * 3) set:
 * type(128,1byte) + member(n bytes)
 * 4) zset:
 * type(128,1byte) + member_len(8 bytes) + member(n bytes) + score(n byte, in string)
 * 5) hash:
 * type(128,1byte) + field_len(8 bytes) + field(n bytes) + value(n bytes)
 */

extern char *encode_list_val(const char* val, size_t v_len, size_t *enc_len);
extern char *encode_set_val(const char* member, size_t m_len, size_t *enc_len);
extern char *encode_zset_val(const char* member, size_t m_len, double score, size_t *enc_len);
extern char *encode_hash_val(const char* field, size_t f_len, const char *val, uint64_t val_len, size_t *enc_len);
extern int decode_zset_val(const char* dbe_val, size_t dbe_v_len, zset_val_attr *attr);
extern int decode_hash_val(const char* dbe_val, size_t dbe_v_len, hash_val_attr *attr);

#endif /* _CODEC_KEY_H_ */

