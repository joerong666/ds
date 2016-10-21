#include "codec_key.h"

#include "ds_log.h"
#include "ds_util.h"
#include "rds_util.h"

extern void key_hash_str(const unsigned char *data, size_t len, char result[17]);

static int int_to_hex_str(char *buf, const char *src, int len)
{
    static const char *hex = "0123456789abcdef";
    char c;
    int i;
    for (i = 0; i < len; i++)
    {
        c = *(src + i);
        *(buf + 2 * i) = hex[(c & 0xf0) >> 4];
        *(buf + 2 * i + 1) = hex[c & 0x0f];
    }
    return len * 2;
}

/* ret:
 * 0 - succ
 * 1 - fail
 */
static int hex_str_to_byte(const char *ptr, int ptr_len, unsigned char *rslt)
{
    if (ptr == NULL || ptr_len <= 0 || ptr_len > 2 || rslt == NULL)
    {
        return 1;
    }

    *rslt = 0;
    if (*ptr >= 'a' && *ptr <= 'f')
    {
        *rslt += *ptr - 'a' + 10;
    }
    else if (*ptr >= 'A' && *ptr <= 'F')
    {
        *rslt += *ptr - 'A' + 10;
    }
    else if (*ptr >= '0' && *ptr <= '9')
    {
        *rslt += *ptr - '0';
    }
    else
    {
        return 1;
    }

    if (ptr_len > 1)
    {
        ptr++;
        *rslt <<= 4;
        if (*ptr >= 'a' && *ptr <= 'f')
        {
            *rslt += *ptr - 'a' + 10;
        }
        else if (*ptr >= 'A' && *ptr <= 'F')
        {
            *rslt += *ptr - 'A' + 10;
        }
        else if (*ptr >= '0' && *ptr <= '9')
        {
            *rslt += *ptr - '0';
        }
        else
        {
            return 1;
        }
    }
    return 0;
}

/* ret:
 * 0 - succ
 * 1 - fail
 */
static int hex_str_to_int(const char *ptr, int ptr_len, unsigned long long *rslt)
{
    char buf[128];
    char *end;
    memcpy(buf, ptr, ptr_len);
    buf[ptr_len] = 0;
    errno = 0;

    unsigned long long result = strtoull(buf, &end, 16);
    if (result == 0 && end == buf)
    {
        /* str was not a number */
        return 1;
    }
    else if (result == ULLONG_MAX && errno)
    {
        /* the value of str does not fit in unsigned long long */
        return 1;
    }
    else if (*end)
    {
        /* str began with a number but has junk left over at the end */
        return 1;
    }

    *rslt = result;
    return 0;
}

char *encode_prefix_key(const char* key, size_t k_len, char key_type, size_t *enc_len)
{
    if (!key || !enc_len || k_len > 255 || k_len == 0)
    {
        return NULL;
    }
    //log_prompt("encode_prefix_key: key=%s, type=%c", key, key_type);
    /* key_len(2 bytes) + key(n bytes) + key_type(1 byte, optional) */
    const unsigned char key_type_len = key_type == 0 ? 0 : 1;
    *enc_len = 2 + k_len + key_type_len;
    char *enc = zmalloc(*enc_len + 1); /* 1 for '\0' */
    char *ptr = enc;

    const unsigned char k_len_ = k_len % 256;
    ptr += int_to_hex_str(ptr, (const char*)&k_len_, sizeof(k_len_));

    memcpy(ptr, key, k_len);
    ptr += k_len;

    *ptr = key_type;
    ptr += key_type_len;

    *ptr = 0;
    return enc;
}

char *encode_string_key(const char* key, size_t k_len, size_t *enc_len)
{
    if (!key || !enc_len || k_len > 255 || k_len == 0)
    {
        return NULL;
    }
    log_debug("encode_string_key: key=%s", key);
    /* key_len(2 bytes) + key(n bytes) + key_type(1 byte) */
    *enc_len = 2 + k_len + 1;
    char *enc = zmalloc(*enc_len + 1); /* 1 for '\0' */
    char *ptr = enc;

    const unsigned char k_len_ = k_len % 256;
    ptr += int_to_hex_str(ptr, (const char*)&k_len_, sizeof(k_len_));

    memcpy(ptr, key, k_len);
    ptr += k_len;

    *ptr = KEY_TYPE_STRING;
    ptr += 1;

    *ptr = 0;
    return enc;
}

char *encode_list_key(const char* key, size_t k_len, int64_t seq, size_t *enc_len)
{
    if (!key || !enc_len || k_len > 255 || k_len == 0)
    {
        return NULL;
    }
    //log_prompt("encode_list_key: key=%s, seq=%"PRId64, key, seq);
    /* key_len(2 bytes) + key(n bytes) + key_type(1 byte) + seq(16bytes) */
    *enc_len = 2 + k_len + 1 + 16;
    char *enc = zmalloc(*enc_len + 1); /* 1 for '\0' */
    char *ptr = enc;

    const unsigned char k_len_ = k_len % 256;
    ptr += int_to_hex_str(ptr, (const char*)&k_len_, sizeof(k_len_));

    memcpy(ptr, key, k_len);
    ptr += k_len;

    *ptr = KEY_TYPE_LIST;
    ptr += 1;

    const int64_t seq_ = (int64_t)htonll(seq);
    ptr += int_to_hex_str(ptr, (const char*)&seq_, sizeof(seq_));

    *ptr = 0;
    return enc;
}

static char *encode_key_subkey(const char* key, size_t k_len, const char* subkey, size_t s_len, char key_type, size_t *enc_len)
{
    if (!key || !enc_len || k_len > 255 || k_len == 0)
    {
        return NULL;
    }
    /* key_len(2 bytes) + key(n bytes) + key_type(1 byte) + hash(16bytes) */
    *enc_len = 2 + k_len + 1 + 16;
    char *enc = zmalloc(*enc_len + 1); /* 1 for '\0' */
    char *ptr = enc;

    const unsigned char k_len_ = k_len % 256;
    ptr += int_to_hex_str(ptr, (const char*)&k_len_, sizeof(k_len_));

    memcpy(ptr, key, k_len);
    ptr += k_len;

    *ptr = key_type;
    ptr += 1;

    key_hash_str((const unsigned char*)subkey, s_len, ptr);
    ptr += 16;

    *ptr = 0;
    return enc;
}

char *encode_set_key(const char* key, size_t k_len, const char* member, size_t m_len, size_t *enc_len)
{
    return encode_key_subkey(key, k_len, member, m_len, KEY_TYPE_SET, enc_len);
}

char *encode_zset_key(const char* key, size_t k_len, const char* member, size_t m_len, size_t *enc_len)
{
    //log_prompt("encode_zset_key: key=%s, member=%s", key, member);
    return encode_key_subkey(key, k_len, member, m_len, KEY_TYPE_ZSET, enc_len);
}

char *encode_hash_key(const char* key, size_t k_len, const char* field, size_t f_len, size_t *enc_len)
{
    return encode_key_subkey(key, k_len, field, f_len, KEY_TYPE_HASH, enc_len);
}


char *encode_list_val(const char* val, size_t v_len, size_t *enc_len)
{
    if (!val || !enc_len || v_len == 0)
    {
        return NULL;
    }
    //log_prompt("encode_list_val: val=%s", val);
    *enc_len = v_len + 1;
    char *enc = zmalloc(*enc_len + 1); /* 1 for '\0' */

    *(unsigned char *)enc = 128;
    memcpy(enc + 1, val, v_len);
    enc[*enc_len] = 0;

    return enc;
}

char *encode_set_val(const char* member, size_t m_len, size_t *enc_len)
{
    if (!member || !enc_len || m_len == 0)
    {
        return NULL;
    }
    //log_prompt("encode_set_val: member=%s", member);
    *enc_len = m_len + 1;
    char *enc = zmalloc(*enc_len + 1); /* 1 for '\0' */

    *(unsigned char *)enc = 128;
    memcpy(enc + 1, member, m_len);
    enc[*enc_len] = 0;

    return enc;
}

/* zset value format
 * type(128,1byte) + member_len(8 bytes) + member(n bytes) + score(n byte, in string)
 */
char *encode_zset_val(const char* member, size_t m_len, double score, size_t *enc_len)
{
    if (!member || !enc_len || m_len == 0)
    {
        return NULL;
    }
    //log_prompt("encode_zset_val: member=%s, score=%lf", member, score);
    char buf[512];
    const int score_len = d2string(buf, sizeof(buf), score);
    *enc_len = 1 + 8 + m_len + score_len;
    char *enc = zmalloc(*enc_len + 1); /* 1 for '\0' */
    char *ptr = enc;

    *(unsigned char *)ptr++ = 128;
    const uint64_t m_len_ = htonll(m_len);
    memcpy(ptr, (const void *)&m_len_, sizeof(m_len_));
    ptr += sizeof(m_len_);

    memcpy(ptr, member, m_len);
    ptr += m_len;

    memcpy(ptr, buf, score_len);
    ptr += score_len;

    *ptr = 0;
    return enc;
}


/* hash value format
 * type(128,1byte) + field_len(8 bytes) + field(n bytes) + value(n bytes)
 */
char *encode_hash_val(const char* field, size_t f_len, const char *val, uint64_t val_len, size_t *enc_len)
{
    if (!field || !val || !enc_len || f_len == 0 || val_len == 0)
    {
        return NULL;
    }
    //log_prompt("encode_hash_val: field=%s, val_len=%"PRIu64, field, val_len);
    *enc_len = 1 + 8 + f_len + val_len;
    char *enc = zmalloc(*enc_len + 1); /* 1 for '\0' */
    char *ptr = enc;

    *(unsigned char *)ptr++ = 128;
    const uint64_t f_len_ = htonll(f_len);
    memcpy(ptr, (const void *)&f_len_, sizeof(f_len_));
    ptr += sizeof(f_len_);

    memcpy(ptr, field, f_len);
    ptr += f_len;

    memcpy(ptr, val, val_len);
    ptr += val_len;

    *ptr = 0;
    return enc;
}

/* ret:
 * 0 - succ
 * 1 - fail
 */
int decode_dbe_key(const char* dbe_key, size_t dbe_k_len, dbe_key_attr *attr)
{
    if (!dbe_key || !attr)
    {
        log_error("decode_dbe_key paramter illegal");
        return 1;
    }
    memset(attr, 0, sizeof(*attr));
    if (server.enc_kv == 0)
    {
        attr->key = (char *)dbe_key;
        attr->key_len = dbe_k_len > 0 ? dbe_k_len - 1 : dbe_k_len;
        attr->type = KEY_TYPE_STRING;
        return 0;
    }

    if (dbe_k_len <= 2)
    {
        log_error("decode_dbe_key paramter illegal, len=%zu", dbe_k_len);
        return 1;
    }

    const char *ptr = dbe_key;
    unsigned char key_len;
    int ret = hex_str_to_byte(ptr, 2, &key_len);
    if (ret != 0)
    {
        log_error("decode key'len fail");
        return 1;
    }
    attr->key_len = key_len;
    ptr += 2;

    attr->key = (char *)ptr;
    ptr += attr->key_len;

    int len = dbe_k_len - (2 + attr->key_len);
    if (len < 0)
    {
        log_error("decode_dbe_key len=%zd illegal, too small, key_len=%zd"
                , dbe_k_len, attr->key_len);
        return 1;
    }
    else if (len == 0)
    {
        attr->type = 0;
        attr->sub_key = 0;
        attr->sub_key_len = 0;
    }
    else if (len == 1)
    {
        attr->type = *ptr;
        attr->sub_key = 0;
        attr->sub_key_len = 0;
    }
    else
    {
        attr->type = *ptr++;
        attr->sub_key = (char *)ptr;
        attr->sub_key_len = len - 1;

        /* decode seq */
        if (attr->type == 'L')
        {
            unsigned long long seq;
            ret = hex_str_to_int(attr->sub_key, attr->sub_key_len, &seq);
            if (ret != 0)
            {
                log_error("decode seq from dbe_key fail, dbe_key_len=%zu, seq_len=%zu"
                        , dbe_k_len, attr->sub_key_len);
                return 1;
            }
            attr->seq = (int64_t)seq;//ntohll((uint64_t)seq);
        }
    }

    return 0;
}

/* ret:
 * 0 - succ
 * 1 - fail
 */
int decode_zset_val(const char* dbe_val, size_t dbe_v_len, zset_val_attr *attr)
{
    if (!dbe_val || dbe_v_len <= 1 + sizeof(uint64_t) || !attr)
    {
        log_error("decode_zset_val paramter illegal, len=%zu", dbe_v_len);
        return 1;
    }

    memset(attr, 0, sizeof(*attr));
    const char *ptr = dbe_val + 1;
    attr->m_len = (size_t)ntohll(*(const uint64_t *)ptr);
    ptr += sizeof(uint64_t);

    attr->member = (char*)ptr;
    ptr += attr->m_len;

    const int64_t v_len = dbe_v_len - (1 + sizeof(uint64_t) + attr->m_len);
    if (v_len <= 0 || v_len >= 128)
    {
        log_error("decode_zset_val: score_len=%"PRId64", illegal, src_len=%zu"
                , v_len, dbe_v_len);
        return 1;
    }
    char buf[128];
    memcpy(buf, ptr, v_len);
    buf[v_len] = 0;
    attr->score = strtod(buf, NULL);

    return 0;
}

/* ret:
 * 0 - succ
 * 1 - fail
 */
int decode_hash_val(const char* dbe_val, size_t dbe_v_len, hash_val_attr *attr)
{
    if (!dbe_val || dbe_v_len <= 1 + sizeof(uint64_t) || !attr)
    {
        log_error("decode_hash_val paramter illegal, len=%zu", dbe_v_len);
        return 1;
    }

    memset(attr, 0, sizeof(*attr));
    const char *ptr = dbe_val + 1;
    attr->f_len = (size_t)ntohll(*(const uint64_t *)ptr);
    ptr += sizeof(uint64_t);

    attr->field = (char*)ptr;
    ptr += attr->f_len;

    const int64_t v_len = dbe_v_len - (1 + sizeof(uint64_t) + attr->f_len);
    if (v_len <= 0)
    {
        log_error("decode_hash_val: val_len=%"PRId64", illegal, src_len=%zu"
                , v_len, dbe_v_len);
        return 1;
    }
    attr->val = (char*)ptr;
    attr->v_len = v_len;

    return 0;
}

#if 0
void codec_test_case1(const char *key, const char *sub_key)
{
    size_t len[6];
    char *dec[6];
    const int key_len = strlen(key);
    const int sub_key_len = strlen(sub_key);
    int64_t seq = atoi(sub_key);
    dec[0] = encode_prefix_key(key, key_len, 0, &len[0]);
    dec[1] = encode_string_key(key, key_len, &len[1]);
    dec[2] = encode_list_key(key, key_len, seq, &len[2]);
    dec[3] = encode_set_key(key, key_len, sub_key, sub_key_len, &len[3]);
    dec[4] = encode_zset_key(key, key_len, sub_key, sub_key_len, &len[4]);
    dec[5] = encode_hash_key(key, key_len, sub_key, sub_key_len, &len[5]);

    printf("key=%s\n"
           "prefix=%s, len=%zd\n"
           "str=%s, len=%zd\n"
           "list=%s, len=%zd\n"
           "set=%s, len=%zd\n"
           "zset=%s, len=%zd\n"
           "hash=%s, len=%zd\n"
          , key, dec[0], len[0], dec[1], len[1], dec[2], len[2], dec[3], len[3], dec[4], len[4], dec[5], len[5]);

    dbe_key_attr attr;
    int ii;
    for (ii=0; ii < 6; ii++)
    {
        int ret = decode_dbe_key(dec[ii], len[ii], &attr);
        if (ret == 0)
        {
            printf("No.%d: succ\n"
               "key_len=%zd, key=%.*s\n"
               "type=%c\n"
               "seq=%"PRId64
               "\n"
               , ii, attr.key_len, (int)attr.key_len, attr.key, attr.type, attr.seq
              );
            if (attr.sub_key)
            {
                printf("sub_key_len=%zd, sub_key=%.*s\n"
                      , attr.sub_key_len, (int)attr.sub_key_len, attr.sub_key
                      );
            }
            else
            {
                printf("sub_key is null, sub_key_len=%zd\n", attr.sub_key_len);
            }
        }
        else
        {
            printf("No.%d: ret=%d\n", ii, ret);
        }

        zfree(dec[ii]);
    }
}

void codec_test_case2(const char *member, double score)
{
    size_t m_len = strlen(member);
    size_t len;
    char *enc = encode_zset_val(member, m_len, score, &len);
    printf("member=%s, score=%lf, enc_len=%zd, enc=%s\n"
          , member, score, len, enc);

    zset_val_attr attr;
    int ret = decode_zset_val(enc, len, &attr);
    if (ret == 0)
    {
        printf("dec succ: member_len=%zd, member=%.*s, score=%lf\n"
              , attr.m_len, (int)attr.m_len, attr.member, attr.score
              );
    }
    else
    {
        printf("decode_zset_val() fail, ret=%d\n", ret);
    }

    zfree(enc);
}

void codec_test_case3(const char *field, const char *val)
{
    size_t f_len = strlen(field);
    size_t v_len = strlen(val);
    size_t len;
    char *enc = encode_hash_val(field, f_len, val, v_len, &len);
    printf("field=%s, val=%s, enc_len=%zd, enc=%s\n"
          , field, val, len, enc);

    hash_val_attr attr;
    int ret = decode_hash_val(enc, len, &attr);
    if (ret == 0)
    {
        printf("dec succ: f_len=%zd, field=%.*s, v_len=%zd, val=%.*s\n"
              , attr.f_len, (int)attr.f_len, attr.field, attr.v_len, (int)attr.v_len, attr.val
              );
    }
    else
    {
        printf("decode_zset_val() fail, ret=%d\n", ret);
    }

    zfree(enc);
}

#endif
