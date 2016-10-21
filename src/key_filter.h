#ifndef _KEY_FILTER_H_
#define _KEY_FILTER_H_

extern int init_filter();

/*return :
 * 0 - valid key
 * 1 - illegal key
 */
extern int gen_filter_server();
extern int master_filter(const char *key);
extern int master_filter_4_dbe(const char *key, size_t);
extern int master_filter_keylen(const char *key, int key_len);

extern int filter_gen(const char *key, int key_len, const char *ds_key);

extern void set_bl_filter_list(const char *list, int list_len, int permission);
extern int bl_filter(const char *key, int key_len);
extern void set_sync_filter_list(const char *list, int list_len, int permission);
extern int sync_filter(const char *key, int key_len);
extern void set_cache_filter_list(const char *list, int list_len, int permission);
extern int cache_filter(const char *key, int key_len);

#endif /* _KEY_FILTER_H_ */

