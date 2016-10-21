#ifndef _DS_UTIL_H_
#define _DS_UTIL_H_

extern int check_and_make_dir(const char *path);
extern int get_host_ipaddr(char *pcIp, int iSize);
extern uint64_t ntohll(uint64_t val);
extern uint64_t htonll(uint64_t val);
extern char** parse_list_str(const char *list_str, int *list_cnt, const char *delim);

#endif /* _DS_UTIL_H_ */

