#ifndef _HTTP_CLIENT_H_
#define _HTTP_CLIENT_H_

/*
 * paramter:
 * url - url of http request, like as: "http://host:port/path?query#fragment"
 * body - data to post to http server in http body field
 * body_len - the length of the body
 * timeout - ms, 0 no timeout, -1 default value(5s), > 0 set timeout
 * rsp - when succ, body of respond, must free by caller using zfree()
 * rsp_len - the length of rsp
 * http_rsp_code - it will be set wehn succ or http fail
 *
 * return:
 * 0 - succ, rsp & rsp_len & http_rsp_code(set to 200)
 * 1 - tcp fail
 * 2 - http fail, http_rsp_code is set
 * 3 - other fail
 * 4 - illegal paramter
 * 5 - create http ctx fail
 * 6 - curl init fail
 * 7 - curl_easy_setopt fail, http_rsp_code is set
 */
extern int http_post(const char *url, const char *body, int body_len, int timeout, char **rsp, int *rsp_len, int *http_rsp_code);

#endif /* _HTTP_CLIENT_H_ */

