#include "curl/curl.h"

#include "http_client_request.h"


static char scErrDesc[CURL_ERROR_SIZE + 1];

static size_t write_body(char *ptr, size_t size, size_t nmemb, void *userdata)
{
    const size_t len = size * nmemb;
    append_rsp_data((HttpClientRequest *)userdata, (const char*)ptr, len);
    return len;
}

int http_post(const char *url, const char *body, int body_len, int timeout, char **rsp, int *rsp_len, int *http_rsp_code)
{
    if (!url || (body && body_len <= 0) || !rsp || !rsp_len || !http_rsp_code)
    {
        /* illegal paramter */
        return 4;
    }

    CURL *curl = 0;
    struct curl_slist *http_headers = 0;
    CURLcode res;
    int ret = 1;

    HttpClientRequest *r = 0;
    r = create_http_request();
    if (r == 0)
    {
        return 5;
    }

    curl = curl_easy_init();
    if (curl == 0)
    {
        free_http_request(r);
        return 6;
    }

    http_headers = curl_slist_append(http_headers, "Connection: close");
    if (http_headers == 0)
    {
        curl_easy_cleanup(curl);
        free_http_request(r);
        return 3;
    }

    http_headers = curl_slist_append(http_headers, "Expect:");
    if (http_headers == 0)
    {
        curl_easy_cleanup(curl);
        free_http_request(r);
        return 3;
    }

    res = curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
    if (res != CURLE_OK)
    {
        ret = 7;
        *http_rsp_code = res;
        goto http_client_over;
    }

    res = curl_easy_setopt(curl, CURLOPT_VERBOSE, 0L);
    if (res != CURLE_OK)
    {
        ret = 7;
        *http_rsp_code = res;
        goto http_client_over;
    }

    res = curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, scErrDesc);
    if (res != CURLE_OK)
    {
        ret = 7;
        *http_rsp_code = res;
        goto http_client_over;
    }

    res = curl_easy_setopt(curl, CURLOPT_FAILONERROR, 1L);
    if (res != CURLE_OK)
    {
        ret = 7;
        *http_rsp_code = res;
        goto http_client_over;
    }

    res = curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 1L);
    if (res != CURLE_OK)
    {
        ret = 7;
        *http_rsp_code = res;
        goto http_client_over;
    }

    res = curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_body);
    if (res != CURLE_OK)
    {
        ret = 7;
        *http_rsp_code = res;
        goto http_client_over;
    }

    res = curl_easy_setopt(curl, CURLOPT_WRITEDATA, r);
    if (timeout != 0)
    {
        res = curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, timeout < 0 ? 5 : timeout);
        if (res != CURLE_OK)
        {
            ret = 7;
            *http_rsp_code = res;
            goto http_client_over;
        }

        res = curl_easy_setopt(curl, CURLOPT_TIMEOUT, timeout < 0 ? 5 : timeout);
        if (res != CURLE_OK)
        {
            ret = 7;
            *http_rsp_code = res;
            goto http_client_over;
        }
    }

    res = curl_easy_setopt(curl, CURLOPT_URL, url);
    if (res != CURLE_OK)
    {
        ret = 7;
        *http_rsp_code = res;
        goto http_client_over;
    }

    res = curl_easy_setopt(curl, CURLOPT_HTTPHEADER, http_headers);
    if (res != CURLE_OK)
    {
        ret = 7;
        *http_rsp_code = res;
        goto http_client_over;
    }

    if (body)
    {
        res = curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body);
        if (res != CURLE_OK)
        {
            ret = 7;
            *http_rsp_code = res;
            goto http_client_over;
        }

        res = curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, body_len);
        if (res != CURLE_OK)
        {
            ret = 7;
            *http_rsp_code = res;
            goto http_client_over;
        }
    }
    else
    {
        res = curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, 0);
        if (res != CURLE_OK)
        {
            ret = 7;
            *http_rsp_code = res;
            goto http_client_over;
        }
    }

    res = curl_easy_perform(curl);
    if (res != CURLE_OK)
    {
        /* print scErrDesc */
        if (res == CURLE_HTTP_RETURNED_ERROR)
        {
            curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, http_rsp_code);
            ret = 2;
        }
        else
        {
            *http_rsp_code = res;
            ret = 1;
        }
    }
    else
    {
        get_rsp(r, rsp, rsp_len);
        *http_rsp_code = 200;
        ret = 0;
    }

http_client_over:
    curl_slist_free_all(http_headers);
    curl_easy_cleanup(curl);
    free_http_request(r);
    return ret;
}

#if 0
#include "string.h"

int main(int argc, char **argv)
{
    //char *url = "http://127.0.0.1:";
    char *url = "http://www.360doc.com/content/12/0316/11/1317564_194762029.shtml";
    char *body = "abc";
    int body_len = strlen(body);
    char *rsp = 0;
    int rsp_len;
    int rsp_code;

    const int ret = http_post(url, body, body_len, -1, &rsp, &rsp_len, &rsp_code);
    printf("ret=%d, rsp_len=%d, rsp_code=%d\n%s\n", ret, rsp_len, rsp_code, rsp);
    return 0;
}

#endif

