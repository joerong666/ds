INC_PATH = -I../../depend/curl/include -I../../util -I../../depend/jemalloc/include

CURL_PATH=../../depend/curl
CURL_DEP=../../depend/curl/lib/.libs/libcurl.a

CC = gcc -O2
CFLAGS = $(INC_PATH)
AR = ar

LIBRARY = libhttpclient.a

CSRCS := http_client.c http_client_request.c

.PHONY : clean all

all: $(LIBRARY)

$(CURL_DEP):
	@cd $(CURL_PATH) && ./configure --without-ssl --disable-ldap --disable-ldaps && $(MAKE)

$(LIBRARY): $(CSRCS:%.c=%.o)
	$(AR) -rs $@ $+

%.o: %.c $(CURL_DEP)
	$(CC) -c $(CFLAGS) $<

clean:
	-rm -f $(CSRCS:%.c=%.o) $(LIBRARY)

