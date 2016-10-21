INC_PATH = -I../../depend/jemalloc/include

CC = gcc -O2 
CFLAGS = $(INC_PATH)
AR = ar

LIBRARY = libch.a

CSRCS := ch.c md5.c

.PHONY : clean all

all: $(LIBRARY)

$(LIBRARY): $(CSRCS:%.c=%.o)
	$(AR) -rs $@ $+

%.o: %.c
	$(CC) -c $(CFLAGS) $<

clean:
	-rm -f $(CSRCS:%.c=%.o) $(LIBRARY)

