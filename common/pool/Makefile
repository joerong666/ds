INC_PATH = -I../../pf_lib/include -I../../util -I../../depend/jemalloc/include

CC = gcc -O2
CFLAGS = $(INC_PATH)
AR = ar

LIBRARY = libucpool.a

CSRCS := thread_manager.c thread_pool.c queue.c

.PHONY : clean all

all: $(LIBRARY)

$(LIBRARY): $(CSRCS:%.c=%.o)
	$(AR) -rs $@ $+

%.o: %.c
	$(CC) -c $(CFLAGS) $<

clean:
	-rm -f $(CSRCS:%.c=%.o) $(LIBRARY)

