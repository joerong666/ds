PF_LIB_PATH = ../../pf_lib/include
UTIL_LIB_PATH = ../../util
INC_PATH = -I$(UTIL_LIB_PATH) -I$(PF_LIB_PATH) -I./ -I../../depend/jemalloc/include

CC = gcc -fPIC 
CFLAGS = $(INC_PATH) -g -O2 -DUSE_JEMALLOC -Wno-write-strings -Wall  -Wextra -Winline -Wunused -Wuninitialized -Wfloat-equal -Wmissing-include-dirs -Wformat=2
AR = ar

LIBRARY = libbl.a

CSRCS :=  bin_log.c bin_log_impl.c  file_util.c  meta_file.c  relay_log.c  replica.c  replica_proto.c  replica_thread_mng.c  rotate_file.c  

.PHONY : clean all

all: $(LIBRARY)

$(LIBRARY): $(CSRCS:%.c=%.o)
	$(AR) -rs $@ $+

%.o: %.c
	$(CC) -c $(CFLAGS) $<

test: test_bin.c test_repl.c libbl.a
	cc -o test_bin test_bin.c libbl.a ../../util/libutil.a ../../pf_lib/src/libpflib.a -I../../pf_lib/include -I../../util -lrt
	cc -o test_repl test_repl.c libbl.a -I../../pf_lib/include -I../../util ../../util/libutil.a ../../pf_lib/src/libpflib.a -lrt
	

clean:
	-rm -f $(CSRCS:%.c=%.o) $(LIBRARY)

