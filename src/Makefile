# Redis Makefile
# Copyright (C) 2009 Salvatore Sanfilippo <antirez at gmail dot com>
# This file is released under the BSD license, see the COPYING file

release_hdr := $(shell sh -c './mkreleasehdr.sh')
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')
OPTIMIZATION?=-O2

CCCOLOR="\033[34m"
LINKCOLOR="\033[34;1m"
SRCCOLOR="\033[33m"
BINCOLOR="\033[37;1m"
MAKECOLOR="\033[32;1m"
ENDCOLOR="\033[0m"

ifndef V
QUIET_CC = @printf '    %b %b\n' $(CCCOLOR)CC$(ENDCOLOR) $(SRCCOLOR)$@$(ENDCOLOR);
QUIET_LINK = @printf '    %b %b\n' $(LINKCOLOR)LINK$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR);
endif

ifeq ($(uname_S),Linux)
  ifneq ($(FORCE_LIBC_MALLOC),yes)
    USE_JEMALLOC=yes
    USE_LEVELDB=no
  endif
endif

ifeq ($(uname_S),SunOS)
  CFLAGS?=-pedantic $(OPTIMIZATION) -Wall -W -D__EXTENSIONS__ -D_XPG6
  CCLINK?=-ldl -lnsl -lsocket -lm -lpthread
  DEBUG?=-g -ggdb
else
  CFLAGS?=-std=c99 -pedantic $(OPTIMIZATION) -Wall -W $(ARCH) $(PROF)
  CCLINK?=-lm -pthread
  DEBUG?=-g -rdynamic -ggdb
endif

ifeq ($(USE_TCMALLOC),yes)
  ALLOC_DEP=
  ALLOC_LINK=-ltcmalloc
  ALLOC_FLAGS=-DUSE_TCMALLOC
endif

ifeq ($(USE_TCMALLOC_MINIMAL),yes)
  ALLOC_DEP=
  ALLOC_LINK=-ltcmalloc_minimal
  ALLOC_FLAGS=-DUSE_TCMALLOC
endif

ifeq ($(USE_JEMALLOC),yes)
  ALLOC_PATH=../../depend/jemalloc
  ALLOC_DEP=$(ALLOC_PATH)/lib/libjemalloc.a
  ALLOC_LINK=$(ALLOC_DEP) -ldl
  ALLOC_FLAGS=-DUSE_JEMALLOC -I$(ALLOC_PATH)/include
endif

ifeq ($(USE_LEVELDB),yes)
#DBE_PATH=../../../redis/deps/leveldb
#DBE_DEP=../../../redis/deps/leveldb/libleveldb.a
#DBE_LINK=$(DBE_DEP) -lstdc++
#DBE_FLAGS=-D_DBE_LEVEL_DB_ -I../../../redis/deps/leveldb/include
DBE_PATH=/home/wujian/svn/fooyun/redis/deps/leveldb
DBE_DEP=$(DBE_PATH)/libleveldb.a
DBE_LINK=$(DBE_DEP) -lstdc++
DBE_FLAGS=-D_DBE_LEVEL_DB_ -I$(DBE_PATH)/include -I../../
DBE2_PATH=
DBE2_DEP=
DBE2_LINK=
DBE2_FLAGS=
else
DBE_PATH=../../hidb/db/src
DBE_DEP=$(DBE_PATH)/libhidb.a
DBE_LINK=$(DBE_DEP)
DBE_FLAGS=-I../../
DBE2_PATH=../../hidb2/src
DBE2_DEP=$(DBE2_PATH)/build/lib/libhidb.a
DBE2_LINK=$(DBE2_DEP)
DBE2_FLAGS=-I../../
endif

PF_DIR=../../pf_lib
PF_PATH=$(PF_DIR)/src
PF_DEP=$(PF_PATH)/libpflib.a 
PF_LINK=$(PF_DEP) -lrt
PF_FLAGS=-I$(PF_DIR)/include

UTIL_PATH=../../util
UTIL_DEP=$(UTIL_PATH)/libutil.a 
UTIL_LINK=$(UTIL_DEP)
UTIL_FLAGS=-I$(UTIL_PATH)

BL_PATH=../../common/binlog/
BL_DEP=$(BL_PATH)/libbl.a
BL_LINK=$(BL_DEP)
BL_FLAGS=-I$(BL_PATH)

HTTPCLIENT_PATH=../../common/http_client
HTTPCLIENT_DEP=$(HTTPCLIENT_PATH)/libhttpclient.a
HTTPCLIENT_LINK=$(HTTPCLIENT_DEP)
HTTPCLIENT_FLAGS=-I$(HTTPCLIENT_PATH)

CH_PATH=../../common/consisten_hash
CH_DEP=$(CH_PATH)/libch.a
CH_LINK=$(CH_DEP)
CH_FLAGS=-I$(CH_PATH)

TPOOL_PATH=../../common/pool
TPOOL_DEP=$(TPOOL_PATH)/libucpool.a
TPOOL_LINK=$(TPOOL_DEP)
TPOOL_FLAGS=-I$(TPOOL_PATH)

DBCONVERT_PATH=../../hidb/tool
DBCONVERT_DEP=$(DBCONVERT_PATH)/libdbconvert.a
DBCONVERT_LINK=$(DBCONVERT_DEP)
DBCONVERT_FLAGS=-I$(DBCONVERT_PATH)

CURL_PATH=../../depend/curl
CURL_DEP=$(CURL_PATH)/lib/.libs/libcurl.a
CURL_LINK=$(CURL_DEP) -lcrypto -lidn -lz

GENERAL_FLAGS=
#GENERAL_FLAGS=-D_DBE_IF_DEBUG_ -D_DS_TEST_DEBUG_ -D_TEST_DBE_IF_

INC_DEP=$(DBE_FLAGS) $(DBE2_FLAGS) $(PF_FLAGS) $(UTIL_FLAGS) $(BL_FLAGS) $(HTTPCLIENT_FLAGS) $(CH_FLAGS) $(TPOOL_FLAGS) $(DBCONVERT_FLAGS) $(GENERAL_FLAGS)
LIB_DEP=$(DBE_DEP) $(DBE2_DEP) $(PF_DEP) $(BL_DEP) $(HTTPCLIENT_DEP) $(CH_DEP) $(TPOOL_DEP) $(UTIL_DEP) $(CURL_DEP) $(DBCONVERT_DEP)
LINK_DEP=$(DBE_LINK) $(DBE2_LINK) $(BL_LINK) $(HTTPCLIENT_LINK) $(CH_LINK) $(TPOOL_LINK) $(UTIL_LINK) $(PF_LINK) $(CURL_LINK) $(DBCONVERT_LINK)

CCOPT= $(CFLAGS) $(ARCH) $(PROF)


OBJ = adlist.o ae.o anet.o dict.o redis.o sds.o lzf_c.o lzf_d.o pqsort.o zipmap.o sha1.o ziplist.o release.o networking.o rds_util.o object.o db.o replication.o rdb.o t_string.o t_list.o t_set.o t_zset.o t_hash.o config.o aof.o vm.o pubsub.o multi.o debug.o sort.o intset.o syncio.o slowlog.o bio.o serialize.o dbmng.o ds_binlog.o bl_ctx.o binlogtab.o db_io_engine.o checkpoint.o op_string.o op_cmd.o op_list.o op_set.o op_zset.o op_hash.o ds_ctrl.o heartbeat.o ds_util.o key_filter.o dbe_if.o ds_zmalloc.o repl_if.o sync_if.o dbe_get.o write_bl.o dynarray.o codec_key.o restore_key.o

PRGNAME = data-server

all:  $(PRGNAME)
	@echo ""
	@echo "Hint: To run 'make test' is a good idea ;)"
	@echo ""

# Deps (use make dep to generate this)
adlist.o: adlist.c adlist.h
ae.o: ae.c ae.h config.h ae_kqueue.c
ae_epoll.o: ae_epoll.c
ae_kqueue.o: ae_kqueue.c
ae_select.o: ae_select.c
anet.o: anet.c fmacros.h anet.h
aof.o: aof.c redis.h fmacros.h config.h ae.h dict.h adlist.h \
  anet.h zipmap.h ziplist.h intset.h version.h rds_util.h
bio.o: bio.c redis.h fmacros.h config.h ae.h dict.h adlist.h \
  anet.h zipmap.h ziplist.h intset.h version.h rds_util.h bio.h
config.o: config.c redis.h fmacros.h config.h ae.h dict.h adlist.h \
  anet.h zipmap.h ziplist.h intset.h version.h rds_util.h
db.o: db.c redis.h fmacros.h config.h ae.h dict.h adlist.h \
  anet.h zipmap.h ziplist.h intset.h version.h rds_util.h
debug.o: debug.c redis.h fmacros.h config.h ae.h dict.h adlist.h \
  anet.h zipmap.h ziplist.h intset.h version.h rds_util.h sha1.h
dict.o: dict.c fmacros.h dict.h 
endian.o: endian.c
intset.o: intset.c intset.h endian.h
lzf_c.o: lzf_c.c lzfP.h
lzf_d.o: lzf_d.c lzfP.h
multi.o: multi.c redis.h fmacros.h config.h ae.h dict.h adlist.h \
  anet.h zipmap.h ziplist.h intset.h version.h rds_util.h
networking.o: networking.c redis.h fmacros.h config.h ae.h dict.h \
  adlist.h anet.h zipmap.h ziplist.h intset.h version.h rds_util.h
object.o: object.c redis.h fmacros.h config.h ae.h dict.h adlist.h \
  anet.h zipmap.h ziplist.h intset.h version.h rds_util.h
pqsort.o: pqsort.c
pubsub.o: pubsub.c redis.h fmacros.h config.h ae.h dict.h adlist.h \
  anet.h zipmap.h ziplist.h intset.h version.h rds_util.h
rdb.o: rdb.c redis.h fmacros.h config.h ae.h dict.h adlist.h \
  anet.h zipmap.h ziplist.h intset.h version.h rds_util.h lzf.h
redis-benchmark.o: redis-benchmark.c fmacros.h ae.h \
  ../deps/hiredis/hiredis.h adlist.h 
redis-check-aof.o: redis-check-aof.c fmacros.h config.h
redis-check-dump.o: redis-check-dump.c lzf.h
redis-cli.o: redis-cli.c fmacros.h version.h ../deps/hiredis/hiredis.h \
  ../deps/linenoise/linenoise.h help.h
redis.o: redis.c redis.h fmacros.h config.h ae.h dict.h adlist.h \
  anet.h zipmap.h ziplist.h intset.h version.h rds_util.h slowlog.h \
  bio.h
release.o: release.c release.h
replication.o: replication.c redis.h fmacros.h config.h ae.h dict.h \
  adlist.h anet.h zipmap.h ziplist.h intset.h version.h rds_util.h
sds.o: sds.c
sha1.o: sha1.c sha1.h
serialize.o: serialize.c
slowlog.o: slowlog.c redis.h fmacros.h config.h ae.h dict.h \
  adlist.h anet.h zipmap.h ziplist.h intset.h version.h rds_util.h \
  slowlog.h
sort.o: sort.c redis.h fmacros.h config.h ae.h dict.h adlist.h \
  anet.h zipmap.h ziplist.h intset.h version.h rds_util.h pqsort.h
syncio.o: syncio.c redis.h fmacros.h config.h ae.h dict.h adlist.h \
  anet.h zipmap.h ziplist.h intset.h version.h rds_util.h
t_hash.o: t_hash.c redis.h fmacros.h config.h ae.h dict.h adlist.h \
  anet.h zipmap.h ziplist.h intset.h version.h rds_util.h
t_list.o: t_list.c redis.h fmacros.h config.h ae.h dict.h adlist.h \
  anet.h zipmap.h ziplist.h intset.h version.h rds_util.h
t_set.o: t_set.c redis.h fmacros.h config.h ae.h dict.h adlist.h \
  anet.h zipmap.h ziplist.h intset.h version.h rds_util.h
t_string.o: t_string.c redis.h fmacros.h config.h ae.h dict.h \
  adlist.h anet.h zipmap.h ziplist.h intset.h version.h rds_util.h
t_zset.o: t_zset.c redis.h fmacros.h config.h ae.h dict.h adlist.h \
  anet.h zipmap.h ziplist.h intset.h version.h rds_util.h
rds_util.o: rds_util.c fmacros.h rds_util.h
vm.o: vm.c redis.h fmacros.h config.h ae.h dict.h adlist.h \
  anet.h zipmap.h ziplist.h intset.h version.h rds_util.h
ziplist.o: ziplist.c rds_util.h ziplist.h endian.h
zipmap.o: zipmap.c endian.h
binlogtab.o: binlogtab.c
ds_binlog.o: ds_binlog.c
bl_ctx.o: bl_ctx.c
dbmng.o: dbmng.c
db_io_engine.o: db_io_engine.c
checkpoint.o: checkpoint.c
op_string.o: op_string.c
op_cmd.o: op_cmd.c
op_list.o: op_list.c
op_set.o: op_set.c
op_zset.o: op_zset.c
op_hash.o: op_hash.c
ds_ctrl.o: ds_ctrl.c
heartbeat.o: heartbeat.c
ds_util.o: ds_util.c
key_filter.o: key_filter.c
dbe_if.o: dbe_if.c
ds_zmalloc.o: ds_zmalloc.c
repl_if.o: repl_if.c
sync_if.o: sync_if.c 
dbe_get.o: dbe_get.c 
write_bl.o: write_bl.c 
dynarray.o: dynarray.c
codec_key.o: codec_key.c
restore_key.o: restore_key.c

.PHONY: dependencies all

dependencies:
	@printf '%b %b\n' $(MAKECOLOR)MAKE$(ENDCOLOR) $(BINCOLOR)hiredis$(ENDCOLOR)
	@cd ../deps/hiredis && $(MAKE) static ARCH="$(ARCH)"
	@printf '%b %b\n' $(MAKECOLOR)MAKE$(ENDCOLOR) $(BINCOLOR)linenoise$(ENDCOLOR)
	@cd ../deps/linenoise && $(MAKE) ARCH="$(ARCH)"

ifeq ($(USE_JEMALLOC),yes)
$(ALLOC_DEP):
	@printf '%b %b\n' $(MAKECOLOR)MAKE$(ENDCOLOR) $(BINCOLOR)jemalloc$(ENDCOLOR)
	@cd $(ALLOC_PATH) && ./configure $(JEMALLOC_CFLAGS) --with-jemalloc-prefix=je_ --with-lg-quantum=3  --with-version=4.2.1-123-g1 && $(MAKE) lib/libjemalloc.a
endif

$(DBE_DEP):
	@cd $(DBE_PATH) && $(MAKE)

$(DBE2_DEP):
	@cd $(DBE2_PATH) && $(MAKE)

$(PF_DEP):
	@cd $(PF_PATH) && $(MAKE)

$(UTIL_DEP):
	@cd $(UTIL_PATH) && $(MAKE)

$(BL_DEP):
	@cd $(BL_PATH) && $(MAKE)

$(HTTPCLIENT_DEP):
	@cd $(HTTPCLIENT_PATH) && $(MAKE)

$(CH_DEP):
	@cd $(CH_PATH) && $(MAKE)

$(TPOOL_DEP):
	@cd $(TPOOL_PATH) && $(MAKE)

$(DBCONVERT_DEP):
	@cd $(DBCONVERT_PATH) && $(MAKE)

$(CURL_DEP):
	@cd $(CURL_PATH) && ./configure --without-ssl --disable-ldap --disable-ldaps && $(MAKE)


$(PRGNAME): $(OBJ) $(LIB_DEP)
	$(QUIET_LINK)$(CC) -Wl,--as-needed -o $(PRGNAME) $(CCOPT) $(DEBUG) $(OBJ) $(CCLINK) $(LINK_DEP) $(ALLOC_LINK)

# Because the jemalloc.h header is generated as a part of the jemalloc build
# process, building it should complete before building any other object.
%.o: %.c $(ALLOC_DEP)
	$(QUIET_CC)$(CC) -c $(CFLAGS) $(ALLOC_FLAGS) $(INC_DEP) $(DEBUG) $(COMPILE_TIME) $<

clean:
	rm -rf $(PRGNAME) *.o *.gcda *.gcno *.gcov

cleanlib:
	@printf 'clean %b\n' $(DBE_DEP)
	@cd $(DBE_PATH) && $(MAKE) clean
	@printf 'clean %b\n' $(DBE2_DEP)
	@cd $(DBE2_PATH) && $(MAKE) clean
	@printf 'clean %b\n' $(CH_DEP)
	@cd $(CH_PATH) && $(MAKE) clean
	@printf 'clean %b\n' $(TPOOL_DEP)
	@cd $(TPOOL_PATH) && $(MAKE) clean
	@printf 'clean %b\n' $(PF_DEP)
	@cd $(PF_PATH) && $(MAKE) clean
	@printf 'clean %b\n' $(UTIL_DEP)
	@cd $(UTIL_PATH) && $(MAKE) clean
	@printf 'clean %b\n' $(BL_DEP)
	@cd $(BL_PATH) && $(MAKE) clean
	@printf 'clean %b\n' $(HTTPCLIENT_DEP)
	@cd $(HTTPCLIENT_PATH) && $(MAKE) clean
	@printf 'clean %b\n' $(DBCONVERT_PATH)
	@cd $(DBCONVERT_PATH) && $(MAKE) clean
	@printf 'clean %b\n' $(CURL_DEP)
	-@cd $(CURL_PATH) && $(MAKE) clean

