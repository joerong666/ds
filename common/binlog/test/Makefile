PF_LIB = ../../../pf_lib
SRC_PATH = $(PF_LIB)/src/*.c ../../../util/zmalloc.c ../src/binlog.c
INC_PATH = -I../../../util -I../

export C_INCLUDE_PATH = $(PF_LIB)

test_binlog: test_binlog.c
	gcc -g -o test_binlog test_binlog.c $(SRC_PATH) $(INC_PATH) -lpthread -lm -lrt

.PHONY: clean test

clean:
	-rm -r test_binlog

test: clean test_binlog
	./test_binlog
	
