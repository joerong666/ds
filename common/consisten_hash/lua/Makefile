INC_PATH = -I../../../depend/lua/src -I../ -I../../../util -I../../../pf_lib/include

CC = gcc
CFLAGS = $(INC_PATH) -fPIC
LDFLAGS = -O -shared -fPIC
AR = ar

LIBRARY = consistent_hash.so

CSRCS := lua_ch.c ch.c
OBJ := lua_ch.o ch.o

.PHONY : clean all

all: $(LIBRARY)

$(LIBRARY):$(OBJ)
	$(CC) $(LDFLAGS) -o $@ $(OBJ)

#%.o: %.c
#	$(CC) -c $(CFLAGS) $<

ch.o: ../ch.c
	$(CC) -c $(CFLAGS) $<

lua_ch.o: lua_ch.c
	$(CC) -c $(CFLAGS) $<

clean:
	-rm -f $(OBJ) $(LIBRARY)

