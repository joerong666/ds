INC_PATH = -I../

CC = gcc
CFLAGS = $(INC_PATH)
LD_FLAGS = ../libch.a

PRGNAME = test

CSRCS := main.c

.PHONY : clean all

all: $(PRGNAME)

$(PRGNAME): $(CSRCS:%.c=%.o)
	$(CC) -o $@ $+ $(LD_FLAGS)

%.o: %.c
	$(CC) -c $(CFLAGS) $<

clean:
	-rm -f $(CSRCS:%.c=%.o) $(PRGNAME)

