OBJS = shell.o
DEPS = $(OBJS:.o=.d)
CC = gcc
CFLAGS = -I../../include -g3 -Wall -fpic -std=c99 -MMD -MP
BIN = ../../chidb
LDFLAGS = -L../../
LDLIBS = -lchidb -ledit

all: $(BIN)

$(BIN): $(OBJS)
	$(CC) $(LDFLAGS) $(LDLIBS) $(OBJS) -o$(BIN)

$(BIN)-main: $(OBJS)
	$(CC) $(LDFLAGS) $(LDLIBS) $(OBJS) ../libchidb/main.o -o$(BIN)-main

%.d: %.c

clean:
	-rm -f $(OBJS) $(LIB) *.d

-include $(DEPS)