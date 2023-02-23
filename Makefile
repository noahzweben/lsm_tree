CC=gcc -std=c99
CFLAGS = -ggdb3 -W -Wall -Wextra -Werror -pthread -O3
LDFLAGS = -pthread
LIBS = 

# if on linux add -luiid to LIBS
# if on mac do nothing
ifeq ($(shell uname),Darwin)
	LIBS += 
else
	LIBS += -luuid
endif


default: main test benchmark

%.o: %.c %.h
	$(CC) -c -o $@ $< $(CFLAGS)

main: lsm.o main.o helpers.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

test: lsm.o test.o helpers.o
	$(CC) $(CFLAGS) -g -o $@ $^ $(LDFLAGS) $(LIBS)

benchmark: lsm.o benchmark.o helpers.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

clean:
	rm -f main test benchmark *.o *.bin
