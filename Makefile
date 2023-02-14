CC=gcc -std=c99
CFLAGS = -ggdb3 -W -Wall -Wextra -Werror -O3
LDFLAGS = -luuid
LIBS = 

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
