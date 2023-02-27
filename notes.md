#uuid install for EC2
sudo yum install libuuid libuuid-devel

# TODO
* Valgrind of multi-threaded writes test (at 1000 some race condition/unspecified behavior)
* bloom filters
* uint vs int
* multiple reads at once (rwlock)
* make sure destroy kills all queued threads
* why does benchmark with i&10000 hang