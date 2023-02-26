#uuid install for EC2
sudo yum install libuuid libuuid-devel

# TODO
* Valgrind of multi-threaded writes
* bloom filters
* range queries
* uint vs int
* multiple reads at once
* make sure destroy kills all queued threads