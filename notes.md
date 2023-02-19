#uuid install for EC2
sudo yum install libuuid libuuid-devel

Q: do merging in background process, what if it takes long enough that buffer fills up again - do we waiot for merge to complete?


# TODO
* bloom filters
* range queries
* delete
* uint vs int

* make things const
* only allow one merge operation at a given time mutex
* does mutex block within same thread