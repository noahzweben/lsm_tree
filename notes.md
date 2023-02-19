#uuid install for EC2
sudo yum install libuuid libuuid-devel

Q: do merging in background process, what if it takes long enough that buffer fills up again - do we waiot for merge to complete?


# TODO
* move all metadata creation to after we know we wont be flushing / layer full (otherwise wasted effort)
* bloom filters
* mutex lock layers for just the metadata/file switching so that its ATOMIC so we never read halfway through an update process
    so the commands where we are updating counts/fence pointers/filepaths are threadsafe, no other thread can read halfway thru this operation
* range queries
* delete
* uint vs int

* only allow one merge operation at a given time mutex
* does mutex block within same thread