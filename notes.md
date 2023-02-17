#uuid install for EC2
sudo yum install libuuid libuuid-devel

Q: do merging in background process, what if it takes long enough that buffer fills up again - do we waiot for merge to complete?


# TODO
* dedup 
* move all metadata creation to after we know we wont be flushing / layer full (otherwise wasted effort)
