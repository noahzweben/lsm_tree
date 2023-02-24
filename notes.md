#uuid install for EC2
sudo yum install libuuid libuuid-devel

# TODO
* bloom filters
* range queries
* delete
* uint vs int
* multiple reads at once
* clear flushbuffer after compaction

* within a buffer there will only every be one key of a variable
* if that key is a delete key, stop the search...
* change search to use a node pointer..., so can keep as NULL if not found