import os
import sys
import struct

# read file
with open("c5c8f6c0-b083-11ed-91fe-ba419ce2236d.bin", 'rb') as f:
    # read in 4 bytes at a time and print them as integers
    key = True
    while True:
        bytes = f.read(4)
        if not bytes:
            break
    st = "Key: " if key else "Value: "
    print(st, struct.unpack('I', bytes))
    key = not key
