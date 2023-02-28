import time
# open fast.txt and parse it into a list of numbers
# then print the list

# open the file
with open('fast.txt', 'r') as f:
    # read into list of ints
    data = [int(x) for x in f.read().split(",")]
    # time how long it takes to sort using mergesort
    start = time.time()
    data.sort()
    end = time.time()
    print("Time to sort FAST: ", end - start)


with open('slow.txt', 'r') as f:
    # read into list of ints
    data = [int(x) for x in f.read().split(",")]
    # time how long it takes to sort
    start = time.time()
    data.sort()
    end = time.time()
    print("Time to sort SLOW: ", end - start)
