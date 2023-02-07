#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

#include "lsm.h"

// This code is designed to test the correctness of your implementation.
// You do not need to significantly change it.
// Compile and run it in the command line by typing:
// make test; ./test

int main(void)
{
    //basic create functionality
    lsmtree *lsm = create(10);
    assert(lsm->buffer_size == 10);
    assert(lsm->buffer_count == 0);

    //first write functionality
    insert(lsm, 5,10);
    int buffer_count = lsm->buffer_count;
    assert(buffer_count == 1);
    node new_node = lsm->buffer[buffer_count-1];
    assert(new_node.key == 5);
    assert(new_node.value == 10);

    //second write functionality
    insert(lsm, 12,13);
    buffer_count = lsm->buffer_count;
    assert(buffer_count == 2);
    new_node = lsm->buffer[buffer_count-1];
    assert(new_node.key == 12);
    assert(new_node.value == 13);



    destroy(lsm);

    return 0;
}
