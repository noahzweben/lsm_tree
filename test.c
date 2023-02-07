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
    lsmtree *lsm = create_lsm(10);
    // print lsm buffer size
    printf("Test: lsm buffer size: %d\n", lsm->buffer_size);
    printf("p0mter: %p\n", lsm);
    // free(lsm->buffer);
    // free(lsm);

    // assert(lsm->buffer_size == 10);
    // assert(lsm->buffer_count == 0);
    // assert(sizeof(*(lsm->buffer)) == sizeof(node) * lsm->buffer_size);

    return 0;
}
