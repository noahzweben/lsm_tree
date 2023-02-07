#include "lsm.h"
#include <stdlib.h>
#include <stdio.h>

// return a pointer to a new LSM tree
lsmtree *create_lsm(int buffer_size)
{
    lsmtree *lsm = (lsmtree *)malloc(sizeof(lsm));
    lsm->buffer_size = buffer_size;
    lsm->buffer = (node *)malloc(sizeof(node) * buffer_size);
    printf("ABC lsm buffer size: %d\n", lsm->buffer_size);
    printf("pomter: %p\n", lsm);
    return lsm;
}

// insert a key-value pair into the LSM tree
void insert(lsmtree *lsm, int key, int value)
{
    // create new node on the stack
    node n = {key, value};
    lsm->buffer[(lsm->buffer_count)++] = n;
}