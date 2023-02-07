#include "lsm.h"
#include <stdlib.h>
#include <stdio.h>

// return a pointer to a new LSM tree
lsm *create(int buffer_size)
{
    lsm *lsm = malloc(sizeof(lsm));
    lsm->buffer_size = buffer_size;
    lsm->buffer = malloc(sizeof(node) * buffer_size);
    return lsm;
}

// insert a key-value pair into the LSM tree
void insert(lsm *lsm, int key, int value)
{
    // create new node on the stack
    node n = {key, value};
    lsm->buffer[0] = n;
}