#include "lsm.h"
#include <stdlib.h>
#include <stdio.h>

// return a pointer to a new LSM tree
lsmtree *create_lsm(int buffer_size)
{
    lsmtree *lsm = (lsmtree *)malloc(sizeof(lsmtree));
    if (lsm == NULL)
    {
        printf("Error: malloc failed in create_lsm\n");
        exit(1);
    }
    lsm->buffer_size = buffer_size;
    lsm->buffer_count = 0;
    lsm->buffer = (node *)malloc(sizeof(node) * buffer_size);
    if (lsm->buffer == NULL)
    {
        printf("Error: malloc failed in create_lsm\n");
        exit(1);
    }
    return lsm;
}


// insert a key-value pair into the LSM tree
void insert_lsm(lsmtree *lsm, int key, int value)
{
    // create new node on the stack
    node n = {key, value};
    lsm->buffer[(lsm->buffer_count)++] = n;
}

void destroy_lsm(lsmtree *lsm){
    free(lsm->buffer);
    free(lsm);
}