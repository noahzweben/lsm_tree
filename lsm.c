#include "lsm.h"
#include "helpers.h"
#include <stdlib.h>
#include <stdio.h>

// return a pointer to a new LSM tree
lsmtree *create(int buffer_size)
{
    lsmtree *lsm = (lsmtree *)malloc(sizeof(lsmtree));
    if (lsm == NULL)
    {
        printf("Error: malloc failed in create\n");
        exit(1);
    }
    lsm->buffer_size = buffer_size;
    lsm->buffer_count = 0;
    lsm->buffer = (node *)malloc(sizeof(node) * buffer_size);
    if (lsm->buffer == NULL)
    {
        printf("Error: malloc failed in create\n");
        exit(1);
    }
    return lsm;
}

// insert a key-value pair into the LSM tree
void insert(lsmtree *lsm, keyType key, valType value)
{
    // create new node on the stack
    node n = {key, value};
    lsm->buffer[(lsm->buffer_count)++] = n;

    // if buffere is full, move to disk
    if (lsm->buffer_count == lsm->buffer_size)
    {
        // move to disk
        // clear buffer
        move_to_disk(lsm, 1);
        lsm->buffer_count = 0;
    }
}

// move the buffer to the disk
void move_to_disk(lsmtree *lsm, int level)
{

    // open File fp for writing
    char filename[64];
    set_filename(filename, level);
    FILE *fp = fopen(filename, "w");
    if (fp == NULL)
    {
        printf("Error: fopen failed in move_to_disk\n");
        exit(1);
    }
    // copy over buffer_count elements to fp
    fwrite(lsm->buffer, sizeof(node), lsm->buffer_count, fp);
    fclose(fp);
}

// get a value
int get(lsmtree *lsm, keyType key)
{
    // search buffer for key
    for (int i = 0; i < lsm->buffer_count; i++)
    {
        if (lsm->buffer[i].key == key)
        {
            return lsm->buffer[i].value;
        }
    }

    // search disk for key
    char filename[64];
    set_filename(filename, 1);
    FILE *fp = fopen(filename, "r");
    if (fp == NULL)
    {
        return -1;
    }
    node n;
    while (fread(&n, sizeof(node), 1, fp) == 1)
    {
        if (n.key == key)
        {
            fclose(fp);
            return n.value;
        }
    }

    // if not found in buffer then return -1
    fclose(fp);
    return -1;
}

void destroy(lsmtree *lsm)
{
    // remove level 1 file
    char filename[64];
    set_filename(filename, 1);
    remove(filename);

    free(lsm->buffer);
    free(lsm);
}
