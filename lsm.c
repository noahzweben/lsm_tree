#include "lsm.h"
#include "helpers.h"
#include <stdlib.h>
#include <stdio.h>
#include <sys/stat.h>

int BLOCK_SIZE_NODES = 512;
// return a pointer to a new LSM tree
lsmtree *create(int buffer_size)
{
    lsmtree *lsm = (lsmtree *)malloc(sizeof(lsmtree));
    if (lsm == NULL)
    {
        printf("Error: malloc failed in create\n");
        exit(1);
    }

    int level_count = 1;
    lsm->levels = (level *)malloc(sizeof(level) * level_count);
    if (lsm->levels == NULL)
    {
        printf("Error: malloc failed in create\n");
        exit(1);
    }
    lsm->levels[0].level = 0;
    lsm->levels[0].count = 0;
    lsm->levels[0].size = buffer_size;
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
    ;
    lsm->buffer[(lsm->levels[0].count)++] = n;

    // if buffer is full, move to disk
    if (lsm->levels[0].count == lsm->levels[0].size)
    {
        // move to disk
        // clear buffer
        flush_from_buffer(lsm);
    }
}

// move from buffer to the disk
void flush_from_buffer(lsmtree *lsm)
{
    if (lsm->level_count == 1)
    {
        // create level 1
        lsm->level_count = 2;
        lsm->levels = (level *)realloc(lsm->levels, sizeof(level) * lsm->level_count);
        if (lsm->levels == NULL)
        {
            printf("Error: realloc failed in flush_from_buffer\n");
            exit(1);
        }
        lsm->levels[1].level = 1;
        lsm->levels[1].count = 0;
        lsm->levels[1].size = BLOCK_SIZE_NODES * 10;
    }

    // open File fp for writing
    char filename[64];
    set_filename(filename, 1);

    FILE *fp = fopen(filename, "w");
    if (fp == NULL)
    {
        printf("Error: fopen failed in move_to_disk\n");
        exit(1);
    }
    // copy over buffer_count elements to fp
    fwrite(lsm->buffer, sizeof(node), lsm->levels[0].count, fp);
    lsm->levels[1].count = lsm->levels[1].count + lsm->levels[0].count;
    fclose(fp);
}

// get a value
int get(lsmtree *lsm, keyType key)
{
    // search buffer for key
    for (int i = 0; i < lsm->levels[0].count; i++)
    {
        if (lsm->buffer[i].key == key)
        {
            return lsm->buffer[i].value;
        }
    }

    int value = get_from_disk(lsm, key, 1);
    return value;
}

int get_from_disk(lsmtree *lsm, keyType key, int level)
{
    (void)lsm;

    int value = -1;
    // open File fp for reading
    char filename[64];
    set_filename(filename, level);
    FILE *fp = fopen(filename, "r");
    if (fp == NULL)
    {
        return -1;
    }

    // copy over buffer_count elements to fp
    node *nodes = (node *)malloc(sizeof(node) * BLOCK_SIZE_NODES);
    fread(nodes, sizeof(node), BLOCK_SIZE_NODES, fp);
    // loop through nodes and search for key
    for (int i = 0; i < BLOCK_SIZE_NODES; i++)
    {
        if (nodes[i].key == key)
        {
            value = nodes[i].value;
            break;
        }
    }

    free(nodes);
    fclose(fp);
    return value;
}

void destroy(lsmtree *lsm)
{
    // remove level 1 file
    char filename[64];
    set_filename(filename, 1);
    remove(filename);

    free(lsm->levels);
    free(lsm->buffer);
    free(lsm);
}
