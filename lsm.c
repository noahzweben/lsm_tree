#include "lsm.h"
#include "helpers.h"
#include <stdlib.h>
#include <stdio.h>
#include <sys/stat.h>
#include <string.h>

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

    // levels metadata
    lsm->max_level = 0;
    lsm->levels = (level *)malloc(sizeof(level) * (lsm->max_level + 1));
    if (lsm->levels == NULL)
    {
        printf("Error: malloc failed in create\n");
        exit(1);
    }
// initialize filepath to empty string
    lsm->levels[0].filepath[0] = '\0';
    lsm->levels[0].level = 0;
    lsm->levels[0].count = 0;
    lsm->levels[0].size = buffer_size;

    // create buffer
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
    lsm->buffer[(lsm->levels[0].count)++] = n;

    // if buffer is full, move to disk
    if (lsm->levels[0].count == lsm->levels[0].size)
    {
        flush_from_buffer(lsm);
    }
}

// move from buffer to the disk
void flush_from_buffer(lsmtree *lsm)
{

    init_level(lsm, 1);

    FILE *fp = fopen(lsm->levels[1].filepath, "a");
    if (fp == NULL)
    {
        printf("Error: fopen failed in move_to_disk\n");
        exit(1);
    }

    fwrite(lsm->buffer, sizeof(node), lsm->levels[0].count, fp);
    lsm->levels[1].count = lsm->levels[1].count + lsm->levels[0].count;
    lsm->levels[0].count = 0;
    fclose(fp);

    // check if level 1 is full
    if (lsm->levels[1].count == lsm->levels[1].size)
    {
        flush_to_level(lsm, 2);
    }
}

void flush_to_level(lsmtree *lsm, int new_level)
{

    init_level(lsm, new_level);
    int old_level = new_level - 1;
    char current_path[64];
    strcpy(current_path, lsm->levels[new_level].filepath);

    char old_path[64];
    strcpy(old_path, lsm->levels[old_level].filepath);

    FILE *fp_old_flush = fopen(lsm->levels[old_level].filepath, "rb");
    if (fp_old_flush == NULL)
    {
        printf("Error1: fopen failed in flush_to_level\n");
        exit(1);
    }

    FILE *fp_current_layer = fopen(current_path, "rb");
    if (fp_current_layer == NULL)
    {
        printf("Error2: fopen failed in flush_to_level\n");
        exit(1);
    }
    char new_path[64];
    set_filename(new_path);
    FILE *fp_temp = fopen(new_path, "wb");

    // create buffer that is size of old level
    int buffer_size = lsm->levels[old_level].count + lsm->levels[new_level].count;
    node *buffer = (node *)malloc(sizeof(node) * buffer_size);

    if (buffer == NULL)
    {
        printf("Error3: malloc failed in flush_to_level\n");
        exit(1);
    }
    // read old level into buffer
    fread(buffer, sizeof(node), lsm->levels[old_level].count, fp_old_flush);
    // read current layer into buffer after old level
    fread(buffer + lsm->levels[old_level].count, sizeof(node), lsm->levels[new_level].count, fp_current_layer);
    // TODO: sort eventually

    // write buffer to new level
    fwrite(buffer, sizeof(node), buffer_size, fp_temp);

    // add to new level count
    lsm->levels[new_level]
        .count = buffer_size;

    char new_old_path[64];
    set_filename(new_old_path);
    // touch new old level file
    FILE *fp_new_old = fopen(new_old_path, "w");
    fclose(fp_new_old);

    // update filepaths
    strcpy(lsm->levels[old_level].filepath, new_old_path);
    strcpy(lsm->levels[new_level].filepath, new_path);

    // update old level count
    lsm->levels[old_level]
        .count = 0;
    remove(current_path);
    remove(old_path);
    // close files
    fclose(fp_old_flush);
    fclose(fp_current_layer);
    fclose(fp_temp);
    // free buffer
    free(buffer);

    // check if new level is full
    if (lsm->levels[new_level].count == lsm->levels[new_level].size)
    {
        flush_to_level(lsm, new_level + 1);
    }
}

void init_level(lsmtree *lsm, int new_level)
{
    if (lsm->max_level < new_level)
    {
        lsm->max_level++;
        // increase memory allocation for levels

        lsm->levels = (level *)realloc(lsm->levels, sizeof(level) * (lsm->max_level + 1));

        if (lsm->levels == NULL)
        {
            printf("Error: realloc failed in flush_from_buffer\n");
            exit(1);
        }

        lsm->levels[new_level]
            .level = new_level;
        lsm->levels[new_level].count = 0;
        lsm->levels[new_level].size = lsm->levels[new_level - 1].size * 10;
        set_filename(lsm->levels[new_level].filepath);
        FILE *fp = fopen(lsm->levels[new_level].filepath, "wb");
        if (fp == NULL)
        {
            printf("Error: fopen failed init_layer\n");
            exit(1);
        }
        fclose(fp);
    }
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

    // loop through levels and search disk
    int value = -1;
    for (int i = 1; i <= lsm->max_level; i++)
    {
        value = get_from_disk(lsm, key, i);
        if (value != -1)
        {
            return value;
        }
    }

    return value;
}

int get_from_disk(lsmtree *lsm, keyType key, int get_level)
{
    int value = -1;
    // open File fp for reading
    FILE *fp = fopen(lsm->levels[get_level].filepath, "r");
    if (fp == NULL)
    {
        return -1;
    }

    // copy over buffer_count elements to fp
    node *nodes = (node *)malloc(sizeof(node) * BLOCK_SIZE_NODES);
    // read in BLOCK_SIZE_NODES nodes and print number of nodes read
    int r = fread(nodes, sizeof(node), BLOCK_SIZE_NODES, fp);
    // loop through nodes and search for key
    for (int i = 0; i < r; i++)
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

    // loop through levels and remove filepath
    for (int i = 1; i <= lsm->max_level; i++)
    {

        remove(lsm->levels[i].filepath);
    }

    free(lsm->levels);
    free(lsm->buffer);
    free(lsm);
}

void print_tree(lsmtree *lsm)
{
    // loop through buffer and print
    for (int i = 0; i < lsm->levels[0].count; i++)
    {
        printf("{%d,%d}, ", lsm->buffer[i].key, lsm->buffer[i].value);
    }
    // loop through levels and print level struct
    for (int i = 0; i <= lsm->max_level; i++)
    {
        printf("\nLevel %d: %d/%d, fp: %s\n", lsm->levels[i].level, lsm->levels[i].count, lsm->levels[i].size, lsm->levels[i].filepath);
    }

    printf("\n");
}