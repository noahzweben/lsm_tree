#include "lsm.h"
#include "helpers.h"
#include <stdlib.h>
#include <stdio.h>
#include <sys/stat.h>
#include <string.h>

int BLOCK_SIZE_NODES = 4096 / sizeof(node);

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
    build_fence_pointers(&(lsm->levels[1]), lsm->buffer, lsm->levels[0].count);

    lsm->levels[0].count = 0;
    fclose(fp);

    // check if level 1 is full
    if (lsm->levels[1].count == lsm->levels[1].size)
    {
        flush_to_level(lsm, 2);
    }
}

void flush_to_level(lsmtree *lsm, int deeper_level)
{

    init_level(lsm, deeper_level);
    int old_level = deeper_level - 1;
    char current_path[64];
    strcpy(current_path, lsm->levels[deeper_level].filepath);

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

    // create buffer that can accomodate both levels
    const int buffer_size = lsm->levels[old_level].count + lsm->levels[deeper_level].count;
    node *buffer = (node *)malloc(sizeof(node) * buffer_size);

    if (buffer == NULL)
    {
        printf("Error3: malloc failed in flush_to_level\n");
        exit(1);
    }
    // read old level into buffer
    fread(buffer, sizeof(node), lsm->levels[old_level].count, fp_old_flush);
    // read current layer into buffer after old level
    fread(buffer + lsm->levels[old_level].count, sizeof(node), lsm->levels[deeper_level].count, fp_current_layer);
    // TODO: sort eventually
    merge_sort(buffer, buffer_size);

    // write buffer to new level
    fwrite(buffer, sizeof(node), buffer_size, fp_temp);

    // add to new level count
    lsm->levels[deeper_level]
        .count = buffer_size;

    char new_old_path[64];
    set_filename(new_old_path);
    // touch new old level file
    FILE *fp_new_old = fopen(new_old_path, "w");
    fclose(fp_new_old);

    // update filepaths
    strcpy(lsm->levels[old_level].filepath, new_old_path);
    strcpy(lsm->levels[deeper_level].filepath, new_path);
    build_fence_pointers(&(lsm->levels[deeper_level]), buffer, buffer_size);
    // remove old fence pointers
    printf("im here\n");
    if (lsm->levels[old_level].fence_pointer_count > 0)
    {
        free(lsm->levels[old_level].fence_pointers);
    }

    lsm->levels[old_level].fence_pointer_count = 0;
    print_tree("test", lsm);
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
    if (lsm->levels[deeper_level].count == lsm->levels[deeper_level].size)
    {
        flush_to_level(lsm, deeper_level + 1);
    }
}

void init_level(lsmtree *lsm, int deeper_level)
{
    if (lsm->max_level < deeper_level)
    {
        lsm->max_level++;
        // increase memory allocation for levels

        lsm->levels = (level *)realloc(lsm->levels, sizeof(level) * (lsm->max_level + 1));

        if (lsm->levels == NULL)
        {
            printf("Error: realloc failed in flush_from_buffer\n");
            exit(1);
        }

        lsm->levels[deeper_level]
            .level = deeper_level;
        lsm->levels[deeper_level].count = 0;
        lsm->levels[deeper_level].size = lsm->levels[deeper_level - 1].size * 10;
        lsm->levels[deeper_level].fence_pointer_count = 0;
        lsm->levels[deeper_level].fence_pointers = NULL;
        set_filename(lsm->levels[deeper_level].filepath);
        FILE *fp = fopen(lsm->levels[deeper_level].filepath, "wb");
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
        if (lsm->levels[i].fence_pointer_count > 0)
        {
            free(lsm->levels[i].fence_pointers);
        }
    }

    free(lsm->levels);
    free(lsm->buffer);
    free(lsm);
}

void print_tree(char *msg, lsmtree *lsm)
{
    printf("%s\n", msg);
    // loop through buffer and print
    for (int i = 0; i < lsm->levels[0].count; i++)
    {
        printf("{%d,%d}, ", lsm->buffer[i].key, lsm->buffer[i].value);
    }
    // loop through levels and print level struct
    for (int i = 0; i <= lsm->max_level; i++)
    {
        printf("\nLevel %d: %d/%d, fp: %s\n", lsm->levels[i].level, lsm->levels[i].count, lsm->levels[i].size, lsm->levels[i].filepath);
        // print fence pointers
        if (i > 1){
        for (int j = 0; j < lsm->levels[i].fence_pointer_count; j++)
        {
            fence_pointer fp = lsm->levels[i].fence_pointers[j];
            printf("fp %d, %d, ", fp.key, fp.offset);
        }
        printf("\n");
        }
    }

    printf("\n");
}

void SORT_TYPE_CPY(node *dst, node *src, const int size)
{
    int i = 0;

    for (; i < size; ++i)
    {
        dst[i] = src[i];
    }
}

void merge_sort_recursive(node *new_buffer, node *buffer, int buffer_size)
{
    const int middle = buffer_size / 2;
    int out = 0;
    int i = 0;
    int j = middle;

    /* don't bother sorting an array of size <= 1 */
    if (buffer_size <= 1)
    {
        return;
    }

    merge_sort_recursive(new_buffer, buffer, middle);
    merge_sort_recursive(new_buffer, &buffer[middle], buffer_size - middle);

    while (out != buffer_size)
    {
        if (i < middle)
        {
            if (j < buffer_size)
            {
                if (buffer[i].key - buffer[j].key <= 0)
                {
                    new_buffer[out] = buffer[i++];
                }
                else
                {
                    new_buffer[out] = buffer[j++];
                }
            }
            else
            {
                new_buffer[out] = buffer[i++];
            }
        }
        else
        {
            new_buffer[out] = buffer[j++];
        }

        out++;
    }

    SORT_TYPE_CPY(buffer, new_buffer, buffer_size);
}

void merge_sort(node *buffer, int buffer_size)
{
    node *new_buffer;

    /* don't bother sorting an array of size <= 1 */
    if (buffer_size <= 1)
    {
        return;
    }

    new_buffer = (node *)malloc(sizeof(node) * buffer_size);
    merge_sort_recursive(new_buffer, buffer, buffer_size);

    free(new_buffer);
}

void build_fence_pointers(level *level, node *buffer, int buffer_size)
{
    // allocate a fence pointer for every BLOCK_SIZE_NODEs nodes
    // todo +1 ?
    int fence_pointer_count = buffer_size / BLOCK_SIZE_NODES + 1;
    fence_pointer *new_fence_pointers = (fence_pointer *)malloc(sizeof(fence_pointer) * fence_pointer_count);
    // loop through blocks of nodes and set fence pointers
    for (int i = 0; i < fence_pointer_count; i++)
    {
        new_fence_pointers[i].key = buffer[i * BLOCK_SIZE_NODES].key;
        new_fence_pointers[i].offset = i * BLOCK_SIZE_NODES;
    }
    if (level->fence_pointer_count > 0){
        free(level->fence_pointers);
    }
    level->fence_pointers = new_fence_pointers;
    level->fence_pointer_count = fence_pointer_count;
}