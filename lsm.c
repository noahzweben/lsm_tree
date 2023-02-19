#include "lsm.h"
#include "helpers.h"
#include <stdlib.h>
#include <stdio.h>
#include <sys/stat.h>
#include <string.h>
#include <pthread.h>

int BLOCK_SIZE_NODES = 4096 / sizeof(node);
// flush mutex
pthread_mutex_t flush_mutex = PTHREAD_MUTEX_INITIALIZER;

struct flush_args
{
    lsmtree *lsm;
    int level;
};

// return a pointer to a new LSM tree
lsmtree *create(int buffer_size)
{
    lsmtree *lsm = (lsmtree *)malloc(sizeof(lsmtree));
    if (lsm == NULL)
    {
        printf("Error1: malloc failed in create\n");
        exit(1);
    }

    // levels metadata
    lsm->max_level = 0;
    lsm->levels = (level *)malloc(sizeof(level) * (lsm->max_level + 1));
    if (lsm->levels == NULL)
    {
        printf("Error2: malloc failed in create\n");
        exit(1);
    }
    pthread_mutex_init(&lsm->memtable_level.level_mutex, NULL);
    init_memtable(lsm, buffer_size);
    return lsm;
}

void init_memtable(lsmtree *lsm, int buffer_size)
{
    // claim memtable mutex
    pthread_mutex_lock(&lsm->memtable_level.level_mutex);
    // initialize filepath to empty string
    lsm->memtable_level.filepath[0] = '\0';
    lsm->memtable_level.level = 0;
    lsm->memtable_level.count = 0;
    lsm->memtable_level.size = buffer_size;
    lsm->memtable_level.fence_pointers = NULL;
    lsm->memtable_level.fence_pointer_count = 0;

    // create memtable buffer
    lsm->memtable = (node *)malloc(sizeof(node) * buffer_size);
    if (lsm->memtable == NULL)
    {
        printf("Error3: malloc failed in create\n");
        exit(1);
    }
    // release memtable mutex
    pthread_mutex_unlock(&lsm->memtable_level.level_mutex);
}

// insert a key-value pair into the LSM tree
void insert(lsmtree *lsm, keyType key, valType value)
{
    // if buffer is full return an error
    if (lsm->memtable_level.count == lsm->memtable_level.size)
    {
        printf("Error: buffer is full\n");
        exit(1);
    }

    // create new node on the stack

    // lock memtable mutex
    pthread_mutex_lock(&lsm->memtable_level.level_mutex);
    node n = {key, value};
    lsm->memtable[(lsm->memtable_level.count)++] = n;
    // unlock memtable mutex
    pthread_mutex_unlock(&lsm->memtable_level.level_mutex);

    // if buffer is full, move to disk
    if (lsm->memtable_level.count == lsm->memtable_level.size)
    {
        // claim mutex
        pthread_mutex_lock(&flush_mutex);
        // copy memtable to flush buffer and copy level metadata to level[0]
        // claim flush buffer mutex
        pthread_mutex_lock(&lsm->levels[0].level_mutex);
        lsm->flush_buffer = lsm->memtable;
        lsm->levels[0] = lsm->memtable_level;
        // release flush buffer mutex
        pthread_mutex_unlock(&lsm->levels[0].level_mutex);

        init_memtable(lsm, lsm->memtable_level.size);

        // call flush_to_level in a nonblocking thread
        pthread_t thread;
        struct flush_args *args = (struct flush_args *)malloc(sizeof(struct flush_args));
        args->lsm = lsm;
        args->level = 1;
        pthread_create(&thread, NULL, init_flush_thread, (void *)args);
        pthread_detach(thread);
    }
}

void *init_flush_thread(void *args)
{
    struct flush_args *flush_args = (struct flush_args *)args;
    flush_to_level(flush_args->lsm, flush_args->level);
    free(flush_args);
    // release mutex
    pthread_mutex_unlock(&flush_mutex);
    return NULL;
}

void flush_to_level(lsmtree *lsm, int deeper_level)
{

    init_level(lsm, deeper_level);
    int old_level = deeper_level - 1;

    // get filepaths
    char current_path[64];
    strcpy(current_path, lsm->levels[deeper_level].filepath);
    char old_path[64];
    strcpy(old_path, lsm->levels[old_level].filepath);

    FILE *fp_old_flush;

    // no file to open if old_level is 0
    if (old_level != 0)
    {
        fp_old_flush = fopen(lsm->levels[old_level].filepath, "rb");
        if (fp_old_flush == NULL)
        {
            printf("Error1: fopen failed in flush_to_level\n");
            exit(1);
        }
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
    int buffer_size = lsm->levels[old_level].count + lsm->levels[deeper_level].count;
    node *buffer = (node *)malloc(sizeof(node) * buffer_size);

    if (buffer == NULL)
    {
        printf("Error3: malloc failed in flush_to_level\n");
        exit(1);
    }
    // read old level into buffer
    if (old_level == 0)
    {
        // read lsm->flush_buffer into buffer
        memcpy(buffer, lsm->flush_buffer, sizeof(node) * lsm->levels[old_level].count);
    }
    else
    {
        fread(buffer, sizeof(node), lsm->levels[old_level].count, fp_old_flush);
    }
    // read current layer into buffer after old level
    fread(buffer + lsm->levels[old_level].count, sizeof(node), lsm->levels[deeper_level].count, fp_current_layer);
    // returns sorted and compacted buffer and updated buffer size
    compact(buffer, &buffer_size);

    // write buffer to new level
    fwrite(buffer, sizeof(node), buffer_size, fp_temp);

    // claim shallow+ deeper level mutex as we are updating their metadata
    pthread_mutex_lock(&lsm->levels[deeper_level].level_mutex);
    pthread_mutex_lock(&lsm->levels[old_level].level_mutex);
    // add to new level count
    lsm->levels[deeper_level]
        .count = buffer_size;

    if (old_level != 0)
    {
        char new_old_path[64];
        set_filename(new_old_path);
        // touch new old level file
        FILE *fp_new_old = fopen(new_old_path, "w");
        fclose(fp_new_old);
        remove(old_path);
        // close files
        fclose(fp_old_flush);
        strcpy(lsm->levels[old_level].filepath, new_old_path);
    }

    // update filepaths
    strcpy(lsm->levels[deeper_level].filepath, new_path);

    // build and remove old fence pointers
    build_fence_pointers(&(lsm->levels[deeper_level]), buffer, buffer_size);
    if (lsm->levels[old_level].fence_pointer_count > 0)
    {
        free(lsm->levels[old_level].fence_pointers);
    }

    lsm->levels[old_level].fence_pointer_count = 0;
    // update old level count
    lsm->levels[old_level]
        .count = 0;
    remove(current_path);
    fclose(fp_current_layer);
    fclose(fp_temp);
    // free buffer
    free(buffer);

    // release mutex
    pthread_mutex_unlock(&lsm->levels[old_level].level_mutex);
    pthread_mutex_unlock(&lsm->levels[deeper_level].level_mutex);

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
        // init level mutex
        pthread_mutex_init(&(lsm->levels[deeper_level].level_mutex), NULL);
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
    // MOST RECENT: search memtable for key starting form back
    // claim memtable mutex
    pthread_mutex_lock(&(lsm->memtable_level.level_mutex));

    for (int i = lsm->memtable_level.count - 1; i >= 0; i--)
    {
        if (lsm->memtable[i].key == key)
        {
            return lsm->memtable[i].value;
        }
    }
    // release memtable mutex
    pthread_mutex_unlock(&(lsm->memtable_level.level_mutex));

    // MIDDLE RECENT: search flush for key starting from back (more recent)
    // claim flush mutex
    pthread_mutex_lock(&(lsm->levels[0].level_mutex));
    for (int i = lsm->levels[0].count - 1; i >= 0; i--)
    {
        if (lsm->flush_buffer[i].key == key)
        {
            return lsm->flush_buffer[i].value;
        }
    }
    // release flush mutex
    pthread_mutex_unlock(&(lsm->levels[0].level_mutex));

    // loop through levels and search disk
    int value = -1;
    for (int i = 1; i <= lsm->max_level; i++)
    {
        // claim level mutex
        pthread_mutex_lock(&(lsm->levels[i].level_mutex));
        value = get_from_disk(lsm, key, i);
        // release level mutex
        pthread_mutex_unlock(&(lsm->levels[i].level_mutex));
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
    if (nodes == NULL)
    {
        printf("Error: malloc failed in get_from_disk\n");
        exit(1);
    }

    int fence_pointer_index = 0;
    // // loop through fence pointers
    for (int i = 0; i < lsm->levels[get_level].fence_pointer_count; i++)
    {
        // if key is less than fence pointer, break
        if (key >= lsm->levels[get_level].fence_pointers[i].key)
        {
            fence_pointer_index = i;
        }
        else
        {
            break;
        }
    }

    // set file pointer to fence pointer index offset bytyes
    int seek_amt = fence_pointer_index * BLOCK_SIZE_NODES * sizeof(node);
    fseek(fp, seek_amt, SEEK_SET);

    // read in BLOCK_SIZE_NODES nodes and print number of nodes read
    int r = fread(nodes, sizeof(node), BLOCK_SIZE_NODES, fp);

    // if fence pointer key is equal to key, return node at index 0 (don't do extra work of binary search)
    if (lsm->levels[get_level].fence_pointer_count > 0 && lsm->levels[get_level].fence_pointers[fence_pointer_index].key == key)
    {
        value = nodes[0].value;
        free(nodes);
        fclose(fp);
        return value;
    }

    // binary search through nodes for key
    int low = 0;
    int high = r - 1;
    int mid;
    while (low <= high)
    {
        mid = (low + high) / 2;
        if (nodes[mid].key == key)
        {
            value = nodes[mid].value;
            break;
        }
        else if (nodes[mid].key < key)
        {
            low = mid + 1;
        }
        else
        {
            high = mid - 1;
        }
    }
    free(nodes);
    fclose(fp);
    return value;
}

void compact(node *buffer, int *buffer_size)
{
    // sort buffer
    merge_sort(buffer, *buffer_size);
    // loop through and remove duplicates (keep last value)
    int i = 0;
    int j = 1;
    while (j < *buffer_size)
    {
        if (buffer[i].key == buffer[j].key)
        {
            buffer[i].value = buffer[j].value;
            j++;
        }
        else
        {
            i++;
            buffer[i].key = buffer[j].key;
            buffer[i].value = buffer[j].value;
            j++;
        }
    }
    *buffer_size = i + 1;
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
    free(lsm->flush_buffer);
    free(lsm);
}

void print_tree(char *msg, lsmtree *lsm)
{
    printf("%s\n", msg);
    // loop through buffer and print
    for (int i = 0; i < lsm->levels[0].count; i++)
    {
        printf("{%d,%d}, ", lsm->flush_buffer[i].key, lsm->flush_buffer[i].value);
    }
    // loop through levels and print level struct
    for (int i = 0; i <= lsm->max_level; i++)
    {
        printf("\nLevel %d: %d/%d, fp: %s\n", lsm->levels[i].level, lsm->levels[i].count, lsm->levels[i].size, lsm->levels[i].filepath);
        // print fence pointers
        if (i > 1)
        {
            for (int j = 0; j < lsm->levels[i].fence_pointer_count; j++)
            {
                fence_pointer fp = lsm->levels[i].fence_pointers[j];
                printf("fp %d, ", fp.key);
            }
            printf("\n");
        }
    }

    printf("\n");
}

void build_fence_pointers(level *level, node *buffer, int buffer_size)
{

    // allocate a fence pointer for every BLOCK_SIZE_NODEs nodes
    int fence_pointer_count = buffer_size / BLOCK_SIZE_NODES + (buffer_size % BLOCK_SIZE_NODES != 0);

    fence_pointer *new_fence_pointers = (fence_pointer *)malloc(sizeof(fence_pointer) * fence_pointer_count);
    // loop through blocks of nodes and set fence pointers
    for (int i = 0; i < fence_pointer_count; i++)
    {
        new_fence_pointers[i].key = buffer[i * BLOCK_SIZE_NODES].key;
    }
    if (level->fence_pointer_count > 0)
    {
        free(level->fence_pointers);
    }
    level->fence_pointers = new_fence_pointers;
    level->fence_pointer_count = fence_pointer_count;
}
