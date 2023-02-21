#include "lsm.h"
#include "helpers.h"
#include <stdlib.h>
#include <stdio.h>
#include <sys/stat.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>

int BLOCK_SIZE_NODES = 4096 / sizeof(node);
// merge mutex
pthread_mutex_t merge_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t write_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t write_cond = PTHREAD_COND_INITIALIZER;
pthread_mutex_t read_mutex = PTHREAD_MUTEX_INITIALIZER;

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
    // create memtable buffer
    lsm->memtable = (node *)malloc(sizeof(node) * buffer_size);
    if (lsm->memtable == NULL)
    {
        printf("Error3: malloc failed in create\n");
        exit(1);
    }

    lsm->flush_buffer = (node *)malloc(sizeof(node) * buffer_size);
    if (lsm->flush_buffer == NULL)
    {
        printf("Error4: malloc failed in create\n");
        exit(1);
    }
    lsm->memtable_level = (level *)malloc(sizeof(level));
    if (lsm->memtable_level == NULL)
    {
        printf("Error5: malloc failed in create\n");
        exit(1);
    }
    reset_level(lsm->memtable_level, 0, buffer_size);
    reset_level(&lsm->levels[0], 0, buffer_size);
    return lsm;
}

void reset_level(level *level, int level_num, int buffer_size)
{
    // initialize filepath to empty string
    level->filepath[0] = '\0';
    level->level = level_num;
    level->count = 0;
    level->size = buffer_size;
    level->fence_pointers = NULL;
    level->fence_pointer_count = 0;
}

void copy_level(level *dest_level, level *src_level, int dest_max_level, int copy_level)
{
    // printf("\nCopying Level %d: %d/%d, fp: %s\n", src_level->level, src_level->count, src_level->size, src_level->filepath);

    // free fence pointers before they're copied over
    if (dest_max_level >= copy_level && dest_level->fence_pointer_count > 0)
    {
        free(dest_level->fence_pointers);
    }

    // check if destination has a filepath and is different from src filepath, remove it
    if (dest_level->filepath[0] != '\0' && strcmp(dest_level->filepath, src_level->filepath) != 0)
    {
        remove(dest_level->filepath);
        // necessary?
        // dest_level->filepath[0] = '\0';
    }
    // copy level
    memcpy(dest_level, src_level, sizeof(level));
    // copy fence pointers if i>= 1 and the source has (otherwise creating a pointer of size 0 that doesnt get cleaned up)
    if (copy_level >= 1 && src_level->fence_pointer_count)
    {
        dest_level->fence_pointers = (fence_pointer *)malloc(sizeof(fence_pointer) * src_level->fence_pointer_count);
        memcpy(dest_level->fence_pointers, src_level->fence_pointers, sizeof(fence_pointer) * src_level->fence_pointer_count);
    }
}

// // make a shadow tree we perform the merge on
void copy_tree(lsmtree *new_lsm, level *src_levels, int depth)
{
    // reallocate levels in new lsm
    level *new_levels = (level *)calloc(sizeof(level), depth + 1);
    if (new_lsm->levels == NULL)
    {
        printf("Error6: malloc failed in create\n");
        exit(1);
    }
    memcpy(new_levels, new_lsm->levels, sizeof(level) * (new_lsm->max_level + 1));
    free(new_lsm->levels);
    new_lsm->levels = new_levels;

    // copy levels
    for (int i = 0; i <= depth; i++)
    {
        printf("\nCopying Level %d, lvl %d: %d/%d, fp: %s\n", i, src_levels[i].level, src_levels[i].count, src_levels[i].size, src_levels[i].filepath);
        copy_level(&(new_lsm->levels[i]), &(src_levels[i]), new_lsm->max_level, i);
    }
    new_lsm->max_level = depth;
}

// insert a key-value pair into the LSM tree
void insert(lsmtree *lsm, keyType key, valType value)
{
    // get write mutex
    pthread_mutex_lock(&write_mutex);
    // while memtable is full wait
    while (lsm->memtable_level->count == lsm->memtable_level->size)
    {
        pthread_cond_wait(&write_cond, &write_mutex);
    }

    // create new node on the stack
    node n = {key, value};
    lsm->memtable[lsm->memtable_level->count] = n;
    // if we do the increment after inserting the node, we can read
    // in a threadsafe manner. Worse case scenario if a read starts before the increment,
    // it will just miss the most recently written node
    lsm->memtable_level->count++;
    // unlock write
    pthread_mutex_unlock(&write_mutex);

    // if buffer is full, move to disk
    if (lsm->memtable_level->count == lsm->memtable_level->size)
    {
        // call flush_to_level in a nonblocking thread
        pthread_t thread;
        // struct flush_args *args = (struct flush_args *)malloc(sizeof(struct flush_args));
        // args->lsm = lsm;
        pthread_create(&thread, NULL, init_flush_thread, (void *)lsm);
        pthread_detach(thread);
    }
}

void *init_flush_thread(void *args)
{
    pthread_mutex_lock(&merge_mutex);
    printf("starting merge\n");
    // struct flush_args *flush_args = (struct flush_args *)args;
    lsmtree *lsm = (lsmtree *)args;
    // free(flush_args);

    // copy memtable to flush buffer and copy level metadata to level[0]

    memcpy(lsm->flush_buffer, lsm->memtable, lsm->memtable_level->size * sizeof(node));
    memcpy(&lsm->levels[0], lsm->memtable_level, sizeof(level));

    pthread_mutex_lock(&write_mutex);
    pthread_mutex_lock(&read_mutex);
    reset_level(lsm->memtable_level, lsm->memtable_level->level, lsm->memtable_level->size);
    pthread_mutex_unlock(&read_mutex);
    pthread_cond_signal(&write_cond);
    pthread_mutex_unlock(&write_mutex);

    int depth = 1;
    lsmtree const *original_lsm = lsm;
    level *new_levels = (level *)malloc(sizeof(level));
    memcpy(&new_levels[0], &lsm->levels[0], sizeof(level));
    flush_to_level(&new_levels, original_lsm, &depth);

    // lock read mutex as we switch over lsm tree
    pthread_mutex_lock(&read_mutex);
    printf("new depth %d\n", depth);
    for (int i = 0; i <= depth; i++)
    {
        printf("ZOINS Level %d: %d/%d, fp: %s\n", i, new_levels[i].count, new_levels[i].size, new_levels[i].filepath);
    }
    copy_tree(lsm, new_levels, depth);

    // unlock read mutex
    pthread_mutex_unlock(&read_mutex);

    // free merge_lsm
    // int keep_files = 1;
    // destroy(merge_lsm, keep_files);

    // relesae merge mutex
    pthread_mutex_unlock(&merge_mutex);
    pthread_exit(NULL);
    return NULL;
}

void flush_to_level(level **c_levels, lsmtree const *lsm, int *depth)
{
    int deeper_level = *depth;
    int fresh_level = deeper_level - 1;
    // realloc using depth
    *c_levels = (level *)realloc(*c_levels, sizeof(level) * (deeper_level + 1));
    level *new_levels = *c_levels;

    printf("AT LEVEL %d\n", deeper_level);
    printf("Shallower Level %d, lvl %d, %d/%d, fp: %s\n", fresh_level, new_levels->level, new_levels->count, new_levels->size, new_levels->filepath);
    reset_level(&(new_levels[deeper_level]), deeper_level, new_levels[fresh_level].size * 10);
    if (deeper_level <= lsm->max_level)
    {
        memcpy(&new_levels[deeper_level], &lsm->levels[deeper_level], sizeof(level));
    }
    // init_level(lsm, original_lsm, deeper_level);

    // get filepaths
    char current_path[64];
    strcpy(current_path, new_levels[deeper_level].filepath);
    char fresh_path[64];
    strcpy(fresh_path, new_levels[fresh_level].filepath);

    FILE *fp_old_flush = NULL;

    // no file to open if fresh_level is 0
    if (fresh_level != 0)
    {
        fp_old_flush = fopen(new_levels[fresh_level].filepath, "rb");
        if (fp_old_flush == NULL)
        {
            printf("Error1: fopen failed in flush_to_level\n");
            exit(1);
        }
    }

    // check if current_path is empty (first time at this level)
    FILE *fp_current_layer = NULL;
    printf("trying to open %d\n", current_path[0]);
    if (current_path[0] != '\0')
    {

        fp_current_layer = fopen(current_path, "rb");
        if (fp_current_layer == NULL)
        {
            printf("Error2: fopen [%s] failed in flush_to_level\n", current_path);
            exit(1);
        }
    }
    char new_path[64];
    set_filename(new_path);
    // printf("merge into level: %d - fp_temp create %s\n", deeper_level, new_path);
    FILE *fp_temp = fopen(new_path, "wb");

    // create buffer that can accomodate both levels
    int buffer_size = new_levels[fresh_level].count + new_levels[deeper_level].count;
    node *buffer = (node *)malloc(sizeof(node) * buffer_size);

    if (buffer == NULL)
    {
        printf("Error3: malloc failed in flush_to_level\n");
        exit(1);
    }

    // read deeper layer into buffer if it exists
    if (fp_current_layer != NULL)
    {
        fread(buffer, sizeof(node), new_levels[deeper_level].count, fp_current_layer);
    }
    // read fresher level into buffer
    if (fresh_level == 0)
    {
        // read lsm->flush_buffer into buffer
        memcpy(buffer + new_levels[deeper_level].count, lsm->flush_buffer, sizeof(node) * new_levels[fresh_level].count);
    }
    else
    {
        fread(buffer + new_levels[deeper_level].count, sizeof(node), new_levels[fresh_level].count, fp_old_flush);
    }

    // returns sorted and compacted buffer and updated buffer size
    compact(buffer, &buffer_size);

    // write buffer to new level
    fwrite(buffer, sizeof(node), buffer_size, fp_temp);

    // add to new level count
    new_levels[deeper_level]
        .count = buffer_size;

    if (fresh_level != 0)
    {
        remove(fresh_path);
        // set old level filepath to empty
        new_levels[fresh_level].filepath[0] = '\0';
        fclose(fp_old_flush);
    }

    // update filepaths
    strcpy(new_levels[deeper_level].filepath, new_path);

    // build and remove old fence pointers
    build_fence_pointers(&(new_levels[deeper_level]), buffer, buffer_size);
    if (new_levels[fresh_level].fence_pointer_count > 0)
    {
        free(new_levels[fresh_level].fence_pointers);
        new_levels[fresh_level].fence_pointers = NULL;
    }

    new_levels[fresh_level].fence_pointer_count = 0;
    // update old level count
    new_levels[fresh_level]
        .count = 0;
    if (fp_current_layer != NULL)
    {
        fclose(fp_current_layer);
    }
    fclose(fp_temp);
    // free buffer
    free(buffer);

    // check if new level is full
    printf("\nLevel %d: %d/%d, fp: %s\n", deeper_level, new_levels[deeper_level].count, new_levels[deeper_level].size, new_levels[deeper_level].filepath);

    if (new_levels[deeper_level].count == new_levels[deeper_level].size)
    {
        *depth = *depth + 1;
        flush_to_level(&new_levels, lsm, depth);
    }

    for (int i = 0; i <= *depth; i++)
    {
        printf("\nFINALE Level %d: %d/%d, fp: %s\n", i, new_levels[i].count, new_levels[i].size, new_levels[i].filepath);
    }
}

// void init_level(lsmtree *lsm, level const *original_lsm, int deeper_level)
// {
//     if (lsm->max_level < deeper_level)
//     {
//         lsm->max_level++;
//         // increase memory allocation for levels

//         lsm->levels = (level *)realloc(lsm->levels, sizeof(level) * (lsm->max_level + 1));

//         if (lsm->levels == NULL)
//         {
//             printf("Error: realloc failed in flush_from_buffer\n");
//             exit(1);
//         }

//         reset_level(&(lsm->levels[deeper_level]), deeper_level, lsm->levels[deeper_level - 1].size * 10);
//     }
// }

// get a value
int get(lsmtree *lsm, keyType key)
{
    //  acquire read mutex
    pthread_mutex_lock(&read_mutex);
    // MOST RECENT: search memtable for key starting form back
    for (int i = lsm->memtable_level->count - 1; i >= 0; i--)
    {
        if (lsm->memtable[i].key == key)
        {
            int value = lsm->memtable[i].value;
            pthread_mutex_unlock(&read_mutex);
            return value;
        }
    }

    // MIDDLE RECENT: search flush for key starting from back (more recent)
    for (int i = lsm->levels[0].count - 1; i >= 0; i--)
    {
        if (lsm->flush_buffer[i].key == key)
        {
            int value = lsm->flush_buffer[i].value;
            pthread_mutex_unlock(&read_mutex);
            return value;
        }
    }

    // loop through levels and search disk
    int value = -1;

    for (int i = 1; i <= lsm->max_level; i++)
    {
        value = get_from_disk(lsm, key, i);
        if (value != -1)
        {
            pthread_mutex_unlock(&read_mutex);
            return value;
        }
    }
    pthread_mutex_unlock(&read_mutex);
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

void destroy(lsmtree *lsm, int keep_files)
{
    // acquire merge and read mutex if getting rid of files (end of function)
    if (keep_files == 0)
    {
        pthread_mutex_lock(&merge_mutex);
        pthread_mutex_lock(&read_mutex);
    }

    // loop through levels and remove filepath
    for (int i = 1; i <= lsm->max_level; i++)
    {
        if (keep_files == 0)
        {
            remove(lsm->levels[i].filepath);
        }

        if (lsm->levels[i].fence_pointer_count > 0)
        {
            free(lsm->levels[i].fence_pointers);
        }
    }

    free(lsm->levels);
    free(lsm->memtable_level);
    free(lsm->flush_buffer);
    free(lsm->memtable);
    free(lsm);
    if (keep_files == 0)
    {
        pthread_mutex_unlock(&merge_mutex);
        pthread_mutex_unlock(&read_mutex);
    }
}

void print_tree(char *msg, lsmtree *lsm)
{
    printf("%s\n", msg);
    // loop through memtalbe and print
    printf("Memtable: ");
    for (int i = 0; i < lsm->memtable_level->count; i++)
    {
        printf("{%d,%d}, ", lsm->memtable[i].key, lsm->memtable[i].value);
    }

    // loop through buffer and print
    printf("Flush Buffer: ");
    for (int i = 0; i < lsm->levels[0].count; i++)
    {
        printf("{%d,%d}, ", lsm->flush_buffer[i].key, lsm->flush_buffer[i].value);
    }
    // loop through levels and print level struct
    for (int i = 0; i <= lsm->max_level; i++)
    {
        printf("\nLevel %d: %d/%d, fp: %s\n", lsm->levels[i].level, lsm->levels[i].count, lsm->levels[i].size, lsm->levels[i].filepath);
        // print fence pointers
        if (i > 0)
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
    // free old
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
