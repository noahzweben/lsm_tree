#include "lsm.h"
#include "helpers.h"
#include <stdlib.h>
#include <stdio.h>
#include <sys/stat.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <stdbool.h>

#define max(a, b) \
    ({ __typeof__ (a) _a = (a); \
       __typeof__ (b) _b = (b); \
     _a > _b ? _a : _b; })

int BLOCK_SIZE_NODES = 4096 / sizeof(node);
// merge mutex
pthread_mutex_t merge_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t write_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t write_cond = PTHREAD_COND_INITIALIZER;
pthread_rwlock_t rwlock = PTHREAD_RWLOCK_INITIALIZER;

/*
Creates an initializes a new lsm tree. Takes memtable size as input
argument.
*/
lsmtree *create(int buffer_size)
{
    lsmtree *lsm = (lsmtree *)malloc(sizeof(lsmtree));
    NULL_pointer_check(lsm, "Error1: malloc failed in create");

    lsm->max_level = 0;
    lsm->levels = (level *)malloc(sizeof(level) * (lsm->max_level + 1));
    NULL_pointer_check(lsm->levels, "Error2: malloc failed in create");

    // create memtable buffer
    lsm->memtable = (node *)malloc(sizeof(node) * buffer_size);
    NULL_pointer_check(lsm->memtable, "Error3: malloc failed in create");

    // where full memtable gets stored so writes can continue
    lsm->flush_buffer = (node *)malloc(sizeof(node) * buffer_size);
    NULL_pointer_check(lsm->flush_buffer, "Error4: malloc failed in create");
    lsm->memtable_level = (level *)malloc(sizeof(level));
    NULL_pointer_check(lsm->memtable_level, "Error5: malloc failed in create");

    // set metadata for level
    reset_level(lsm->memtable_level, 0, buffer_size);
    reset_level(&lsm->levels[0], 0, buffer_size);
    return lsm;
}

// INSERTION
// ------------------------------------------------------------------------------------------------------------------------
void insert(lsmtree *lsm, keyType key, valType value)
{
    node n = {.delete = false, .key = key, .value = value};
    __insert(lsm, n);
}

void delete_key(lsmtree *lsm, keyType key)
{
    node n = {.delete = true, .key = key, .value = 0};
    __insert(lsm, n);
}
void __insert(lsmtree *lsm, node n)
{
    // get write mutex
    pthread_mutex_lock(&write_mutex);
    // while memtable is full wait
    while (lsm->memtable_level->count == lsm->memtable_level->size)
    {
        pthread_cond_wait(&write_cond, &write_mutex);
    }
    // create new node on the stack
    lsm->memtable[lsm->memtable_level->count] = n;
    // if we do the increment after inserting the node, we can read
    // in a threadsafe manner. Worse case scenario if a read starts before the increment,
    // it will just miss the most recently written node
    lsm->memtable_level->count++;
    // allow subsequnt writes

    // if buffer is full, move to disk
    if (lsm->memtable_level->count == lsm->memtable_level->size)
    {
        // call flush_to_level in a nonblocking thread
        pthread_t thread;
        pthread_create(&thread, NULL, init_flush_thread, (void *)lsm);
        pthread_detach(thread);
    }

    pthread_cond_signal(&write_cond);
    pthread_mutex_unlock(&write_mutex);
}

// MERGING
// ------------------------------------------------------------------------------------------------------------------------

void *init_flush_thread(void *args)
{
    pthread_mutex_lock(&merge_mutex);
    lsmtree *lsm = (lsmtree *)args;

    // code to empty memtable and start accepting writes again
    // signals to waiting write threads that they can claim mutex
    pthread_mutex_lock(&write_mutex);
    pthread_rwlock_wrlock(&rwlock);
    // copy memtable to flush buffer and copy level metadata to level[0]
    memcpy(lsm->flush_buffer, lsm->memtable, lsm->memtable_level->size * sizeof(node));
    memcpy(&lsm->levels[0], lsm->memtable_level, sizeof(level));
    reset_level(lsm->memtable_level, lsm->memtable_level->level, lsm->memtable_level->size);
    // unlock rwlock for writing
    pthread_rwlock_unlock(&rwlock);
    pthread_cond_signal(&write_cond);
    pthread_mutex_unlock(&write_mutex);

    // create a shadow copy of levels where merge will happen
    // when merge is done, we will copy over the new levels to lsm
    int depth = 1;
    level *new_levels = (level *)malloc(sizeof(level) * (1 + lsm->max_level));
    memcpy(&new_levels[0], &lsm->levels[0], sizeof(level));

    // when this function is complete, new_levels will be set
    // with the merge files and depth will be set to the
    // number of layers merge
    flush_to_level(&new_levels, lsm, &depth);

    // we now copy over the new levels to the lsm tree
    // while this is happening the tree is in an inconsistent state
    // so we lock reads from it
    // writes are still ok as they only go to the memtable

    pthread_rwlock_wrlock(&rwlock);
    copy_tree(lsm, new_levels, depth);
    // printf("max depth %d\n", lsm->max_level);
    pthread_rwlock_unlock(&rwlock);

    // release merge mutex
    pthread_mutex_unlock(&merge_mutex);
    pthread_exit(NULL);
    return NULL;
}

void flush_to_level(level **new_levels_wrapper, lsmtree const *lsm, int *depth)
{
    int deeper_level = *depth;
    int fresh_level = deeper_level - 1;

    // realloc using depth - if we need to create a new layer, we must allocate
    // additional space for it in the layers object
    if (deeper_level > lsm->max_level)
    {
        *new_levels_wrapper = (level *)realloc(*new_levels_wrapper, sizeof(level) * (deeper_level + 1));
    }

    level *new_levels = *new_levels_wrapper;

    // initialize and copy over layer metadata (one at a time as needed to avoid additional work)
    reset_level(&(new_levels[deeper_level]), deeper_level, new_levels[fresh_level].size * 10);
    if (deeper_level <= lsm->max_level)
    {
        memcpy(&new_levels[deeper_level], &lsm->levels[deeper_level], sizeof(level));
    }

    // get filepaths
    char deep_path[64];
    strcpy(deep_path, new_levels[deeper_level].filepath);
    char fresh_path[64];
    strcpy(fresh_path, new_levels[fresh_level].filepath);

    FILE *fp_fresher_layer = NULL;
    // if the fresher level is >0 then we are reading from disk
    // if fresh level is == 0 this means we are going to read from flush buffer
    if (fresh_level > 0)
    {
        fp_fresher_layer = fopen(new_levels[fresh_level].filepath, "rb");
        NULL_pointer_check(fp_fresher_layer, "Error1: fopen failed in flush_to_level");
    }

    FILE *fp_deep_layer = NULL;
    // check if deep_path is empty (first time at this level)
    if (deep_path[0] != '\0')
    {
        fp_deep_layer = fopen(deep_path, "rb");
        if (fp_deep_layer == NULL)
        {
            printf("trying to open %s\n", deep_path);
            print_tree("help", (lsmtree *)lsm);
        }
        NULL_pointer_check(fp_deep_layer, "Error2: fopen failed in flush_to_level");
    }

    // create a new file to store the merged layers
    char new_path[64];
    set_filename(new_path);
    FILE *fp_temp = fopen(new_path, "wb");

    // create buffer that can accomodate both levels
    int buffer_size = new_levels[fresh_level].count + new_levels[deeper_level].count;
    node *buffer = (node *)malloc(sizeof(node) * buffer_size);
    NULL_pointer_check(buffer, "Error3: malloc failed in flush_to_level");

    // read deeper layer into buffer if it exists
    // (it may not if this is our first time at this layer)
    if (fp_deep_layer != NULL)
    {
        fread(buffer, sizeof(node), new_levels[deeper_level].count, fp_deep_layer);
    }

    // Read fresher level into buffer
    // if fresher_level == 0, this means we need to pull from the flush_buffer
    // otherwise pull from disk
    if (fresh_level == 0)
    {
        memcpy(buffer + new_levels[deeper_level].count, lsm->flush_buffer, sizeof(node) * new_levels[fresh_level].count);
    }
    else
    {
        fread(buffer + new_levels[deeper_level].count, sizeof(node), new_levels[fresh_level].count, fp_fresher_layer);
    }

    // BUILD THE NEW LAYER + INDECES

    // returns sorted and compacted buffer and updated buffer size
    compact(buffer, &buffer_size);
    // write buffer to new level
    fwrite(buffer, sizeof(node), buffer_size, fp_temp);
    // Store new fileapth you created
    strcpy(new_levels[deeper_level].filepath, new_path);
    build_fence_pointers(&(new_levels[deeper_level]), buffer, buffer_size);
    build_bloom_filter(&(new_levels[deeper_level]), buffer, buffer_size);
    // add to new level count
    new_levels[deeper_level]
        .count = buffer_size;

    // CLEANUP OF OLD RESOURCES

    // Remove the old files to free memory
    if (fresh_level > 0)
    {
        remove(fresh_path);
        new_levels[fresh_level].filepath[0] = '\0';
        fclose(fp_fresher_layer);
        // remove old bloom filter
        bloom_filter_t_destroy(&(new_levels[fresh_level].bloom_filter));
    }

    // Remove old fence pointers, these are safe to free (won't interfere with main lsm reads)
    // because  fence pointers shallower in the tree at this point have already been overwritten
    // as part of the new_levels context
    if (new_levels[fresh_level].fence_pointer_count > 0)
    {
        free(new_levels[fresh_level].fence_pointers);
        new_levels[fresh_level].fence_pointers = NULL;
        new_levels[fresh_level].fence_pointer_count = 0;
    }
    // update old level count
    new_levels[fresh_level]
        .count = 0;

    if (fp_deep_layer != NULL)
    {
        fclose(fp_deep_layer);
    }
    fclose(fp_temp);
    free(buffer);

    // CHECK IF NEW LAYER IS FULL

    if (new_levels[deeper_level].count >= new_levels[deeper_level].size)
    {
        *depth = *depth + 1;
        flush_to_level(new_levels_wrapper, lsm, depth);
    }
}

// RETRIEVAL
// ------------------------------------------------------------------------------------------------------------------------

int get(lsmtree *lsm, keyType key)
{
    node *found_node = NULL;
    __get(lsm, &found_node, key);

    if (found_node == NULL)
    {
        return -1;
    }
    if (found_node->delete == true)
    {
        free(found_node);
        return -2;
    }
    int value = found_node->value;
    free(found_node);
    return value;
}

void set_find_node(node **find_node, node *n)
{
    if (*find_node == NULL)
    {
        *find_node = (node *)malloc(sizeof(node));
        NULL_pointer_check(*find_node, "Error: malloc failed in set_find_node");
    }
    (*find_node)->key = n->key;
    (*find_node)->value = n->value;
    (*find_node)->delete = n->delete;
}

// get a value
void __get(lsmtree *lsm, node **find_node, keyType key)
{
    //  get read lock for rwlock
    pthread_rwlock_rdlock(&rwlock);
    // MOST RECENT: search memtable for key starting form back
    for (int i = lsm->memtable_level->count - 1; i >= 0; i--)
    {
        if (lsm->memtable[i].key == key)
        {
            set_find_node(find_node, &(lsm->memtable[i]));
            pthread_rwlock_unlock(&rwlock);
            return;
        }
    }

    // MIDDLE RECENT: search flush for key starting from back (more recent)
    for (int i = lsm->levels[0].count - 1; i >= 0; i--)
    {
        if (lsm->flush_buffer[i].key == key)
        {
            set_find_node(find_node, &(lsm->flush_buffer[i]));
            pthread_rwlock_unlock(&rwlock);
            return;
        }
    }

    // loop through levels and search disk
    for (int i = 1; i <= lsm->max_level; i++)
    {
        get_from_disk(lsm, find_node, key, i);
        if (*find_node != NULL)
        {
            pthread_rwlock_unlock(&rwlock);
            return;
        }
    }
    pthread_rwlock_unlock(&rwlock);
    return;
}

void get_from_disk(lsmtree *lsm, node **find_node, keyType key, int get_level)
{
    // check if key is in bloom filter
    bloom_filter_t bf = lsm->levels[get_level].bloom_filter;
    if (bloom_filter_t_lookup(&bf, key) == false)
    {
        return;
    }

    // open File fp for reading
    if (lsm->levels[get_level].filepath[0] == '\0')
    {
        return;
    }

    FILE *fp = fopen(lsm->levels[get_level].filepath, "r");
    NULL_pointer_check(fp, "ERROR: get_from_disk");

    // copy over buffer_count elements to fp
    node *nodes = (node *)malloc(sizeof(node) * BLOCK_SIZE_NODES);
    NULL_pointer_check(nodes, "Error: malloc failed in get_from_disk");

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
        set_find_node(find_node, &(nodes[0]));
        free(nodes);
        fclose(fp);
        return;
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
            set_find_node(find_node, &(nodes[mid]));
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
    return;
}

void range(lsmtree *lsm, node **nodes, int *n_results, keyType start, keyType finish)
{
    if (start > finish)
    {
        printf("Error: start key must be less than finish key in range\n");
        return;
    }
    //  acquire read mutex
    pthread_rwlock_rdlock(&rwlock);

    // LOAD matching nodes from memory

    *nodes = (node *)malloc(10000 * lsm->levels[0].size);
    *n_results = 0;

    // start from back bc of how compact works ( newest should be in back)
    for (int i = lsm->max_level; i >= 1; i--)
    {
        range_from_disk(&(lsm->levels[i]), nodes, n_results, start, finish);
    }

    for (int i = 0; i < lsm->levels[0].count; i++)
    {
        if (lsm->flush_buffer[i].key >= start && lsm->flush_buffer[i].key < finish)
        {
            (*nodes)[(*n_results)++] = lsm->flush_buffer[i];
        }
    }

    for (int i = 0; i < lsm->memtable_level->count; i++)
    {
        if (lsm->memtable[i].key >= start && lsm->memtable[i].key < finish)
        {
            (*nodes)[(*n_results)++] = lsm->memtable[i];
        }
    }
    compact(*nodes, n_results);
    pthread_rwlock_unlock(&rwlock);
}

void range_from_disk(level *range_level, node **nodes, int *n_results, int start, int finish)
{
    // open File fp for reading
    if (range_level->filepath[0] == '\0')
    {
        return;
    }
    // get start and finish fence pointers
    int start_fence_pointer_index = 0;
    int finish_fence_pointer_index = 0;

    // TODO check this
    for (int i = 0; i < range_level->fence_pointer_count; i++)
    {
        if (start >= range_level->fence_pointers[i].key)
        {
            start_fence_pointer_index = i;
        }
        if (finish >= range_level->fence_pointers[i].key)
        {
            finish_fence_pointer_index = i;
        }
    }
    // set file fp to start fence pointer index
    FILE *fp = fopen(range_level->filepath, "r");
    NULL_pointer_check(fp, "ERROR: range_from_disk");

    // read blocks through finish_fence_pointer into nodes array
    int seek_amt = start_fence_pointer_index * BLOCK_SIZE_NODES * sizeof(node);
    fseek(fp, seek_amt, SEEK_SET);
    node *buffer = (node *)malloc(sizeof(node) * BLOCK_SIZE_NODES * (finish_fence_pointer_index - start_fence_pointer_index + 1));
    NULL_pointer_check(buffer, "Error: malloc failed in range_from_disk");
    int buffer_size = 0;
    for (int i = start_fence_pointer_index; i <= finish_fence_pointer_index; i++)
    {
        int r = fread(buffer + buffer_size, sizeof(node), BLOCK_SIZE_NODES, fp);
        buffer_size += r;
    }
    // for anything in buffer within start and finish, add to nodes array, and increase range
    for (int i = 0; i < buffer_size; i++)
    {
        if (buffer[i].key >= start && buffer[i].key < finish)
        {
            (*nodes)[(*n_results)++] = buffer[i];
        }
    }
    free(buffer);
    fclose(fp);
}

// HELPERS
// ------------------------------------------------------------------------------------------------------------------------
void compact(node *buffer, int *buffer_size)
{
    if (*buffer_size == 0)
    {
        return;
    }
    // sort buffer
    merge_sort(buffer, *buffer_size);
    // loop through and remove duplicates (keeps LAST occurence)
    // if node.delete == true, remove all other duplicate nodes (just keep delete node)
    int i = 0;
    int j = 1;
    while (j < *buffer_size)
    {
        if (buffer[i].key == buffer[j].key)
        {
            buffer[i].key = buffer[j].key;
            buffer[i].value = buffer[j].value;
            buffer[i].delete = buffer[j].delete || buffer[i].delete;
            j++;
        }
        else
        {
            i++;
            buffer[i].key = buffer[j].key;
            buffer[i].value = buffer[j].value;
            buffer[i].delete = buffer[j].delete;
            j++;
        }
    }
    *buffer_size = i + 1;
}

void destroy(lsmtree *lsm)
{
    pthread_mutex_lock(&merge_mutex);
    pthread_mutex_lock(&write_mutex);
    pthread_rwlock_wrlock(&rwlock);

    // loop through levels and remove filepath
    for (int i = 1; i <= lsm->max_level; i++)
    {

        if (lsm->levels[i].filepath[0] != '\0')
        {
            remove(lsm->levels[i].filepath);
        }

        if (lsm->levels[i].fence_pointer_count > 0)
        {
            free(lsm->levels[i].fence_pointers);
        }
        if (lsm->levels[i].count > 0)
        {
            bloom_filter_t_destroy(&(lsm->levels[i].bloom_filter));
        }
    }

    free(lsm->levels);
    free(lsm->memtable_level);
    free(lsm->flush_buffer);
    free(lsm->memtable);
    free(lsm);

    pthread_rwlock_unlock(&rwlock);
    pthread_mutex_unlock(&write_mutex);
    pthread_mutex_unlock(&merge_mutex);
    // destory all mutexes and locks
    // pthread_mutex_destroy(&merge_mutex);
    // pthread_rwlock_destroy(&rwlock);
    // pthread_mutex_destroy(&write_mutex);

    // TODO make sure no rads/writes/merges start post destory or werird erros
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
    level->fence_pointers = new_fence_pointers;
    level->fence_pointer_count = fence_pointer_count;
}

void build_bloom_filter(level *level, node *buffer, int buffer_size)
{
    bloom_filter_t bf;
    bloom_filter_t_init(&bf, buffer_size * 1000, 5);
    // add to buffer if delete == false
    for (int i = 0; i < buffer_size; i++)
    {
        bloom_filter_t_add(&bf, buffer[i].key);

    }
    level->bloom_filter = bf;
}

/*
Helper method to initialize level metadata
*/
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
    // Check if the destination has fence pointers, if so free them
    // check if dest_max_level is greater than copy_level
    // because we only want to free fence pointers for existing levels that we are copying over
    if (dest_max_level >= copy_level && dest_level->fence_pointer_count > 0)
    {
        free(dest_level->fence_pointers);
    }

    // check if destination has a filepath and is different from src filepath, remove it
    if (dest_level->filepath[0] != '\0')
    {
        remove(dest_level->filepath);
    }
    // if destination level has at least one item, free the bloom filter
    if (copy_level >= 1 && dest_level->count > 0)
    {
        bloom_filter_t_destroy(&dest_level->bloom_filter);
    }

    // copy level
    memcpy(dest_level, src_level, sizeof(level));
    // copy fence pointers if i>= 1 (past flush buffer level) a
    if (copy_level >= 1 && src_level->fence_pointer_count)
    {
        dest_level->fence_pointers = (fence_pointer *)malloc(sizeof(fence_pointer) * src_level->fence_pointer_count);
        memcpy(dest_level->fence_pointers, src_level->fence_pointers, sizeof(fence_pointer) * src_level->fence_pointer_count);
    }

    free(src_level->fence_pointers);
}

// // make a shadow tree we perform the merge on
void copy_tree(lsmtree *dest_lsm, level *src_levels, int depth)
{
    // reallocate levels in new lsm
    // make sure there is enough space to accomodate newer levels
    // if its only a partial merge, then make sure enough new space
    int new_depth = max(dest_lsm->max_level, depth);
    level *new_levels = (level *)calloc(sizeof(level), new_depth + 1);
    NULL_pointer_check(new_levels, "Error1: malloc failed in copy_tree");
    memcpy(new_levels, dest_lsm->levels, sizeof(level) * (dest_lsm->max_level + 1));
    free(dest_lsm->levels);
    dest_lsm->levels = new_levels;
    for (int i = 0; i <= depth; i++)
    {
        copy_level(&(dest_lsm->levels[i]), &(src_levels[i]), dest_lsm->max_level, i);
    }
    dest_lsm->max_level = new_depth;
    free(src_levels);
}
