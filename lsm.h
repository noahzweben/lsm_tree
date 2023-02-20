#include <pthread.h>

#ifndef CS265_LSM // This is a header guard. It prevents the header from being included more than once.
#define CS265_LSM

typedef int keyType;
typedef int valType;

extern int BLOCK_SIZE_NODES;
extern pthread_mutex_t merge_mutex;
extern pthread_mutex_t write_mutex;

typedef struct node
{
    keyType key;
    valType value;
} node;

typedef struct fence_pointer
{
    keyType key;
} fence_pointer;

typedef struct level
{
    int level;
    int count;
    int size;
    char filepath[64];
    int fence_pointer_count;
    fence_pointer *fence_pointers;

} level;

typedef struct lsmtree
{
    node *memtable;
    level *memtable_level;
    int max_level;
    node *flush_buffer;
    level *levels;

} lsmtree;

lsmtree *create(int buffer_size);
void cleanup_copy(lsmtree *lsm);
void destroy(lsmtree *lsm, int keep_files);
void insert(lsmtree *lsm, keyType key, valType value);
void reset_level(level *level, int level_num, int level_size);
void flush_to_level(lsmtree *lsm, int level);
int get(lsmtree *lsm, keyType key);
int get_from_disk(lsmtree *lsm, keyType key, int level);
void init_level(lsmtree *lsm, int level);
void reset_level(level *level, int level_num, int level_size);
void compact(node *buffer, int *buffer_size);
void build_fence_pointers(level *level, node *buffer, int buffer_size);
void copy_tree(lsmtree *new_lsm, lsmtree *src_lsm);

// threaded
void *init_flush_thread(void *arg);

#endif
