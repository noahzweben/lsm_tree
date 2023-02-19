#include <pthread.h>

#ifndef CS265_LSM // This is a header guard. It prevents the header from being included more than once.
#define CS265_LSM

typedef int keyType;
typedef int valType;

extern int BLOCK_SIZE_NODES;

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
    // mutex
    pthread_mutex_t level_mutex;
    fence_pointer *fence_pointers;

} level;

typedef struct lsmtree
{
    node *memtable;
    level memtable_level;

    int max_level;
    node *flush_buffer;
    level *levels;

} lsmtree;

lsmtree *create(int buffer_size);
void init_memtable(lsmtree *lsm, int buffer_size);
void destroy(lsmtree *lsm);
void insert(lsmtree *lsm, keyType key, valType value);
void flush_to_level(lsmtree *lsm, int level);
int get(lsmtree *lsm, keyType key);
int get_from_disk(lsmtree *lsm, keyType key, int level);
void init_level(lsmtree *lsm, int level);
void compact(node *buffer, int *buffer_size);
void build_fence_pointers(level *level, node *buffer, int buffer_size);

// threaded
void *init_flush_thread(void *arg);

#endif
