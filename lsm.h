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

typedef struct level
{
    int level;
    int count;
    int size;
    char filepath[64];

} level;

typedef struct lsmtree
{
    node *buffer;
    int max_level;
    level *levels;

} lsmtree;

lsmtree *create(int buffer_size);
void destroy(lsmtree *lsm);
void insert(lsmtree *lsm, keyType key, valType value);
int get(lsmtree *lsm, keyType key);
int get_from_disk(lsmtree *lsm, keyType key, int level);
void flush_from_buffer(lsmtree *lsm);
void flush_to_level(lsmtree *lsm, int level);
void init_level(lsmtree *lsm, int level);
#endif
