#ifndef CS265 // This is a header guard. It prevents the header from being included more than once.
#define CS265

typedef int keyType;
typedef int valType;

typedef struct node
{
    keyType key;
    valType value;
} node;

typedef struct lsmtree
{
    int buffer_size;
    int buffer_count;
    node *buffer;
} lsmtree;

lsmtree *create(int buffer_size);
void insert(lsmtree *lsm, keyType key, valType value);
void destroy(lsmtree *lsm);
int get(lsmtree *lsm, keyType key);
#endif
