#ifndef CS265_LSM // This is a header guard. It prevents the header from being included more than once.
#define CS265_LSM

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

#endif
