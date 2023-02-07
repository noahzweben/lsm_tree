#ifndef CS265_LSM // This is a header guard. It prevents the header from being included more than once.
#define CS265_LSM

typedef int keyType;
typedef int valType;

struct _node
{
    keyType key;
    valType value;
} node;

typedef struct _lsm
{
    int buffer_size;
    struct node *buffer;

} hashtable;

*lsm create(int buffer_size);

#endif
