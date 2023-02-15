#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <stdbool.h>
#include "lsm.h"
#include <uuid/uuid.h>
#include "helpers.h"

// This code is designed to test the correctness of your implementation.
// You do not need to significantly change it.
// Compile and run it in the command line by typing:
// make test; ./test
void clean_files()
{
    remove("level0.txt");
    remove("level1.txt");
    remove("level2.txt");
    remove("level3.txt");
    remove("level4.txt");
    remove("level5.txt");
    remove("level6.txt");
    remove("level7.txt");
    remove("level8.txt");
    remove("level9.txt");
}

void basic_buffer_test()
{
    // basic create functionality
    lsmtree *lsm = create(10);
    assert(lsm->levels[0].size == 10);
    assert(lsm->levels[0].count == 0);

    // first write functionality
    insert(lsm, 5, 10);
    int buffer_count = lsm->levels[0].count;
    assert(buffer_count == 1);
    node new_node = lsm->buffer[buffer_count - 1];
    assert(new_node.key == 5);
    assert(new_node.value == 10);

    // second write functionality
    insert(lsm, 12, 13);
    buffer_count = lsm->levels[0].count;
    assert(buffer_count == 2);
    new_node = lsm->buffer[buffer_count - 1];
    assert(new_node.key == 12);
    assert(new_node.value == 13);

    // test buffer reads
    assert(get(lsm, 5) == 10);
    assert(get(lsm, 12) == 13);
    assert(get(lsm, 1) == -1);
    destroy(lsm);
}

void level_1_test()
{
    lsmtree *lsm = create(10);
    // put 10 nodes in the buffer - triggers a move to disk (level 1)
    for (int i = 0; i < 10; i++)
    {
        insert(lsm, i, 2 * i);
    }

    assert(lsm->levels[0].count == 0);
    assert(lsm->levels[1].count == 10);
    assert(get(lsm, 7) == 14);
    assert(get(lsm, 2) == 4);
    assert(get(lsm, 12) == -1);
    FILE *fp = fopen(lsm->levels[1].filepath, "r");
    assert(fp != NULL);
    node *nodes = (node *)malloc(sizeof(node) * BLOCK_SIZE_NODES);
    int r = fread(nodes, sizeof(node), BLOCK_SIZE_NODES, fp);
    assert(r == 10);
    for (int i = 0; i < r; i++)
    {
        assert(nodes[i].key == i);
        assert(nodes[i].value == 2 * i);
    }
    fclose(fp);
    free(nodes);
    destroy(lsm);
}

void level_2_test()
{
    lsmtree *lsm = create(10);
    // put 10 nodes in the buffer - triggers a move to disk (level 1)
    int max_int = 209;
    for (int i = 0; i < max_int; i++)
    {
        insert(lsm, i, 2 * i);
    }
    assert(lsm->levels[0].count == 9);
    assert(lsm->levels[1].count == 0);
    assert(lsm->levels[2].count == 200);

    // print lsm key 7
    for (int i = 0; i < max_int; i++)
    {
        // printf("key %d: %d\n", i, get(lsm, i));
        assert(get(lsm, i) == 2 * i);
    }
    assert(get(lsm, 210) == -1);

    destroy(lsm);
}

void level_3_test()
{
    lsmtree *lsm = create(10);
    // put 10 nodes in the buffer - triggers a move to disk (level 1)
    int max_int = 509;
    for (int i = 0; i < max_int; i++)
    {
        insert(lsm, i, 2 * i);
    }

    // print lsm key 7
    for (int i = 0; i < max_int; i++)
    {
        // printf("key %d: %d\n", i, get(lsm, i));
        assert(get(lsm, i) == 2 * i);
    }
    assert(get(lsm, 511) == -1);
    destroy(lsm);
}

void sort_test()
{
    lsmtree *lsm = create(10);
    // put 10 nodes in the buffer - triggers a move to disk (level 1)
    int max_int = 509;
    for (int i = 0; i < max_int; i++)
    {
        insert(lsm, i, 2 * i);
    }

    // for each level 1 and on, ensure that the keys are sorted
    for (int i = 1; i <= lsm->max_level; i++)
    {
        const int level_count = lsm->levels[i].count;
        if (level_count == 0)
        {
            continue;
        }
        FILE *fp = fopen(lsm->levels[i].filepath, "r");
        assert(fp != NULL);
        node *nodes = (node *)malloc(sizeof(node) * level_count);
        int r = fread(nodes, sizeof(node), level_count, fp);
        assert(r == lsm->levels[i].count);
        for (int j = 0; j < r - 1; j++)
        {
            assert(nodes[j].key < nodes[j + 1].key);
        }
        fclose(fp);
        free(nodes);
    }

    assert(get(lsm, 511) == -1);
    destroy(lsm);
}

int main(void)
{

    basic_buffer_test();
    level_1_test();
    level_2_test();
    level_3_test();
    sort_test();

    return 0;
}
