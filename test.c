#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <stdbool.h>
#include "lsm.h"
#include <uuid/uuid.h>
#include "helpers.h"
#include <unistd.h>
#include <pthread.h>

void basic_buffer_test()
{
    // basic create functionality
    lsmtree *lsm = create(10);
    assert(lsm->memtable_level->size == 10);
    assert(lsm->memtable_level->count == 0);

    // first write functionality
    insert(lsm, 5, 10);
    int buffer_count = lsm->memtable_level->count;
    assert(buffer_count == 1);
    node new_node = lsm->memtable[buffer_count - 1];
    assert(new_node.key == 5);
    assert(new_node.value == 10);

    // second write functionality
    insert(lsm, 12, 13);
    buffer_count = lsm->memtable_level->count;
    assert(buffer_count == 2);
    new_node = lsm->memtable[buffer_count - 1];
    assert(new_node.key == 12);
    assert(new_node.value == 13);

    // test buffer reads
    assert(get(lsm, 5) == 10);
    assert(get(lsm, 12) == 13);
    assert(get(lsm, 1) == -1);

    // newer write should be returned from buffer
    insert(lsm, 5, 11);
    assert(get(lsm, 5) == 11);
    destroy(lsm, 0);
}

void level_1_test()
{
    lsmtree *lsm = create(10);
    // put 10 nodes in the buffer - triggers a move to disk (level 1)
    for (int i = 0; i < 10; i++)
    {
        insert(lsm, i, 2 * i);
    }
    int getR = get(lsm, 7);
    assert(getR == 14);
    assert(get(lsm, 2) == 4);
    assert(get(lsm, 12) == -1);
    // since were testing level 1, we need to wait for the thread to finish to reason
    // about the internal state of the system
    // GETS should be available immediately
    sleep(1);
    assert(lsm->memtable_level->count == 0);
    assert(lsm->levels[0].count == 0);
    assert(lsm->levels[1].count == 10);

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
    destroy(lsm, 0);
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

    for (int i = 0; i < max_int; i++)
    {
        int getR = get(lsm, i);
        assert(getR == 2 * i);
    }
    assert(get(lsm, 210) == -1);

    // since were testing the internal state of the system, need to wait for it to settle
    sleep(1);

    assert(lsm->memtable_level->count == 9);
    assert(lsm->levels[1].count == 0);
    assert(lsm->levels[2].count == 200);

    destroy(lsm, 0);
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
        int getR = get(lsm, i);
        assert(getR == 2 * i);
    }
    assert(get(lsm, 511) == -1);
    destroy(lsm, 0);
}

void sort_test()
{
    lsmtree *lsm = create(10);

    // create an array of 500 random numbers
    int max_int = 525;
    int random_array[max_int];
    srand(time(NULL));
    for (int i = 0; i < max_int; i++)
    {
        int num = rand() % 1000;
        random_array[i] = num;
        insert(lsm, num, 2 * num);
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

    // for each key in random_array, ensure that the value is correct
    for (int i = 0; i < max_int; i++)
    {
        assert(get(lsm, random_array[i]) == 2 * random_array[i]);
    }

    destroy(lsm, 0);
}

void fence_pointers_correct()
{
    lsmtree *lsm = create(BLOCK_SIZE_NODES);
    int max_int = BLOCK_SIZE_NODES * 2;
    int offset = 11;
    for (int i = offset; i < max_int + offset; i++)
    {

        insert(lsm, i, 2 * i);
    }
    // ensure that the values are correct

    for (int i = offset; i < max_int + offset; i++)
    {
        int getR = get(lsm, i);
        assert(getR == 2 * i);
    }

    // ensure that the fence pointers are correct
    assert(lsm->levels[1].fence_pointers[0].key == 0 + offset);
    assert(lsm->levels[1].fence_pointers[1].key == 512 + offset);

    destroy(lsm, 0);
}

void large_buffer_size_complex()
{
    lsmtree *lsm = create(BLOCK_SIZE_NODES);
    int max_int = BLOCK_SIZE_NODES * 214.1234;
    int random_array[max_int];
    for (int i = 0; i < max_int; i++)
    {
        int num = rand() % 1000;
        random_array[i] = num;
        insert(lsm, num, 2 * num);
    }
    // ensure that the values are correct

    for (int i = 0; i < max_int; i++)
    {
        int getR = get(lsm, random_array[i]);
        assert(getR == 2 * random_array[i]);
    }
    // assert that each level is sorted correctly
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

    destroy(lsm, 0);
}

void compact_test()
{
    node buffer[10] = {
        {1, 2},
        {2, 4},
        {3, 0},
        {4, 8},
        {3, 9},
        {6, 27},
        {7, 14},
        {8, 16},
        {6, 11},
    };
    int buffer_size = 10;
    // result should be sorted by key and duplicates removed with later values kept ({3,9} and {6,11})
    compact(buffer, &buffer_size);
    assert(buffer_size == 8);
    for (int i = 0; i < buffer_size - 1; i++)
    {
        assert(buffer[i].key < buffer[i + 1].key);
    }
}

void dedup_test()
{
    lsmtree *lsm = create(10);
    // insert 400 nodes with the same key and increasing values
    for (int i = 0; i < 400; i++)
    {
        insert(lsm, 1, i);
    }
    // ensure that the value is the last value inserted
    int getR = get(lsm, 1);
    assert(getR == 399);
    destroy(lsm, 0);
}

int main(void)
{
    basic_buffer_test();
    level_1_test();
    // level_2_test();
    // level_3_test();
    // sort_test();
    // fence_pointers_correct();
    // large_buffer_size_complex();
    // compact_test();
    // dedup_test();

    return 0;
}
