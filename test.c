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
#include <stdbool.h>

void basic_buffer_test()
{
    printf("basic_buffer_test\n");
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
    destroy(lsm);
}

void level_1_test()
{
    printf("level_1_test\n");
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
    pthread_mutex_unlock(&merge_mutex);
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
    pthread_mutex_unlock(&merge_mutex);
    destroy(lsm);
}

void level_2_test()
{
    printf("level_2_test\n");

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
    pthread_mutex_lock(&merge_mutex);
    assert(lsm->memtable_level->count == 9);
    assert(lsm->levels[1].count == 0);
    assert(lsm->levels[2].count == 200);
    pthread_mutex_unlock(&merge_mutex);

    destroy(lsm);
}

void level_3_test()
{
    printf("level_3_test\n");
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
    destroy(lsm);
}

void sort_test()
{
    printf("sort_test\n");
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
    sleep(1);
    pthread_mutex_lock(&merge_mutex);
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
    pthread_mutex_unlock(&merge_mutex);
    destroy(lsm);
}

void fence_pointers_correct()
{
    printf("fence_pointers_correct\n");
    lsmtree *lsm = create(BLOCK_SIZE_NODES);
    int max_int = BLOCK_SIZE_NODES * 2;
    int offset = 0;
    for (int i = offset; i < max_int + offset; i++)
    {

        insert(lsm, i, 2 * i);
    }
    // ensure that the values are correct
    for (int i = offset; i < max_int + offset; i++)
    {
        int getR = get(lsm, i);
        if (getR != 2 * i)
        {
            printf("{%d,%d}\n", i, getR);
        }

        // assert(getR == 2 * i);
    }
    // since were testing internals of level 1, we need to wait for the thread to finish to reason
    // ensure that the fence pointers are correct
    // get merge_lock
    sleep(1);
    pthread_mutex_lock(&merge_mutex);
    assert(lsm->levels[1].fence_pointers[0].key == 0 + offset);
    assert(lsm->levels[1].fence_pointers[1].key == BLOCK_SIZE_NODES + offset);
    pthread_mutex_unlock(&merge_mutex);
    destroy(lsm);
}

void large_buffer_size_complex()
{
    printf("large_buffer_size_complex\n");
    lsmtree *lsm = create(BLOCK_SIZE_NODES);
    int max_int = BLOCK_SIZE_NODES * 14.1234;
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
    sleep(1);
    pthread_mutex_lock(&merge_mutex);
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
    pthread_mutex_unlock(&merge_mutex);
    destroy(lsm);
}

void compact_test()
{
    printf("compact_test\n");
    node buffer[11] = {
        {.delete = false, 1, 2},
        {.delete = true, 4, 10},
        {.delete = false, 2, 4},
        {.delete = false, 3, 0},
        {.delete = false, 4, 8},
        {.delete = false, 3, 9},
        {.delete = false, 6, 27},
        {.delete = false, 7, 14},
        {.delete = false, 8, 16},
        {.delete = false, 6, 11},
        {.delete = true, 8, 11},

    };
    int buffer_size = 11;
    // result should be sorted by key and duplicates removed
    compact(buffer, &buffer_size);
    assert(buffer_size == 7);
    for (int i = 0; i < buffer_size - 1; i++)
    {
        assert(buffer[i].key < buffer[i + 1].key);
    }
    assert(buffer[2].value == 9);
    assert(buffer[3].delete == true);
    assert(buffer[3].key == 4);
    assert(buffer[6].delete == true);
    assert(buffer[6].key == 8);

    node buffer_2[2] =
        {
            {.delete = false, 1, 100},
            {.delete = false, 1, 11},
        };
    int buffer_2_size = 2;
    compact(buffer_2, &buffer_2_size);
    assert(buffer_2_size == 1);
    assert(buffer_2[0].value == 11);
    assert(buffer_2[0].key == 1);
    assert(buffer_2[0].delete == false);
}

void dedup_test()
{
    printf("dedup_test\n");
    lsmtree *lsm = create(10);
    // insert 400 nodes with the same key and increasing values
    for (int i = 0; i < 400; i++)
    {
        insert(lsm, 1, i);
    }

    // make sure merge is finished, and latest value used from file system merge
    sleep(1);
    assert(get(lsm, 1) == 399);
    destroy(lsm);

    // make sure latest value kept for values just in memtable
    lsmtree *lsm2 = create(10);
    for (int i = 0; i < 8; i++)
    {
        insert(lsm2, 2, i);
    }
    assert(get(lsm2, 2) == 7);
    sleep(1);
    destroy(lsm2);
}

void delete_test()
{
    printf("delete_test\n");
    lsmtree *lsm = create(10);
    int writes = 401;
    // insert 400 nodes with the same key and increasing values
    for (int i = 0; i < writes; i++)
    {
        insert(lsm, i, 2 * i);
    }
    // delete all multiples of 5
    for (int i = 0; i < writes; i++)
    {
        if (i % 5 == 0)
        {
            delete_key(lsm, i);
        }
    }
    // ensure that the values are correct
    sleep(2);
    for (int i = 0; i < writes; i++)
    {
        int getR = get(lsm, i);
        if (i % 5 == 0)
        {
            assert(getR == -2);
        }
        else
        {
            assert(getR == 2 * i);
        }
    }

    destroy(lsm);
}
void *threaded_write(void *args)
{
    void **args_a = (void **)args;
    lsmtree *lsm = (lsmtree *)args_a[0];
    int i = *((int *)args_a[1]);
    insert(lsm, i, 2 * i);
    free(args);
    pthread_exit(NULL);
}

void multi_thread_writes_test()
{

    printf("multi_thread_writes_test\n");
    lsmtree *lsm = create(10);
    int n = 1000;
    int write_array[n];
    pthread_t thread_array[n];

    for (int i = 0; i < n; i++)
    {
        write_array[i] = i;
        void **args = malloc(sizeof(void *) * 2);
        args[0] = (void *)lsm;
        args[1] = (void *)&write_array[i];
        pthread_create(&(thread_array[i]), NULL, threaded_write, args);
    }
    for (int i = 0; i < n; i++)
    {
        pthread_join(thread_array[i], NULL);
    }

    for (int i = 0; i < n; i++)
    {
        int getR = get(lsm, i);
        if (2 * i != getR)
        {
            printf("{%d,%d}\n", i, getR);
            print_tree("u", lsm);
        }
        assert(getR == 2 * i);
    }

    destroy(lsm);
}

void *threaded_read(void *args)
{
    void **args_a = (void **)args;
    lsmtree *lsm = (lsmtree *)args_a[0];
    int i = *((int *)args_a[1]);
    int getR = get(lsm, i);
    assert(getR == 2 * i);
    free(args);
    pthread_exit(NULL);
}

void multi_thread_read_test()
{

    printf("multi_thread_read_test\n");
    lsmtree *lsm = create(10);
    int n = 1000;
    int read_array[n];
    pthread_t thread_array[n];

    for (int i = 0; i < n; i++)
    {
        insert(lsm, i, 2 * i);
    }

    for (int i = 0; i < n; i++)
    {
        read_array[i] = i;
        void **args = malloc(sizeof(void *) * 2);
        args[0] = (void *)lsm;
        args[1] = (void *)&read_array[i];
        pthread_create(&(thread_array[i]), NULL, threaded_read, args);
    }
    for (int i = 0; i < n; i++)
    {
        pthread_join(thread_array[i], NULL);
    }

    destroy(lsm);
}

int main(void)
{
    printf("size of bool %lu\n", sizeof(bool));
    printf("size of node %lu\n", sizeof(node));

    printf("BLOCK SIZE NODES %d\n", BLOCK_SIZE_NODES);

    basic_buffer_test();
    level_1_test();
    level_2_test();
    level_3_test();
    sort_test();
    fence_pointers_correct();
    large_buffer_size_complex();
    compact_test();
    dedup_test();
    delete_test();
    multi_thread_writes_test();
    multi_thread_read_test();

    return 0;
}
