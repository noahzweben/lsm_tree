#include <sys/time.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <math.h>
#include "lsm.h"
#include <stdbool.h>
#include <string.h>
#include <math.h>

struct args
{
    lsmtree *lsm;
    int num_inserts;
    int num_gets;
    node *random_array;
    int *gets_array;
};

void *benchmarkThread(void *arguments)
{
    struct timeval stop, start;
    gettimeofday(&start, NULL);
    struct args *args = arguments;

    printf("Thread NUM_INSERTS: %d\n", args->num_inserts);

    // insert each node in random_array
    for (int i = 0; i < args->num_inserts; i++)
    {
        insert(args->lsm, args->random_array[i].key, args->random_array[i].value);
    }
    // for (int i = 0; i < args->num_gets; i++)
    // {
    //     get(args->lsm, args->gets_array[i]);
    // }

    gettimeofday(&stop, NULL);
    double secs = (double)(stop.tv_usec - start.tv_usec) / 1000000 + (double)(stop.tv_sec - start.tv_sec);
    printf("Thread: %f\n", secs);

    return NULL;
}

void shuffle_list(node *random_array, int size)
{
    // shuffle random_array
    for (int i = 0; i < size; i++)
    {
        int j = rand() % size;
        node temp = random_array[i];
        random_array[i] = random_array[j];
        random_array[j] = temp;
    }
}

int main(int argc, char *argv[])
{
    // setup benchmark

    int seed = 2;
    srand(seed);
    printf("Performing stress test.\n");

    int num_inserts = (int)(1 * pow(10, 6));
    int num_gets = 100000;
    int NODE_NUM = 512;
    lsmtree *lsm = create(NODE_NUM);

    node *random_array = (node *)malloc(sizeof(node) * num_inserts);
    for (int i = 0; i < num_inserts; i++)
    {
        random_array[i] = (node){.delete = false, rand(), rand()};
    }

    // take last num_gets keys from random_array and put them into gets_array
    int *gets_array = (int *)malloc(sizeof(int) * num_gets);
    for (int i = 0; i < num_gets; i++)
    {
        gets_array[i] = random_array[num_inserts - num_gets + i].key;
    }

    shuffle_list(random_array, num_inserts);

    // get number of threads from command line args
    int num_thread = 1;
    if (argc > 1)
    {
        num_thread = atoi(argv[1]);
    }

    pthread_t thread_array[num_thread];

    // measure total time
    struct timeval stop, start;
    gettimeofday(&start, NULL);

    for (int i = 0; i < num_thread; i++)
    {
        struct args *args = malloc(sizeof(struct args));
        args->lsm = lsm;
        args->num_inserts = num_inserts / num_thread;
        args->num_gets = num_gets / num_thread;
        args->random_array = random_array + i * num_inserts / num_thread;
        args->gets_array = gets_array + i * num_gets / num_thread;

        pthread_create(&(thread_array[i]), NULL, benchmarkThread, args);
    }

    // wait for all threads to finish
    for (int i = 0; i < num_thread; i++)
    {
        pthread_join(thread_array[i], NULL);
    }

    gettimeofday(&stop, NULL);
    double secs = (double)(stop.tv_usec - start.tv_usec) / 1000000 + (double)(stop.tv_sec - start.tv_sec);
    printf("Total: %f\n", secs);

    destroy(lsm);
    free(random_array);
    free(gets_array);
    return 0;
}
