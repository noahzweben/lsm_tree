#include <sys/time.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <math.h>
#include "lsm.h"
#include <string.h>

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

int main(int argc, char **argv)
{
  // setup benchmark
  lsmtree *lsm = create(0.3 * BLOCK_SIZE_NODES);

  int seed = 2;
  srand(seed);
  printf("Performing stress test.\n");

  int num_inserts = (int)(1 * pow(10, 7));

  node *random_array = (node *)malloc(sizeof(node) * num_inserts);
  for (int i = 0; i < num_inserts; i++)
  {
    random_array[i] = (node){i, rand()};
  }
  shuffle_list(random_array, num_inserts);

  // run INSERT benchmark
  struct timeval stop, start;
  gettimeofday(&start, NULL);

  // insert each node in random_array
  for (int i = 0; i < num_inserts; i++)
  {
    insert(lsm, random_array[i].key, random_array[i].value);
  }

  gettimeofday(&stop, NULL);
  double secs = (double)(stop.tv_usec - start.tv_usec) / 1000000 + (double)(stop.tv_sec - start.tv_sec);
  printf("%d insertions took %f seconds\n", num_inserts, secs);

  // if command line arg writes is passed, end program
  if (argc > 1 && strcmp(argv[1], "writes") == 0)
  {
    destroy(lsm, 0);
    free(random_array);
    return 0;
  }

  shuffle_list(random_array, num_inserts);

  // run GET benchmark
  gettimeofday(&start, NULL);
  // get each node in random_array
  for (int i = 0; i < num_inserts; i++)
  {
    assert(get(lsm, random_array[i].key) == random_array[i].value);
  }
  gettimeofday(&stop, NULL);
  secs = (double)(stop.tv_usec - start.tv_usec) / 1000000 + (double)(stop.tv_sec - start.tv_sec);
  printf("%d gets took %f seconds\n", num_inserts, secs);

  destroy(lsm, 0);
  free(random_array);
  return 0;
}
