#include <sys/time.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <math.h>
#include "lsm.h"

int main(void)
{
  // setup benchmark
  lsmtree *lsm = create(BLOCK_SIZE_NODES);

  int seed = 2;
  srand(seed);
  printf("Performing stress test.\n");

  int num_inserts = (int)(1 * pow(10, 5));

  node *random_array = (node *)malloc(sizeof(node) * num_inserts);
  for (int i = 0; i < num_inserts; i++)
  {
    random_array[i] = (node){rand(), rand()};
  }

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

  // shuffle random_array
  for (int i = 0; i < num_inserts; i++)
  {
    int j = rand() % num_inserts;
    node temp = random_array[i];
    random_array[i] = random_array[j];
    random_array[j] = temp;
  }

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

  destroy(lsm);
  free(random_array);
  return 0;
}
