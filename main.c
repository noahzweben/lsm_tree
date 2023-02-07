#include <stdlib.h>
#include <stdio.h>

#include "hash_table.h"

// This is where you can implement your own tests for the hash table
// implementation.
int main(void)
{
  int bob[100] = {0};
  int *stacy = malloc(100 * sizeof(int));

  printf("size of bob %lu\n", sizeof(bob));
  printf("size of stacy %lu\n", sizeof(stacy));
  hashtable *ht = NULL;
  int size = 10;
  allocate(&ht, size);

  int key = 0;
  int value = -1;

  put(ht, key, value);
  put(ht, 0, 40);

  int num_values = 2;

  valType *values = malloc(num_values * sizeof(valType));

  int num_results;

  get(ht, key, values, num_values, &num_results);
  printf("values is %d, %d \n", values[0], values[1]);

  if (num_results > num_values)
  {
    values = realloc(values, (num_results) * sizeof(valType));
    get(ht, 0, values, num_values, &num_results);
  }

  for (int i = 0; i < num_results; i++)
  {
    printf("value of %d is %d \n", i, values[i]);
  }
  free(values);
  erase(ht, 0);

  deallocate(ht);
  return 0;
}
