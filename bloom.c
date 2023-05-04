#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include "bloom.h"
#include <stdio.h>

void bloom_filter_t_init(bloom_filter_t *bf, int size, int num_hashes)
{
    // make sure bf_size is not
    uint32_t bf_size = ((uint32_t)size + 31) / 32;
    bf->filter = (uint32_t *)calloc(bf_size, sizeof(uint32_t));
    if (bf->filter == NULL)
    {
        printf("size: %d\n", bf_size);
        fprintf(stderr, "Failed to allocate memory for bloom filter\n");
        exit(1);
    }
    bf->filter_size = size;
    bf->num_hashes = num_hashes;
}

void bloom_filter_t_add(bloom_filter_t *bf, int item)
{
    for (int i = 0; i < bf->num_hashes; i++)
    {
        int32_t hash = item * (i + 1);
        uint32_t index = (uint32_t)hash % bf->filter_size;

        bf->filter[index >> 5] |= 1 << (index & 31);
    }
}

bool bloom_filter_t_lookup(bloom_filter_t *bf, int item)
{

    if (bf->filter == NULL)
    {
        return false;
    }
    for (int i = 0; i < bf->num_hashes; i++)
    {
        int32_t hash = item * (i + 1);
        uint32_t index = (uint32_t)hash % bf->filter_size;
        if ((bf->filter[index >> 5] & (1 << (index & 31))) == 0)
            return false;
    }
    return true;
}

void bloom_filter_t_destroy(bloom_filter_t *bf)
{
    free(bf->filter);
    bf->filter = NULL;
}