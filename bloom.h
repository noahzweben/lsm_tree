#ifndef CS265_BLOOM
#define CS265_BLOOM

#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>

typedef struct bloom_filter_t
{
    uint32_t *filter;
    int filter_size;
    int num_hashes;
} bloom_filter_t;

void bloom_filter_t_init(bloom_filter_t *bf, int size, int num_hashes);
void bloom_filter_t_add(bloom_filter_t *bf, int item);
bool bloom_filter_t_lookup(bloom_filter_t *bf, int item);
void bloom_filter_t_destroy(bloom_filter_t *bf);

#endif