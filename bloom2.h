#ifndef CS265_BLOOM2
#define CS265_BLOOM2

#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>

typedef struct
{
    uint32_t num_hashes;
    uint32_t filter_size;
    uint32_t num_bits;
    uint8_t *filter;
} bloom_filter_t;

// Function prototypes
void bloom_filter_t_init(bloom_filter_t *bf, int num_bits, int num_hashes);
void bloom_filter_t_add(bloom_filter_t *bf, int key);
bool bloom_filter_t_lookup(bloom_filter_t *bf, int key);
void bloom_filter_t_destroy(bloom_filter_t *bf);

void bloom_filter_t_init(bloom_filter_t *bf, int size, int num_hashes);
void bloom_filter_t_add(bloom_filter_t *bf, int item);
bool bloom_filter_t_lookup(bloom_filter_t *bf, int item);
void bloom_filter_t_destroy(bloom_filter_t *bf);
#endif