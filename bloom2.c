#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>
#include "bloom2.h"

// FNV-1a hash function
uint32_t fnv1a_hash(int key, uint32_t seed)
{
    size_t len = 4;
    const uint32_t prime = 0x01000193;
    const uint32_t offset_basis = 0x811C9DC5;
    uint32_t hash = offset_basis ^ seed;

    const int *data = &key;

    const uint8_t *byte_data = (const uint8_t *)data;
    for (size_t i = 0; i < len; i++)
    {
        hash ^= byte_data[i];
        hash *= prime;
    }

    return hash;
}

void bloom_filter_t_init(bloom_filter_t *bf, int num_bits, int num_hashes)
{

    bf->num_hashes = (uint32_t)num_hashes;
    bf->num_bits = (uint32_t)num_bits;
    bf->filter_size = (num_bits + 7) / 8;
    bf->filter = (uint8_t *)calloc(bf->filter_size, sizeof(uint8_t));
    if (!bf->filter)
    {
        free(bf);
    }
}

void bloom_filter_t_add(bloom_filter_t *bf, int key)
{
    for (uint32_t i = 0; i < bf->num_hashes; ++i)
    {
        uint32_t hash = fnv1a_hash(key, i);
        uint32_t bit_pos = hash % bf->num_bits;
        bf->filter[bit_pos / 8] |= 1 << (bit_pos % 8);
    }
}

bool bloom_filter_t_lookup(bloom_filter_t *bf, int key)
{
    for (uint32_t i = 0; i < bf->num_hashes; ++i)
    {
        uint32_t hash = fnv1a_hash(key, i);
        uint32_t bit_pos = hash % bf->num_bits;
        if (!(bf->filter[bit_pos / 8] & (1 << (bit_pos % 8))))
        {
            return false;
        }
    }

    return true;
}

void bloom_filter_t_destroy(bloom_filter_t *bf)
{
    if (bf)
    {
        if (bf->filter)
        {
            free(bf->filter);
        }
        free(bf);
    }
}
