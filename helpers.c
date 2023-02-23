#include "helpers.h"
#include <stdio.h>
#include <stdlib.h>
#include <uuid/uuid.h>


void print_tree(char *msg, lsmtree *lsm)
{
    printf("%s\n", msg);
    // loop through memtalbe and print
    printf("Memtable: ");
    for (int i = 0; i < lsm->memtable_level->count; i++)
    {
        printf("{%d,%d}, ", lsm->memtable[i].key, lsm->memtable[i].value);
    }

    // loop through buffer and print
    printf("Flush Buffer: ");
    for (int i = 0; i < lsm->levels[0].count; i++)
    {
        printf("{%d,%d}, ", lsm->flush_buffer[i].key, lsm->flush_buffer[i].value);
    }
    // loop through levels and print level struct
    for (int i = 0; i <= lsm->max_level; i++)
    {
        printf("\nLevel %d: %d/%d, fp: %s\n", lsm->levels[i].level, lsm->levels[i].count, lsm->levels[i].size, lsm->levels[i].filepath);
        // print fence pointers
        if (i > 0)
        {
            for (int j = 0; j < lsm->levels[i].fence_pointer_count; j++)
            {
                fence_pointer fp = lsm->levels[i].fence_pointers[j];
                printf("fp %d, ", fp.key);
            }
            printf("\n");
        }
    }

    printf("\n");
}


void set_filename(char *filename)
{
    uuid_t uuid;
    char uuid_str[37];
    uuid_generate_time(uuid);
    uuid_unparse_lower(uuid, uuid_str);
    sprintf(filename, "%s.bin", uuid_str);
}

void SORT_TYPE_CPY(node *dst, node *src, const int size)
{
    int i = 0;

    for (; i < size; ++i)
    {
        dst[i] = src[i];
    }
}

void merge_sort_recursive(node *new_buffer, node *buffer, int buffer_size)
{
    const int middle = buffer_size / 2;
    int out = 0;
    int i = 0;
    int j = middle;

    /* don't bother sorting an array of size <= 1 */
    if (buffer_size <= 1)
    {
        return;
    }

    merge_sort_recursive(new_buffer, buffer, middle);
    merge_sort_recursive(new_buffer, &buffer[middle], buffer_size - middle);

    while (out != buffer_size)
    {
        if (i < middle)
        {
            if (j < buffer_size)
            {
                if (buffer[i].key - buffer[j].key <= 0)
                {
                    new_buffer[out] = buffer[i++];
                }
                else
                {
                    new_buffer[out] = buffer[j++];
                }
            }
            else
            {
                new_buffer[out] = buffer[i++];
            }
        }
        else
        {
            new_buffer[out] = buffer[j++];
        }

        out++;
    }

    SORT_TYPE_CPY(buffer, new_buffer, buffer_size);
}

void merge_sort(node *buffer, int buffer_size)
{
    node *new_buffer;

    /* don't bother sorting an array of size <= 1 */
    if (buffer_size <= 1)
    {
        return;
    }

    new_buffer = (node *)malloc(sizeof(node) * buffer_size);
    merge_sort_recursive(new_buffer, buffer, buffer_size);

    free(new_buffer);
}

void NULL_pointer_check(void *pointer, char * msg){
    if (pointer == NULL){
        printf("%s\n", msg);
        exit(1);
    }
}