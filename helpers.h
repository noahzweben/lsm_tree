#ifndef HELPERS_H
#define HELPERS_H
#include "lsm.h"

void NULL_pointer_check(void *pointer, char *msg);
void set_filename(char *filename);
void print_tree(char *msg, lsmtree *lsm);

// TODO github link
// merge sort implementation taken from <sort.h>
void merge_sort(node *buffer, int buffer_size);
void merge_sort_recursive(node *new_buffer, node *buffer, int buffer_size);
void SORT_TYPE_CPY(node *dst, node *src, const int size);

#endif