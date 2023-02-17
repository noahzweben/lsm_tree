#ifndef HELPERS_H
#define HELPERS_H
#include "lsm.h"

void set_filename(char *filename);
void print_tree(char *msg, lsmtree *lsm);

void merge_sort(node *buffer, int buffer_size);
void merge_sort_recursive(node *new_buffer, node *buffer, int buffer_size);
void SORT_TYPE_CPY(node *dst, node *src, const int size);
#endif