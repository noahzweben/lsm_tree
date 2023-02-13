#include "helpers.h"
#include <stdio.h>

void set_filename(char *filename, int level)
{
    // create a dynamic string and place in malloc
    sprintf(filename, "level%d.txt", level);
}