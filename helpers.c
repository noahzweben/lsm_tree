#include "helpers.h"
#include <stdio.h>

void set_filename(char *filename, int level)
{
    // set filename to "level0.bin", "level1.bin", etc.
    sprintf(filename, "level%d.txt", level);
}