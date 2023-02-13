#include "helpers.h"
#include <stdio.h>

void set_filename(char *filename, int level)
{
    sprintf(filename, "level%i.txt", level);
}