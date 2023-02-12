#include "helpers.h"
#include <stdio.h>

void set_filename(char *filename, int level)
{

    snprintf(filename, sizeof(char) * 64, "level%i.txt", level);
}