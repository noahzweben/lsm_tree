#include "helpers.h"
#include <stdio.h>
#include <uuid/uuid.h>

void set_filename(char *filename)
{
    uuid_t uuid;
    uuid_generate_time(uuid);
    uuid_unparse_lower(uuid, filename);
    sprintf(filename, "%s.bin", filename);
}