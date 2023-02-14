#include "helpers.h"
#include <stdio.h>
#include <uuid/uuid.h>

void set_filename(char *filename)
{
    uuid_t uuid;
    char uuid_str[37];
    uuid_generate_time(uuid);
    uuid_unparse_lower(uuid, uuid_str);
    sprintf(filename, "%s.bin", uuid_str);
}