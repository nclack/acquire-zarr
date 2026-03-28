#pragma once

#include "acquire.zarr.h"

struct ZarrStream_s
{
    char* store_path;
    size_t estimated_memory;
    int has_custom_metadata;
};
