#pragma once

#include "acquire.zarr.h"
#include "shim_sink.h"

struct tile_stream_cpu;

struct shim_array
{
    char* key;
    struct dimension* dims;
    uint8_t rank;
    struct tile_stream_cpu* stream;
    struct shim_sink sink;
    size_t frame_bytes;
};

struct ZarrStream_s
{
    struct shim_array* arrays;
    size_t n_arrays;
    char* store_path;
    size_t estimated_memory;
    int has_custom_metadata;
};
