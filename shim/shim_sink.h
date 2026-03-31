#pragma once

#include "zarr_fs_sink.h"

struct shard_sink;

enum shim_sink_kind
{
    SHIM_SINK_FS,
    SHIM_SINK_FS_MULTISCALE,
};

struct shim_sink
{
    enum shim_sink_kind kind;
    union
    {
        struct zarr_fs_sink* fs;
        struct zarr_fs_multiscale_sink* fs_ms;
    };
};

struct shard_sink*
shim_sink_as_shard_sink(struct shim_sink* s);

void
shim_sink_flush(struct shim_sink* s);

void
shim_sink_destroy(struct shim_sink* s);
