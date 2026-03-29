#include "shim_sink.h"

struct shard_sink*
shim_sink_as_shard_sink(struct shim_sink* s)
{
    switch (s->kind) {
        case SHIM_SINK_FS_MULTISCALE:
            return zarr_fs_multiscale_sink_as_shard_sink(s->fs_ms);
    }
    return NULL;
}

void
shim_sink_flush(struct shim_sink* s)
{
    switch (s->kind) {
        case SHIM_SINK_FS_MULTISCALE:
            zarr_fs_multiscale_sink_flush(s->fs_ms);
            break;
    }
}

void
shim_sink_destroy(struct shim_sink* s)
{
    switch (s->kind) {
        case SHIM_SINK_FS_MULTISCALE:
            zarr_fs_multiscale_sink_destroy(s->fs_ms);
            s->fs_ms = NULL;
            break;
    }
}
