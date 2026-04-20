#include "shim_array.h"

#include "shim_convert.h"
#include "shim_util.h"

#include "log/log.h"
#include "multiarray/multiarray.h"
#include "zarr/store.h"

#include <stdlib.h>
#include <string.h>

int
shim_configure_multiscale_array(struct ZarrStream_s* stream,
                                const ZarrArraySettings* as,
                                struct shim_array* sa)
{
    sa->rank = (uint8_t)as->dimension_count;
    sa->dims = shim_convert_dimensions(
      as->dimensions, as->dimension_count, as->storage_dimension_order, true);
    if (!sa->dims) {
        return 0;
    }

    sa->axes = shim_convert_ngff_axes(as->dimensions, as->dimension_count);
    if (!sa->axes) {
        return 0;
    }

    enum dtype dt = shim_convert_dtype(as->data_type);
    struct codec_config codec = shim_convert_codec(as->compression_settings);

    size_t ndims = as->dimension_count;
    sa->frame_bytes = dtype_bpe(dt) * as->dimensions[ndims - 2].array_size_px *
                      as->dimensions[ndims - 1].array_size_px;

    struct ngff_multiscale_config ms_cfg = {
        .data_type = dt,
        .fill_value = 0.0,
        .rank = sa->rank,
        .dimensions = sa->dims,
        .nlod = 0,
        .codec = codec,
        .axes = sa->axes,
    };
    sa->sink.kind = SHIM_SINK_MULTISCALE;
    sa->sink.multiscale =
      ngff_multiscale_create(stream->store, sa->key, &ms_cfg);
    if (!sa->sink.multiscale) {
        return 0;
    }

    sa->config = (struct tile_stream_configuration){
        .buffer_capacity_bytes = sa->frame_bytes,
        .dtype = dt,
        .rank = sa->rank,
        .dimensions = sa->dims,
        .codec = codec,
        .reduce_method = shim_convert_reduce_method(as->downsampling_method),
        .append_reduce_method =
          shim_convert_reduce_method(as->downsampling_method),
        .epochs_per_batch = 0,
        .target_batch_chunks = 0,
        .metadata_update_interval_s = 1.0f,
        .max_threads = stream->max_threads,
    };

    return 1;
}

int
shim_create_flat_array(struct ZarrStream_s* stream,
                       const ZarrArraySettings* as,
                       struct shim_array* sa)
{
    if (as->output_key) {
        sa->key = strdup(as->output_key);
        if (!sa->key) {
            return 0;
        }
    }

    if (as->multiscale) {
        return shim_configure_multiscale_array(stream, as, sa);
    }

    sa->rank = (uint8_t)as->dimension_count;
    sa->dims = shim_convert_dimensions(
      as->dimensions, as->dimension_count, as->storage_dimension_order, false);
    if (!sa->dims) {
        return 0;
    }

    enum dtype dt = shim_convert_dtype(as->data_type);
    struct codec_config codec = shim_convert_codec(as->compression_settings);

    size_t ndims = as->dimension_count;
    sa->frame_bytes = dtype_bpe(dt) * as->dimensions[ndims - 2].array_size_px *
                      as->dimensions[ndims - 1].array_size_px;

    struct zarr_array_config arr_cfg = {
        .data_type = dt,
        .fill_value = 0.0,
        .rank = sa->rank,
        .dimensions = sa->dims,
        .codec = codec,
    };

    // Write intermediate group zarr.json for each path component and ensure
    // the leaf directory exists for zarr_array_create.
    if (shim_write_intermediate_groups(stream->store, sa->key) != 0) {
        return 0;
    }
    if (sa->key && stream->store->mkdirs(stream->store, sa->key) != 0) {
        log_error("mkdirs failed for array directory '%s'", sa->key);
        return 0;
    }

    sa->sink.kind = SHIM_SINK_ARRAY;
    sa->sink.array = zarr_array_create(stream->store, sa->key, &arr_cfg);
    if (!sa->sink.array) {
        return 0;
    }

    sa->config = (struct tile_stream_configuration){
        .buffer_capacity_bytes = sa->frame_bytes,
        .dtype = dt,
        .rank = sa->rank,
        .dimensions = sa->dims,
        .codec = codec,
        .reduce_method = shim_convert_reduce_method(as->downsampling_method),
        .append_reduce_method =
          shim_convert_reduce_method(as->downsampling_method),
        .epochs_per_batch = 0,
        .target_batch_chunks = 0,
        .metadata_update_interval_s = 1.0f,
        .max_threads = stream->max_threads,
    };

    return 1;
}

void
shim_array_destroy(struct shim_array* a)
{
    if (!a) {
        return;
    }
    shim_sink_flush(&a->sink);
    shim_sink_destroy(&a->sink);
    free(a->dims);
    a->dims = NULL;
    free(a->axes);
    a->axes = NULL;
    free(a->key);
    a->key = NULL;
}
