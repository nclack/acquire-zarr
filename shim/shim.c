#include "shim_internal.h"
#include "shim_convert.h"
#include "shim_log.h"
#include "shim_util.h"
#include "log/log.h"
#include "multiarray/multiarray.h"
#include "writer.h"
#include "zarr/store.h"
#include "zarr/store_fs.h"
#include "zarr/zarr_group.h"
#include "hcs.h"
#include "zarr/json_writer.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

// Forward declarations for HCS metadata helpers
static int
find_row_index(const ZarrHCSPlate* plate, const char* name);
static int
find_col_index(const ZarrHCSPlate* plate, const char* name);
static int
shim_hcs_plate_attributes_json(char* buf,
                               size_t cap,
                               const ZarrHCSPlate* plate);
static int
shim_hcs_well_attributes_json(char* buf, size_t cap, const ZarrHCSWell* well);

// Configure `sa` as a multiscale array: builds dims/axes, creates the
// ngff_multiscale sink under `sa->key`, and fills the tile_stream config.
// `sa->key` must be set by the caller (NULL == root). Returns 1 on success,
// 0 on failure; partial state is cleaned up by the caller via
// shim_array_destroy.
static int
configure_multiscale_array(struct ZarrStream_s* stream,
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

static int
create_flat_array(struct ZarrStream_s* stream,
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
        return configure_multiscale_array(stream, as, sa);
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

// Find the row index for a name in the plate's row_names array
static int
find_row_index(const ZarrHCSPlate* plate, const char* name)
{
    for (size_t i = 0; i < plate->row_count; ++i) {
        if (plate->row_names[i] && strcmp(plate->row_names[i], name) == 0) {
            return (int)i;
        }
    }
    return -1;
}

// Find the column index for a name in the plate's column_names array
static int
find_col_index(const ZarrHCSPlate* plate, const char* name)
{
    for (size_t i = 0; i < plate->column_count; ++i) {
        if (plate->column_names[i] &&
            strcmp(plate->column_names[i], name) == 0) {
            return (int)i;
        }
    }
    return -1;
}

static int
create_hcs_arrays(struct ZarrStream_s* stream,
                  const ZarrStreamSettings* settings,
                  size_t* array_idx)
{
    const ZarrHCSSettings* hcs = settings->hcs_settings;

    stream->n_plates = hcs->plate_count;
    stream->plates = calloc(hcs->plate_count, sizeof(struct hcs_plate*));
    if (!stream->plates) {
        return 0;
    }

    for (size_t p = 0; p < hcs->plate_count; ++p) {
        const ZarrHCSPlate* zplate = &hcs->plates[p];

        // Build per-well/per-FOV config for chucky
        // We need to build one hcs_plate_config per plate
        // The chucky HCS takes row/col counts, a well_mask, field_count,
        // and a single fov config. But our new API needs per-well/per-FOV
        // heterogeneity. Since the current chucky API is uniform, we need
        // to create the hierarchy ourselves.

        // Write root group (if not already written)
        zarr_group_write_with_raw_attrs(stream->store, "zarr.json", "{}");

        // Write plate group with attributes
        const char* plate_path = zplate->path ? zplate->path : "plate";
        stream->store->mkdirs(stream->store, plate_path);

        // Build plate attributes JSON
        {
            size_t attr_cap = 2048 + zplate->well_count * 128 +
                              zplate->acquisition_count * 256 +
                              zplate->row_count * 32 +
                              zplate->column_count * 32;
            char* attrs = malloc(attr_cap);
            if (!attrs) {
                return 0;
            }

            int alen = shim_hcs_plate_attributes_json(attrs, attr_cap, zplate);
            if (alen < 0) {
                free(attrs);
                return 0;
            }

            char* key = shim_alloc_printf("%s/zarr.json", plate_path);
            if (!key) {
                free(attrs);
                return 0;
            }
            int rc = zarr_group_write_with_raw_attrs(stream->store, key, attrs);
            free(key);
            free(attrs);
            if (rc != 0) {
                return 0;
            }
        }

        // Write row groups, well groups, and create FOV multiscale sinks
        for (size_t w = 0; w < zplate->well_count; ++w) {
            const ZarrHCSWell* well = &zplate->wells[w];
            const char* row_name = well->row_name;
            const char* col_name = well->column_name;

            // Row group
            char* row_dir = shim_alloc_printf("%s/%s", plate_path, row_name);
            if (!row_dir) {
                return 0;
            }
            stream->store->mkdirs(stream->store, row_dir);
            {
                char* key = shim_alloc_printf("%s/zarr.json", row_dir);
                if (!key) {
                    free(row_dir);
                    return 0;
                }
                zarr_group_write_with_raw_attrs(stream->store, key, "{}");
                free(key);
            }
            free(row_dir);

            // Well group with attributes
            char* well_dir =
              shim_alloc_printf("%s/%s/%s", plate_path, row_name, col_name);
            if (!well_dir) {
                return 0;
            }
            stream->store->mkdirs(stream->store, well_dir);
            {
                // Generous cap scaled to image count so writers with many
                // FOVs per well don't overflow silently. Each image
                // contributes ~64 bytes of JSON in the worst case.
                size_t attrs_cap = 512 + well->image_count * 96;
                char* attrs = malloc(attrs_cap);
                if (!attrs) {
                    free(well_dir);
                    return 0;
                }
                int alen =
                  shim_hcs_well_attributes_json(attrs, attrs_cap, well);
                if (alen < 0) {
                    free(attrs);
                    free(well_dir);
                    return 0;
                }
                char* key = shim_alloc_printf("%s/zarr.json", well_dir);
                if (!key) {
                    free(attrs);
                    free(well_dir);
                    return 0;
                }
                int rc =
                  zarr_group_write_with_raw_attrs(stream->store, key, attrs);
                free(key);
                free(attrs);
                if (rc != 0) {
                    free(well_dir);
                    return 0;
                }
            }
            free(well_dir);

            // Create FOV multiscale sinks
            for (size_t f = 0; f < well->image_count; ++f) {
                const ZarrHCSFieldOfView* fov = &well->images[f];
                const ZarrArraySettings* as = fov->array_settings;
                struct shim_array* sa = &stream->arrays[*array_idx];

                const char* fov_path = fov->path ? fov->path : "0";
                sa->key = shim_alloc_printf(
                  "%s/%s/%s/%s", plate_path, row_name, col_name, fov_path);
                if (!sa->key) {
                    return 0;
                }

                if (!configure_multiscale_array(stream, as, sa)) {
                    return 0;
                }

                ++(*array_idx);
            }
        }

        // We don't use chucky's hcs_plate_create — we build the hierarchy
        // ourselves. Set plates[p] = NULL to indicate no cleanup needed.
        stream->plates[p] = NULL;
    }

    return 1;
}

/* --- HCS metadata JSON helpers ------------------------------------------ */

static int
shim_hcs_plate_attributes_json(char* buf, size_t cap, const ZarrHCSPlate* plate)
{
    struct json_writer jw;
    jw_init(&jw, buf, cap);

    jw_object_begin(&jw); // attributes root

    jw_key(&jw, "ome");
    jw_object_begin(&jw);
    jw_key(&jw, "version");
    jw_string(&jw, "0.5");

    jw_key(&jw, "plate");
    jw_object_begin(&jw);
    jw_key(&jw, "name");
    jw_string(&jw, plate->name ? plate->name : "plate");

    jw_key(&jw, "version");
    jw_string(&jw, "0.5");

    // field_count = max FOV count across all wells
    int field_count = 0;
    for (size_t w = 0; w < plate->well_count; ++w) {
        int n = (int)plate->wells[w].image_count;
        if (n > field_count) {
            field_count = n;
        }
    }
    jw_key(&jw, "field_count");
    jw_int(&jw, field_count);

    // acquisitions
    jw_key(&jw, "acquisitions");
    jw_array_begin(&jw);
    if (plate->acquisition_count > 0) {
        for (size_t a = 0; a < plate->acquisition_count; ++a) {
            const ZarrHCSAcquisition* acq = &plate->acquisitions[a];

            // Compute maximumfieldcount for this acquisition:
            // count how many FOVs reference this acquisition across all wells
            int max_fov_count = 0;
            for (size_t w = 0; w < plate->well_count; ++w) {
                const ZarrHCSWell* well = &plate->wells[w];
                int count = 0;
                for (size_t f = 0; f < well->image_count; ++f) {
                    if (well->images[f].has_acquisition_id &&
                        well->images[f].acquisition_id == acq->id) {
                        ++count;
                    }
                }
                if (count > max_fov_count) {
                    max_fov_count = count;
                }
            }

            jw_object_begin(&jw);
            jw_key(&jw, "id");
            jw_int(&jw, (int64_t)acq->id);
            jw_key(&jw, "maximumfieldcount");
            jw_int(&jw, max_fov_count);
            if (acq->name) {
                jw_key(&jw, "name");
                jw_string(&jw, acq->name);
            }
            if (acq->has_start_time) {
                jw_key(&jw, "starttime");
                jw_uint(&jw, acq->start_time);
            }
            if (acq->has_end_time) {
                jw_key(&jw, "endtime");
                jw_uint(&jw, acq->end_time);
            }
            jw_object_end(&jw);
        }
    } else {
        // Single default acquisition
        jw_object_begin(&jw);
        jw_key(&jw, "id");
        jw_int(&jw, 0);
        jw_object_end(&jw);
    }
    jw_array_end(&jw);

    // columns
    jw_key(&jw, "columns");
    jw_array_begin(&jw);
    for (size_t c = 0; c < plate->column_count; ++c) {
        jw_object_begin(&jw);
        jw_key(&jw, "name");
        jw_string(&jw, plate->column_names[c]);
        jw_object_end(&jw);
    }
    jw_array_end(&jw);

    // rows
    jw_key(&jw, "rows");
    jw_array_begin(&jw);
    for (size_t r = 0; r < plate->row_count; ++r) {
        jw_object_begin(&jw);
        jw_key(&jw, "name");
        jw_string(&jw, plate->row_names[r]);
        jw_object_end(&jw);
    }
    jw_array_end(&jw);

    // wells
    jw_key(&jw, "wells");
    jw_array_begin(&jw);
    for (size_t w = 0; w < plate->well_count; ++w) {
        const ZarrHCSWell* well = &plate->wells[w];
        int row_idx = find_row_index(plate, well->row_name);
        int col_idx = find_col_index(plate, well->column_name);

        jw_object_begin(&jw);
        jw_key(&jw, "path");
        char* path = shim_alloc_printf("%s/%s", well->row_name, well->column_name);
        if (!path) {
            return -1;
        }
        jw_string(&jw, path);
        free(path);
        jw_key(&jw, "rowIndex");
        jw_int(&jw, row_idx);
        jw_key(&jw, "columnIndex");
        jw_int(&jw, col_idx);
        jw_object_end(&jw);
    }
    jw_array_end(&jw);

    jw_object_end(&jw); // plate
    jw_object_end(&jw); // ome
    jw_object_end(&jw); // attributes root

    if (jw_error(&jw)) {
        return -1;
    }
    return (int)jw_length(&jw);
}

static int
shim_hcs_well_attributes_json(char* buf, size_t cap, const ZarrHCSWell* well)
{
    struct json_writer jw;
    jw_init(&jw, buf, cap);

    jw_object_begin(&jw); // attributes root

    jw_key(&jw, "ome");
    jw_object_begin(&jw);
    jw_key(&jw, "version");
    jw_string(&jw, "0.5");

    jw_key(&jw, "well");
    jw_object_begin(&jw);

    jw_key(&jw, "version");
    jw_string(&jw, "0.5");

    jw_key(&jw, "images");
    jw_array_begin(&jw);
    for (size_t f = 0; f < well->image_count; ++f) {
        const ZarrHCSFieldOfView* fov = &well->images[f];
        jw_object_begin(&jw);
        jw_key(&jw, "acquisition");
        if (fov->has_acquisition_id) {
            jw_int(&jw, (int64_t)fov->acquisition_id);
        } else {
            jw_int(&jw, 0);
        }
        jw_key(&jw, "path");
        jw_string(&jw, fov->path ? fov->path : "0");
        jw_object_end(&jw);
    }
    jw_array_end(&jw);

    jw_object_end(&jw); // well
    jw_object_end(&jw); // ome
    jw_object_end(&jw); // attributes root

    if (jw_error(&jw)) {
        return -1;
    }
    return (int)jw_length(&jw);
}

/* --- Stream lifecycle ---------------------------------------------------- */

static void
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

ZarrStream*
ZarrStream_create(ZarrStreamSettings* settings)
{
    if (!settings || !settings->store_path) {
        return NULL;
    }
    // Need at least flat arrays or HCS settings
    if (!settings->arrays && !settings->hcs_settings) {
        return NULL;
    }

    // Make sure chucky's log threshold matches the requested level even if
    // the caller never called Zarr_set_log_level.
    shim_apply_log_level();

    ZarrStream* stream = calloc(1, sizeof(ZarrStream));
    if (!stream) {
        return NULL;
    }

    stream->store_path = strdup(settings->store_path);
    if (!stream->store_path) {
        free(stream);
        return NULL;
    }

    stream->max_threads = (int)settings->max_threads;

    // Upper-bound memory estimate (same formula as the pre-create estimator;
    // no runtime tracking — allocations happen once at create and don't grow).
    {
        size_t usage = 0;
        (void)ZarrStreamSettings_estimate_max_memory_usage(settings, &usage);
        stream->estimated_memory = usage;
    }

    // Create store
    if (settings->s3_settings) {
        struct store_s3_config s3cfg = {
            .bucket = settings->s3_settings->bucket_name,
            .prefix = settings->store_path,
            .region = settings->s3_settings->region
                        ? settings->s3_settings->region
                        : "us-east-1",
            .endpoint = settings->s3_settings->endpoint,
        };
        store_s3_config_set_defaults(&s3cfg);
        stream->store = store_s3_create(&s3cfg);
    } else {
        stream->store = store_fs_create(settings->store_path, 0);
    }
    if (!stream->store) {
        goto fail;
    }

    // Refuse to overwrite existing data unless the caller opted in. The
    // public API has no error-code return for create, so the caller only
    // sees NULL — the log line documents why.
    // `store_has_existing_data` returns 1=exists, 0=absent, -1=error.
    // A transient HEAD failure shouldn't masquerade as "exists".
    if (!settings->overwrite) {
        int existing = store_has_existing_data(stream->store);
        if (existing > 0) {
            log_error("refusing to overwrite existing data at %s "
                      "(set settings.overwrite=true to replace)",
                      settings->store_path);
            goto fail;
        }
        if (existing < 0) {
            log_error("could not check for existing data at %s "
                      "(store HEAD failed); aborting stream create",
                      settings->store_path);
            goto fail;
        }
    }

    stream->store->mkdirs(stream->store, ".");

    // Count total arrays
    size_t total_arrays = ZarrStreamSettings_get_array_count(settings);
    stream->n_arrays = total_arrays;
    stream->arrays = calloc(total_arrays, sizeof(struct shim_array));
    if (!stream->arrays && total_arrays > 0) {
        goto fail;
    }

    // Write root group
    zarr_group_write_with_raw_attrs(stream->store, "zarr.json", "{}");

    // Create flat arrays
    for (size_t i = 0; i < settings->array_count; ++i) {
        if (!create_flat_array(
              stream, &settings->arrays[i], &stream->arrays[i])) {
            goto fail;
        }
    }

    // Create HCS arrays
    if (settings->hcs_settings) {
        size_t array_idx = settings->array_count;
        if (!create_hcs_arrays(stream, settings, &array_idx)) {
            goto fail;
        }
    }

    // Build configs[] and sinks[] for the multiarray stream.
    if (stream->n_arrays > 0) {
        struct tile_stream_configuration* configs =
          calloc(stream->n_arrays, sizeof(struct tile_stream_configuration));
        struct shard_sink** sinks =
          calloc(stream->n_arrays, sizeof(struct shard_sink*));
        if (!configs || !sinks) {
            free(configs);
            free(sinks);
            goto fail;
        }

        for (size_t i = 0; i < stream->n_arrays; ++i) {
            configs[i] = stream->arrays[i].config;
            sinks[i] = shim_sink_as_shard_sink(&stream->arrays[i].sink);
            if (!sinks[i]) {
                free(configs);
                free(sinks);
                goto fail;
            }
        }

        stream->multi_stream = multiarray_tile_stream_create(
          (int)stream->n_arrays, configs, sinks, 0);
        free(configs);
        free(sinks);
        if (!stream->multi_stream) {
            goto fail;
        }
        stream->writer = multiarray_tile_stream_writer(stream->multi_stream);
        if (!stream->writer) {
            goto fail;
        }
    }

    return stream;

fail:
    ZarrStream_destroy(stream);
    return NULL;
}

void
ZarrStream_destroy(ZarrStream* stream)
{
    if (!stream) {
        return;
    }
    if (stream->writer) {
        stream->writer->flush(stream->writer);
        stream->writer = NULL;
    }
    if (stream->multi_stream) {
        multiarray_tile_stream_destroy(stream->multi_stream);
        stream->multi_stream = NULL;
    }
    if (stream->arrays) {
        for (size_t i = 0; i < stream->n_arrays; ++i) {
            shim_array_destroy(&stream->arrays[i]);
        }
        free(stream->arrays);
    }
    if (stream->plates) {
        for (size_t i = 0; i < stream->n_plates; ++i) {
            if (stream->plates[i]) {
                hcs_plate_destroy(stream->plates[i]);
            }
        }
        free(stream->plates);
    }
    if (stream->store) {
        stream->store->destroy(stream->store);
    }
    free(stream->store_path);
    free(stream);
}

ZarrStatusCode
ZarrStream_append(ZarrStream* stream,
                  const void* data,
                  size_t bytes_in,
                  size_t* bytes_out,
                  const char* key)
{
    if (!stream || !bytes_out) {
        return ZarrStatusCode_InvalidArgument;
    }

    *bytes_out = 0;

    if (bytes_in == 0) {
        return ZarrStatusCode_Success;
    }

    if (!stream->writer) {
        return ZarrStatusCode_InternalError;
    }

    // Find the target array index
    int array_index = -1;
    if (!key && stream->n_arrays == 1) {
        array_index = 0;
    } else if (key) {
        for (size_t i = 0; i < stream->n_arrays; ++i) {
            if (stream->arrays[i].key &&
                strcmp(stream->arrays[i].key, key) == 0) {
                array_index = (int)i;
                break;
            }
        }
        // If key didn't match any named array and there's exactly one with
        // no key, use that
        if (array_index < 0 && stream->n_arrays == 1 &&
            !stream->arrays[0].key) {
            array_index = 0;
        }
    }

    if (array_index < 0) {
        return ZarrStatusCode_InvalidArgument;
    }

    // NULL data means "write zeros" — allocate a zeroed frame
    const void* frame = data;
    void* zeros = NULL;
    if (!data) {
        zeros = calloc(1, bytes_in);
        if (!zeros) {
            return ZarrStatusCode_OutOfMemory;
        }
        frame = zeros;
    }

    const char* cur = (const char*)frame;
    const char* end = cur + bytes_in;
    ZarrStatusCode rc = ZarrStatusCode_Success;

    while (cur < end) {
        struct slice s = { .beg = cur, .end = end };
        struct multiarray_writer_result r =
          stream->writer->update(stream->writer, array_index, s);

        const char* rest_beg = (const char*)r.rest.beg;
        const char* next = rest_beg ? rest_beg : end;

        if (r.error == multiarray_writer_finished) {
            // Chucky returns `finished` both for natural completion and for
            // post-capacity appends (as a silent no-op). Distinguish: if the
            // writer failed to consume the full input, the caller tried to
            // write past the array's capacity.
            cur = next;
            if (cur < end) {
                rc = ZarrStatusCode_WriteOutOfBounds;
            }
            break;
        }
        if (r.error == multiarray_writer_not_flushable) {
            // The caller tried to switch to this array while the previously
            // active array is mid-epoch. That's a programming error — the
            // multi-array writer shares chunk pools across arrays and can
            // only switch at epoch boundaries. Report it distinctly so the
            // caller can diagnose.
            const char* k = key ? key : "(no key)";
            log_error("ZarrStream_append: cannot switch to array '%s' "
                      "mid-epoch of a different array; finish the current "
                      "epoch first",
                      k);
            rc = ZarrStatusCode_InvalidArgument;
            break;
        }
        if (r.error != multiarray_writer_ok) {
            log_error("ZarrStream_append: writer error %d", r.error);
            rc = ZarrStatusCode_InternalError;
            break;
        }
        if (next <= cur) {
            // Writer reported ok without advancing — guard against a spin.
            rc = ZarrStatusCode_InternalError;
            break;
        }
        cur = next;
    }

    size_t consumed = (size_t)(cur - (const char*)frame);
    free(zeros);

    *bytes_out = consumed;
    return rc;
}

ZarrStatusCode
ZarrStream_write_custom_metadata(ZarrStream* stream,
                                 const char* array_key,
                                 const char* metadata_key,
                                 const char* metadata)
{
    if (!stream || !metadata_key || !metadata) {
        return ZarrStatusCode_InvalidArgument;
    }

    // Find the target array by key. NULL array_key selects the single
    // root-level array (only valid when there is exactly one flat array
    // without an output_key).
    struct shim_array* target = NULL;
    for (size_t i = 0; i < stream->n_arrays; ++i) {
        struct shim_array* sa = &stream->arrays[i];
        if (array_key == NULL) {
            if (sa->key == NULL) {
                target = sa;
                break;
            }
        } else if (sa->key && strcmp(sa->key, array_key) == 0) {
            target = sa;
            break;
        }
    }

    if (!target) {
        return ZarrStatusCode_KeyNotFound;
    }

    int rc;
    switch (target->sink.kind) {
        case SHIM_SINK_ARRAY:
            rc = zarr_array_set_attribute(
              target->sink.array, metadata_key, metadata);
            break;
        case SHIM_SINK_MULTISCALE:
            rc = ngff_multiscale_set_attribute(
              target->sink.multiscale, metadata_key, metadata);
            break;
        default:
            return ZarrStatusCode_InternalError;
    }

    return rc == 0 ? ZarrStatusCode_Success : ZarrStatusCode_InternalError;
}

ZarrStatusCode
ZarrStream_get_current_memory_usage(const ZarrStream* stream, size_t* usage)
{
    if (!stream || !usage) {
        return ZarrStatusCode_InvalidArgument;
    }
    *usage = stream->estimated_memory;
    return ZarrStatusCode_Success;
}
