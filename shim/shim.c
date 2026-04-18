#include "shim_internal.h"
#include "shim_convert.h"
#include "log/log.h"
#include "multiarray/multiarray.h"
#include "writer.h"
#include "zarr/store.h"
#include "zarr/store_fs.h"
#include "zarr/zarr_group.h"
#include "hcs.h"
#include "zarr/json_writer.h"
#include "chucky_log.h"

#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#ifndef ACQUIRE_ZARR_API_VERSION
#define ACQUIRE_ZARR_API_VERSION "0.6.0"
#endif

static ZarrLogLevel current_log_level = ZarrLogLevel_Info;

// Ensure chucky's log level matches our stored level. Called from the
// public setter and at stream create time so that the default applies
// even when the user never calls Zarr_set_log_level.
static void
apply_log_level(void);

// Write intermediate group zarr.json for each path component of key.
// For key "a/b/c", writes groups at "a/zarr.json" and "a/b/zarr.json".
static void
write_intermediate_groups(struct store* store, const char* key);

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
shim_hcs_well_attributes_json(char* buf,
                              size_t cap,
                              const ZarrHCSWell* well);

/* --- Version / status / logging ----------------------------------------- */

const char*
Zarr_get_api_version(void)
{
    return ACQUIRE_ZARR_API_VERSION;
}

// Forward current_log_level to chucky's log dispatcher. Default chucky level
// is CHUCKY_LOG_TRACE (0), so without this chucky emits everything to stderr
// regardless of the acquire-zarr log level.
static void
apply_log_level(void)
{
    switch (current_log_level) {
        case ZarrLogLevel_Debug:
            chucky_log_set_quiet(0);
            chucky_log_set_level(CHUCKY_LOG_DEBUG);
            break;
        case ZarrLogLevel_Info:
            chucky_log_set_quiet(0);
            chucky_log_set_level(CHUCKY_LOG_INFO);
            break;
        case ZarrLogLevel_Warning:
            chucky_log_set_quiet(0);
            chucky_log_set_level(CHUCKY_LOG_WARN);
            break;
        case ZarrLogLevel_Error:
            chucky_log_set_quiet(0);
            chucky_log_set_level(CHUCKY_LOG_ERROR);
            break;
        case ZarrLogLevel_None:
        default:
            chucky_log_set_quiet(1);
            break;
    }
}

ZarrStatusCode
Zarr_set_log_level(ZarrLogLevel level)
{
    if (level < 0 || level >= ZarrLogLevelCount) {
        return ZarrStatusCode_InvalidArgument;
    }
    current_log_level = level;
    apply_log_level();
    return ZarrStatusCode_Success;
}

ZarrLogLevel
Zarr_get_log_level(void)
{
    return current_log_level;
}

const char*
Zarr_get_status_message(ZarrStatusCode code)
{
    switch (code) {
        case ZarrStatusCode_Success:
            return "Success";
        case ZarrStatusCode_InvalidArgument:
            return "Invalid argument";
        case ZarrStatusCode_Overflow:
            return "Buffer overflow";
        case ZarrStatusCode_InvalidIndex:
            return "Invalid index";
        case ZarrStatusCode_NotYetImplemented:
            return "Not yet implemented";
        case ZarrStatusCode_InternalError:
            return "Internal error";
        case ZarrStatusCode_OutOfMemory:
            return "Out of memory";
        case ZarrStatusCode_IOError:
            return "I/O error";
        case ZarrStatusCode_CompressionError:
            return "Error compressing";
        case ZarrStatusCode_InvalidSettings:
            return "Invalid settings";
        case ZarrStatusCode_WillNotOverwrite:
            return "Refusing to overwrite existing data";
        case ZarrStatusCode_PartialWrite:
            return "Data partially written";
        case ZarrStatusCode_WriteOutOfBounds:
            return "Attempted write beyond array boundary";
        case ZarrStatusCode_KeyNotFound:
            return "Array key not found";
        default:
            return "Unknown error";
    }
}

/* --- Allocator helpers -------------------------------------------------- */

ZarrStatusCode
ZarrStreamSettings_create_arrays(ZarrStreamSettings* settings,
                                 size_t array_count)
{
    if (!settings) {
        return ZarrStatusCode_InvalidArgument;
    }

    ZarrArraySettings* arrays = calloc(array_count, sizeof(ZarrArraySettings));
    if (!arrays) {
        return ZarrStatusCode_OutOfMemory;
    }

    ZarrStreamSettings_destroy_arrays(settings);
    settings->arrays = arrays;
    settings->array_count = array_count;

    return ZarrStatusCode_Success;
}

void
ZarrStreamSettings_destroy_arrays(ZarrStreamSettings* settings)
{
    if (!settings) {
        return;
    }
    if (!settings->arrays) {
        settings->array_count = 0;
        return;
    }
    for (size_t i = 0; i < settings->array_count; ++i) {
        ZarrArraySettings_destroy_dimension_array(&settings->arrays[i]);
    }
    free(settings->arrays);
    settings->arrays = NULL;
    settings->array_count = 0;
}

ZarrStatusCode
ZarrArraySettings_create_dimension_array(ZarrArraySettings* settings,
                                         size_t dimension_count)
{
    if (!settings) {
        return ZarrStatusCode_InvalidArgument;
    }
    if (dimension_count < 2) {
        return ZarrStatusCode_InvalidArgument;
    }

    ZarrDimensionProperties* dims =
      calloc(dimension_count, sizeof(ZarrDimensionProperties));
    if (!dims) {
        return ZarrStatusCode_OutOfMemory;
    }

    ZarrArraySettings_destroy_dimension_array(settings);
    settings->dimensions = dims;
    settings->dimension_count = dimension_count;

    return ZarrStatusCode_Success;
}

void
ZarrArraySettings_destroy_dimension_array(ZarrArraySettings* settings)
{
    if (!settings) {
        return;
    }
    free(settings->dimensions);
    settings->dimensions = NULL;
    settings->dimension_count = 0;
}

ZarrStatusCode
ZarrHCSWell_create_image_array(ZarrHCSWell* well, size_t image_count)
{
    if (!well) {
        return ZarrStatusCode_InvalidArgument;
    }
    if (image_count == 0) {
        return ZarrStatusCode_InvalidArgument;
    }

    ZarrHCSFieldOfView* images =
      calloc(image_count, sizeof(ZarrHCSFieldOfView));
    if (!images) {
        return ZarrStatusCode_OutOfMemory;
    }

    ZarrHCSWell_destroy_image_array(well);
    well->images = images;
    well->image_count = image_count;

    return ZarrStatusCode_Success;
}

void
ZarrHCSWell_destroy_image_array(ZarrHCSWell* well)
{
    if (!well) {
        return;
    }
    if (well->images) {
        for (size_t i = 0; i < well->image_count; ++i) {
            if (well->images[i].array_settings) {
                ZarrArraySettings_destroy_dimension_array(
                  well->images[i].array_settings);
                well->images[i].array_settings = NULL;
            }
        }
        free(well->images);
        well->images = NULL;
    }
    well->image_count = 0;
}

ZarrStatusCode
ZarrHCSPlate_create_well_array(ZarrHCSPlate* plate, size_t well_count)
{
    if (!plate) {
        return ZarrStatusCode_InvalidArgument;
    }
    if (well_count == 0) {
        return ZarrStatusCode_InvalidArgument;
    }

    ZarrHCSWell* wells = calloc(well_count, sizeof(ZarrHCSWell));
    if (!wells) {
        return ZarrStatusCode_OutOfMemory;
    }

    ZarrHCSPlate_destroy_well_array(plate);
    plate->wells = wells;
    plate->well_count = well_count;

    return ZarrStatusCode_Success;
}

void
ZarrHCSPlate_destroy_well_array(ZarrHCSPlate* plate)
{
    if (!plate) {
        return;
    }
    if (plate->wells) {
        for (size_t i = 0; i < plate->well_count; ++i) {
            ZarrHCSWell_destroy_image_array(&plate->wells[i]);
        }
        free(plate->wells);
        plate->wells = NULL;
    }
    plate->well_count = 0;
}

ZarrStatusCode
ZarrHCSPlate_create_acquisition_array(ZarrHCSPlate* plate,
                                      size_t acquisition_count)
{
    if (!plate) {
        return ZarrStatusCode_InvalidArgument;
    }
    if (acquisition_count == 0) {
        return ZarrStatusCode_InvalidArgument;
    }

    ZarrHCSAcquisition* acqs =
      calloc(acquisition_count, sizeof(ZarrHCSAcquisition));
    if (!acqs) {
        return ZarrStatusCode_OutOfMemory;
    }

    ZarrHCSPlate_destroy_acquisition_array(plate);
    plate->acquisitions = acqs;
    plate->acquisition_count = acquisition_count;

    return ZarrStatusCode_Success;
}

void
ZarrHCSPlate_destroy_acquisition_array(ZarrHCSPlate* plate)
{
    if (!plate) {
        return;
    }
    free(plate->acquisitions);
    plate->acquisitions = NULL;
    plate->acquisition_count = 0;
}

ZarrStatusCode
ZarrHCSPlate_create_row_name_array(ZarrHCSPlate* plate, size_t row_count)
{
    if (!plate) {
        return ZarrStatusCode_InvalidArgument;
    }
    if (row_count == 0) {
        return ZarrStatusCode_InvalidArgument;
    }

    const char** names = calloc(row_count, sizeof(const char*));
    if (!names) {
        return ZarrStatusCode_OutOfMemory;
    }

    ZarrHCSPlate_destroy_row_name_array(plate);
    plate->row_names = names;
    plate->row_count = row_count;

    return ZarrStatusCode_Success;
}

void
ZarrHCSPlate_destroy_row_name_array(ZarrHCSPlate* plate)
{
    if (!plate) {
        return;
    }
    free((void*)plate->row_names);
    plate->row_names = NULL;
    plate->row_count = 0;
}

ZarrStatusCode
ZarrHCSPlate_create_column_name_array(ZarrHCSPlate* plate, size_t column_count)
{
    if (!plate) {
        return ZarrStatusCode_InvalidArgument;
    }
    if (column_count == 0) {
        return ZarrStatusCode_InvalidArgument;
    }

    const char** names = calloc(column_count, sizeof(const char*));
    if (!names) {
        return ZarrStatusCode_OutOfMemory;
    }

    ZarrHCSPlate_destroy_column_name_array(plate);
    plate->column_names = names;
    plate->column_count = column_count;

    return ZarrStatusCode_Success;
}

void
ZarrHCSPlate_destroy_column_name_array(ZarrHCSPlate* plate)
{
    if (!plate) {
        return;
    }
    free((void*)plate->column_names);
    plate->column_names = NULL;
    plate->column_count = 0;
}

ZarrStatusCode
ZarrHCSSettings_create_plate_array(ZarrHCSSettings* settings,
                                   size_t plate_count)
{
    if (!settings) {
        return ZarrStatusCode_InvalidArgument;
    }
    if (plate_count == 0) {
        return ZarrStatusCode_InvalidArgument;
    }

    ZarrHCSPlate* plates = calloc(plate_count, sizeof(ZarrHCSPlate));
    if (!plates) {
        return ZarrStatusCode_OutOfMemory;
    }

    ZarrHCSSettings_destroy_plate_array(settings);
    settings->plates = plates;
    settings->plate_count = plate_count;

    return ZarrStatusCode_Success;
}

void
ZarrHCSSettings_destroy_plate_array(ZarrHCSSettings* settings)
{
    if (!settings) {
        return;
    }
    if (settings->plates) {
        for (size_t i = 0; i < settings->plate_count; ++i) {
            ZarrHCSPlate_destroy_well_array(&settings->plates[i]);
            ZarrHCSPlate_destroy_acquisition_array(&settings->plates[i]);
            ZarrHCSPlate_destroy_row_name_array(&settings->plates[i]);
            ZarrHCSPlate_destroy_column_name_array(&settings->plates[i]);
        }
        free(settings->plates);
        settings->plates = NULL;
    }
    settings->plate_count = 0;
}

/* --- Settings queries --------------------------------------------------- */

// Estimate the heap+frame bytes a single array will use. HCS FOVs are always
// multiscale; for flat arrays pass as->multiscale. Returns 0 on success.
static int
estimate_one_array_bytes(const ZarrArraySettings* as,
                         bool force_multiscale,
                         size_t* out_bytes)
{
    const size_t ndims = as->dimension_count;
    if (ndims < 2 || !as->dimensions) {
        return 1;
    }

    enum dtype dt = shim_convert_dtype(as->data_type);
    struct codec_config codec = shim_convert_codec(as->compression_settings);
    struct dimension* dims =
      shim_convert_dimensions(as->dimensions,
                              ndims,
                              as->storage_dimension_order,
                              force_multiscale || as->multiscale);
    if (!dims) {
        return 1;
    }

    size_t frame_bytes = dtype_bpe(dt) *
                         as->dimensions[ndims - 2].array_size_px *
                         as->dimensions[ndims - 1].array_size_px;

    struct tile_stream_configuration cfg = {
        .buffer_capacity_bytes = frame_bytes,
        .dtype = dt,
        .rank = (uint8_t)ndims,
        .dimensions = dims,
        .codec = codec,
        .reduce_method = shim_convert_reduce_method(as->downsampling_method),
        .append_reduce_method =
          shim_convert_reduce_method(as->downsampling_method),
    };

    tile_stream_memory_info_t info = { 0 };
    int err = tile_stream_memory_estimate(&cfg, 0, &info);
    free(dims);

    if (err) {
        return 1;
    }

    *out_bytes = TILE_STREAM_TOTAL_BYTES(info) + frame_bytes;
    return 0;
}

ZarrStatusCode
ZarrStreamSettings_estimate_max_memory_usage(
  const ZarrStreamSettings* settings,
  size_t* usage)
{
    if (!settings || !usage) {
        return ZarrStatusCode_InvalidArgument;
    }
    if (!settings->arrays && !settings->hcs_settings) {
        return ZarrStatusCode_InvalidArgument;
    }

    size_t total = 0;

    for (size_t i = 0; i < settings->array_count; ++i) {
        size_t bytes = 0;
        if (estimate_one_array_bytes(&settings->arrays[i], false, &bytes)) {
            return ZarrStatusCode_InternalError;
        }
        total += bytes;
    }

    if (settings->hcs_settings) {
        const ZarrHCSSettings* hcs = settings->hcs_settings;
        for (size_t p = 0; p < hcs->plate_count; ++p) {
            const ZarrHCSPlate* plate = &hcs->plates[p];
            for (size_t w = 0; w < plate->well_count; ++w) {
                const ZarrHCSWell* well = &plate->wells[w];
                for (size_t f = 0; f < well->image_count; ++f) {
                    const ZarrArraySettings* as =
                      well->images[f].array_settings;
                    if (!as) {
                        return ZarrStatusCode_InvalidArgument;
                    }
                    size_t bytes = 0;
                    if (estimate_one_array_bytes(as, true, &bytes)) {
                        return ZarrStatusCode_InternalError;
                    }
                    total += bytes;
                }
            }
        }
    }

    *usage = total;
    return ZarrStatusCode_Success;
}

size_t
ZarrStreamSettings_get_array_count(const ZarrStreamSettings* settings)
{
    if (!settings) {
        return 0;
    }

    size_t count = settings->array_count;

    if (settings->hcs_settings) {
        const ZarrHCSSettings* hcs = settings->hcs_settings;
        for (size_t i = 0; i < hcs->plate_count; ++i) {
            const ZarrHCSPlate* plate = &hcs->plates[i];
            for (size_t j = 0; j < plate->well_count; ++j) {
                count += plate->wells[j].image_count;
            }
        }
    }

    return count;
}

ZarrStatusCode
ZarrStreamSettings_get_array_key(const ZarrStreamSettings* settings,
                                 size_t index,
                                 char** key)
{
    if (!settings || !key) {
        return ZarrStatusCode_InvalidArgument;
    }

    // Flat arrays first
    if (index < settings->array_count) {
        const ZarrArraySettings* as = &settings->arrays[index];
        if (!as->output_key) {
            *key = NULL;
            return ZarrStatusCode_Success;
        }
        *key = strdup(as->output_key);
        return *key ? ZarrStatusCode_Success : ZarrStatusCode_OutOfMemory;
    }

    // HCS FOVs
    size_t idx = settings->array_count;
    if (settings->hcs_settings) {
        const ZarrHCSSettings* hcs = settings->hcs_settings;
        for (size_t p = 0; p < hcs->plate_count; ++p) {
            const ZarrHCSPlate* plate = &hcs->plates[p];
            for (size_t w = 0; w < plate->well_count; ++w) {
                const ZarrHCSWell* well = &plate->wells[w];
                for (size_t f = 0; f < well->image_count; ++f) {
                    if (idx == index) {
                        const ZarrHCSFieldOfView* fov = &well->images[f];
                        const char* plate_path =
                          plate->path ? plate->path : "plate";
                        const char* fov_path = fov->path ? fov->path : "0";

                        // "plate_path/row_name/col_name/fov_path"
                        size_t len =
                          strlen(plate_path) + 1 + strlen(well->row_name) +
                          1 + strlen(well->column_name) + 1 +
                          strlen(fov_path) + 1;
                        char* buf = malloc(len);
                        if (!buf) {
                            return ZarrStatusCode_OutOfMemory;
                        }
                        snprintf(buf,
                                 len,
                                 "%s/%s/%s/%s",
                                 plate_path,
                                 well->row_name,
                                 well->column_name,
                                 fov_path);
                        *key = buf;
                        return ZarrStatusCode_Success;
                    }
                    ++idx;
                }
            }
        }
    }

    return ZarrStatusCode_InvalidIndex;
}

/* --- Helpers for creating arrays from settings -------------------------- */

// printf into a freshly-allocated buffer sized to the formatted length.
// Returns NULL on allocation failure. Caller frees.
static char*
alloc_printf(const char* fmt, ...)
{
    va_list ap, ap2;
    va_start(ap, fmt);
    va_copy(ap2, ap);
    int n = vsnprintf(NULL, 0, fmt, ap);
    va_end(ap);
    if (n < 0) {
        va_end(ap2);
        return NULL;
    }
    char* buf = malloc((size_t)n + 1);
    if (!buf) {
        va_end(ap2);
        return NULL;
    }
    vsnprintf(buf, (size_t)n + 1, fmt, ap2);
    va_end(ap2);
    return buf;
}

static void
write_intermediate_groups(struct store* store, const char* key)
{
    if (!key) {
        return;
    }

    size_t len = strlen(key);
    // Prefix buffer: holds the evolving "a/b/c" path (null-terminated at
    // each '/' for mkdirs). Group-key buffer: prefix + "/zarr.json".
    // Both sized for the full key to avoid any fixed-size truncation.
    static const char SUFFIX[] = "/zarr.json";
    char* prefix = malloc(len + 1);
    char* group_key = malloc(len + sizeof(SUFFIX));
    if (!prefix || !group_key) {
        free(prefix);
        free(group_key);
        return;
    }
    memcpy(prefix, key, len + 1);

    for (size_t i = 0; i < len; ++i) {
        if (prefix[i] == '/') {
            prefix[i] = '\0';
            store->mkdirs(store, prefix);
            memcpy(group_key, prefix, i);
            memcpy(group_key + i, SUFFIX, sizeof(SUFFIX));
            zarr_group_write_with_raw_attrs(store, group_key, "{}");
            prefix[i] = '/';
        }
    }

    free(prefix);
    free(group_key);
}

// Configure `sa` as a multiscale array: builds dims/axes, creates the
// ngff_multiscale sink under `sa->key`, and fills the tile_stream config.
// `sa->key` must be set by the caller (NULL == root). Returns 1 on success,
// 0 on failure; partial state is cleaned up by the caller via shim_array_destroy.
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
    sa->dims = shim_convert_dimensions(as->dimensions,
                                       as->dimension_count,
                                       as->storage_dimension_order,
                                       false);
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
    write_intermediate_groups(stream->store, sa->key);
    if (sa->key) {
        stream->store->mkdirs(stream->store, sa->key);
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
            size_t attr_cap =
              2048 + zplate->well_count * 128 +
              zplate->acquisition_count * 256 +
              zplate->row_count * 32 + zplate->column_count * 32;
            char* attrs = malloc(attr_cap);
            if (!attrs) {
                return 0;
            }

            int alen = shim_hcs_plate_attributes_json(
              attrs, attr_cap, zplate);
            if (alen < 0) {
                free(attrs);
                return 0;
            }

            char* key = alloc_printf("%s/zarr.json", plate_path);
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
            char* row_dir = alloc_printf("%s/%s", plate_path, row_name);
            if (!row_dir) {
                return 0;
            }
            stream->store->mkdirs(stream->store, row_dir);
            {
                char* key = alloc_printf("%s/zarr.json", row_dir);
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
              alloc_printf("%s/%s/%s", plate_path, row_name, col_name);
            if (!well_dir) {
                return 0;
            }
            stream->store->mkdirs(stream->store, well_dir);
            {
                char attrs[4096];
                int alen = shim_hcs_well_attributes_json(
                  attrs, sizeof(attrs), well);
                if (alen < 0) {
                    free(well_dir);
                    return 0;
                }
                char* key = alloc_printf("%s/zarr.json", well_dir);
                if (!key) {
                    free(well_dir);
                    return 0;
                }
                int rc =
                  zarr_group_write_with_raw_attrs(stream->store, key, attrs);
                free(key);
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
                sa->key = alloc_printf("%s/%s/%s/%s",
                                       plate_path,
                                       row_name,
                                       col_name,
                                       fov_path);
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
shim_hcs_plate_attributes_json(char* buf,
                               size_t cap,
                               const ZarrHCSPlate* plate)
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
        char path[256];
        snprintf(
          path, sizeof(path), "%s/%s", well->row_name, well->column_name);
        jw_string(&jw, path);
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
shim_hcs_well_attributes_json(char* buf,
                              size_t cap,
                              const ZarrHCSWell* well)
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
    apply_log_level();

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
    if (!settings->overwrite &&
        store_has_existing_data(stream->store)) {
        log_error("refusing to overwrite existing data at %s "
                  "(set settings.overwrite=true to replace)",
                  settings->store_path);
        goto fail;
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
        stream->writer =
          multiarray_tile_stream_writer(stream->multi_stream);
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
            cur = next;
            break;
        }
        if (r.error != multiarray_writer_ok) {
            // fail or not_flushable: caller switched arrays mid-epoch, or
            // the writer returned an internal error. Stop consuming.
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
