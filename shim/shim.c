#include "shim_internal.h"

#include <stdlib.h>
#include <string.h>

#ifndef ACQUIRE_ZARR_API_VERSION
#define ACQUIRE_ZARR_API_VERSION "0.6.0"
#endif

static ZarrLogLevel current_log_level = ZarrLogLevel_Info;

/* --- Version / status / logging ----------------------------------------- */

const char*
Zarr_get_api_version(void)
{
    return ACQUIRE_ZARR_API_VERSION;
}

ZarrStatusCode
Zarr_set_log_level(ZarrLogLevel level)
{
    if (level < 0 || level >= ZarrLogLevelCount) {
        return ZarrStatusCode_InvalidArgument;
    }
    current_log_level = level;
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
            return "Compression error";
        case ZarrStatusCode_InvalidSettings:
            return "Invalid settings";
        case ZarrStatusCode_WillNotOverwrite:
            return "Will not overwrite existing data";
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
    if (dimension_count < 3) {
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

/* --- Settings queries (stubs) ------------------------------------------- */

ZarrStatusCode
ZarrStreamSettings_estimate_max_memory_usage(
  const ZarrStreamSettings* settings,
  size_t* usage)
{
    (void)settings;
    (void)usage;
    return ZarrStatusCode_NotYetImplemented;
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
    (void)settings;
    (void)index;
    (void)key;
    return ZarrStatusCode_NotYetImplemented;
}

/* --- Stream lifecycle (stubs) ------------------------------------------- */

ZarrStream*
ZarrStream_create(ZarrStreamSettings* settings)
{
    (void)settings;
    return NULL;
}

void
ZarrStream_destroy(ZarrStream* stream)
{
    if (!stream) {
        return;
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
    (void)stream;
    (void)data;
    (void)bytes_in;
    (void)bytes_out;
    (void)key;
    return ZarrStatusCode_NotYetImplemented;
}

ZarrStatusCode
ZarrStream_write_custom_metadata(ZarrStream* stream,
                                 const char* custom_metadata,
                                 bool overwrite)
{
    (void)stream;
    (void)custom_metadata;
    (void)overwrite;
    return ZarrStatusCode_NotYetImplemented;
}

ZarrStatusCode
ZarrStream_get_current_memory_usage(const ZarrStream* stream, size_t* usage)
{
    (void)stream;
    (void)usage;
    return ZarrStatusCode_NotYetImplemented;
}
