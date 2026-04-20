#include "shim_hcs.h"

#include "shim_array.h"
#include "shim_hcs_json.h"
#include "shim_util.h"

#include "zarr/store.h"
#include "zarr/zarr_group.h"

#include <stdlib.h>

// Write "<plate_path>/zarr.json" with OME plate attributes. Returns 0 on
// success, 1 on failure. Owns all intermediate buffers.
static int
write_plate_group_metadata(struct store* store,
                           const char* plate_path,
                           const ZarrHCSPlate* plate)
{
    size_t attr_cap = 2048 + plate->well_count * 128 +
                      plate->acquisition_count * 256 + plate->row_count * 32 +
                      plate->column_count * 32;
    char* attrs = malloc(attr_cap);
    char* key = shim_alloc_printf("%s/zarr.json", plate_path);
    int rc = 1;
    if (!attrs || !key) {
        goto cleanup;
    }

    int alen = shim_hcs_plate_attributes_json(attrs, attr_cap, plate);
    if (alen < 0) {
        goto cleanup;
    }

    if (zarr_group_write_with_raw_attrs(store, key, attrs) != 0) {
        goto cleanup;
    }
    rc = 0;

cleanup:
    free(attrs);
    free(key);
    return rc;
}

// Write "<plate_path>/<row_name>/zarr.json" with an empty `{}` attribute
// body. Returns 0 on success, 1 on failure. mkdirs is best-effort.
static int
write_row_group(struct store* store,
                const char* plate_path,
                const char* row_name)
{
    char* row_dir = shim_alloc_printf("%s/%s", plate_path, row_name);
    char* key = NULL;
    int rc = 1;
    if (!row_dir) {
        goto cleanup;
    }
    store->mkdirs(store, row_dir);

    key = shim_alloc_printf("%s/zarr.json", row_dir);
    if (!key) {
        goto cleanup;
    }
    zarr_group_write_with_raw_attrs(store, key, "{}");
    rc = 0;

cleanup:
    free(row_dir);
    free(key);
    return rc;
}

// Write "<well_dir>/zarr.json" with OME well attributes. Returns 0 on
// success, 1 on failure.
static int
write_well_group_metadata(struct store* store,
                          const char* well_dir,
                          const ZarrHCSWell* well)
{
    // Generous cap scaled to image count so writers with many FOVs per
    // well don't overflow silently. Each image contributes ~64 bytes of
    // JSON in the worst case.
    size_t attrs_cap = 512 + well->image_count * 96;
    char* attrs = malloc(attrs_cap);
    char* key = shim_alloc_printf("%s/zarr.json", well_dir);
    int rc = 1;
    if (!attrs || !key) {
        goto cleanup;
    }

    int alen = shim_hcs_well_attributes_json(attrs, attrs_cap, well);
    if (alen < 0) {
        goto cleanup;
    }

    if (zarr_group_write_with_raw_attrs(store, key, attrs) != 0) {
        goto cleanup;
    }
    rc = 0;

cleanup:
    free(attrs);
    free(key);
    return rc;
}

// Create one FOV multiscale array at
// "<plate_path>/<row>/<col>/<fov>". Sets sa->key and delegates to
// shim_configure_multiscale_array. Returns 1 on success, 0 on failure;
// partial state is cleaned up by the caller via shim_array_destroy.
static int
create_fov_array(struct ZarrStream_s* stream,
                 const char* plate_path,
                 const ZarrHCSWell* well,
                 const ZarrHCSFieldOfView* fov,
                 struct shim_array* sa)
{
    const char* fov_path = fov->path ? fov->path : "0";
    sa->key = shim_alloc_printf(
      "%s/%s/%s/%s", plate_path, well->row_name, well->column_name, fov_path);
    if (!sa->key) {
        return 0;
    }

    return shim_configure_multiscale_array(stream, fov->array_settings, sa);
}

// Create all FOV arrays + group metadata for one plate. Writes root/plate
// groups, then row/well/FOV groups for each well. Returns 1 on success,
// 0 on failure.
static int
create_plate(struct ZarrStream_s* stream,
             const ZarrHCSPlate* plate,
             size_t* array_idx)
{
    // Write root group (idempotent; may have been written already).
    zarr_group_write_with_raw_attrs(stream->store, "zarr.json", "{}");

    const char* plate_path = plate->path ? plate->path : "plate";
    stream->store->mkdirs(stream->store, plate_path);

    if (write_plate_group_metadata(stream->store, plate_path, plate) != 0) {
        return 0;
    }

    for (size_t w = 0; w < plate->well_count; ++w) {
        const ZarrHCSWell* well = &plate->wells[w];

        if (write_row_group(stream->store, plate_path, well->row_name) != 0) {
            return 0;
        }

        char* well_dir = shim_alloc_printf(
          "%s/%s/%s", plate_path, well->row_name, well->column_name);
        if (!well_dir) {
            return 0;
        }
        stream->store->mkdirs(stream->store, well_dir);
        int well_rc = write_well_group_metadata(stream->store, well_dir, well);
        free(well_dir);
        if (well_rc != 0) {
            return 0;
        }

        for (size_t f = 0; f < well->image_count; ++f) {
            struct shim_array* sa = &stream->arrays[*array_idx];
            if (!create_fov_array(
                  stream, plate_path, well, &well->images[f], sa)) {
                return 0;
            }
            ++(*array_idx);
        }
    }

    return 1;
}

int
shim_create_hcs_arrays(struct ZarrStream_s* stream,
                       const ZarrStreamSettings* settings,
                       size_t* array_idx)
{
    const ZarrHCSSettings* hcs = settings->hcs_settings;

    for (size_t p = 0; p < hcs->plate_count; ++p) {
        if (!create_plate(stream, &hcs->plates[p], array_idx)) {
            return 0;
        }
    }
    return 1;
}
