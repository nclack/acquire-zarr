#pragma once

#include "acquire.zarr.h"

#include <stddef.h>

// Serialize OME/NGFF plate attributes for `plate` into `buf` (cap bytes).
// Returns the JSON byte length on success, -1 on buffer overflow or
// allocation failure inside the helper.
int
shim_hcs_plate_attributes_json(char* buf,
                               size_t cap,
                               const ZarrHCSPlate* plate);

// Serialize OME/NGFF well attributes for `well` into `buf` (cap bytes).
// Returns the JSON byte length on success, -1 on buffer overflow.
int
shim_hcs_well_attributes_json(char* buf, size_t cap, const ZarrHCSWell* well);
