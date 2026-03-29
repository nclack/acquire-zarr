# Shim Implementation Plan

## Current State (2026-03-29)

3 of 12 integration tests passing:
- `stream-raw-to-filesystem` — PASS
- `stream-multi-frame-append` — PASS (fixed: chucky PR #15)
- `stream-with-ragged-final-shard` — PASS

## Chucky submodule

Currently pinned to `fix/omit-null-unit` branch (PR #14). Once merged, update
to main.

Open chucky issues filed during shim work:
- [#2](https://github.com/acquire-project/chucky/issues/2) — Group metadata for HCS, multiarray, custom attributes
- [#5](https://github.com/acquire-project/chucky/issues/5) — CPU stream + zarr_fs_sink EFAULT (FIXED)
- [#8](https://github.com/acquire-project/chucky/issues/8) — consolidated_metadata field (FIXED)
- [#12](https://github.com/acquire-project/chucky/issues/12) — unit/scale on struct dimension (FIXED, unit omit behavior in PR #14)
- [PR #15](https://github.com/acquire-project/chucky/pull/15) — Fix final shape for append dims (rounds up to chunk boundary → exact count)

## Test Failures and Required Work

### DONE: shape[0] counting on unbounded append dimensions

Fixed in chucky PR #15. `decompose_append_sizes` rounds up to chunk boundary;
now overridden at flush time with exact cursor count via `dim_info_exact_dim0`.

### Phase 3: Compression

**Test:** `stream-compressed-to-filesystem`
**Error:** `Expected second codec to be 'blosc', got lz4`
**Details:** acquire-zarr wraps codecs in Blosc1. Chucky uses raw lz4/zstd.
The zarr.json codec chain differs: acquire-zarr writes `blosc` as the codec
name, chucky writes `lz4` or `zstd` directly.
**Fix:** This is a known, accepted metadata difference. The test's expectations
need updating to accept chucky's codec names, OR the test should be excluded
from the shim test suite. Both produce valid Zarr v3 stores — the difference
is in the compression wrapper, not the data.

### Phase 4: Multiscale

**Tests:** `stream-2d-multiscale-to-filesystem`, `stream-3d-multiscale-to-filesystem`,
`stream-multiscale-trivial-3rd-dim`
**Errors:**
- 2d/3d: `cannot use operator[] with string argument with null` — the multiscale
  group metadata is missing expected fields when `multiscale=true`
- trivial-3rd-dim: `datasets.size() 1 != 3` — only 1 LOD level generated, expected 3

**Fix in shim:**
- When `multiscale=true`, set `downsample=1` on spatial dimensions
- Use `nlod=0` (auto) to let chucky determine the number of LOD levels
- Currently the shim always uses `nlod=1` for non-multiscale and `nlod=1` for
  multiscale too (bug) — needs to use `nlod=0` when `multiscale=true`

Wait — the shim already does `nlod = as->multiscale ? 0 : 1`. But the
multiscale tests might be failing because `downsample` is never set. Chucky
needs `dimension.downsample = 1` on the dimensions to include in the LOD
pyramid. The shim currently sets `downsample = 0` on all dimensions.

**Action:** When `multiscale=true`, set `downsample=1` on spatial dimensions
(dimensions where `type == ZarrDimensionType_Space`).

### Phase 5: Named and Multiple Arrays

**Tests:** `stream-named-array-to-filesystem`, `stream-multiple-arrays-to-filesystem`
**Errors:**
- named-array: `Expected file 'path/zarr.json' to exist` — the output_key isn't
  being used as the array subdirectory name
- multiple-arrays: `Expected key 'attributes' in metadata` — root group metadata
  doesn't have OME attributes when multiple arrays are present

**Fix in shim:**
- `output_key` is passed as `array_name` in `zarr_multiscale_config`. The
  `zarr_fs_multiscale_sink` should create the array at `store_path/output_key/0/`.
  Need to verify chucky's behavior when `array_name` is non-NULL.
- Multiple arrays: need to create multiple `shim_array` entries and route
  `ZarrStream_append` by key. Also need `ZarrStreamSettings_get_array_key`.
- The root group metadata for multiple arrays may need a different structure
  than single-array (no OME multiscales at root, each array has its own group).

### Phase 6: HCS (blocked on chucky #2)

**Tests:** `stream-pure-hcs-acquisition`, `stream-mixed-flat-and-hcs-acquisition`
**Error:** `Not yet implemented` — `ZarrStreamSettings_get_array_key` is stubbed
**Blocked on:** chucky #2 (group metadata, HCS hierarchy)

### Phase 8: Memory estimation

**Test:** `estimate-memory-usage`
**Error:** `Not yet implemented` — `ZarrStreamSettings_estimate_max_memory_usage`
is stubbed
**Fix:** Delegate to `tile_stream_cpu_memory_estimate` for each array and sum.

## Implementation Order

### Immediate (shim-only fixes)

1. **Multiscale downsample flags** — set `downsample=1` on spatial dims when
   `multiscale=true`. Should fix `stream-multiscale-trivial-3rd-dim` and
   unblock the 2d/3d multiscale tests.

2. **Named array routing** — verify `output_key` → `array_name` mapping works
   with `zarr_fs_multiscale_sink`. Fix `stream-named-array-to-filesystem`.

3. **Multiple arrays** — implement multi-array create/append/destroy +
   `ZarrStreamSettings_get_array_key`. Fix `stream-multiple-arrays-to-filesystem`.

4. **Memory estimation** — implement `ZarrStreamSettings_estimate_max_memory_usage`
   using `tile_stream_cpu_memory_estimate`.

### Needs investigation / chucky changes

5. **shape[0] counting** — investigate chucky's metadata update for unbounded
   append dimensions. The reported extent should be the actual frame count,
   not the padded chunk boundary. May need a chucky fix.

6. **Compression codec names** — decide: adapt the test expectations for the
   shim, or exclude `stream-compressed-to-filesystem` from shim tests.

### Blocked on chucky

7. **HCS** — blocked on chucky #2 (group metadata, HCS hierarchy, custom
   metadata). Tests: `stream-pure-hcs-acquisition`,
   `stream-mixed-flat-and-hcs-acquisition`.

## Files

```
shim/
  CMakeLists.txt          # builds chucky, shim lib, integration tests
  Dockerfile              # CUDA base, all deps, builds shim (supports CMAKE_BUILD_TYPE arg)
  docker-compose.yml      # MinIO + test service
  README.md               # build/test docs
  shim.c                  # 28 API functions (18 allocators done, 5 stream lifecycle done, 5 stubs)
  shim_internal.h         # ZarrStream_s, shim_array
  shim_convert.h/.c       # type conversion (dtype, codec, dimension, axis)
  shim_sink.h/.c          # discriminated union sink (currently FS_MULTISCALE only)
  compat/
    logger.hh/.cpp/.types.h  # C++ logger for test macro compat
  chucky/                 # submodule
```
