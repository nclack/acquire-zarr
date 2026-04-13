# Shim Implementation Plan

## Current State (2026-04-13)

All 17 integration tests passing (all original acquire-zarr tests ported):
- `stream-raw-to-filesystem` — PASS
- `stream-named-array-to-filesystem` — PASS
- `stream-compressed-to-filesystem` (blosc) — PASS
- `stream-zstd-compressed-to-filesystem` — PASS
- `stream-2d-multiscale-to-filesystem` — PASS
- `stream-3d-multiscale-to-filesystem` — PASS
- `stream-multi-frame-append` — PASS
- `stream-multiscale-trivial-3rd-dim` — PASS
- `stream-multiple-arrays-to-filesystem` — PASS
- `estimate-memory-usage` — PASS
- `stream-pure-hcs-acquisition` — PASS
- `stream-mixed-flat-and-hcs-acquisition` — PASS
- `stream-with-ragged-final-shard` — PASS
- `stream-raw-to-s3` — PASS (via minio in docker-compose)
- `stream-named-array-to-s3` — PASS
- `stream-compressed-to-s3` — PASS
- `stream-append-nullptr` — PASS (tests both filesystem and S3)

Ported shim to chucky's public API (store → zarr_array/ngff_multiscale).
Pool management removed — each array/multiscale creates its own pool internally.
S3 store support wired via chucky's `store_s3_create` (aws-c-s3).
HCS support fully wired: plate/well/FOV metadata, per-FOV multiscale sinks, data routing.

## Chucky submodule

On main at bc940a9 ("GPU CSR builder for LOD reduce #75").

## Architecture

The shim uses chucky's public API:
- **store** (`store_fs_create`) — filesystem key-value store
- **zarr_array** (`zarr_array_create`) — non-multiscale arrays (shard geometry computed internally)
- **ngff_multiscale** (`ngff_multiscale_create`) — multiscale arrays (auto LOD levels, writes NGFF group metadata)
- **tile_stream_cpu** — streaming pipeline (chunk tiling, LOD pyramid, compression)

Internal APIs used only where needed:
- `zarr/store.h` — for `store->mkdirs()` in HCS hierarchy and intermediate groups
- `zarr/zarr_group.h` — for `zarr_write_group()`
- `zarr/json_writer.h` — for HCS metadata JSON helpers

HCS is built directly in the shim (not using chucky's `hcs_plate_create`) to support
per-well/per-FOV heterogeneous configs from the acquire-zarr API.

Intermediate groups are written for each path component of array keys.

## Store Layout

- **Non-multiscale, no output_key**: flat at root via `zarr_array`
  - `store.zarr/zarr.json` = array metadata
  - `store.zarr/c/...` = shard data
- **Multiscale**: group + subdirectory via `ngff_multiscale`
  - `store.zarr/zarr.json` = group (OME multiscales)
  - `store.zarr/0/zarr.json` = L0 array
- **Named array (output_key)**: under subdirectory
  - `store.zarr/key/zarr.json` = array or group
  - Intermediate groups written at each path component
- **HCS**: plate → row → well → FOV hierarchy
  - `store.zarr/plate/zarr.json` = plate group with OME plate attributes
  - `store.zarr/plate/row/zarr.json` = row group
  - `store.zarr/plate/row/col/zarr.json` = well group with OME well attributes
  - `store.zarr/plate/row/col/fov/zarr.json` = FOV multiscale group

## LOD Behavior Spec

The integration tests encode the desired LOD behavior.
Implemented in chucky via #70 (fix scale factors) and #74 (fix epoch LOD shard geometry).

**Key rules:**
1. All LOD dimensions are halved together at each level
2. Dimensions are clamped at chunk_size: `max((size+1)/2, chunk_size)`
3. Chunk sizes are **constant** across all levels (no shrinking)
4. Scale = `base_scale * (1 << n_times_downsampled)` per dimension
5. Stopping conditions (first wins):
   - `preserve_aspect_ratio && any LOD dim at chunk_size` (optional)
   - `all LOD dims at chunk_size` (always)
   - `nlod >= max_nlod` (always)
6. Dimensions that reach chunk_size drop from the LOD set for subsequent levels

## Remaining Work

No known test failures. All shim API functions implemented.
All 17 original acquire-zarr integration tests ported and passing.

## Files

```
shim/
  CMakeLists.txt          # builds chucky, shim lib, integration tests
  Dockerfile              # CUDA base (for Docker builds)
  docker-compose.yml      # MinIO + test service
  README.md               # build/test docs
  plan.md                 # this file
  shim.c                  # API functions + HCS metadata + intermediate group helpers
  shim_internal.h         # ZarrStream_s, shim_array (with store/plates)
  shim_convert.h/.c       # type conversion (dims, ngff_axes, codec, dtype)
  shim_sink.h/.c          # discriminated union sink (ARRAY + MULTISCALE + NONE)
  compat/
    logger.hh/.cpp/.types.h  # C++ logger for test macro compat
  chucky/                 # submodule
```
