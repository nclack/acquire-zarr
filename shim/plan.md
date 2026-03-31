# Shim Implementation Plan

## Current State (2026-03-30)

4 of 13 integration tests passing (estimate-memory-usage excluded):
- `stream-raw-to-filesystem` — PASS
- `stream-multi-frame-append` — PASS
- `stream-multiscale-trivial-3rd-dim` — PASS
- `stream-with-ragged-final-shard` — PASS

Building locally via nix flake (no Docker needed for dev iteration).

## Chucky submodule

Currently on `fix/allow-null-array-name` branch (PR #17).

Open chucky PRs/issues:
- [#2](https://github.com/acquire-project/chucky/issues/2) — Group metadata for HCS, multiarray, custom attributes
- [PR #16](https://github.com/acquire-project/chucky/pull/16) — Fix LOD level count termination
- [PR #17](https://github.com/acquire-project/chucky/pull/17) — Allow NULL array_name in fs sink (flat layout)

Previously merged: #5 (EFAULT fix), #8 (consolidated_metadata), #12 (unit/scale), #14 (omit null unit), #15 (append shape fix)

## Store Layout

- **Non-multiscale, no output_key**: flat at root via `zarr_fs_sink(array_name=NULL)`
  - `store.zarr/zarr.json` = array metadata
  - `store.zarr/c/...` = shard data
- **Multiscale**: group + subdirectory via `zarr_fs_multiscale_sink`
  - `store.zarr/zarr.json` = group (OME multiscales)
  - `store.zarr/0/zarr.json` = L0 array
- **Named array (output_key)**: under subdirectory
  - `store.zarr/key/zarr.json` = array or group

## Remaining Test Failures

### Compression (lz4/zstd tests)
**Tests:** `stream-lz4-compressed-to-filesystem`, `stream-zstd-compressed-to-filesystem`
**Error:** JSON type mismatch — chucky doesn't write `level` in lz4/zstd codec config
**Fix:** Small — either add level to chucky's codec config JSON, or accept its absence

**Test:** `stream-compressed-to-filesystem` (blosc)
**Error:** `Expected 'blosc', got lz4` — shim will never support blosc
**Fix:** Exclude from shim test list

### Multiscale (2d/3d)
**Tests:** `stream-2d-multiscale-to-filesystem`, `stream-3d-multiscale-to-filesystem`
**Error:** Likely codec metadata differences (both use compression + multiscale)
**Needs:** Investigation after compression tests are resolved

### Named/Multiple Arrays
**Tests:** `stream-named-array-to-filesystem`, `stream-multiple-arrays-to-filesystem`
**Fix:** Verify output_key → array_name routing. Implement `ZarrStreamSettings_get_array_key`.

### Memory Estimation
**Test:** `estimate-memory-usage`
**Fix:** Implement using `tile_stream_cpu_memory_estimate`.

### HCS (blocked on chucky #2)
**Tests:** `stream-pure-hcs-acquisition`, `stream-mixed-flat-and-hcs-acquisition`

## Next Steps

1. ~~Fix compression codec config~~ → Blocked on chucky#19 (codec struct + blosc)
2. Named array routing
3. Multiple arrays + get_array_key
4. Memory estimation
5. 3d-multiscale scale computation mismatch (separate from codec/LOD issues)

## Files

```
shim/
  CMakeLists.txt          # builds chucky, shim lib, integration tests
  Dockerfile              # CUDA base (for Docker builds)
  docker-compose.yml      # MinIO + test service
  README.md               # build/test docs
  plan.md                 # this file
  shim.c                  # 28 API functions
  shim_internal.h         # ZarrStream_s, shim_array
  shim_convert.h/.c       # type conversion
  shim_sink.h/.c          # discriminated union sink (FS + FS_MULTISCALE)
  compat/
    logger.hh/.cpp/.types.h  # C++ logger for test macro compat
  chucky/                 # submodule
```
