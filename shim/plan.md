# Shim Implementation Plan

## Current State (2026-04-16)

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
All arrays coordinated by a single `multiarray_tile_stream` (CPU or GPU,
selected at compile time via `shim_backend.h`) with shared pools sized to
the maximum across arrays (constant memory for N arrays).
S3 store support wired via chucky's `store_s3_create` (aws-c-s3).
HCS support fully wired: plate/well/FOV metadata, per-FOV multiscale sinks, data routing.
Logging wired to chucky's public `chucky_log.h` API; Python module routes
events into `logging.getLogger("acquire_zarr")` via a chucky callback and
silences chucky's default stderr sink on import (see divergence #9).

### Multiarray constraint (HCS tests updated)

The multiarray tile stream requires that switching arrays happens at an
**epoch boundary** (so shared buffers can be reused without flushing partial
state). A write of one (y, x) frame to a FOV must equal one epoch:
`epoch_elements = chunks_per_epoch * chunk_elements = frame_size`.

HCS tests updated to use chunk sizes that evenly divide the frame:
`y_chunk=240` (2 chunks over 480), `x_chunk=320` (2 chunks over 640). This
is how production acquisitions should configure chunks.

## Chucky submodule

On main, including GPU multiarray writer (#81), shared-LOD split (#82),
CPU multiarray heap-overflow fixes (#83), and the public log header (#87).
The two local fixes previously listed here have been upstreamed.

## Architecture

The shim uses chucky's public API:
- **store** (`store_fs_create`) — filesystem key-value store
- **zarr_array** (`zarr_array_create`) — non-multiscale arrays (shard geometry computed internally)
- **ngff_multiscale** (`ngff_multiscale_create`) — multiscale arrays (auto LOD levels, writes NGFF group metadata)
- **multiarray tile stream** — streaming pipeline for N arrays with shared
  pools (chunk tiling, LOD pyramid, compression). CPU via
  `multiarray_tile_stream_cpu`, GPU via `multiarray_tile_stream_gpu`;
  selected at compile time by `shim_backend.h`. Switching between arrays
  only valid at epoch boundaries.

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

## Behavioral divergences from baseline acquire-zarr

Documented where the shim-backed library behaves differently than the main
branch implementation. Callers moving from baseline should review these.

### 1. LOD / multiscale geometry

Chucky's LOD rules (implemented via #70, #74, #fef0e1f):
1. All LOD dimensions are halved together at each level
2. Dimensions are clamped at chunk_size: `max((size+1)/2, chunk_size)`
3. **Chunk sizes are constant across all levels** (baseline shrank chunks)
4. Scale = `base_scale * (1 << n_times_downsampled)` per dimension
5. Stopping conditions (first wins):
   - `preserve_aspect_ratio && any LOD dim at chunk_size` (optional)
   - `all LOD dims at chunk_size` (always)
   - `nlod >= max_nlod` (always)
6. Dimensions that reach chunk_size drop from the LOD set for subsequent levels

Integration tests `stream-2d-multiscale`, `stream-3d-multiscale`,
`stream-multiscale-trivial-3rd-dim`, and LOD2 shape in
`stream-multiple-arrays-to-filesystem` were updated to expect this behavior.
Three of those — `stream-2d-multiscale-to-filesystem`,
`stream-3d-multiscale-to-filesystem`, and
`stream-multiple-arrays-to-filesystem` — cannot pass against the baseline
library and are therefore **disabled in `tests/integration/CMakeLists.txt`**
(commented out with a pointer to this divergence). They are still exercised
by the shim via `shim/CMakeLists.txt`.

### 2. Multiarray epoch-boundary constraint

The multiarray tile stream shares chunk/compressed/LUT pools across N arrays
(constant-memory design for 100s–1000s of arrays). Switching the active array
mid-epoch is rejected (`not_flushable`).

Practical requirement: for common "one frame per append" workflows, configure
chunk sizes so that one frame equals one epoch:
`epoch_elements = chunks_per_epoch * chunk_elements = frame_size`. Chunks in
the non-append dims must evenly divide the corresponding array sizes.

Baseline had independent per-array streams and allowed arbitrary interleaved
partial writes. HCS tests updated: `y_chunk=240` / `x_chunk=320` over
`480×640` frames (4 chunks = 1 epoch = 1 frame).

### 3. `settings->max_threads` — wired

Forwarded to `tile_stream_configuration.max_threads` for every array config
(flat + HCS). 0 means "auto" on both sides (chucky uses
`omp_get_max_threads()`).

### 4. `ZarrStream_get_current_memory_usage` — upper-bound estimate

Returns a value set once at stream create time from
`ZarrStreamSettings_estimate_max_memory_usage` (extended to walk HCS FOVs
as well as flat arrays). This is an upper bound, not runtime-tracked
usage, since chucky allocates pools once at create and they don't grow.

### 5. `ZarrStream_write_custom_metadata` — not implemented (TODO)

Returns `ZarrStatusCode_NotYetImplemented`. Needs a chucky-side API to write
JSON under a given `<array_key>/zarr.json`'s `attributes` with a
caller-chosen inner key (`ome` is reserved). This is per-array (array_key
selects the target; NULL means the root). Open as a chucky issue and wire
from the shim.

### 6. `settings->overwrite` — ignored (TODO)

Chucky is overwrite-by-default — individual shard writes replace existing
files in place — so the functional behavior when `overwrite=true` works
today. The missing piece is the **`overwrite=false` guard**: refuse with
`ZarrStatusCode_WillNotOverwrite` if the store already has data.

Plan: cheap coarse check at create time — `stat(store_path + "/zarr.json")`
for filesystem, or a single HEAD on the root metadata key for S3. O(1),
runs once per stream create. Baseline's stricter "scan and remove" on
overwrite=true isn't required since chucky clobbers per-shard anyway.

### 7. No frame queue (intentional)

Writes flow synchronously through chucky's pipeline; no 1 GiB buffered
frame queue like baseline. For GPU this will be partially replaced by
chucky's own h2d accumulation buffer (TBD how that shows up in memory
estimates). The `estimate-memory-usage` test was rewritten to check
relational properties (compressed > uncompressed, multiscale > single-scale)
rather than exact bytes.

### 8. Stock LZ4 codec removed (upstream, not shim-specific)

`ZarrCompressor_Lz4` / `ZarrCompressionCodec_Lz4` and the
`stream-lz4-compressed-to-filesystem` test were removed in acquire-zarr
c2be1a6 on main. Blosc-LZ4 is still supported.

### 9. Logging wired to chucky's public API

C API `Zarr_set_log_level` forwards to `chucky_log_set_level` /
`chucky_log_set_quiet` (gates chucky's stderr sink).

Python module registers a `chucky_log_add_callback` at import that routes
events into `logging.getLogger("acquire_zarr")` and calls
`chucky_log_set_quiet(1)` to silence chucky's stderr. Python users control
verbosity via `logging` — `Zarr_set_log_level` still round-trips but no
longer affects output.

## Remaining Work

### Nice-to-haves

- Wire `ZarrStream_write_custom_metadata` to chucky's attributes path (file
  a chucky issue first — the write-to-attributes-key primitive is missing).
  API is per-array: `array_key` selects target (NULL → root); `metadata_key`
  is the inner attributes key; `ome` is reserved.
- Honor `settings->overwrite=false` via a coarse existence check
  (`stat(store_path/zarr.json)` for FS, HEAD for S3). Chucky is
  overwrite-by-default at the shard level, so the only missing behavior is
  the guard; no per-shard scan needed.
- gpu-dependent tests once cpu testing looks good

## CPU wheel (Phase 1 — done)

- `shim/pybind/CMakeLists.txt` — pybind11 module linked against the selected
  backend (`acquire-zarr-chucky-cpu` or `acquire-zarr-chucky-gpu`)
- `shim/CMakeLists.txt` — `BUILD_PYTHON` option gates the pybind subdirectory
- `shim/python/pyproject.toml` + `setup.py` — package `acquire-zarr-cpu`, no vcpkg
- `shim/Dockerfile` — `wheel-deps` stage builds lz4/zstd/blosc/aws from source as
  static+PIC libs; `wheel-build` stage runs `python -m build`; `wheel` stage exports `.whl`
- Build: `docker build -f shim/Dockerfile --target wheel --output wheels .`
- Tested: import, create stream, write frames, verify Zarr output
- Runtime dep: `libgomp1` (OpenMP)
- Fixed `python/acquire-zarr-py.cpp` lambda deleter → struct for C++17 compat

## GPU wheel (Phase 2 — done)

`multiarray_gpu` landed in chucky as #81/#82/#83. Built on top via:

- `shim/shim_backend.h` — preprocessor dispatch; one header swaps
  `multiarray_tile_stream_create/destroy/writer`, `tile_stream_memory_estimate`,
  and the memory-info typedef/total-bytes macro based on `SHIM_BACKEND_GPU`.
- `shim/shim.c` / `shim/shim_internal.h` now use the backend-agnostic names
  (3 call sites + 2 includes + 2 type refs replaced).
- `shim/CMakeLists.txt` — conditional `acquire-zarr-chucky-gpu` static lib
  compiles the same three sources with `SHIM_BACKEND_GPU=1` and links
  chucky's `stream` (GPU) + `multiarray_gpu`.
- `shim/python-gpu/pyproject.toml` + `setup.py` — package `acquire-zarr-gpu`;
  setup.py passes `-DCHUCKY_ENABLE_GPU=ON -DCMAKE_CUDA_ARCHITECTURES=80;86;89;90;100`
  and uses `build-wheel-gpu/` so CPU and GPU builds don't collide.
- `shim/Dockerfile.gpu` — `nvidia/cuda:12.8.0-devel-ubuntu24.04` base,
  nvcomp 5.1 from NVIDIA's redist tarball at `/opt/nvcomp`, reuses the same
  PIC from-source builds of lz4/zstd/blosc/aws-c-* as the CPU image.
- Build: `docker build -f shim/Dockerfile.gpu --target wheel --output wheels-gpu .`
- Integration tests still link CPU only (no GPU runner in CI).

## CI

- `.github/workflows/test-shim.yml` — runs `docker compose run --rm test`
  which brings up minio alongside the test container and invokes
  `ctest -L shim` (only shim-labeled tests). Triggers: push to `main`,
  PRs to `main`. The `test` service in `shim/docker-compose.yml` has no
  GPU device requirement (shim has no GPU tests yet). `uv` is installed
  in `shim/Dockerfile` so chucky's `test_ome_validate` also works when
  someone runs `docker build --target test` locally.
- `.github/workflows/wheels.yml` — two parallel jobs (`cpu-wheel`,
  `gpu-wheel`) that build the Dockerfiles and upload the resulting `.whl`
  files as workflow artifacts. Triggers: push to `main`, push to `shim`,
  manual `workflow_dispatch`. No publishing.
- `python/acquire-zarr-py.cpp` gates its chucky log callback behind
  `#ifdef ACQUIRE_ZARR_WITH_CHUCKY_LOG`, which only `shim/pybind/CMakeLists.txt`
  defines — so the baseline `build.yml` / `benchmark.yml` / `release.yml`
  pipelines that compile the shared pybind source without chucky still work.

## Files

```
.github/workflows/
  test-shim.yml           # docker compose run --rm test (shim ctest via compose)
  wheels.yml              # cpu-wheel + gpu-wheel jobs, upload artifacts
shim/
  CMakeLists.txt          # builds chucky, shim lib (cpu+gpu), integration tests
  Dockerfile              # CPU build/test + CPU wheel stages
  Dockerfile.gpu          # GPU wheel stages (CUDA 12.8 + nvcomp 5.1)
  docker-compose.yml      # MinIO + test service
  README.md               # build/test docs
  plan.md                 # this file
  shim.c                  # API functions + HCS metadata + intermediate group helpers
  shim_backend.h          # preprocessor dispatch — CPU vs GPU backend names
  shim_internal.h         # ZarrStream_s, shim_array (with store/plates)
  shim_convert.h/.c       # type conversion (dims, ngff_axes, codec, dtype)
  shim_sink.h/.c          # discriminated union sink (ARRAY + MULTISCALE + NONE)
  pybind/
    CMakeLists.txt          # pybind11 module linked against selected backend
  python/
    pyproject.toml          # wheel metadata (acquire-zarr-cpu)
    setup.py                # CMake-driven CPU wheel build
  python-gpu/
    pyproject.toml          # wheel metadata (acquire-zarr-gpu)
    setup.py                # CMake-driven GPU wheel build
  compat/
    logger.hh/.cpp/.types.h  # C++ logger for test macro compat
  chucky/                 # submodule
```
