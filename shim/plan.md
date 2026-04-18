# Shim Implementation Plan

## Current State (2026-04-18)

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
CPU multiarray heap-overflow fixes (#83), the public log header (#87),
the `zarr_write_attribute` API (#88), `store_has_existing_data` (#89),
idempotent multiarray flush (#91), and the explicit stream commit
point (#92). The two local fixes previously listed here have been
upstreamed.

#88 and #89 add the primitives needed to close divergences #5 and #6
respectively; wiring on the shim side is still pending (see Remaining Work).

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
- `zarr/zarr_group.h` — for `zarr_group_write_with_raw_attrs()`
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

### 5. `ZarrStream_write_custom_metadata` — primitive ready, shim wiring pending

Still returns `ZarrStatusCode_NotYetImplemented`. Chucky now exposes
`zarr_write_attribute` (#88), which is the primitive the shim needs to
write JSON under a given `<array_key>/zarr.json`'s `attributes` with a
caller-chosen inner key (`ome` is reserved). This is per-array (array_key
selects the target; NULL means the root). Wire from `shim.c`.

### 6. `settings->overwrite` — primitive ready, shim wiring pending

Chucky is overwrite-by-default — individual shard writes replace existing
files in place — so the functional behavior when `overwrite=true` works
today. The missing piece is the **`overwrite=false` guard**: refuse with
`ZarrStatusCode_WillNotOverwrite` if the store already has data.

Chucky now exposes `store_has_existing_data` (#89) — an O(1) existence
check against the store's root metadata key that works for both filesystem
and S3 backends. Wire from `shim.c` at stream-create time and return
`WillNotOverwrite` when the guard trips. Baseline's stricter "scan and
remove" on overwrite=true isn't required since chucky clobbers per-shard.

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

Python module registers a `chucky_log_add_callback` at import and calls
`chucky_log_set_quiet(1)` to silence chucky's stderr. Python users
control verbosity via `logging` — `Zarr_set_log_level` still round-trips
but no longer affects output.

Chucky callbacks fire on arbitrary threads (including IO workers that
run while the main thread has released the GIL). Delivering events
straight into Python from the producer thread is a deadlock risk: if
the Python handler blocks (e.g. on pytest's captured-stderr pipe) while
the main thread is waiting on an IO fence, the worker never retires its
job and nothing drains the pipe. The producer-side callback therefore
pushes events into a bounded mutex-protected ring and never touches
Python; a `drain_log_ring()` runs on the Python thread (GIL held) from
an RAII `LogDrainGuard` placed at the top of each bound method, so
events reach Python on both normal and exception-propagating return
paths. Overflow drops oldest silently and reports a count as a
`warning`. See `python/acquire-zarr-py.cpp`.

## Remaining Work

### Known issues (triage separately)

- **Windows shim benchmark**: `zarr.open(az_path)` on a shim-written zarr
  fails with `UnicodeDecodeError: 'utf-8' codec can't decode byte 0x80 in
  position 599` reading `zarr.json`. Ubuntu + ARM writes produce a
  zarr-python-readable store on the same run. Excluded via matrix
  `exclude:` in `.github/workflows/benchmark.yml`. Likely a
  Windows-specific `zarr.json` write path issue in chucky's `store_fs` or
  `zarr_metadata` module.
- **macOS shim benchmark** (both arm + intel): FindOpenMP + chucky's
  `enable_openmp()` don't cooperate on Apple clang when the shim wheel is
  built standalone. Tried space-string then list form for
  `OpenMP_C_FLAGS`; both produce a miscomposed compile command
  (`-Xclang -MD`, `-MF ... unused`). Baseline path works because
  `OpenMP::OpenMP_C` is used directly in the baseline build. Excluded via
  matrix `exclude:`.
- **Windows test-order interaction**: in the default collection order the
  whole suite took ~25 min with a 10-min gap around
  `test_append_throws_on_overflow`. Randomizing test order via
  `pytest-randomly` drops total runtime to ~15 min with
  `test_anisotropic_downsampling` (83s) as the slowest test and no 10-min
  gap anywhere — so the slowness was ordering-dependent, not intrinsic.
  `pytest-randomly` is now installed in CI; prints a seed on each run for
  reproducibility. Root ordering-sensitivity is still a loose end.

### Nice-to-haves

- GPU-dependent tests on the self-hosted `[self-hosted, gpu]` runner
  registered for the `acquire-project` org (auk laptop). Approach TBD:
  `docker compose` via `shim/Dockerfile.gpu`, or native via chucky's inner
  nix flake (nvcc 12.9).

## CI

- `.github/workflows/test-shim.yml` — runs `docker compose run --rm test`,
  which brings up minio alongside the test container and invokes
  `ctest -L shim`. Triggers: push to `main`, PRs to `main`. No GPU tests
  yet. BuildKit GHA layer cache reuses from-source aws-c-* / lz4 / zstd /
  blosc layers across runs.
- `.github/workflows/wheels.yml` — parallel `cpu-wheel` and `gpu-wheel`
  jobs build the Dockerfiles and upload `.whl` artifacts. Triggers: push
  to `main`, push to `shim`, manual `workflow_dispatch`. No publishing.
- `python/acquire-zarr-py.cpp` gates its chucky log callback behind
  `#ifdef ACQUIRE_ZARR_WITH_CHUCKY_LOG`, which only
  `shim/pybind/CMakeLists.txt` defines — so the baseline `build.yml` /
  `benchmark.yml` / `release.yml` pipelines that compile the shared
  pybind source without chucky still work.
- `tests/integration/s3-test-helpers.hh` has two backends selected by
  `-DS3_TEST_HELPERS_USE_AWS_CLI`: miniocpp (default, used by the
  baseline vcpkg build on all platforms incl. Windows) and `aws` CLI via
  `popen` (used by the shim Linux-docker build, which avoids vcpkg).
- Python test job (`test-python` in `test.yml`) runs pytest under
  `pytest-timeout` — `--timeout-method=signal` on POSIX (SIGALRM can
  preempt C-extension hangs and emit a traceback), `--timeout-method=thread`
  on Windows (signal method not supported). Job-level
  `timeout-minutes: 25` caps runaway runners at 25m rather than GitHub's
  6h default. `test_anisotropic_downsampling` carries an explicit
  `@pytest.mark.timeout(300)` because it writes ~4 GB and is legitimately
  slow on Windows.

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
