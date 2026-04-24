# Build Configuration

## CMake Flags

| Flag | Default | Description |
|---|---|---|
| `CMAKE_BUILD_TYPE` | `Release` | `Release` or `Debug` |
| `TURBOLYNX_PORTABLE_DISK_IO` | `OFF` on Linux, `ON` on macOS | Use the portable synchronous disk I/O backend |
| `ENABLE_TCMALLOC` | `ON` | Link TCMalloc (requires `libgoogle-perftools-dev`) |
| `BUILD_UNITTESTS` | `ON` | Build Catch2 unit test binary |
| `BUILD_TOOLS` | `ON` | Build the CLI binary (`turbolynx`) |
| `BUILD_PYTHON` | `OFF` | Build the Python bindings |
| `NATIVE_ARCH` | `ON` | Compile with `-march=native` |
| `ENABLE_AVX` | `ON` | Enable AVX2 SIMD instructions |
| `ENABLE_TBB` | `ON` | Enable Intel TBB |
| `ENABLE_OPENMP` | `ON` | Enable OpenMP |
| `TBB_TEST` | `ON` | Build vendored oneTBB tests (recommend `OFF`) |

## Recommended Configurations

### Release (Linux fast path)

```bash
cmake -GNinja \
      -DCMAKE_BUILD_TYPE=Release \
      -DENABLE_TCMALLOC=OFF \
      -DBUILD_UNITTESTS=OFF \
      -DTBB_TEST=OFF \
      -B build
cmake --build build
```

### Release (portable/macOS)

```bash
cmake -GNinja \
      -DCMAKE_BUILD_TYPE=Release \
      -DTURBOLYNX_PORTABLE_DISK_IO=ON \
      -DENABLE_TCMALLOC=OFF \
      -DBUILD_UNITTESTS=OFF \
      -DTBB_TEST=OFF \
      -B build-portable
cmake --build build-portable
```

### Debug (with tests)

```bash
cmake -GNinja \
      -DCMAKE_BUILD_TYPE=Debug \
      -DENABLE_TCMALLOC=OFF \
      -DBUILD_UNITTESTS=ON \
      -DTBB_TEST=OFF \
      -B build-debug
cmake --build build-debug
```

## Bundled Dependencies

The following are fetched and compiled automatically at configure time.
No manual installation is required.

| Library | Version | Purpose |
|---|---|---|
| TBB (oneTBB) | v2021.10.0 | Thread task scheduler |
| hwloc | 2.8.0 | CPU topology / thread affinity |
| GP-Xerces | greenplum fork | XML parsing (ORCA optimizer) |

Additionally, all libraries in `third_party/` are compiled from source as part of the build:
antlr4, simdjson, linenoise, spdlog, fmt, re2, fastpfor, nlohmann/json, utf8proc, yyjson, cuckoofilter, and more.
