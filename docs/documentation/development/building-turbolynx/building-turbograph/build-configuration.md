# Build Configuration

## CMake Flags

| Flag | Default | Description |
|---|---|---|
| `CMAKE_BUILD_TYPE` | `Release` | `Release` or `Debug` |
| `ENABLE_TCMALLOC` | `ON` | Link TCMalloc (requires `libgoogle-perftools-dev`) |
| `BUILD_UNITTESTS` | `ON` | Build Catch2 unit test binary |
| `BUILD_TOOLS` | `ON` | Build CLI tools (`client`, `bulkload`) |
| `NATIVE_ARCH` | `ON` | Compile with `-march=native` |
| `ENABLE_AVX` | `ON` | Enable AVX2 SIMD instructions |
| `TBB_TEST` | `ON` | Build TBB's internal tests (recommend `OFF`) |

## Recommended Configurations

### Release (no tests)

```bash
cmake -GNinja \
      -DCMAKE_BUILD_TYPE=Release \
      -DENABLE_TCMALLOC=OFF \
      -DBUILD_UNITTESTS=OFF \
      -DTBB_TEST=OFF \
      ..
```

### Debug (with tests)

```bash
cmake -GNinja \
      -DCMAKE_BUILD_TYPE=Debug \
      -DENABLE_TCMALLOC=OFF \
      -DBUILD_UNITTESTS=ON \
      -DTBB_TEST=OFF \
      ..
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
