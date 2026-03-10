# Building TurboLynx

## Prerequisites

TurboLynx requires only standard compiler toolchain packages. All graph-related libraries are bundled.

### Ubuntu 22.04 / Debian

```bash
sudo apt-get install -y \
    build-essential gcc-11 g++-11 \
    cmake ninja-build git \
    autoconf automake libtool pkg-config \
    ca-certificates
```

### Fedora / RHEL

```bash
sudo dnf install -y gcc-c++ cmake ninja-build git autoconf automake libtool
```

### macOS

macOS is not currently supported.

---

## Build

```bash
git clone https://github.com/postech-dblab-iitp/turbograph-v3
cd turbograph-v3
mkdir build && cd build

cmake -GNinja \
      -DCMAKE_BUILD_TYPE=Release \
      -DENABLE_TCMALLOC=OFF \
      -DBUILD_UNITTESTS=OFF \
      -DTBB_TEST=OFF \
      ..

ninja
```

The first build downloads and compiles several bundled dependencies (TBB, hwloc, numactl, GP-Xerces), which takes approximately 3–5 minutes. Subsequent incremental builds complete in seconds.

## Build Outputs

| File | Description |
|---|---|
| `src/libturbolynx.so` | Main shared library (~30 MB) |
| `tools/client` | Interactive Cypher shell |
| `tools/bulkload` | Dataset loader |

## Verify

```bash
./tools/client --help
```

---

## See Also

- [Build Configuration](build-configuration.md) — CMake flags and bundled dependency details
- [Linux Guide](linux.md) — step-by-step walkthrough for Ubuntu 22.04
- [Testing](../testing.md) — running the test suite
