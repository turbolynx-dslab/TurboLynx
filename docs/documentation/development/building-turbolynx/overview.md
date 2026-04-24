# Building TurboLynx

TurboLynx builds from source on Linux and macOS. Linux can use the native AIO
fast path in `build/`, while macOS uses the portable disk I/O backend in
`build-portable/`. For language-specific installation flows, see the
[installation guide](../../../installation/overview.md).

## Prerequisites

TurboLynx requires only standard compiler toolchain packages. All graph-related
libraries are bundled.

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

```bash
xcode-select --install
brew install cmake ninja git autoconf automake libtool pkg-config
```

---

## Quick Builds

### Linux fast path

```bash
git clone https://github.com/turbolynx-dslab/TurboLynx
cd TurboLynx
cmake -GNinja \
      -DCMAKE_BUILD_TYPE=Release \
      -DENABLE_TCMALLOC=OFF \
      -DBUILD_UNITTESTS=OFF \
      -DTBB_TEST=OFF \
      -B build
cmake --build build
```

### Portable build (macOS, or Linux when you want the same build recipe)

```bash
git clone https://github.com/turbolynx-dslab/TurboLynx
cd TurboLynx
cmake -GNinja \
      -DCMAKE_BUILD_TYPE=Release \
      -DTURBOLYNX_PORTABLE_DISK_IO=ON \
      -DENABLE_TCMALLOC=OFF \
      -DBUILD_UNITTESTS=OFF \
      -DTBB_TEST=OFF \
      -B build-portable
cmake --build build-portable
```

The first build downloads and compiles several bundled dependencies (TBB,
hwloc, and GP-Xerces), which takes approximately 3-5 minutes. Subsequent
incremental builds complete in seconds.

## Build Outputs

| Build | Outputs |
|---|---|
| Linux fast path (`build/`) | `build/src/libturbolynx.so`, `build/tools/turbolynx` |
| Portable build (`build-portable/`) | `build-portable/src/libturbolynx.so` on Linux or `build-portable/src/libturbolynx.dylib` on macOS, plus `build-portable/tools/turbolynx` |

## Verify

Run the binary from the build directory you created:

```bash
./build/tools/turbolynx --help
./build-portable/tools/turbolynx --help
```

---

## See Also

- [Build Configuration](build-configuration.md) — CMake flags and bundled dependency details
- [Linux Guide](linux.md) — step-by-step walkthrough for Ubuntu 22.04
- [Testing](../testing.md) — running the test suite
