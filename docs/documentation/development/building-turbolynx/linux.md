# Building on Linux

Step-by-step guide for Ubuntu 22.04.

## 1. Install Build Tools

```bash
sudo apt-get update
sudo apt-get install -y \
    build-essential gcc-11 g++-11 \
    cmake ninja-build git \
    autoconf automake libtool pkg-config \
    ca-certificates
```

Set GCC 11 as default compiler:

```bash
sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-11 60 \
                         --slave   /usr/bin/g++ g++ /usr/bin/g++-11
```

Verify:

```bash
gcc --version    # gcc (Ubuntu 11.x.x) 11.x.x
cmake --version  # cmake 3.22+
ninja --version  # 1.10+
```

## 2. Clone and Configure

```bash
git clone https://github.com/turbolynx-dslab/TurboLynx
cd TurboLynx
mkdir build && cd build

cmake -GNinja \
      -DCMAKE_BUILD_TYPE=Release \
      -DENABLE_TCMALLOC=OFF \
      -DBUILD_UNITTESTS=OFF \
      -DTBB_TEST=OFF \
      ..
```

## 3. Build

```bash
ninja
```

Expected final output:

```
[N/N] Linking CXX shared library src/libturbolynx.so
[N/N] Linking CXX executable tools/turbolynx
```

## 4. Verify

```bash
./tools/turbolynx --help
```

## Troubleshooting

### FetchContent download fails

Check internet connectivity and that `ca-certificates` is installed.
If behind a proxy, set the `https_proxy` environment variable before running cmake.

### `autoconf: not found` during configure

```bash
sudo apt-get install autoconf automake libtool
```

### Build with unit tests

```bash
cmake -GNinja -DCMAKE_BUILD_TYPE=Debug -DENABLE_TCMALLOC=OFF -DBUILD_UNITTESTS=ON -DTBB_TEST=OFF ..
ninja
./test/unittest "[catalog]"
./test/unittest "[storage]"
./test/unittest "[common]"
```
