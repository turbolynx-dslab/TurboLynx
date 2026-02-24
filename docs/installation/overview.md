# Installation

## Vision: Embedded Distribution

TurboLynx is designed to be distributed as a **self-contained embedded library** — similar to DuckDB.

The goal is that integrating TurboLynx into your application requires nothing beyond cloning the source:

```cmake
# Future: FetchContent-based embedding (planned)
include(FetchContent)
FetchContent_Declare(turbograph GIT_REPOSITORY https://github.com/your-org/turbograph-v3.git)
FetchContent_MakeAvailable(turbograph)
target_link_libraries(your_app PRIVATE turbograph)
```

No system library installation. No separate server process. No configuration files.

## Current Status

TurboLynx is currently distributed as source and built locally.
The build requires only standard compiler toolchain packages — no special system libraries.

All runtime dependencies are compiled directly into the binary.
The resulting shared library links only against standard C/C++ runtime libraries (`libstdc++`, `libc`, `libpthread`).

See [Building TurboLynx](../documentation/development/building-turbograph/overview.md) for build instructions.

## Roadmap

| Milestone | Status |
|---|---|
| Zero system library build | ✅ Done |
| Single-process embedded architecture | ✅ Done |
| FetchContent / CMake package integration | 🚧 Planned |
| Python wheel (`pip install turbograph`) | 🚧 Planned |
| Pre-built binary releases | 🚧 Planned |
