# TurboLynx

Fast, scalable OLAP graph database in C++17, with a Cypher query interface and cost-based optimizer (ORCA).

**Single-process embedded architecture** (DuckDB-style) — no separate daemon required.

---

## Quick Start (no Docker required)

### 1. Install system dependencies

Ubuntu 22.04:

```bash
sudo apt-get install -y \
    build-essential gcc-11 g++-11 \
    cmake ninja-build git \
    autoconf automake libtool pkg-config \
    libaio-dev
```

> **All** other dependencies (TBB, libnuma, hwloc, simdjson, linenoise, GP-Xerces, antlr4, fmt, re2, …) are bundled in `third_party/` or auto-downloaded and built at configure time.

### 2. Build

```bash
git clone <repo-url> turbograph-v3
cd turbograph-v3
mkdir build && cd build
cmake -GNinja -DCMAKE_BUILD_TYPE=Release \
      -DENABLE_TCMALLOC=OFF \
      -DBUILD_UNITTESTS=OFF \
      ..
ninja
```

The first build auto-downloads and compiles GP-Xerces, TBB, libnuma, and hwloc into the build directory (~3 min extra). Subsequent builds are fast.

Output:
- `build/src/libs62gdb.so` — main shared library (~30 MB, no boost runtime deps)
- `build/tools/client`, `build/tools/bulkload`, `build/tools/socket_server_run`

### 3. Optional CMake flags

| Flag | Default | Description |
|---|---|---|
| `CMAKE_BUILD_TYPE` | `Release` | `Debug` or `Release` |
| `ENABLE_TCMALLOC` | `ON` | Link TCMalloc (requires `libgoogle-perftools-dev`) |
| `BUILD_UNITTESTS` | `ON` | Build unit tests |
| `BUILD_TOOLS` | `ON` | Build CLI tools |
| `NATIVE_ARCH` | `ON` | `-march=native` |
| `ENABLE_AVX` | `ON` | AVX2 SIMD |

---

## Dataset

### Supported file formats

- **CSV** — headers required; edge files need `:START_ID(TYPE)` / `:END_ID(TYPE)` columns (backward edge files use `:END_ID(TYPE)` / `:START_ID(TYPE)`)
- **JSON** — list of objects with consistent labels and unique properties per object

A sample LDBC SF1 dataset is available [here](https://drive.google.com/file/d/1PqXw_Fdp9CDVwbUqTQy0ET--mgakGmOA/view?usp=drive_link).

---

## Running the database

TurboLynx is **single-process and embedded** — no store daemon to start. Just run `bulkload` or `client` directly.

### Step 1 — Load dataset

```bash
cd build
bash ../scripts/bulkload/run-ldbc-bulkload.sh <db_dir> <data_dir>
```

Or directly:

```bash
cd build
./tools/bulkload --workspace <db_dir> --data <data_dir>
```

- `<db_dir>` — directory where the database will be stored (catalog + data files)
- `<data_dir>` — directory containing the source CSV/JSON files

The catalog is persisted as `<db_dir>/catalog.bin` and loaded automatically on next startup.

### Step 2 — Run statistics (first time only)

```bash
cd build
./tools/client --workspace <db_dir>
TurboLynx >> analyze
TurboLynx >> :exit
```

### Step 3 — Interactive client

```bash
cd build
./tools/client --workspace <db_dir>
TurboLynx >> MATCH (n:Person) RETURN n.firstName LIMIT 10;
TurboLynx >> :exit
```

---

## Client execution options

| Option | Description |
|---|---|
| `--workspace <path>` | Database workspace directory |
| `--query <cypher>` | Execute a single query and exit |
| `--iterations <n>` | Repeat query N times (for benchmarking) |
| `--warmup` | Run one extra warmup iteration (excluded from timing) |
| `--profile` | Print query plan profiling output |
| `--explain` | Print query execution plan |
| `--output-file <path>` | Dump query results to file |
| `--compile-only` | Compile query without executing |
| `--join-order-optimizer <mode>` | `query` / `greedy` / `exhaustive` / `exhaustive2` |
| `--disable-merge-join` | Disable merge join optimization |
| `--disable-index-join` | Disable index join optimization |
| `--debug-orca` | Enable ORCA optimizer debug output |
| `--log-level <level>` | `trace` / `debug` / `info` / `warn` / `error` |

## Interactive commands

| Command | Description |
|---|---|
| `:exit` | Exit the client |
| `analyze` | Rebuild optimizer statistics |
| `flush_file_meta` | Flush file metadata cache (speeds up next startup) |

---

## Query support

- `MATCH`, `WHERE`, `RETURN`, `WITH`, `ORDER BY`, `LIMIT`, `SKIP`
- `CREATE`, `SET`, `DELETE`
- `COUNT(*)`, `COUNT(column)` — `COUNT()` not supported
- Joins: index join, hash join, merge join (cost-based selection via ORCA)

---

## Socket server API

Start the server (no daemon needed):

```bash
cd build
./tools/socket_server_run --workspace <db_dir>    # listens on port 8080 by default
```

### API calls (TCP, port 8080)

| API_ID | Name | Description |
|---|---|---|
| `0` | `PrepareStatement` | Send Cypher query string |
| `1` | `ExecuteStatement` | Execute using prepared client ID |
| `2` | `Fetch` | Fetch one row |
| `3` | `FetchAll` | Fetch all rows (CSV format) |

### Python / Flask integration

See `api/server/test/python-example/` for a Flask server/client example.

```bash
python3 flask-server.py    # listens on http://localhost:6543
python3 flask-client.py    # sends a test query
```

POST `/execute` with JSON body:
```json
{ "query": "MATCH (n:Person) RETURN n.firstName LIMIT 5;" }
```

---

## Testing

```bash
cd build
cmake -GNinja -DBUILD_UNITTESTS=ON ..
ninja unittest

# Run all modules
ctest --output-on-failure

# Run a specific module
ctest -R catalog --output-on-failure

# Run by label (fast = common + execution)
ctest -L fast --output-on-failure

# Run directly (with Catch2 tag filter)
./test/unittest "[catalog]"
./test/unittest "[common]"
```

Or use the helper script:

```bash
bash scripts/run_tests.sh          # build + run all
bash scripts/run_tests.sh catalog  # run [catalog] module only
bash scripts/run_tests.sh rebuild  # clean build + run all
```

### Test modules

| Module | Tag | Tests | Description |
|---|---|---|---|
| common | `[common]` | 10 | SimpleHistogram, Value, DataChunk, CpuTimer |
| catalog | `[catalog]` | 7 | Schema, Graph, Partition CRUD |
| storage | `[storage]` | 3 | ExtentCatalogEntry, ChunkDefinitionCatalogEntry |
| execution | `[execution]` | — | Physical operator tests (TBD) |

---

## Third-party libraries (all bundled)

| Library | Purpose |
|---|---|
| antlr4 | Cypher lexer / parser |
| GP-Xerces (auto-downloaded) | XML for ORCA optimizer |
| TBB (auto-downloaded) | Thread Building Blocks |
| libnuma (auto-downloaded) | NUMA-aware memory allocation |
| hwloc (auto-downloaded) | Hardware topology |
| simdjson | Fast JSON parsing |
| linenoise | Lightweight readline replacement |
| MD5/SHA1 (bundled) | Non-crypto hash in cuckoo filter |
| duckdb fmt / re2 / fastpfor | Formatting, regex, integer compression |
| nlohmann/json | JSON serialization |
| spdlog | Logging |
| utf8proc / yyjson | UTF-8, YYJSON |
| cuckoofilter | Cuckoo filter |

---

## Architecture overview

TurboLynx uses a **single-process embedded architecture** (DuckDB-style).
The catalog is stored in a binary file (`catalog.bin`) and loaded into heap memory on startup.
There is no shared-memory store daemon or inter-process communication.

```
TurboLynx
├── src/
│   ├── catalog/       — schema, type system, binary catalog persistence
│   ├── common/        — vectors, chunks, types (DuckDB-style)
│   ├── execution/     — pipeline executor
│   ├── function/      — built-in functions
│   ├── main/          — Database, ClientContext (embedded, single-process)
│   ├── optimizer/     — ORCA cost-based optimizer, kuzu binder/planner
│   ├── parser/        — ANTLR4 Cypher grammar
│   ├── planner/       — logical → physical plan
│   └── storage/       — extent-based columnar storage, AIO, cache
├── third_party/       — all dependencies bundled as source
├── tools/             — client, bulkload, socket_server_run
├── test/              — Catch2 unit tests (4 modules, 21 tests)
└── docker/            — Dockerfile.lightweight-test (reference build env)
```

### Runtime dependencies

```
libaio.so     — async I/O (kernel AIO)
libstdc++     — C++ standard library
libc          — glibc
```

No boost. No store daemon. No shared memory.

## C/C++ API

The API follows DuckDB's design. See the DuckDB API documentation for usage patterns.
