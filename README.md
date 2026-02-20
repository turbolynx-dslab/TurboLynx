# TurboLynx

Fast, scalable OLAP graph database in C++17, with a Cypher query interface and cost-based optimizer (ORCA).

---

## Quick Start (no Docker required)

### 1. Install system dependencies

Ubuntu 22.04:

```bash
sudo apt-get install -y \
    build-essential gcc-11 g++-11 \
    cmake ninja-build git \
    autoconf automake libtool pkg-config \
    libtbb-dev libssl-dev libaio-dev libnuma-dev libhwloc-dev \
    libboost-all-dev python3-dev
```

> All other dependencies (simdjson, linenoise, GP-Xerces, antlr4, fmt, re2, …) are bundled in `third_party/` and built automatically.

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

The first build auto-downloads and compiles GP-Xerces into the build directory (takes ~2 min extra). Subsequent builds are fast.

Output:
- `build/src/libs62gdb.so` — main shared library (~37 MB)
- `build/tools/client`, `bulkload`, `store`, `socket_server_run`, …

### 3. Optional CMake flags

| Flag | Default | Description |
|---|---|---|
| `CMAKE_BUILD_TYPE` | `Release` | `Debug` or `Release` |
| `ENABLE_TCMALLOC` | `ON` | Link TCMalloc (requires `libgoogle-perftools-dev`) |
| `BUILD_UNITTESTS` | `ON` | Build unit tests (requires catch) |
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

### Step 1 — Load dataset

Open two terminals:

```bash
# Terminal 1: start storage server
cd build
./tools/store <storage-size>          # e.g., 10GB

# Terminal 2: bulk-load data
cp scripts/bulkload/run-ldbc-bulkload.sh build/
cd build
bash run-ldbc-bulkload.sh <db_dir> <data_dir>
```

- `<db_dir>` — directory where the database will be stored
- `<data_dir>` — directory containing the source CSV/JSON files

### Step 2 — Run statistics (first time only)

```bash
cd build
./tools/client --workspace <db_dir>
TurboLynx >> analyze
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

Start the server:

```bash
cd build
./tools/store <storage-size> &
./tools/socket_server_run <workspace>    # listens on port 8080 by default
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
cd build/test
./unittest
```

---

## Third-party libraries (all bundled)

| Library | Purpose |
|---|---|
| antlr4 | Cypher lexer / parser |
| GP-Xerces (auto-downloaded) | XML for ORCA optimizer (built with gnuiconv, no ICU) |
| simdjson | Fast JSON parsing |
| linenoise | Lightweight readline replacement |
| duckdb fmt / re2 / fastpfor | Formatting, regex, integer compression |
| nlohmann/json | JSON serialization |
| spdlog | Logging |
| utf8proc / yyjson | UTF-8, YYJSON |
| cuckoofilter | Cuckoo filter |

---

## Architecture overview

```
TurboLynx
├── src/
│   ├── catalog/       — schema, type system
│   ├── common/        — vectors, chunks, types (DuckDB-style)
│   ├── execution/     — pipeline executor
│   ├── function/      — built-in functions
│   ├── main/          — Database, ClientContext
│   ├── optimizer/     — ORCA cost-based optimizer, kuzu binder/planner
│   ├── parser/        — ANTLR4 Cypher grammar
│   ├── planner/       — logical → physical plan
│   └── storage/       — extent-based columnar storage, AIO, cache
├── third_party/       — all dependencies bundled as source
├── tools/             — client, bulkload, store, socket_server_run
└── docker/            — Dockerfile.lightweight-test (reference build env)
```

## C/C++ API

The API follows DuckDB's design. See the DuckDB API documentation for usage patterns.
