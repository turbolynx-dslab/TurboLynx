# TurboLynx — Technical Specification

## Overview

TurboLynx is an **OLAP graph database** written in C++17 — single-process, embedded, zero-install.

- **Query language:** Cypher (openCypher subset)
- **Architecture:** Single-process embedded (DuckDB-style); no daemon, no IPC
- **Storage:** Extent-based columnar store packed into a single binary file (`store.db`)
- **Optimizer:** ORCA (Cascades-framework cost-based optimizer, ported from Greenplum)
- **Primary use case:** Read-heavy analytical graph workloads (LDBC SNB, DBpedia, TPC-H)

---

## Architecture

### Query Pipeline

```
Cypher text
  → ANTLR4 Parser               (src/parser/)
  → TurboLynx Binder            (src/binder/)
  → Cypher2OrcaConverter        (src/converter/)
  → ORCA Optimizer              (src/optimizer/orca/)   ← Statistics + MD Provider
  → Physical Plan
  → Pipeline Executor           (src/execution/)
  → BufferPool + AIO            (src/storage/)
```

Each stage has a well-defined interface:

| Stage | Input | Output |
|-------|-------|--------|
| Parser | Cypher string | `RegularQuery` AST |
| Binder | `RegularQuery` + Catalog | `BoundRegularQuery` |
| Converter | `BoundRegularQuery` | ORCA `CExpression` tree |
| Optimizer | Logical `CExpression` | Physical `CExpression` |
| Executor | Physical plan | Result tuples |

### Subsystems

| Layer | Source Path | Notes |
|-------|-------------|-------|
| Parser | `src/parser/` | ANTLR4 Cypher grammar → `RegularQuery` AST |
| Binder | `src/binder/` | Schema resolution, `BoundNodeExpression` / `BoundRelExpression` |
| Converter | `src/converter/` | Bound AST → ORCA logical operators |
| Optimizer | `src/optimizer/orca/gporca/` | ORCA cost-based engine; GEM join ordering |
| MD Provider | `src/optimizer/mdprovider/` | Serves catalog metadata to ORCA |
| Planner | `src/planner/` | Bridges converter output into ORCA; physical plan construction |
| Executor | `src/execution/` | Column-at-a-time pipeline execution |
| Catalog | `src/catalog/` | In-memory schema; persisted to `catalog.bin` |
| Storage | `src/storage/` | Extent-based columnar store; compression; statistics |
| BufferPool | `src/storage/cache/` | In-process page cache; Second-Chance Clock eviction |
| Statistics | `src/storage/statistics/` | Numeric / String / Validity histograms; zone-map pruning |

### Workspace Layout

```
WORKSPACE/
├── catalog.bin    ← serialized catalog (schema, partitions, extents, chunk definitions)
├── store.db       ← single-file block store (all chunk data, 512B-aligned)
└── .store_meta    ← chunk offset table: [chunk_id | file_offset | alloc_size | req_size]
```

---

## Key Design Decisions

### Graphlets (Columnar Graph Partitioning)

Nodes with the same label but different property sets are grouped into **Graphlets** — compact columnar tables with a uniform schema and no NULLs. The CGC (Compact Graphlet Clustering) algorithm selects the cost-optimal partition. Each graphlet maps to one or more extents in `store.db`.

### Single-File Block Store

All chunk data is packed into a single file (`store.db`). Chunks are 512B-aligned and addressed by absolute byte offsets tracked in `.store_meta`. A single shared file descriptor is used for all I/O.

### Embedded, Single-Process

TurboLynx embeds directly into the calling process like DuckDB — no separate server, no shared memory, no IPC. The C API (`libturbolynx.so`) exposes `s62_connect`, `s62_query`, `s62_disconnect`.

### BufferPool

The buffer pool manages in-memory page frames for chunks loaded from `store.db`. Eviction uses the **Second-Chance Clock** algorithm. Allocation is 512B-aligned (`posix_memalign`). Dirty chunks are flushed on release or shutdown.

### AIO

Disk I/O uses Linux Kernel AIO (`io_setup` / `io_submit` / `io_getevents`) directly — no `libaio-dev` system library. One shared file descriptor; all reads and writes are absolute-offset requests into `store.db`.

### Catalog Persistence

The catalog (graph schema, partition metadata, extent definitions, chunk definition IDs) is serialized via `CatalogSerializer` to `catalog.bin`. On startup it is fully restored — no re-scan of data files is required.

### Multi-Process Access

File locking via `fcntl` (`F_WRLCK` / `F_RDLCK`) protects `store.db` across processes. A `read_only_` mode allows multiple concurrent readers (`s62_connect_readonly()`). Only one writer is allowed at a time.

---

## Build

**Toolchain:** GCC 11+, CMake 3.22+, Ninja, C++17

```bash
# Development build (with unit tests)
mkdir build-lwtest && cd build-lwtest
cmake -GNinja -DCMAKE_BUILD_TYPE=Debug -DENABLE_TCMALLOC=OFF -DBUILD_UNITTESTS=ON -DTBB_TEST=OFF ..
ninja

# Release build (no tests)
mkdir build && cd build
cmake -GNinja -DCMAKE_BUILD_TYPE=Release -DENABLE_TCMALLOC=OFF -DBUILD_UNITTESTS=OFF -DTBB_TEST=OFF ..
ninja
```

**Key outputs:**

| File | Description |
|------|-------------|
| `src/libturbolynx.so` | Main shared library (~30 MB) |
| `tools/client` | Interactive Cypher shell |
| `tools/bulkload` | Dataset loader |
| `test/unittest` | Catch2 unit test binary (Debug build only) |

---

## Dependencies

All heavy dependencies are bundled. No special system libraries are required beyond standard build tools.

| Library | Source | Purpose |
|---------|--------|---------|
| ANTLR4 runtime | `third_party/antlr4/` | Cypher parsing |
| oneTBB v2021.10 | FetchContent | Thread task scheduling |
| hwloc 2.8.0 | FetchContent | CPU topology / thread affinity |
| numactl v2.0.14 | FetchContent | NUMA memory control |
| gp-xerces | FetchContent | XML parsing (ORCA plan serialization) |
| simdjson, spdlog, fmt, re2, fastpfor, nlohmann/json, utf8proc, yyjson, linenoise | `third_party/` | Various utilities |

**System requirements:** `build-essential`, `cmake`, `ninja-build`, `git`, `autoconf`, `automake`, `libtool`

---

## Test Suite

| Module | Tag | Count | Scope |
|--------|-----|-------|-------|
| common | `[common]` | 10 | Value types, DataChunk, histograms, CpuTimer |
| catalog | `[catalog]` | 51 | Schema/Graph/Partition CRUD, persistence round-trip |
| storage | `[storage]` | 68 | BufferPool, CCM, extent I/O, multi-process locking |

**E2E bulkload tests** (`ctest -L bulkload`): LDBC SF1, TPC-H SF1, DBpedia — verify vertex/edge counts after a full load.

**Query tests** (`test/query/query_test`): LDBC Q1 (21 queries), LDBC Q2 (9 queries) against a preloaded LDBC SF1 database.

---

## Current Capabilities and Limitations

### Supported Cypher

| Feature | Status |
|---------|--------|
| `MATCH`, `WHERE`, `RETURN` | ✅ Full |
| `WITH` (pipeline) | ✅ Full |
| `ORDER BY`, `LIMIT`, `SKIP` | ✅ Full |
| `OPTIONAL MATCH` | ✅ Parsed; left-outer join handled by ORCA |
| `UNWIND` | ✅ Supported |
| Inline node/rel property filters `{k:v}` | ✅ Synthesized as WHERE predicates |
| `COUNT(*)`, `COUNT(expr)` | ✅ Supported |
| `CREATE`, `SET`, `DELETE` | ❌ Not yet implemented |
| `UNION` / `UNION ALL` | 🚧 Single-query only (multi-query UNION pending) |
| Subqueries | ❌ Not yet implemented |

### Known Limitations

- Write operations (CREATE / SET / DELETE) are not available through the Cypher interface.
- DBpedia edge type `rdf:type` has an empty forward CSV; its forward adjacency list cannot be built.
- Statistics must be re-built manually after loading a dataset (`analyze` command in the client shell).
