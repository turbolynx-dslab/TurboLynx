# TurboLynx — Technical Specification

## Vision

TurboLynx is an **OLAP graph database** in C++17 — single-process, embedded, zero-install. The north-star goal is to become a **credible, production-ready alternative to Kuzu** by end of March 2026.

This means:
- **Usability on par with DuckDB / Kuzu**: one-command build, one-command run, no external service, no container required
- **Robustness**: no crashes on core workloads; predictable error handling at every layer boundary
- **Performance**: measurable gains from the GEM algorithm and scan-level parallelism
- **Self-contained**: all dependencies bundled or eliminated; `apt-get` is optional, not required

Non-goal (this cycle): full feature parity with Kuzu or DuckDB.

---

## Release Gates (March 2026 RC)

| Gate | Why It Matters | Minimum Bar |
|------|---------------|-------------|
| **Reliability** | System must not fail unpredictably | No critical crash on core demo/benchmark flows; graceful error handling at Parse / Optimize / Execute / Storage boundaries |
| **Coverage** | Users need practical language/DDL/DML support | Required Cypher subset + basic DDL/DML reachable from the query interface |
| **Operability** | Adoption depends on workflow simplicity | Single-command startup; usable CLI introspection; configurable logging and options |
| **Performance** | Demo workloads must complete in reasonable time | No single-query timeout on benchmark suite; measurable gains from GEM optimization vs. naive baseline |

---

## Scope for March RC

### Must (RC blockers)
- Crash hardening across parser / optimizer / executor / storage
- Error-boundary handling (return errors, not `exit(-1)` or `abort()`)
- Unit + regression test baseline for all modules
- Required Cypher / DDL / DML subset documented and executable
- Single-command install and run path
- Basic CLI introspection (schema listing, query interface)
- Structured logging + runtime configuration
- License and legacy code cleanup

### Should (if time permits)
- Python API MVP
- Multi-threaded scan and join operators
- Improved cross-platform install UX

### Later (post-RC)
- Advanced compression schemes
- Broader expression and subquery support
- Full CALL / subquery expansion

---

## Architecture

### Query Path

```
Cypher text
  → Parser (ANTLR4)
  → Logical Plan
  → Optimizer (GPORCA / rule-based)
  → Physical Plan
  → Executor
  → Storage (Chunk Cache Manager → store.db)
```

### Subsystems

| Layer | Key Paths | Notes |
|-------|-----------|-------|
| Catalog | `src/catalog/`, `src/include/catalog/` | Binary serialization to `catalog.bin` |
| Storage | `src/storage/`, `src/include/storage/` | Column store, compression, statistics |
| Cache | `src/storage/cache/` | Single shared fd into `store.db`; Lightning in-process cache |
| Execution | `src/execution/`, `src/planner/` | Target: parallel scan/join |
| Optimizer | `src/optimizer/orca/gporca/` | GPORCA-based; GEM algorithm for join ordering |
| Parser | `src/parser/` | ANTLR4 Cypher grammar |
| Statistics | `src/storage/statistics/` | Numeric / String / Validity; zonemap pruning |
| Compression | `src/storage/compression/` | Per-column encoding |

### Workspace Layout

```
WORKSPACE/
├── catalog.bin    ← serialized catalog (schema, partition, extent, chunkdef)
├── store.db       ← single-file block store (all chunk data, 512B aligned)
└── .store_meta    ← offset table: [chunk_id | file_offset | alloc_size | req_size]
```

---

## Build

**Toolchain:** GCC 11+, CMake + Ninja, C++17

**Current build environment:** `turbograph-s62` Docker container

```bash
# Inside turbograph-s62:
cd /turbograph-v3/build-lwtest
cmake -GNinja -DCMAKE_BUILD_TYPE=Release -DBUILD_UNITTESTS=ON ..
ninja
```

**Key outputs:** `libturbolynx.so` (~30 MB), `unittest`, `bulkload`, `client`

**Goal:** `cmake + ninja` works on any Ubuntu 22.04 host with only standard build tools — no Docker, no Intel OneAPI, no system graph libraries.

---

## Dependencies

All heavy dependencies have been removed or bundled. The only remaining system requirements are standard C runtime libraries.

| Library | Source | Status |
|---------|--------|--------|
| libxerces-c | FetchContent (gp-xerces) | Bundled |
| libtbb | FetchContent (oneTBB v2021.10) | Bundled |
| libhwloc | FetchContent (hwloc-2.8.0) | Bundled |
| libnuma | FetchContent (numactl v2.0.14) | Bundled |
| libicu (icuuc, icudata) | system (`apt`) | Remaining |

**Removed:** Velox, Boost, Python3, libaio-dev

---

## Key Design Decisions

- **Single-file block store**: All chunks packed into `store.db` via a single shared fd; per-chunk offsets tracked in `.store_meta`
- **Embedded, single-process**: No separate server process, no shared memory IPC (DuckDB-style embedding)
- **No Velox**: `evalPredicateSIMD` replaced with scalar loop + local `BigintRange` struct
- **No Boost**: All types replaced with `std::` equivalents; SHM architecture removed
- **No Python**: GMM clustering removed; no `Python.h`
- **Catalog persistence**: Binary serialization (`CatalogSerializer`) — not mmap, not SQL
- **AIO**: Direct kernel AIO syscalls (`io_setup`, `io_submit`) — no `libaio-dev`

---

## Current Gap Assessment

### Established Foundation
- CMake/build foundation is in good shape; all heavy deps removed or bundled
- Core research features (CGC / GEM / SSRF) exist and are functional
- Demo scenarios can operate end-to-end

### Reliability Gaps
- Limited test coverage: module and regression tests are sparse
- Frequent crashes in storage, optimizer, and executor paths
- Failures often call `exit(-1)` instead of propagating exceptions

### Productization Gaps
- Container-dependent build/run workflow
- CLI lacks schema introspection and usability features
- DDL surface incomplete through the query interface
- No Python API
- Key operators are single-threaded
- Logging and configuration controls are minimal
- Legacy code and license cleanup is incomplete

---

## Definition of Done (March RC)

- Benchmark suite completes predefined runs without critical crash
- Core demo scenarios remain stable under repeated execution
- Required Cypher / DDL / DML subset is documented and executable
- Build and run path is documented and repeatable on a clean host
- Lightweight, embedded database execution works out of the box

## Release Artifacts

- Source release + build/run guide
- Benchmark scripts + representative query set
- Reproduction scripts for phase-wise evaluation
- Supported-features matrix + known limitations document
