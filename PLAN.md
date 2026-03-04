# TurboLynx — Execution Plan

## Current Status

Core build is stable. Catalog, Storage, Execution layers tested.
Build runs inside `turbograph-s62` Docker container.

## Completed Milestones

| # | Milestone | Status |
|---|-----------|--------|
| 1 | Remove Velox dependency | ✅ Done |
| 2 | Remove Boost dependency | ✅ Done |
| 3 | Remove Python3 dependency | ✅ Done |
| 4 | Bundle TBB / libnuma / hwloc | ✅ Done |
| 5 | Single-file block store (`store.db`) | ✅ Done |
| 6 | Test suite: catalog, storage, execution | ✅ Done |
| 7 | Remove libaio-dev system dependency (direct syscalls) | ✅ Done |
| 8 | Rename library: `libs62gdb.so` → `libturbolynx.so` | ✅ Done |

---

## Milestone 9 — End-to-End Bulkload Test Suite

### Goal

Run `bulkload` against real benchmark datasets (LDBC, TPCH, DBpedia) and
verify the loaded graph is structurally correct: right labels, right counts,
correct property schema. These are the first true E2E tests.

---

### Design Overview

```
ctest -L bulkload               ← opt-in label, NOT run by default
  └─ bulkload_test binary
       ├─ DatasetLocator        ← find data or download it
       ├─ BulkloadRunner        ← exec bulkload subprocess
       ├─ DbVerifier            ← open DB via C++ internal API, inspect catalog
       └─ per-dataset test cases (Catch2 tags)
```

The existing `unittest` binary (unit tests) is unchanged.
A **new separate binary** `bulkload_test` is built only when
`-DENABLE_BULKLOAD_TESTS=ON` (default: OFF).

---

### Data Discovery & Download

Priority order when locating a dataset:

1. **Env var** `TURBOLYNX_DATA_DIR` — explicit override
2. **CLI arg** `--data-dir <path>` passed to `bulkload_test`
3. **Default** `/source-data` (available inside `turbograph-s62` container)

If the dataset directory is missing:

- **Default (CI/offline):** skip the test with `WARN` message and exit code `SKIP`.
  CTest treats skipped tests as pass to avoid blocking CI.
- **Opt-in auto-download:** if env var `TURBOLYNX_AUTO_DOWNLOAD=1` is set,
  invoke `scripts/download_test_data.sh <dataset>` which uses
  `huggingface_hub` to pull from HuggignHajae/TurboLynx-* repos.

```bash
# Run with data already present:
ctest -L bulkload

# Run with auto-download enabled:
TURBOLYNX_AUTO_DOWNLOAD=1 ctest -L bulkload

# Override data root:
TURBOLYNX_DATA_DIR=/mnt/data ctest -L bulkload

# Run a single dataset directly:
./test/bulkload_test "[bulkload][ldbc][sf1]" --data-dir /source-data
```

---

### Bulkload Runner

`BulkloadRunner` forks and exec's the `bulkload` binary with appropriate
`--nodes` / `--relationships` / `--relationships_backward` / `--output_dir`
arguments, then waits for it to complete.

The bulkload command line is constructed from a **dataset descriptor**:

```cpp
struct DatasetDescriptor {
    std::string name;         // e.g. "ldbc-sf1"
    fs::path    data_dir;     // /source-data/ldbc/sf1
    fs::path    workspace;    // temp dir created per test run
    // vertex files: label → filename
    std::vector<std::pair<std::string, std::string>> vertices;
    // edge files: type → {fwd_file, bwd_file}
    std::vector<EdgeDesc> edges;
};
```

Workspace is a `std::filesystem::temp_directory_path()` subdirectory,
created before each test case and removed on teardown (even on failure).

---

### Verification Steps (per dataset)

After a successful `bulkload` run, open the workspace DB via **C++ internal
APIs** — the same pattern used by the existing unit tests (`TestDB`).
No C API, no query engine.

```cpp
// Open loaded DB (same as existing catalog/storage tests)
DuckDB db(workspace.string());
ClientContext ctx(db);
auto &catalog = db.GetCatalog();
auto *graph = (GraphCatalogEntry *)catalog.GetEntry(
    ctx, CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, DEFAULT_GRAPH);
REQUIRE(graph != nullptr);
```

#### 1. Label presence
Use `GraphCatalogEntry::GetVertexLabels()` and `GetEdgeTypes()`.

```cpp
vector<string> labels, edge_types;
graph->GetVertexLabels(labels);
graph->GetEdgeTypes(edge_types);

REQUIRE(contains(labels, "Person"));
REQUIRE(contains(labels, "Comment"));
REQUIRE(contains(edge_types, "knows"));
```

#### 2. Vertex / edge counts

Traverse the catalog hierarchy to sum row counts without running any query:

```
GraphCatalogEntry
 └─ GetVertexPartitionIndexesInLabel(ctx, "Person", indexes)
      → vector<idx_t>  (partition OIDs)
         └─ PartitionCatalogEntry  (one per physical partition)
              └─ sub-partition OIDs  (PropertySchemaCatalogEntry)
                   └─ ps_cat->GetNumberOfRowsApproximately()
                        → uint64_t  (row count for that sub-partition)
```

`DbVerifier` helper sums across all partitions for a label:

```cpp
uint64_t count_vertices(ClientContext &ctx, Catalog &catalog,
                         GraphCatalogEntry *graph, const string &label);
uint64_t count_edges   (ClientContext &ctx, Catalog &catalog,
                         GraphCatalogEntry *graph, const string &type);
```

Then assert against expected constants:

```cpp
// LDBC SNB official expected counts (from LDBC spec)
static constexpr uint64_t LDBC_SF1_PERSON_COUNT  = 9892;
static constexpr uint64_t LDBC_SF1_COMMENT_COUNT = 2052169;
static constexpr uint64_t LDBC_SF1_POST_COUNT     = 1003605;

REQUIRE(count_vertices(ctx, catalog, graph, "Person")  == LDBC_SF1_PERSON_COUNT);
REQUIRE(count_vertices(ctx, catalog, graph, "Comment") == LDBC_SF1_COMMENT_COUNT);
```

Note: `GetNumberOfRowsApproximately()` is what the planner/optimizer already
uses for cardinality estimation — it is stored during bulkload and reflects
actual loaded row counts.

#### 3. Property schema check
Use `PropertySchemaCatalogEntry` to verify expected column names and types.

```cpp
// Get property schemas for a label
vector<idx_t> part_indexes;
graph->GetVertexPartitionIndexesInLabel(ctx, "Person", part_indexes);
// ... navigate to PropertySchemaCatalogEntry, check GetColumnNames() / GetTypes()
```

#### 4. Forward / backward symmetry check (edge counts)
For each edge type, verify that forward and backward partition counts agree
(total adjacency list entries must match).

This detects truncated or missing backward files.

---

### Expected Count Strategy

| Dataset | Source of expected counts |
|---------|--------------------------|
| LDBC SNB | Official LDBC specification (deterministic datagen) |
| TPCH | TPC-H spec formula: `SF × base_count` per table |
| DBpedia | Generated from a manifest file alongside the HuggingFace dataset |

For TPCH, vertex/edge label names depend on how the preprocessing maps
relational tables to a graph (to be confirmed from the actual data files
before implementing the expected-count headers).

---

### File Layout

```
test/
└── bulkload/
    ├── CMakeLists.txt
    ├── bulkload_test_main.cpp       ← Catch2 main + --data-dir arg parsing
    ├── test_tpch.cpp                ← TPCH SF1, SF10 test cases
    ├── test_ldbc.cpp                ← LDBC SF1, SF10, SF100 test cases
    ├── test_dbpedia.cpp             ← DBpedia test cases
    ├── helpers/
    │   ├── dataset_locator.hpp      ← data discovery logic
    │   ├── bulkload_runner.hpp      ← exec bulkload subprocess
    │   └── db_verifier.hpp          ← internal C++ catalog helpers for count/schema checks
    └── expected/
        ├── ldbc_sf1.hpp
        ├── ldbc_sf10.hpp
        ├── ldbc_sf100.hpp
        ├── tpch_sf1.hpp
        ├── tpch_sf10.hpp
        └── dbpedia.hpp

scripts/
└── download_test_data.sh            ← huggingface_hub pull for each dataset
```

---

### CMake Integration

In `test/CMakeLists.txt` (addition, not replacing existing logic):

```cmake
option(ENABLE_BULKLOAD_TESTS "Build end-to-end bulkload test binary" OFF)

if(ENABLE_BULKLOAD_TESTS)
    add_subdirectory(bulkload)
endif()
```

In `test/bulkload/CMakeLists.txt`:

```cmake
add_executable(bulkload_test
    bulkload_test_main.cpp
    test_tpch.cpp
    test_ldbc.cpp
    test_dbpedia.cpp
)
target_link_libraries(bulkload_test turbolynx)
target_include_directories(bulkload_test PRIVATE helpers ../../third_party/catch)

# CTest registration — label "bulkload" so they are opt-in
add_test(NAME bulkload_tpch_sf1
    COMMAND bulkload_test "[bulkload][tpch][sf1]" --data-dir "${TURBOLYNX_DATA_DIR}")
add_test(NAME bulkload_ldbc_sf1
    COMMAND bulkload_test "[bulkload][ldbc][sf1]" --data-dir "${TURBOLYNX_DATA_DIR}")
# ... etc.

set_tests_properties(
    bulkload_tpch_sf1 bulkload_ldbc_sf1 ...
    PROPERTIES LABELS "bulkload" SKIP_RETURN_CODE 77)
```

CMake configure:
```bash
cmake -GNinja -DBUILD_UNITTESTS=ON -DENABLE_BULKLOAD_TESTS=ON \
      -DTURBOLYNX_DATA_DIR=/source-data ..
```

---

### Implementation Phases

| Phase | Tasks | Prerequisite |
|-------|-------|-------------|
| **9a** | Scaffold: CMake option, new dirs, `bulkload_test_main.cpp` with `--data-dir`, DatasetLocator skeleton | — |
| **9b** | BulkloadRunner: subprocess exec/wait, temp workspace management | 9a |
| **9c** | DbVerifier helpers: C++ internal catalog API for label list + row count per label | 9b |
| **9d** | LDBC-SF1 test case: label presence + count checks (counts from LDBC spec) | 9c |
| **9e** | TPCH-SF1 test case: inspect data files to determine labels + counts | 9c |
| **9f** | DBpedia test case + download script | 9c |
| **9g** | Remaining scale factors (LDBC SF10/SF100, TPCH SF10) | 9d–9f |
| **9h** | Spot-check queries, CI integration | 9g |

Start with **9a → 9d** (LDBC SF1 only) to validate the full pipeline end-to-end
before adding the remaining datasets.

---

## Notes

- Keep this file updated at milestone completion.
- One milestone at a time. Validate with `ctest` before closing a milestone.
- Build always in `turbograph-s62` container: `cd /turbograph-v3/build-lwtest && ninja`
