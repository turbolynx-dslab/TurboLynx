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
ctest -L bulkload                  ← opt-in label, NOT run by default
  └─ bulkload_test binary
       ├─ datasets.json            ← single source of truth (paths, files, expected counts)
       ├─ DatasetRegistry          ← parses datasets.json at startup (yyjson)
       ├─ DatasetLocator           ← resolves local_path; skips or downloads if missing
       ├─ BulkloadRunner           ← exec bulkload subprocess with args from DatasetConfig
       ├─ DbVerifier               ← opens DB via C++ internal API, checks catalog
       └─ per-dataset TEST_CASEs   ← tags hardcoded; content driven by DatasetRegistry
```

The existing `unittest` binary (unit tests) is unchanged.
A **new separate binary** `bulkload_test` is built only when
`-DENABLE_BULKLOAD_TESTS=ON` (default: OFF).

---

### `datasets.json` — Single Source of Truth

All dataset-specific knowledge lives in one JSON file checked into the repo:
`test/bulkload/datasets.json`.

This file defines **what to load** and **what to expect** for each dataset
and scale factor. Adding a new dataset or adjusting expected counts requires
editing only this file — no recompile needed.

#### Schema

```json
{
  "datasets": [
    {
      "name":       "ldbc-sf1",
      "tags":       ["ldbc", "sf1"],
      "hf_repo":    "HuggignHajae/TurboLynx-LDBC-SF1",
      "local_path": "ldbc/sf1",

      "vertices": [
        {
          "label":          "Person",
          "files":          ["dynamic/person.csv"],
          "expected_count": 9892
        },
        {
          "label":          "Comment",
          "files":          ["dynamic/comment.csv"],
          "expected_count": 2052169
        }
      ],

      "edges": [
        {
          "type":               "knows",
          "fwd_files":          ["dynamic/person_knows_person.csv"],
          "bwd_files":          ["dynamic/person_knows_person.csv.backward"],
          "expected_fwd_count": 180623
        }
      ]
    },
    {
      "name":       "ldbc-sf10",
      "tags":       ["ldbc", "sf10"],
      "hf_repo":    "HuggignHajae/TurboLynx-LDBC-SF10",
      "local_path": "ldbc/sf10",
      "vertices":   [ ... ],
      "edges":      [ ... ]
    },
    {
      "name":       "tpch-sf1",
      "tags":       ["tpch", "sf1"],
      "hf_repo":    "HuggignHajae/TurboLynx-TPCH-SF1",
      "local_path": "tpch/sf1",
      "vertices":   [ ... ],
      "edges":      [ ... ]
    },
    {
      "name":       "dbpedia",
      "tags":       ["dbpedia"],
      "hf_repo":    "HuggignHajae/TurboLynx-DBPEDIA",
      "local_path": "dbpedia",
      "vertices":   [ ... ],
      "edges":      [ ... ]
    }
  ]
}
```

#### Fields

| Field | Description |
|-------|-------------|
| `name` | Unique identifier; used by test cases to look up config |
| `tags` | Catch2 tags for filtering (e.g. `["ldbc","sf1"]`) |
| `hf_repo` | HuggingFace repo to pull from when auto-download is enabled |
| `local_path` | Path relative to data root (`TURBOLYNX_DATA_DIR`) |
| `vertices[].label` | Vertex label as it appears in the CSV header `:ID(Label)` |
| `vertices[].files` | List of CSV files for this label (relative to `local_path`) |
| `vertices[].expected_count` | Expected total vertex count after bulkload |
| `edges[].type` | Edge type label |
| `edges[].fwd_files` | Forward CSV files |
| `edges[].bwd_files` | Backward CSV files |
| `edges[].expected_fwd_count` | Expected forward adjacency list size |

> **Note:** `files` is a list to support datasets where a label is split
> across multiple CSV files (common in LDBC SF100).

---

### Data Discovery & Download

Priority order when resolving a dataset's `local_path`:

1. **Env var** `TURBOLYNX_DATA_DIR` — explicit override
2. **CLI arg** `--data-dir <path>` passed to `bulkload_test`
3. **Default** `/source-data` (available inside `turbograph-s62` container)

Resolved as: `<data_root> / <dataset.local_path>`.

If the resolved directory does not exist:

- **Default:** skip the test with a `WARN` message; exit with `SKIP_RETURN_CODE 77`.
  CTest treats code 77 as SKIP (not failure) — CI is not blocked.
- **Opt-in auto-download:** if `TURBOLYNX_AUTO_DOWNLOAD=1` is set, invoke
  `scripts/download_test_data.sh <hf_repo> <target_dir>` before running the test.

```bash
# Run all bulkload tests (data must be in /source-data):
ctest -L bulkload

# Run only LDBC tests:
ctest -L bulkload -R ldbc

# Run with custom data root:
TURBOLYNX_DATA_DIR=/mnt/data ctest -L bulkload

# Run with auto-download:
TURBOLYNX_AUTO_DOWNLOAD=1 ctest -L bulkload

# Run a single dataset directly from the binary:
./test/bulkload_test "[bulkload][ldbc][sf1]" --data-dir /source-data
```

---

### Test Structure

Test case tags are **hardcoded** in C++ for compile-time Catch2/CTest
filtering. The actual implementation reads from `DatasetRegistry` at
runtime.

```cpp
// test_ldbc.cpp
TEST_CASE("Bulkload LDBC SF1", "[bulkload][ldbc][sf1]") {
    const auto& cfg = DatasetRegistry::get("ldbc-sf1");

    auto data_dir = DatasetLocator::resolve(cfg, g_data_dir);
    if (!data_dir) {
        WARN("Dataset not found: " + cfg.name + ". Skipping.");
        return;  // exit code 77 → SKIP
    }

    auto ws = BulkloadRunner::run(cfg, *data_dir);    // exec bulkload subprocess
    REQUIRE(ws.exit_code == 0);

    DbVerifier v(ws.workspace_path);
    v.check_labels(cfg);   // all labels/edge types present
    v.check_counts(cfg);   // GetNumberOfRowsApproximately() per label
    v.check_symmetry(cfg); // fwd edge count == bwd edge count per type
}
```

`DatasetRegistry` is populated at program startup by parsing `datasets.json`
with yyjson. The JSON file path is embedded at compile time as
`TEST_DATASETS_JSON` (set via CMake `configure_file` or `target_compile_definitions`).

---

### BulkloadRunner

Constructs and executes the `bulkload` command from a `DatasetConfig`:

```cpp
// Equivalent shell command:
// bulkload
//   --nodes Person   ldbc/sf1/dynamic/person.csv
//   --nodes Comment  ldbc/sf1/dynamic/comment.csv
//   --relationships       knows  ldbc/sf1/dynamic/person_knows_person.csv
//   --relationships_backward knows  ldbc/sf1/dynamic/person_knows_person.csv.backward
//   --output_dir /tmp/turbolynx_test_abc123
```

- Workspace is created as a `mkdtemp` temp directory before each test.
- Workspace is removed in teardown (even on failure) using RAII.
- `bulkload` binary path: resolved from `CMAKE_BINARY_DIR/tools/bulkload` via
  `TEST_BULKLOAD_BIN` compile definition.

---

### DbVerifier — Verification Steps

Opens the loaded workspace directly as a `DuckDB` instance (C++ internal API).
No Cypher, no query engine.

```cpp
DuckDB db(workspace_path);
ClientContext ctx(db);
auto& catalog = db.GetCatalog();
auto* graph = (GraphCatalogEntry*)catalog.GetEntry(
    ctx, CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, DEFAULT_GRAPH);
REQUIRE(graph != nullptr);
```

#### 1. Label presence

```cpp
vector<string> actual_labels, actual_edges;
graph->GetVertexLabels(actual_labels);
graph->GetEdgeTypes(actual_edges);
for (auto& v : cfg.vertices) REQUIRE(contains(actual_labels, v.label));
for (auto& e : cfg.edges)    REQUIRE(contains(actual_edges,  e.type));
```

#### 2. Count verification

Catalog traversal (no query engine):

```
GraphCatalogEntry
 └─ GetVertexPartitionIndexesInLabel(ctx, label, indexes)
      → vector<idx_t>  (partition OIDs)
         └─ PartitionCatalogEntry
              └─ sub-partition OIDs → PropertySchemaCatalogEntry
                   └─ GetNumberOfRowsApproximately() → uint64_t
```

```cpp
for (auto& v : cfg.vertices) {
    uint64_t actual = count_vertices(ctx, catalog, graph, v.label);
    REQUIRE(actual == v.expected_count);
}
```

#### 3. Forward / backward symmetry

```cpp
for (auto& e : cfg.edges) {
    uint64_t fwd = count_fwd_edges(ctx, catalog, graph, e.type);
    uint64_t bwd = count_bwd_edges(ctx, catalog, graph, e.type);
    REQUIRE(fwd == bwd);
    if (e.expected_fwd_count > 0)
        REQUIRE(fwd == e.expected_fwd_count);
}
```

---

### File Layout

```
test/bulkload/
├── CMakeLists.txt
├── datasets.json                    ← source of truth; edit to add datasets / update counts
├── bulkload_test_main.cpp           ← main(), --data-dir arg, DatasetRegistry init
├── test_ldbc.cpp                    ← TEST_CASEs for ldbc-sf1, sf10, sf100
├── test_tpch.cpp                    ← TEST_CASEs for tpch-sf1, sf10
├── test_dbpedia.cpp                 ← TEST_CASEs for dbpedia
└── helpers/
    ├── dataset_registry.hpp         ← parse datasets.json (yyjson), store DatasetConfig
    ├── dataset_locator.hpp          ← resolve local_path; skip or trigger download
    ├── bulkload_runner.hpp          ← exec bulkload subprocess, return workspace path
    └── db_verifier.hpp              ← C++ catalog helpers: count_vertices, count_edges, etc.

scripts/
└── download_test_data.sh            ← huggingface_hub pull: $1=hf_repo $2=target_dir
```

---

### CMake Integration

`test/CMakeLists.txt` (append, no changes to existing tests):

```cmake
option(ENABLE_BULKLOAD_TESTS "Build E2E bulkload test binary" OFF)
set(TURBOLYNX_DATA_DIR "/source-data" CACHE PATH "Root directory for benchmark datasets")

if(ENABLE_BULKLOAD_TESTS)
    add_subdirectory(bulkload)
endif()
```

`test/bulkload/CMakeLists.txt`:

```cmake
set(DATASETS_JSON "${CMAKE_CURRENT_SOURCE_DIR}/datasets.json")
set(BULKLOAD_BIN  "$<TARGET_FILE:bulkload>")

add_executable(bulkload_test
    bulkload_test_main.cpp
    test_ldbc.cpp
    test_tpch.cpp
    test_dbpedia.cpp
)
target_link_libraries(bulkload_test turbolynx)
target_include_directories(bulkload_test PRIVATE helpers ../../../third_party/catch)
target_compile_definitions(bulkload_test PRIVATE
    TEST_DATASETS_JSON="${DATASETS_JSON}"
    TEST_BULKLOAD_BIN="${BULKLOAD_BIN}"
    TEST_DEFAULT_DATA_DIR="${TURBOLYNX_DATA_DIR}"
)

# CTest: one entry per dataset; label "bulkload" makes them opt-in
foreach(DS ldbc_sf1 ldbc_sf10 ldbc_sf100 tpch_sf1 tpch_sf10 dbpedia)
    string(REPLACE "_" ";" TAG_LIST ${DS})   # ldbc_sf1 → ldbc;sf1
    list(JOIN TAG_LIST "][" TAG_STR)         # → ldbc][sf1
    add_test(NAME bulkload_${DS}
        COMMAND bulkload_test "[bulkload][${TAG_STR}]")
    set_tests_properties(bulkload_${DS} PROPERTIES
        LABELS "bulkload"
        SKIP_RETURN_CODE 77
        ENVIRONMENT "TURBOLYNX_DATA_DIR=${TURBOLYNX_DATA_DIR}")
endforeach()
```

Build & run:

```bash
# Configure
cmake -GNinja -DBUILD_UNITTESTS=ON -DENABLE_BULKLOAD_TESTS=ON \
      -DTURBOLYNX_DATA_DIR=/source-data \
      /turbograph-v3 -B /turbograph-v3/build-lwtest

# Build
cd /turbograph-v3/build-lwtest && ninja bulkload_test

# Run all bulkload tests
ctest -L bulkload --output-on-failure

# Run only LDBC SF1
ctest -R bulkload_ldbc_sf1 --output-on-failure
```

---

### Implementation Phases

| Phase | Tasks | Prerequisite |
|-------|-------|-------------|
| **9a** | `datasets.json` skeleton (LDBC SF1 entry only); CMake option + `bulkload_test_main.cpp`; `DatasetRegistry` (yyjson parse) | — |
| **9b** | `DatasetLocator` (resolve path, SKIP logic, auto-download hook); `BulkloadRunner` (subprocess fork/exec/wait, temp workspace) | 9a |
| **9c** | `DbVerifier` (DuckDB open, `count_vertices`, `count_edges`, `check_symmetry`) | 9b |
| **9d** | `test_ldbc.cpp` LDBC SF1 test case end-to-end; fill expected counts from LDBC spec | 9c |
| **9e** | `datasets.json` TPCH SF1 entry (inspect data files for labels); `test_tpch.cpp` | 9c |
| **9f** | `datasets.json` DBpedia entry; `test_dbpedia.cpp`; `download_test_data.sh` | 9c |
| **9g** | Add SF10/SF100/SF10 entries to `datasets.json`; add TEST_CASEs | 9d–9f |
| **9h** | CI label notes in README; PLAN.md milestone closed | 9g |

Start with **9a → 9d** (LDBC SF1 end-to-end) to validate the full pipeline
before adding remaining datasets.

---

## Notes

- Keep this file updated at milestone completion.
- One milestone at a time. Validate with `ctest` before closing a milestone.
- Build always in `turbograph-s62` container: `cd /turbograph-v3/build-lwtest && ninja`
