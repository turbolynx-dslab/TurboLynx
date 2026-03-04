# Testing

TurboLynx uses [Catch2](https://github.com/catchorg/Catch2) for unit testing.

## Build with Tests

```bash
cmake -GNinja \
      -DCMAKE_BUILD_TYPE=Debug \
      -DENABLE_TCMALLOC=OFF \
      -DBUILD_UNITTESTS=ON \
      -DTBB_TEST=OFF \
      ..
ninja
```

## Run All Tests

```bash
ctest --output-on-failure
```

## Run a Specific Module

```bash
# Via CTest
ctest -R catalog --output-on-failure

# Via Catch2 tag filter
./test/unittest "[catalog]"
./test/unittest "[common]"
./test/unittest "[storage]"
```

## Helper Script

```bash
bash scripts/run_tests.sh              # build + run all
bash scripts/run_tests.sh catalog      # run [catalog] module only
bash scripts/run_tests.sh rebuild      # clean build + run all
```

## Test Modules

| Module | Tag | Tests | Description |
|---|---|---|---|
| common | `[common]` | 10 | Value types, DataChunk, SimpleHistogram, CpuTimer |
| catalog | `[catalog]` | 51 | Schema/Graph/Partition CRUD, catalog persistence |
| storage | `[storage]` | 15 | Extent and ChunkDefinition catalog entries |
| execution | `[execution]` | — | Physical operator tests (TBD) |

---

## Bulkload Tests

Runs the `bulkload` binary against real benchmark datasets and verifies the loaded graph is structurally correct (labels, edge types, vertex counts).

### Build

```bash
cmake -GNinja \
      -DBUILD_UNITTESTS=ON \
      -DENABLE_BULKLOAD_TESTS=ON \
      -DTURBOLYNX_DATA_DIR=/source-data \
      ..
ninja bulkload_test
```

`TURBOLYNX_DATA_DIR` is the root directory where datasets are stored locally (default: `/source-data`).

### Dataset Configuration — `datasets.json`

`test/bulkload/datasets.json` is the single source of truth: which files to load, download location, and expected counts after loading. It is checked into the repository.

```json
{
  "datasets": [{
    "name":       "ldbc-sf1",
    "hf_repo":    "HuggignHajae/TurboLynx-LDBC-SF1",
    "local_path": "ldbc/sf1",
    "vertices": [
      { "label": "Person", "files": ["dynamic/Person.csv"], "expected_count": 9892 }
    ],
    "edges": [
      { "type": "KNOWS", "fwd_files": ["dynamic/Person_knows_Person.csv"],
                         "bwd_files": ["dynamic/Person_knows_Person.csv.backward"] }
    ]
  }]
}
```

`expected_count: 0` (or omitted) skips count verification — useful when adding a new dataset before running `--generate`.

### Run Modes

```
bulkload_test [catch2-tag] --data-dir <path> [--download] [--generate]
```

| Flag | Behavior |
|------|----------|
| *(none)* | Run bulkload, verify labels + counts against `datasets.json`. Skip if data not found. |
| `--download` | Download dataset from `hf_repo` (HuggingFace) if not present locally, then proceed. |
| `--generate` | After bulkload, measure actual counts and **overwrite** `expected_count` in `datasets.json`. Run once when adding a new dataset. |

### Typical Workflows

**Regular regression test (data already on disk):**
```bash
ctest -L bulkload --output-on-failure
ctest -R bulkload_ldbc_sf1 --output-on-failure   # single dataset
```

**Adding a new dataset (first time):**
```bash
# 1. Run bulkload + generate expected counts
./test/bulkload/bulkload_test "[bulkload][ldbc][sf1]" \
    --data-dir /source-data --generate

# 2. Review changes and commit
git diff test/bulkload/datasets.json
git add test/bulkload/datasets.json
git commit -m "chore: update expected counts for ldbc-sf1"

# 3. From now on, use normal regression test
ctest -L bulkload
```

**First-time setup without data:**
```bash
./test/bulkload/bulkload_test "[bulkload]" \
    --data-dir /source-data --download --generate
```

### How Verification Works

`DbVerifier` uses TurboLynx's internal C++ catalog API directly — no Cypher queries needed.

1. Opens the bulkloaded workspace via `DuckDB` + `ClientContext`
2. Checks all expected vertex labels exist (`GraphCatalogEntry::GetVertexLabels`)
3. Checks all expected edge types exist (`GraphCatalogEntry::GetEdgeTypes`)
4. For each vertex label, sums row counts across its property schemas (`GetNumberOfRowsApproximately`) and compares to `expected_count`

### Currently Available Datasets

| Dataset | Tag | Vertices | Edges | Status |
|---------|-----|----------|-------|--------|
| LDBC SF1 | `[ldbc][sf1]` | 8 labels, ~3.1M vertices | 23 types | ✅ verified |

### File Structure

```
test/bulkload/
├── CMakeLists.txt
├── datasets.json                  ← single source of truth
├── bulkload_test_main.cpp         ← main(), CLI flag parsing
├── test_ldbc.cpp                  ← LDBC SF1 TEST_CASE
└── helpers/
    ├── dataset_registry.hpp       ← yyjson parsing, DatasetConfig
    ├── dataset_locator.hpp        ← path resolution, download trigger
    ├── bulkload_runner.hpp        ← fork/exec bulkload, temp workspace RAII
    └── db_verifier.hpp            ← label/count verification, --generate write-back
```

---

## Adding Tests

1. Add a `.cpp` file to the appropriate `test/<module>/` directory
2. Tag the test case: `TEST_CASE("description", "[module-tag]")`
3. Re-run `cmake .` to pick up the new file
4. Rebuild and run
