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

## Run All Unit Tests

```bash
# Run each module separately (AND filter makes combined tags match nothing)
./test/unittest "[catalog]"
./test/unittest "[storage]"
./test/unittest "[common]"
```

## Test Modules

| Module | Tag | Tests | Description |
|---|---|---|---|
| common | `[common]` | 10 | Value types, DataChunk, SimpleHistogram, CpuTimer |
| catalog | `[catalog]` | 51 | Schema/Graph/Partition CRUD, catalog persistence round-trip |
| storage | `[storage]` | 68 | BufferPool (17), CCM, extent I/O, multi-process locking |

---

## Bulkload Tests

Runs the `bulkload` binary against real benchmark datasets and verifies the loaded graph is structurally correct (labels, edge types, vertex counts, edge counts).

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

`test/bulkload/datasets.json` is the single source of truth: which files to load, download location, and expected counts after loading.

```json
{
  "datasets": [{
    "name":           "ldbc-sf1",
    "hf_repo":        "HuggignHajae/TurboLynx-LDBC-SF1",
    "local_path":     "ldbc/sf1",
    "skip_histogram": false,
    "vertices": [
      { "label": "Person", "files": ["dynamic/Person.csv"], "expected_count": 9892 }
    ],
    "edges": [
      { "type": "KNOWS",
        "fwd_files": ["dynamic/Person_knows_Person.csv"],
        "bwd_files": ["dynamic/Person_knows_Person.csv.backward"],
        "expected_fwd_count": 2889968 }
    ]
  }]
}
```

- `expected_count: 0` (or omitted) skips vertex count verification.
- `expected_fwd_count: 0` (or omitted) skips edge count verification.
- `skip_histogram: true` passes `--skip-histogram` to the `bulkload` binary.

### Run Modes

```
bulkload_test [catch2-tag] --data-dir <path> [--download] [--generate]
```

| Flag | Behavior |
|------|----------|
| *(none)* | Run bulkload, verify labels + counts against `datasets.json`. Skip if data not found. |
| `--download` | Download dataset from `hf_repo` (HuggingFace) if not present locally, then proceed. |
| `--generate` | After bulkload, measure actual counts and **overwrite** expected values in `datasets.json`. Run once when adding a new dataset. |

### Typical Workflows

**Regular regression test (data already on disk):**
```bash
ctest -L bulkload --output-on-failure
ctest -R bulkload_ldbc_sf1 --output-on-failure   # single dataset
```

**Adding a new dataset:**
```bash
# 1. Run bulkload + generate expected counts
./test/bulkload/bulkload_test "[bulkload][ldbc][sf1]" \
    --data-dir /source-data --generate

# 2. Review and commit
git diff test/bulkload/datasets.json
git add test/bulkload/datasets.json && git commit -m "chore: update expected counts"

# 3. From now on, use normal regression test
ctest -L bulkload
```

### Currently Available Datasets

| Dataset | Tag | Vertices | Edges | Notes | Status |
|---------|-----|----------|-------|-------|--------|
| LDBC SF1 | `[ldbc][sf1]` | 8 labels, ~3.1M vertices | 23 types, all counts verified | — | ✅ |
| TPC-H SF1 | `[tpch][sf1]` | 7 labels, ~7.9M vertices | 8 types, all counts verified | — | ✅ |
| DBpedia | `[dbpedia]` | 1 label (NODE), ~77M vertices | 32 types (selected) | `skip_histogram: true` | ✅ |

Larger scale factors (LDBC SF10, SF100; TPC-H SF10) are not wired as automated CTest entries due to multi-hour load times. They can be run manually via `bulkload_test` with an appropriate `--data-dir`.

---

## Query Tests

End-to-end Cypher query tests run against a preloaded LDBC SF1 database.

### Build

Query tests are built alongside unit tests when `BUILD_UNITTESTS=ON`.

### Run

```bash
./test/query/query_test "[q1]" --db-path "/path/to/ldbc_sf1"
./test/query/query_test "[q2]" --db-path "/path/to/ldbc_sf1"
```

### Query Test Coverage

| Suite | Tag | Tests | Description |
|-------|-----|-------|-------------|
| LDBC Q1 | `[q1]` | 21 | Node scans, filtering, aggregation, multi-hop traversal |
| LDBC Q2 | `[q2]` | 9 | Edge traversal with inline property filters, projections |

---

## Adding Tests

1. Add a `.cpp` file to the appropriate `test/<module>/` directory
2. Tag the test case: `TEST_CASE("description", "[module-tag]")`
3. Re-run `cmake .` to pick up the new file
4. Rebuild and run
