# Testing

TurboLynx uses [Catch2](https://github.com/catchorg/Catch2) for unit testing.
All commands below assume you are running from the repository root.

## Build with Tests

```bash
cmake -GNinja \
      -DCMAKE_BUILD_TYPE=Debug \
      -DENABLE_TCMALLOC=OFF \
      -DBUILD_UNITTESTS=ON \
      -DTBB_TEST=OFF \
      -B build-debug
cmake --build build-debug
```

## Run All Unit Tests

```bash
# Run each module separately (AND filter makes combined tags match nothing)
./build-debug/test/unittest "[catalog]"
./build-debug/test/unittest "[storage]"
./build-debug/test/unittest "[common]"
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
      -B build-bulkload
cmake --build build-bulkload --target bulkload_test
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
./build-bulkload/test/bulkload/bulkload_test [catch2-tag] --data-dir <path> [--download] [--generate]
```

| Flag | Behavior |
|------|----------|
| *(none)* | Run bulkload, verify labels + counts against `datasets.json`. Skip if data not found. |
| `--download` | Download dataset from `hf_repo` (HuggingFace) if not present locally, then proceed. |
| `--generate` | After bulkload, measure actual counts and **overwrite** expected values in `datasets.json`. Run once when adding a new dataset. |

### Typical Workflows

**Regular regression test (data already on disk):**
```bash
ctest --test-dir build-bulkload -L bulkload --output-on-failure
ctest --test-dir build-bulkload -R bulkload_ldbc_sf1 --output-on-failure   # single dataset
```

**Adding a new dataset:**
```bash
# 1. Run bulkload + generate expected counts
./build-bulkload/test/bulkload/bulkload_test "[bulkload][ldbc][sf1]" \
    --data-dir /source-data --generate

# 2. Review and commit
git diff test/bulkload/datasets.json
git add test/bulkload/datasets.json && git commit -m "chore: update expected counts"

# 3. From now on, use normal regression test
ctest --test-dir build-bulkload -L bulkload
```

### Currently Available Datasets

| Dataset | Tag | Vertices | Edges | Notes | Status |
|---------|-----|----------|-------|-------|--------|
| LDBC SF1 | `[ldbc][sf1]` | 8 labels, ~3.1M vertices | 23 types, all counts verified | — | ✅ |
| TPC-H SF1 | `[tpch][sf1]` | 7 labels, ~7.9M vertices | 8 types, all counts verified | — | ✅ |
| DBpedia | `[dbpedia]` | 1 label (NODE), ~77M vertices | 32 types (selected) | `skip_histogram: true` | ✅ |
| DBpedia mini (committed fixture) | `[bulkload][dbpedia-mini]` | ~576 NODE vertices | 3 edge types | Committed at `test/data/dbpedia-mini/`; CI default | ✅ |

Larger scale factors (LDBC SF10, SF100; TPC-H SF10) are not wired as automated CTest entries due to multi-hour load times. They can be run manually via `bulkload_test` with an appropriate `--data-dir`.

The DBpedia mini fixture is the only bulkload regression that runs in CI by default — it ships with the repo (no `--download` needed) and exercises the schemaless JSONL loader plus three CSV edge types end-to-end. Run locally with:

```bash
./build-portable/test/bulkload/bulkload_test "[bulkload][dbpedia-mini]" --data-dir test/data
```

---

## Fuzz Tests

Multi-layer fuzz harness (PR #242) lives under `test/fuzz/`. 35 tests across four layers, each with its own oracle and dependency set. All four are CMake-gated so default builds skip them.

| Layer | Binary | Tag | Oracle | External deps | CMake gate |
|---|---|---|---|---|---|
| **L1** crash / sanitizer | `fuzz_l1` | `[fuzz][l1]` | none — a crash *is* the verdict | clang + libFuzzer + ASAN/UBSAN | `BUILD_FUZZ_L1=ON` |
| **L2** metamorphic | `fuzz_l2` | `[fuzz][l2]` | self (`T(Q) == T(rewrite(Q))`) | none | `BUILD_FUZZ_L2=ON` |
| **L3** differential vs Neo4j | `fuzz_oracle` | `[fuzz][l3]` | Neo4j 5.24 via `cypher-shell` | Java 17 + Neo4j tarball | `BUILD_FUZZ_ORACLE=ON` |
| **L4** stateful CRUD | `fuzz_oracle` | `[fuzz][l4]` | Neo4j + internal `FuzzModel` invariants | Java 17 + Neo4j tarball | `BUILD_FUZZ_ORACLE=ON` |

`BUILD_FUZZ_TESTS=ON` turns all three binaries on. Default OFF — no Java/clang/fuzz toolchain is needed for `unittest` and `query_test`.

The first batch of fuzz-found bugs (#236 UNION ALL planner, #238 multi-label CREATE, #240 multi-WITH drops rows, #270 VLE/SP CREATE-only SEGV) are all closed and the harness runs skip-free in CI.

### Run

```bash
# Configure with fuzz enabled
cmake -GNinja \
      -DBUILD_FUZZ_TESTS=ON \
      -B build-fuzz
cmake --build build-fuzz

# Per binary
./build-fuzz/test/fuzz/l1_crash/fuzz_l1 -max_total_time=300
./build-fuzz/test/fuzz/l2_metamorphic/fuzz_l2 "[fuzz][l2]"
./build-fuzz/test/fuzz/oracle/fuzz_oracle "[fuzz][l4]" --seed 42 --ops 1000

# All under ctest
ctest --test-dir build-fuzz -L fuzz --output-on-failure
```

The CI gates wire the same binaries under PR-level latency budgets (~80 s total). See `PLAN.md` for layer-by-layer detail and the bug-class → layer coverage map.

---

## Sanitizer Lane (CI)

`.github/workflows/build-test.yml` runs a second job, `build-sanitize`, on every PR: a **Debug + ASan** build that re-runs `unittest`, `[types]`, `[tpch]`, and every LDBC suite under `-fsanitize=address`. It is blocking — any new ASan finding holds the PR.

```bash
cmake -GNinja \
      -DCMAKE_BUILD_TYPE=Debug \
      -DENABLE_SANITIZER=ON \
      -DENABLE_TCMALLOC=OFF \
      -DBUILD_UNITTESTS=ON \
      -DTURBOLYNX_LDBC_FIXTURE_MINI=ON \
      -DTURBOLYNX_TPCH_FIXTURE_MINI=ON \
      -B build-sanitize
cmake --build build-sanitize

ASAN_OPTIONS=halt_on_error=1:abort_on_error=1:detect_leaks=0 \
    ctest --test-dir build-sanitize --output-on-failure
```

`detect_leaks=0` is temporary while ORCA's `CMemoryPool` teardown leaks get a suppressions file; promoting to `detect_leaks=1` is tracked in #276. UBSan is intentionally off — ORCA has known pre-existing UB patterns that would dominate the signal.

---

## Query Tests

End-to-end Cypher query tests run against a preloaded LDBC SF1 database.

### Build

Query tests are built alongside unit tests when `BUILD_UNITTESTS=ON`.

### Run

```bash
./build-debug/test/query/query_test "[q1]" --db-path "/path/to/ldbc_sf1"
./build-debug/test/query/query_test "[q2]" --db-path "/path/to/ldbc_sf1"
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
3. Re-run the configure command (for example, `cmake -B build-debug`) to pick up the new file
4. Rebuild and run
