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

## Adding Tests

1. Add a `.cpp` file to the appropriate `test/<module>/` directory
2. Tag the test case: `TEST_CASE("description", "[module-tag]")`
3. Re-run `cmake .` to pick up the new file
4. Rebuild and run
