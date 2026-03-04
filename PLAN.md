# TurboLynx — Execution Plan

## Current Status

Core build is stable. Catalog, Storage, Execution layers tested.
Build runs inside `turbograph-s62` Docker container.

LDBC SF1 E2E bulkload test (Milestone 9d) is passing. `datasets.json` has verified expected counts.
Next: TPCH SF1 (9e), DBpedia (9f), larger scale factors (9g).

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
| 9a–9d | E2E bulkload test: LDBC SF1 | ✅ Done |

---

## Milestone 9 — End-to-End Bulkload Test Suite

### Goal

Run `bulkload` against real benchmark datasets (LDBC, TPCH, DBpedia) and
verify the loaded graph is structurally correct: right labels, right counts,
correct property schema.

---

### `datasets.json` — Single Source of Truth

`test/bulkload/datasets.json` is checked into the repo and drives everything:
which files to load, how to download, and what to expect after loading.

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
    }
  ]
}
```

`expected_count`가 0이거나 필드가 없으면 카운트 검증을 건너뜀 (새 데이터셋 추가 시 초기 상태).

---

### 실행 모드

```
bulkload_test <catch2-tag> --data-dir <path> [--download] [--generate]
```

| 모드 | 설명 |
|------|------|
| 기본 | 데이터 있으면 bulkload → 검증. 없으면 SKIP. |
| `--download` | 데이터 없을 때 `hf_repo`에서 다운로드 후 진행. `datasets.json` 변경 없음. |
| `--generate` | bulkload 후 실제 카운트를 측정해 `datasets.json`의 `expected_count`를 **덮어씀**. 새 데이터셋 추가 시 1회 실행. |
| `--download --generate` | 다운로드 + generate 동시 수행. |

#### 워크플로우 예시

**평소 회귀 테스트:**
```bash
ctest -L bulkload                            # /source-data 에 데이터 있어야 함
ctest -L bulkload --data-dir /mnt/data       # 경로 지정
ctest -R bulkload_ldbc_sf1                   # 특정 데이터셋만
```

**처음 환경 구축 (데이터 없음):**
```bash
# 1. 다운로드
./test/bulkload_test "[bulkload]" --data-dir /source-data --download

# 2. expected_count 아직 없으면 generate로 JSON 업데이트
./test/bulkload_test "[bulkload][ldbc][sf1]" --data-dir /source-data --generate

# 3. git diff로 확인 후 커밋
git diff test/bulkload/datasets.json
git add test/bulkload/datasets.json && git commit -m "chore: update expected counts for ldbc-sf1"

# 4. 이후부터는 일반 회귀 테스트로 실행
ctest -L bulkload
```

---

### 컴포넌트 구조

```
bulkload_test 시작
  └─ DatasetRegistry::load(datasets.json)   ← yyjson으로 파싱, 메모리에 보유
       └─ TEST_CASE("[bulkload][ldbc][sf1]")
            ├─ DatasetLocator::find(cfg, data_dir)
            │    → 경로 있으면 진행 / 없으면 SKIP (또는 --download 시 다운로드)
            ├─ BulkloadRunner::run(cfg, data_dir)
            │    → bulkload 바이너리 subprocess 실행
            │    → --nodes / --relationships 인자를 cfg.vertices / cfg.edges 에서 구성
            │    → mkdtemp로 임시 workspace 생성, 완료 후 RAII로 삭제
            └─ DbVerifier::verify(workspace, cfg)   (또는 --generate 시 write-back)
                 → DuckDB + C++ 내부 API로 카탈로그 직접 조회
                 → 레이블 존재 확인
                 → 카운트 비교 (GetNumberOfRowsApproximately())
                 → fwd/bwd 대칭 확인
```

---

### DbVerifier — 검증 방법

Cypher 없이 C++ 내부 카탈로그 API만 사용.

```cpp
DuckDB db(workspace_path);
ClientContext ctx(db);
auto* graph = (GraphCatalogEntry*)catalog.GetEntry(
    ctx, CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, DEFAULT_GRAPH);

// 1. 레이블 존재
graph->GetVertexLabels(labels);   // cfg의 모든 label 포함 여부 확인
graph->GetEdgeTypes(edge_types);

// 2. 카운트 (파티션 순회, 쿼리 불필요)
// GraphCatalogEntry → GetVertexPartitionIndexesInLabel()
//   → PartitionCatalogEntry → PropertySchemaCatalogEntry
//       → GetNumberOfRowsApproximately()  ← 합산
uint64_t actual = count_vertices(ctx, catalog, graph, "Person");
REQUIRE(actual == cfg.vertex("Person").expected_count);

// 3. fwd/bwd 대칭
REQUIRE(count_fwd_edges(...) == count_bwd_edges(...));
```

`--generate` 모드는 `REQUIRE` 대신 `expected_count`에 `actual`을 기록한 뒤
yyjson mutation API로 `datasets.json`을 제자리에서 수정.

---

### 파일 구조

```
test/bulkload/
├── CMakeLists.txt
├── datasets.json                    ← 유일한 데이터셋 설정 파일
├── bulkload_test_main.cpp           ← main(), --data-dir / --download / --generate 파싱
├── test_ldbc.cpp                    ← ldbc-sf1, sf10, sf100 TEST_CASEs
├── test_tpch.cpp                    ← tpch-sf1, sf10 TEST_CASEs
├── test_dbpedia.cpp
└── helpers/
    ├── dataset_registry.hpp         ← yyjson 파싱, DatasetConfig 보관
    ├── dataset_locator.hpp          ← 경로 해석, 다운로드 트리거
    ├── bulkload_runner.hpp          ← subprocess fork/exec/wait, workspace 관리
    └── db_verifier.hpp              ← count_vertices / count_edges / write-back

scripts/
└── download_test_data.sh            ← huggingface_hub 사용: $1=hf_repo $2=target_dir
```

---

### CMake

`test/CMakeLists.txt`에 추가 (기존 unittest 변경 없음):

```cmake
option(ENABLE_BULKLOAD_TESTS "Build E2E bulkload test binary" OFF)
set(TURBOLYNX_DATA_DIR "/source-data" CACHE PATH "Root directory for benchmark datasets")

if(ENABLE_BULKLOAD_TESTS)
    add_subdirectory(bulkload)
endif()
```

`test/bulkload/CMakeLists.txt`:

```cmake
add_executable(bulkload_test
    bulkload_test_main.cpp test_ldbc.cpp test_tpch.cpp test_dbpedia.cpp)
target_link_libraries(bulkload_test turbolynx)
target_compile_definitions(bulkload_test PRIVATE
    TEST_DATASETS_JSON="${CMAKE_CURRENT_SOURCE_DIR}/datasets.json"
    TEST_BULKLOAD_BIN="$<TARGET_FILE:bulkload>"
    TEST_DEFAULT_DATA_DIR="${TURBOLYNX_DATA_DIR}")

foreach(DS ldbc_sf1 ldbc_sf10 ldbc_sf100 tpch_sf1 tpch_sf10 dbpedia)
    string(REPLACE "_" "][" TAG ${DS})
    add_test(NAME bulkload_${DS}
        COMMAND bulkload_test "[bulkload][${TAG}]" --data-dir "${TURBOLYNX_DATA_DIR}")
    set_tests_properties(bulkload_${DS} PROPERTIES
        LABELS "bulkload" SKIP_RETURN_CODE 77)
endforeach()
```

빌드 및 실행:

```bash
cmake -GNinja -DBUILD_UNITTESTS=ON -DENABLE_BULKLOAD_TESTS=ON \
      -DTURBOLYNX_DATA_DIR=/source-data ..
ninja bulkload_test
ctest -L bulkload --output-on-failure
```

---

### 구현 순서

| 단계 | 내용 | 선행 | 상태 |
|------|------|------|------|
| **9a** | `datasets.json` 스켈레톤 (ldbc-sf1만); CMake 옵션; `bulkload_test_main.cpp` (`--data-dir` / `--download` / `--generate` 파싱); `DatasetRegistry` yyjson 파싱 | — | ✅ Done |
| **9b** | `DatasetLocator` (경로 확인, SKIP, 다운로드); `BulkloadRunner` (subprocess, temp workspace) | 9a | ✅ Done |
| **9c** | `DbVerifier` (`count_vertices`, `check_labels`, `check_edge_types`, `--generate` write-back) | 9b | ✅ Done |
| **9d** | `test_ldbc.cpp` LDBC SF1 end-to-end; `--generate`로 expected_count 채운 뒤 커밋 | 9c | ✅ Done |
| **9e** | TPCH SF1: 실제 데이터 파일 보고 `datasets.json` 채움; `test_tpch.cpp` | 9c | ⬜ Next |
| **9f** | DBpedia; `download_test_data.sh` | 9c | ⬜ |
| **9g** | SF10 / SF100 나머지 스케일 팩터 추가 | 9d–9f | ⬜ |

### 9d에서 발견·수정된 버그

**카탈로그 OID 충돌 (SIGSEGV on `GetPropertySchemaIDs`):**

`catalog.bin`을 OID 오름차순으로 저장하므로 EXTENT(477)이 PARTITION(479)보다 먼저
로드된다. `ExtentCatalogEntry::Deserialize`가 카운터 스티어링 없이 ChunkDef를 생성하면
OID 479 슬롯을 ChunkDef가 먼저 점유하고, 이후 PARTITION(479)의 `insert()`가 묵묵히
실패 → `oid_to_catalog_entry_array[479]` = ChunkDef → 잘못된 포인터 캐스팅 → SIGSEGV.

**수정 (2026-03-04):**
- `extent_catalog_entry.cpp` `Deserialize`: `chunks.push_back(cdf->oid)` → 이름에서
  파싱한 ID 저장 (`std::stoull(cdf_name.substr(sizeof("cdf_") - 1))`).
  Serialize가 OID가 아닌 이름으로 ChunkDef를 조회하도록 보장.
- `schema_catalog_entry.cpp` `AddEntry`: `insert` → `insert_or_assign`.
  카운터-스티어된 진짜 엔트리(Partition)가 먼저 도착한 ChunkDef를 덮어쓰도록 보장.

---

## Notes

- Keep this file updated at milestone completion.
- One milestone at a time. Validate with `ctest` before closing a milestone.
- Build always in `turbograph-s62` container: `cd /turbograph-v3/build-lwtest && ninja`
