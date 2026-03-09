# TurboLynx — Execution Plan

## Current Status

Core build is stable. Catalog, Storage, Execution layers tested.
Build runs inside `turbograph-s62` Docker container.

LDBC SF1 + TPC-H SF1 + DBpedia bulkload E2E tests all passing.
Milestone 14 (LightningClient Dead Code Removal) complete.

**Active milestone: 13 — LDBC Query Test Suite** (Stage 1 Q1-10~21 edge count 버그 미해결)
**Next: 16 — Multi-Process Read/Write 지원**

---

## Completed Milestones

| # | Milestone | 핵심 변경 | Status |
|---|-----------|-----------|--------|
| 1 | Remove Velox dependency | Velox 빌드 제거, 대체 구현 인라인 | ✅ |
| 2 | Remove Boost dependency | Boost 헤더/라이브러리 전면 제거 | ✅ |
| 3 | Remove Python3 dependency | Python 바인딩 제거, 빌드 단순화 | ✅ |
| 4 | Bundle TBB / libnuma / hwloc | 시스템 의존성 → 소스 번들로 교체 | ✅ |
| 5 | Single-file block store (`store.db`) | 컬럼별 개별 파일 → `store.db` 단일 파일 + `base_offset_` region 분할 | ✅ |
| 6 | Test suite: catalog, storage, execution | `[catalog]` `[storage]` `[execution]` 단위 테스트 51개 | ✅ |
| 7 | Remove libaio-dev system dependency | `libaio` → 직접 `io_submit` syscall, 시스템 패키지 불필요 | ✅ |
| 8 | Rename library | `libs62gdb.so` → `libturbolynx.so`, CAPI 정리 | ✅ |
| 9 | E2E bulkload test suite | LDBC SF1 / TPC-H SF1 / DBpedia 전체 검증 자동화 | ✅ |
| 10 | Extract `BulkloadPipeline` | `tools/bulkload.cpp` 모놀리식 → `BulkloadPipeline` 클래스 분리 | ✅ |
| 11 | Multi-client support (prototype) | `g_connections` 맵으로 세션 분리, `s62_connect` CAPI | ✅ |
| 12 | Bulkload performance optimization | 백그라운드 flush 스레드, ThrottleIfNeeded, fwd/bwd interleave, DBpedia OOM 수정 (12a–12h) | ✅ |
| 13 | LDBC Query Test Suite | Stage 2–5 (30/30) ✅, Stage 1 Q1-01~09 ✅, Q1-10~21 edge count 0 반환 (미해결) | 🔄 |
| 14 | LightningClient Dead Code 제거 | MultiPut/Get/Update, ObjectLog, UndoLogDisk, MPK, 생성자 파라미터 전부 제거 | ✅ |
| 15 | Catalog ChunkDefinitionID 복원 수정 | Serialize에 cdf_id 직접 기록 → Deserialize 파싱 오류 수정, 51개 catalog 테스트 전체 패스 | ✅ |

---

## Milestone 13 — LDBC Query Test Suite

### Goal

LDBC SF1 DB 위에서 Cypher 쿼리를 실행하고 결과를 검증하는 단위 테스트 스위트.
쿼리 엔진의 정확성과 안정성을 커버리지 있게 검증하는 것이 목표.

Expected values below are ground truth from Neo4j 5.24.0 loaded with the same LDBC SF1 dataset.

---

### Edge Type Names (Our System vs Neo4j)

Our system uses fine-grained edge types split by source/target label:

| Our Type | Neo4j | Direction |
|----------|-------|-----------|
| `HAS_CREATOR` | `HAS_CREATOR` | Comment→Person |
| `POST_HAS_CREATOR` | `HAS_CREATOR` | Post→Person |
| `REPLY_OF` | `REPLY_OF` | Comment→Post |
| `REPLY_OF_COMMENT` | `REPLY_OF` | Comment→Comment |
| `LIKES` | `LIKES` | Person→Comment |
| `LIKES_POST` | `LIKES` | Person→Post |
| `HAS_TAG` | `HAS_TAG` | Comment→Tag |
| `POST_HAS_TAG` | `HAS_TAG` | Post→Tag |
| `FORUM_HAS_TAG` | `HAS_TAG` | Forum→Tag |
| `COMMENT_IS_LOCATED_IN` | `IS_LOCATED_IN` | Comment→Place |
| `POST_IS_LOCATED_IN` | `IS_LOCATED_IN` | Post→Place |
| `ORG_IS_LOCATED_IN` | `IS_LOCATED_IN` | Organisation→Place |
| `IS_LOCATED_IN` | `IS_LOCATED_IN` | Person→Place |

---

## Milestone 13 — Stage 1 현황 (2026-03-08 기준)

### 테스트 결과

- **Stage 2–5**: 30/30 PASS ✅
- **Q1-01~08** (노드 카운트): 8/8 PASS ✅
- **Q1-09** `MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN count(r)`: PASS ✅ (2889968)
- **Q1-10~21** (다른 엣지 카운트): 12개 모두 count=0, FAIL ❌

### 확인된 사실

1. **엣지는 노드와 동일하게 property extent에 저장된다.**
   - Forward edge는 `PropertySchemaCatalogEntry::extent_ids`에 extent 단위로 저장 (`bulkload_pipeline.cpp:1050~1051`)
   - `ExtentIterator` + `GraphStorageWrapper`로 full-scan 지원 — 별도 `PhysicalEdgeScan` 불필요
   - CSR adjlist는 별도 인덱스 (edge property extents와 분리)

2. **실패한 엣지 쿼리는 크래시 없이 0 반환** — "Unknown exception"은 이미 수정됨.

3. **CSV 컬럼 순서는 문제 아님** — 파서가 `START_ID`/`END_ID` 마커로 src/dst를 정확히 식별.
   `CreateEdgeCatalogInfos`의 `src_key_col_idx=1, dst_key_col_idx=2` 하드코딩은 올바름.

4. **KNOWS만 통과하는 이유**: KNOWS는 Person→Person (동일 타입). 나머지 실패 엣지들은
   Comment, Post 등 다른 타입 간 엣지임.

### 미해결: 왜 KNOWS(OID 885)는 작동하고 다른 엣지(OID 787 등)는 count=0인가?

NodeScan이 엣지 OID에 대해 0 row를 반환하는 원인 후보:

| 후보 | 근거 | 검증 방법 |
|------|------|----------|
| **A. 잘못된 OID** — 플래너가 partition OID를 PropertySchema OID로 잘못 전달 | PropertySchemaCatalogEntry로 캐스팅 시 `extent_ids`가 쓰레기값/빈값 | `InitializeScan`에 OID + extent_ids.size() 로그 추가 |
| **B. extent_ids 미등록** — 특정 엣지 타입의 AddExtent가 실패/누락 | bulkload에서 `property_schema_cat->AddExtent()`가 조건부로 실행됨 | catalog.bin 덤프 또는 bulkload 재실행 후 비교 |
| **C. planner 경로 차이** — KNOWS는 DSI 경로(`pruned_table_oids.size()>1`), 다른 엣지는 non-DSI 경로 → OID 의미 다름 | planner_logical.cpp:1497 분기 | 두 케이스의 `pruned_table_oids` 값 로그 |

**가장 유력**: 후보 C → B 조합. planner non-DSI 경로에서 전달되는 OID가 PropertySchema가 아닌
Partition을 가리키거나, Partition의 `property_schema_ids[0]`으로 조회된 PropertySchema의
`extent_ids`가 비어있음.

### 다음 조사 단계

1. `src/storage/graph_storage_wrapper.cpp` `InitializeScan()`에 임시 로그 추가:
   ```cpp
   spdlog::debug("[InitializeScan] oid={}, entry_type={}, extent_ids.size()={}",
       oid, ps_cat_entry->type, ps_cat_entry->extent_ids.size());
   ```
2. KNOWS(oid 885) vs REPLY_OF_COMMENT(oid 787)의 `extent_ids.size()` 비교
3. 두 케이스에서 `pruned_table_oids` 값 확인 → DSI vs non-DSI 분기 차이 확인
4. 원인에 따라 플래너 또는 bulkload 수정

### 참고: Forward vs Backward

- **Forward edge**: property extent에 저장 → COUNT 가능 (Q1-09~21 모두 forward 방향)
- **Backward edge**: CSR adjlist에만 존재 → COUNT 불가 (정상 동작, 중복 카운트 방지)

---

---

## Milestone 14 — LightningClient Dead Code 제거

### 배경

LightningClient는 원래 S62 내부 Lightning 데몬(소켓 기반 IPC)에 연결하는 클라이언트였다.
현재는 **standalone 모드만 존재** — 생성자가 `store_socket`, `password`, `standalone` 파라미터를
전부 무시하고 항상 `InitializeStandalone()`만 호출한다. 나머지는 잔재(vestige).

### 제거 대상

#### 확실한 데드 코드

| 항목 | 위치 | 이유 |
|------|------|------|
| `MultiPut()`, `MultiGet()`, `MultiUpdate()` | `client.h:21-33`, `client.cc:641-833` | 코드베이스 전체에서 호출 없음 |
| `ObjectLog` 생성/삭제 | `client.cc:127,135` | `OpenObject`/`CloseObject` 전부 주석 처리 |
| `object_log.h`, `object_log.cc` | `src/include/storage/cache/`, `src/storage/cache/` | ObjectLog 제거 시 완전히 불필요 |
| `object_log_fd_`, `object_log_base_`, `object_log_` 멤버 | `client.h:89-91` | ObjectLog 제거 따라 삭제 |
| `init_mpk()` | `client.cc:158-170` | 호출 자체가 없음 |
| `mpk_unlock()`, `mpk_lock()` | `client.cc:172-182` | `USE_MPK` 미정의 → 빈 함수. 모든 메서드에서 호출되지만 no-op |
| `#ifdef USE_MPK` 블록 전체 | `client.cc` | 미사용 |
| 생성자 파라미터 `store_socket`, `password` | `client.h:15`, `client.cc:38-43` | `/*standalone*/`처럼 무시됨. CCM에서 `"/tmp/lightning"`, `"password"` 전달하는 것도 제거 |
| `InitializeStandalone()` public 메서드 | `client.h:17` | 생성자 본문으로 인라인 후 제거 |
| `log_fd_` 멤버 | `client.h:63` | 실제로 사용되지 않는 fd (store_fd_와 혼동, `client.cc:49`에서 지역변수 섀도잉됨) |

#### 검토 필요 (제거 여부 결정 필요)

| 항목 | 위치 | 비고 |
|------|------|------|
| `UndoLogDisk` / `LOGGED_WRITE` / `BeginTx` / `CommitTx` | `log_disk.h`, `log_disk.cc`, `client.cc` 전반 | 단일 프로세스 standalone에서 크래시 복구 필요성이 없으면 제거 가능. 제거 시 코드 대폭 단순화. |

`UndoLogDisk` 제거 시 `LOGGED_WRITE(x, val, ...)` → `x = val` 으로 단순화되고
`disk_->BeginTx()` / `disk_->CommitTx()` 호출도 전부 사라짐.

### 변경 대상 파일

| 파일 | 변경 내용 |
|------|----------|
| `src/include/storage/cache/client.h` | 생성자 파라미터 제거, `InitializeStandalone` 선언 제거, MultiPut/Get/Update/Subscribe 선언 제거, 죽은 멤버 제거 |
| `src/storage/cache/client.cc` | 위 항목 전부 구현 제거, MPK 코드 제거, ObjectLog 제거 |
| `src/storage/cache/chunk_cache_manager.cc` | `new LightningClient("/tmp/lightning", "password", standalone)` → `new LightningClient()` |
| `src/include/storage/cache/object_log.h` | 파일 삭제 |
| `src/storage/cache/object_log.cc` | 파일 삭제 |
| `src/include/storage/cache/log_disk.h` | UndoLogDisk 제거 시 삭제 |
| `src/storage/cache/log_disk.cc` | UndoLogDisk 제거 시 삭제 |

### 검증

```bash
cd /turbograph-v3/build-lwtest && ninja
./test/unittest "[catalog]" 2>&1 | tail -5
./test/unittest "[storage]" 2>&1 | tail -5
ctest --output-on-failure -E "bulkload_dbpedia"
```

---

---

## Milestone 15 — Multi-Process Read/Write 지원

### 배경 & 범위

현재 TurboGraph는 **단일 프로세스 전제**로 설계되어 있다.
- 파일 락 없음 → 두 프로세스가 동시에 같은 `store.db`를 열면 데이터 손상
- WAL 없음 (이번 마일스톤도 WAL은 포함하지 않음)

목표: **Single Writer + Multiple Reader** 모델 구현.
DuckDB와 동일한 수준. Crash 중 Writer 종료 시 DB 손상 가능성은 허용 (WAL 없으므로).

---

### 현재 구조 (멀티 프로세스 관점)

```
store.db        ← AIO pwrite (offset 기반, 프로세스마다 독립 할당)
.store_meta     ← chunk_id → (offset, size) 맵, tmp+rename으로 atomic 기록 ✅
catalog.bin     ← 카탈로그 직렬화, catalog_version 파일로 버전 관리 ✅
LightningClient ← shm_open + 즉시 shm_unlink = 프로세스별 독립 익명 shm ✅
```

이미 안전한 것들은 그대로 두고, 빠진 것만 추가한다.

---

### 필요한 변경

#### 1. `store.db` 파일 락 (`fcntl` advisory lock)

**파일:** `src/storage/cache/chunk_cache_manager.cc`

```cpp
// store.db open 직후 적용
// Write 모드 (bulkload, 쿼리 중 write)
struct flock lock = {.l_type = F_WRLCK, .l_whence = SEEK_SET, .l_start = 0, .l_len = 0};
if (fcntl(store_fd_, F_SETLK, &lock) < 0) {
    throw IOException("store.db is already locked by another process (PID %d)", ...);
}

// Read 모드 (쿼리 전용)
lock.l_type = F_RDLCK;
fcntl(store_fd_, F_SETLK, &lock);
```

락은 `store_fd_` 소멸(프로세스 종료 or CCM destructor) 시 자동 해제.

#### 2. Open 모드 구분 (`read_only` flag)

**파일:** `src/storage/cache/chunk_cache_manager.h` / `.cc`, `src/main/capi/s62-c.cpp`

| 항목 | Write 모드 | Read-only 모드 |
|------|-----------|--------------|
| 파일 락 | `F_WRLCK` | `F_RDLCK` |
| `fallocate` | ✅ (현재처럼) | ❌ (건너뜀) |
| `.store_meta` 저장 | ✅ | ❌ |
| `catalog.bin` 저장 | ✅ | ❌ |

`ChunkCacheManager(const char* path, bool read_only = false)` 파라미터 추가.
`s62_connect` / `s62_connect_readonly` API 분리 또는 플래그 추가.

#### 3. Reader의 `.store_meta` / `catalog.bin` 재로드

Read-only 프로세스가 오래 살아있을 경우 Writer가 DB를 업데이트했을 때 stale view 방지.

**방법:** `s62_reopen()` 또는 `s62_connect()` 재호출 시:
1. `F_RDLCK` 재획득 (Writer가 활성이면 대기)
2. `catalog_version` 파일 읽어 현재 버전 비교
3. 버전 변경 시 → `catalog.bin` 재로드, `.store_meta` 재로드

실시간 감지는 이번 마일스톤 범위 밖 (inotify 등 별도 작업).

---

### 변경 대상 파일

| 파일 | 변경 내용 |
|------|----------|
| `src/storage/cache/chunk_cache_manager.h` | `read_only` 멤버 추가, 생성자 시그니처 변경 |
| `src/storage/cache/chunk_cache_manager.cc` | `open()` 후 `fcntl` 락, `fallocate` / `.store_meta` 저장 조건부 |
| `src/main/capi/s62-c.cpp` | `s62_connect_readonly()` API 추가 or 플래그 파라미터 |

---

### 미지원 항목 (의도적 제외)

| 항목 | 이유 |
|------|------|
| WAL / Crash recovery | 범위 밖 (명시적으로 제외) |
| 실시간 catalog 변경 감지 | inotify 별도 작업 |
| Concurrent multi-writer | 원천적으로 지원 불가 (WAL 없으므로) |

---

### 검증

```bash
# 빌드
cd /turbograph-v3/build-lwtest && ninja

# 같은 DB에 두 프로세스 동시 접근 테스트
# 프로세스 A (writer) 실행 중, 프로세스 B (writer) 시도 → 락 에러 확인
# 프로세스 A (writer) 실행 중, 프로세스 B (reader) 시도 → 대기 후 성공 확인
# 프로세스 A (reader) + B (reader) 동시 → 둘 다 성공 확인

ctest --output-on-failure -E "bulkload_dbpedia"
```

---

## Notes

- Keep this file updated at milestone completion.
- One milestone at a time. Validate with `ctest` before closing a milestone.
- Build always in `turbograph-s62` container: `cd /turbograph-v3/build-lwtest && ninja`
- **E2E bulkload 테스트 실행:**
  `ctest --output-on-failure -R "bulkload_ldbc_sf1|bulkload_tpch_sf1"` (staged 제외)
- **DBpedia 테스트** (수 시간 소요, CI 제외):
  `ctest --output-on-failure -R "bulkload_dbpedia"`
  staged subset: `[dbpedia][vertex]`, `[dbpedia][edge-small]`, `[dbpedia][edge-medium]`, `[dbpedia][edge-large]`
