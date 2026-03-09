# TurboLynx — Execution Plan

## Current Status

Core build is stable. All unit tests (catalog 51, storage 43, common 10) pass.
LDBC SF1 + TPC-H SF1 + DBpedia bulkload E2E tests all passing.

**All milestones complete. Next milestone TBD.**

---

## Completed Milestones

| # | Milestone | 핵심 변경 | Status |
|---|-----------|-----------|--------|
| 1 | Remove Velox dependency | Velox 빌드 제거, 대체 구현 인라인 | ✅ |
| 2 | Remove Boost dependency | Boost 헤더/라이브러리 전면 제거 | ✅ |
| 3 | Remove Python3 dependency | Python 바인딩 제거, 빌드 단순화 | ✅ |
| 4 | Bundle TBB / libnuma / hwloc | 시스템 의존성 → 소스 번들로 교체 | ✅ |
| 5 | Single-file block store (`store.db`) | 컬럼별 개별 파일 → `store.db` 단일 파일 + `base_offset_` region 분할 | ✅ |
| 6 | Test suite: catalog, storage, execution | `[catalog]` `[storage]` `[execution]` 단위 테스트 | ✅ |
| 7 | Remove libaio-dev system dependency | `libaio` → 직접 `io_submit` syscall | ✅ |
| 8 | Rename library | `libs62gdb.so` → `libturbolynx.so`, CAPI 정리 | ✅ |
| 9 | E2E bulkload test suite | LDBC SF1 / TPC-H SF1 / DBpedia 전체 검증 자동화 | ✅ |
| 10 | Extract `BulkloadPipeline` | `tools/bulkload.cpp` 모놀리식 → `BulkloadPipeline` 클래스 분리 | ✅ |
| 11 | Multi-client support (prototype) | `g_connections` 맵으로 세션 분리, `s62_connect` CAPI | ✅ |
| 12 | Bulkload performance optimization | 백그라운드 flush 스레드, ThrottleIfNeeded, fwd/bwd interleave, DBpedia OOM 수정 | ✅ |
| 13 | LDBC Query Test Suite | Q1-01~21 ✅, Q2-01~09 ✅ — is_reserved 버그 수정, DATE→ms 변환, 공유 Runner | ✅ |
| 14 | LightningClient Dead Code 제거 | MultiPut/Get/Update, ObjectLog, UndoLogDisk, MPK, 생성자 파라미터 제거 | ✅ |
| 15 | Catalog ChunkDefinitionID 복원 수정 | Serialize에 cdf_id 직접 기록 → Deserialize 파싱 오류(`stoull("0_0")→0`) 수정 | ✅ |
| 16 | Multi-Process Read/Write 지원 | `fcntl` F_WRLCK/F_RDLCK, `read_only_` CCM 플래그, `s62_connect_readonly()`, `s62_reopen()` CAPI | ✅ |

---

## Milestone 16 — Multi-Process Read/Write 지원

**Goal:** Single Writer + Multiple Reader 모델. DuckDB 수준. WAL 미포함.

### 현재 구조 (멀티 프로세스 관점)

```
store.db        ← AIO pwrite (offset 기반)  ← 파일 락 필요
.store_meta     ← tmp+rename으로 atomic 기록 ✅
catalog.bin     ← catalog_version 파일로 버전 관리 ✅
LightningClient ← shm_open + 즉시 unlink = 프로세스별 독립 익명 shm ✅
```

### 필요한 변경

#### 1. `store.db` 파일 락 (`fcntl` advisory lock)

```cpp
// ChunkCacheManager::Open() 내부
struct flock lock = {.l_type = F_WRLCK, .l_whence = SEEK_SET, .l_start = 0, .l_len = 0};
if (fcntl(store_fd_, F_SETLK, &lock) < 0)
    throw IOException("store.db is locked by another process");
// read_only 모드: l_type = F_RDLCK
```

락은 `store_fd_` 닫힐 때 자동 해제.

#### 2. `read_only` flag

| 항목 | Write 모드 | Read-only 모드 |
|------|-----------|--------------|
| 파일 락 | `F_WRLCK` | `F_RDLCK` |
| `fallocate` | ✅ | ❌ |
| `.store_meta` 저장 | ✅ | ❌ |
| `catalog.bin` 저장 | ✅ | ❌ |

`ChunkCacheManager(const char* path, bool read_only = false)` 파라미터 추가.
`s62_connect_readonly()` CAPI 추가.

#### 3. Reader의 catalog 재로드

`s62_reopen()` 호출 시: `F_RDLCK` 재획득 → `catalog_version` 비교 → 변경 시 `catalog.bin` + `.store_meta` 재로드.

### 변경 대상 파일

| 파일 | 변경 내용 |
|------|----------|
| `src/storage/cache/chunk_cache_manager.h` | `read_only` 멤버, 생성자 시그니처 |
| `src/storage/cache/chunk_cache_manager.cc` | `fcntl` 락, `fallocate` / 저장 조건부 |
| `src/main/capi/s62-c.cpp` | `s62_connect_readonly()` 추가 |

### 미지원 (의도적 제외)

- WAL / Crash recovery
- 실시간 catalog 변경 감지 (inotify)
- Concurrent multi-writer

---

## Notes

- Build: `cd /turbograph-v3/build-lwtest && ninja`
- Unit tests: `./test/unittest "[catalog]"` / `"[storage]"` / `"[common]"` (각각 별도 실행)
- E2E bulkload: `ctest --output-on-failure -R "bulkload_ldbc_sf1|bulkload_tpch_sf1"`
- DBpedia (수 시간): `ctest --output-on-failure -R "bulkload_dbpedia"`
- **Never modify a test just because it fails. Fix the implementation.**
