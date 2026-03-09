# TurboLynx — Execution Plan

## Current Status

Core build is stable. All unit tests (catalog 51, storage 68, common 10) pass.
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
| 17 | Multi-Process 테스트 | `[storage][multiproc]` — fcntl lock 시맨틱 5개 + CCM 락 충돌 3개, 총 8 테스트 | ✅ |
| 18 | LightningClient → BufferPool 교체 | shm/300GB mmap 제거, malloc + Second-Chance Clock eviction, UnPinSegment 실제 활성화 | ✅ |

---

## Milestone 18 — LightningClient → BufferPool 교체

**Goal:** 300GB virtual mmap + shm 기반 LightningClient를 `malloc` + `unordered_map` 기반 BufferPool로 교체. CCM 외부 API는 변경 없음.

### 동기

| 항목 | 현재 (LightningClient) | 이후 (BufferPool) |
|------|----------------------|-----------------|
| 메모리 예약 | 300GB virtual mmap (shm) | RAM 80% — 실제 사용량만 |
| 자료구조 | HashMap 2M + ObjectEntry 512K (고정 크기) | `unordered_map<ChunkID, Entry>` (동적) |
| 잠금 | spinlock (`lock_flag` CAS) | `std::mutex` |
| Seal / Subscribe | 멀티 writer 동기화용 sem | 불필요 (단일 writer 경로) |
| UnPinSegment | **no-op** (Release 주석 처리) | `pin_count--` 실제 동작 |
| Eviction | 없음 (메모리 부족 시 crash) | Second-Chance Clock |

### 파일 변경

**제거:**
```
src/include/storage/cache/client.h       ← LightningClient
src/storage/cache/client.cc
src/include/storage/cache/memory.h       ← LightningStoreHeader, ObjectEntry, HashMap
src/include/storage/cache/config.h       ← STORE_NAME, DEFAULT_STORE_SIZE(300GB), LIGHTNING_MMAP_ADDR
src/include/storage/cache/malloc.h       ← MemAllocator (shm 슬랩)
```

**추가:**
```
src/include/storage/cache/buffer_pool.h
src/storage/cache/buffer_pool.cc
```

**수정:**
```
src/include/storage/cache/chunk_cache_manager.h   ← client* → pool_
src/storage/cache/chunk_cache_manager.cc           ← client-> 호출 전부 교체
```

### BufferPool 인터페이스

```cpp
class BufferPool {
public:
    struct Entry {
        uint8_t* ptr;
        size_t   size;
        int      pin_count;
        bool     dirty;
        bool     clock_bit;   // Second-Chance eviction
    };

    explicit BufferPool(size_t memory_limit);

    // cache miss: posix_memalign(512, size), pin_count=1
    // OOM 시 내부 Evict 후 재시도. 모두 pinned이면 false.
    bool   Alloc(ChunkID cid, size_t size, uint8_t** ptr);

    // cache hit: pin_count++, ptr/size 반환
    bool   Get(ChunkID cid, uint8_t** ptr, size_t* size);

    void   Release(ChunkID cid);      // pin_count--
    void   SetDirty(ChunkID cid);
    void   ClearDirty(ChunkID cid);
    bool   GetDirty(ChunkID cid) const;
    void   Delete(ChunkID cid);       // 즉시 free (DestroySegment 경로)

    std::vector<ChunkID> DirtyUnpinned() const;  // flush 대상
    size_t FreeMemory()  const;
    int    RefCount(ChunkID cid) const;

private:
    // Second-Chance Clock: unpinned entry 1개 반환
    // on_dirty: dirty이면 caller가 AIO write 수행
    ChunkID EvictOne(std::function<void(ChunkID, Entry&)> on_dirty);

    duckdb::unordered_map<ChunkID, Entry> entries_;
    std::vector<ChunkID>                  clock_keys_;
    size_t                                clock_hand_ = 0;
    size_t                                memory_limit_;
    size_t                                used_memory_ = 0;
    mutable std::mutex                    mu_;
};
```

### CCM API별 변경

| 호출 | Before | After |
|------|--------|-------|
| cache hit | `client->Get(cid, ptr, size)` | `pool_.Get(cid, ptr, size)` |
| cache miss | `client->Create` → `MemAlign` → `ReadData` → `Seal` | `pool_.Alloc(cid, size, ptr)` → `ReadData` |
| unpin | `// client->Release(cid)` (no-op) | `pool_.Release(cid)` |
| dirty | `client->Set/Get/ClearDirty` | `pool_.Set/Get/ClearDirty` |
| destroy | `client->Delete(cid)` | `pool_.Delete(cid)` |
| OOM 처리 | 없음 | `Alloc` 내부 `EvictOne` → AIO flush → free |
| 메모리 조회 | `client->GetRemainingMemory()` | `pool_.FreeMemory()` |

`MemAlign()` 제거: `posix_memalign(512, size)` 로 Alloc 내부에서 해결.

### Second-Chance Clock Eviction

```
EvictOne(on_dirty):
  최대 2 * entries_.size() 회 sweep:
    entry = clock_keys_[clock_hand_]
    if pin_count > 0:      → 스킵 (pinned)
    elif clock_bit == 1:   → clock_bit = 0, 스킵 (second chance)
    else:                  → evict
        if dirty: on_dirty(cid, entry)   ← AIO write
        free(ptr), entries_.erase(cid)
        return cid
    clock_hand_ = (clock_hand_ + 1) % size
  return -1  // 모두 pinned
```

### 메모리 한도

```cpp
// memory_limit=0 → sysconf(_SC_PHYS_PAGES) * page_size * 0.8
ChunkCacheManager(const char* path,
                  bool standalone    = false,
                  bool read_only     = false,
                  size_t memory_limit = 0);
```

### 구현 순서

| 단계 | 작업 |
|------|------|
| 18a | `BufferPool` 신규 작성 + `[storage]` 유닛 테스트 |
| 18b | CCM에서 `client_` → `pool_` 교체 (Seal/Subscribe 제거 포함) |
| 18c | `UnPinSegment`에서 `pool_.Release()` 실제 활성화 |
| 18d | `MemAlign()` 제거 (`posix_memalign` 내재화) |
| 18e | LightningClient 관련 파일 전체 삭제 |
| 18f | E2E 검증 (bulkload LDBC SF1 + TPC-H SF1) |

### 위험 요소

| 항목 | 내용 |
|------|------|
| UnPinSegment 활성화 | pin_count 실제 사용 시 flush thread와 race → `mu_` 로 커버 |
| Subscribe 제거 | 동일 cid 동시 PinSegment 경합 → `mu_` serialize로 경합 자체가 사라짐 |
| clock_keys_ 정합성 | entries_ 삽입/삭제 시 동기화 필요 |
| posix_memalign 실패 | 극단적 메모리 부족 → IOException throw |

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
