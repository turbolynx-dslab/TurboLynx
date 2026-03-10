# TurboLynx — Execution Plan

## Current Status

Core build is stable. All unit tests (catalog 51, storage 68, common 10) pass.
LDBC SF1 + TPC-H SF1 + DBpedia bulkload E2E tests all passing.

**M21 완료 ✅. 다음: M22 — 단일 바이너리 통합.**

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
| 19 | Storage Dead Code 제거 | 항상 false인 validator, 미호출 함수 5개, exit(-1) → throw IOException 교체 | ✅ |
| 20 | Kuzu 제거 — TurboLynx Parser/Binder/Converter | Kuzu Parser·Binder 전면 교체, TurboLynx-native 4단계 파이프라인 구현, `optimizer/kuzu/` 삭제 | ✅ |
| 21 | `s62` legacy naming → `turbolynx` 전면 교체 | C API 함수·타입·파일명, 소켓 서버 클래스, enum 상수 전부 rename | ✅ |
| 22 | 단일 바이너리 통합 (`turbolynx import` + `turbolynx shell`) | `bulkload` 바이너리 제거, `client` → `turbolynx` 서브커맨드 구조로 통합 | 🔲 |

---

## Milestone 22 — 단일 바이너리 통합

**Goal:** `bulkload`와 `client` 두 개의 바이너리를 `turbolynx` 단일 바이너리로 통합.
Neo4j Admin 패턴을 따라 `turbolynx import` (벌크로딩) / `turbolynx shell` (쿼리 쉘)로 분기.

### 목표 인터페이스

```bash
# 벌크로딩 (초기 데이터 로딩, 오프라인 전용)
./turbolynx import \
    --workspace /path/to/db \
    --nodes Person:dynamic/Person.csv \
    --nodes Post:dynamic/Post.csv \
    --edges KNOWS:dynamic/Person_knows_Person.csv \
    --skip-histogram

# 쿼리 쉘 (인터랙티브 / 단일 쿼리)
./turbolynx shell --workspace /path/to/db
./turbolynx shell --workspace /path/to/db --query "MATCH (n:Person) RETURN count(*)"

# shell이 기본값 — 서브커맨드 생략 시 shell로 동작
./turbolynx --workspace /path/to/db
```

### 설계 원칙

- **import는 오프라인 전용**: 벌크로딩 중 다른 프로세스 접근 불가 (exclusive write lock, 기존 동일)
- **`BulkloadPipeline`은 이미 `libturbolynx.so`에 내장**: 링크 추가 없이 재사용 가능
- **기존 `--nodes`/`--edges` 인터페이스 유지**: 현재 E2E 테스트 스크립트 호환
- **`--output_dir` → `--workspace`로 통일**: 기존 `--output_dir`도 alias로 유지

### 구현 순서

| 단계 | 작업 |
|------|------|
| 22a | `tools/client.cpp` → `RunShell(int, char**)` 함수로 추출 |
| 22b | `tools/bulkload.cpp` → `RunImport(int, char**)` 함수로 추출, `--workspace` alias 추가 |
| 22c | `tools/turbolynx.cpp` 신규 작성 — `main()` 서브커맨드 분기 |
| 22d | `tools/CMakeLists.txt` 수정 — `turbolynx` 타겟 추가, `client`/`bulkload` 제거 |
| 22e | E2E 테스트 경로 업데이트 (`test/bulkload/CMakeLists.txt` 등 `bulkload` → `turbolynx import`) |
| 22f | 빌드 + 전체 테스트 통과 확인 |

### 영향 범위

| 파일 | 변경 내용 |
|------|---------|
| `tools/turbolynx.cpp` | 신규 — `main()` + 서브커맨드 분기 |
| `tools/client.cpp` | `main()` → `RunShell()` 함수로 변환 |
| `tools/bulkload.cpp` | `main()` → `RunImport()` 함수로 변환, `--workspace` alias |
| `tools/CMakeLists.txt` | `turbolynx` 타겟 추가, 기존 2개 타겟 제거 |
| `test/bulkload/CMakeLists.txt` | bulkload 바이너리 경로 → turbolynx import |
| `TECHSPEC.md` | 빌드 산출물 표 업데이트 |

---

## Known Technical Debt (미래 Milestone 후보)

### Persistence Tier (미구현 — 의도적)
- `storage_manager.cpp`: WAL, Checkpoint, Transaction Manager 전체 주석 처리 상태
- in-memory only 모드. 디스크 persistence 구현 시 일괄 해제

### 버그 (수정 필요)
| 파일 | 위치 | 내용 |
|------|------|------|
| `extent_iterator.cpp` | line 205 | `target_idx[j++] - target_idxs_offset` 인덱스 계산 오류 (`// TODO bug..`) |
| `adjlist_iterator.cpp` | line 99 | adjacency direction hard-coded `true` (forward/backward 미구분) |
| `adjlist_iterator.cpp` | line 342 | 양방향 BFS meeting point 레벨 체크 오류 |
| `histogram_generator.cpp` | line 215 | boundary 값 strictly ascending 보장 미흡 |

### 성능 최적화 (기능 정상, 느림)
- `histogram_generator.cpp`: 전체 컬럼/전체 행 스캔 — 샘플링 미구현
- `graph_storage_wrapper.cpp`: 쿼리 루프마다 catalog lookup 반복 (컴파일 타임으로 이동 필요)
- `graph_storage_wrapper.cpp`: 단일 레이블 제한 (`D_ASSERT(labels.size() == 1)`)
- `buffer_manager.cpp:314`: eviction queue housekeeping 없음 (장시간 실행 시 누적)

---

## Notes

- Build: `cd /turbograph-v3/build-lwtest && ninja`
- Unit tests: `./test/unittest "[catalog]"` / `"[storage]"` / `"[common]"` (각각 별도 실행)
- E2E bulkload: `ctest --output-on-failure -R "bulkload_ldbc_sf1|bulkload_tpch_sf1"`
- DBpedia (수 시간): `ctest --output-on-failure -R "bulkload_dbpedia"`
- **Never modify a test just because it fails. Fix the implementation.**
