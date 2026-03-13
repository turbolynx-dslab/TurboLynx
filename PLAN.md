# TurboLynx — Execution Plan

## Current Status

Core build is stable. All unit tests (catalog 51, storage 68, common 10) pass.
LDBC SF1 + TPC-H SF1 + DBpedia bulkload E2E tests all passing.

**M26 — 양방향 엣지 쿼리 지원 (Active)**

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
| 22 | 단일 바이너리 통합 (`turbolynx import` + `turbolynx shell`) | `bulkload` 바이너리 제거, `client` → `turbolynx` 서브커맨드 구조로 통합 | ✅ |
| 23 | Shell subdirectory 리팩터링 | `tools/shell/` 분리, renderer(4종), dot commands(9개), executor callback 패턴 | ✅ |
| 24 | Shell UX — dot commands + output modes | 누락 dot commands 완성, 출력 모드 확장, 설정 파일 | ✅ |
| 25 | Shell UX — 자동완성 + 문법 하이라이팅 | linenoise 탭 자동완성, ANSI 컬러 키워드 하이라이팅 | ✅ |

---

## M26 — 양방향 엣지 쿼리 지원

**목표:** Cypher 무방향 패턴 `(a)-[:KNOWS]-(b)` 지원. Physical operator 레벨에서 forward + backward adj list를 순차 스캔(dual-phase)하여 결과 통합.

### 현재 상태 (파이프라인 전체)

```
Parser ──→ Binder ──→ Converter ──→ Physical Operator ──→ Storage
  ✅          ✅          ❌              ❌                 ✅
```

- **Parser**: `-[:KNOWS]-` → `RelDirection::BOTH` 정상 파싱 (`cypher_transformer.cpp:266-294`)
- **Binder**: `BoundRelExpression`에 direction 보존 (`binder.cpp:442`)
- **Converter**: `PlanRegularMatch()`에서 `BOTH` 처리 없음 — SID/TID 중 하나만 선택 (`cypher2orca_converter.cpp:191-222`)
- **Physical Operator**: `getAdjListFromVid()`에서 `D_ASSERT`로 `BOTH` 거부 (`graph_storage_wrapper.cpp:651`)
- **Storage**: forward/backward adj list 모두 이미 저장됨 (M12에서 구현)

### 구현 계획

#### M26-B: Physical Operator — dual adj list scan

**파일:** `src/storage/graph_storage_wrapper.cpp`, `src/execution/.../physical_adjidxjoin.cpp`

`getAdjListFromVid()`에서 `ExpandDirection::BOTH` 처리:

```
1. forward adj list로 Initialize → iterate (기존 OUTGOING 로직)
2. forward 소진 후 backward adj list로 Initialize → iterate (기존 INCOMING 로직)
3. 상위로 concat된 결과 전달
```

구체적 변경:
1. `graph_storage_wrapper.cpp:651` — `D_ASSERT` 제거, `BOTH` 분기 추가
2. `AdjListIterator`에 dual-phase state 추가 (현재 phase: FWD/BWD, 전환 시점 관리)
3. `PhysicalAdjIdxJoin`의 `GetNextBatch` 루프에서 forward 소진 → backward 전환

**⚠️ Pitfall — Batch 경계 조건:** 배치 사이즈(예: 1024) 도중 Forward 리스트가 소진될 수 있음. 동일 배치 내에서 즉시 Backward로 전환하여 나머지를 채워야 하며, Backward마저 비었을 때 정확히 EOF를 반환해야 함. Iterator의 State Machine이 mid-batch phase 전환을 매끄럽게 처리하도록 제어 흐름을 정교하게 설계할 것.

#### M26-A: Converter — `BOTH` direction 전파

**파일:** `src/converter/cypher2orca_converter.cpp`

`PlanRegularMatch()`에서 `RelDirection::BOTH`일 때:
- self-referential (Person→Person 등): `ExpandDirection::BOTH`를 physical plan에 전달
- 이종 (Person→Place 등): 논리적으로 양방향 의미 — 역시 `BOTH` 전달
- `lhs_edge_key` / `rhs_edge_key` 선택 로직: `BOTH`일 때는 SID 기준으로 plan 생성 (forward scan이 primary, backward를 보조)

`PlanVarLenMatch()`에도 동일 적용.

**⚠️ Pitfall — Optimizer 확장성:** "BOTH일 때 SID 고정"은 초기 구현으로 충분하나, 추후 옵티마이저 고도화 시 발목을 잡을 수 있음 (예: RHS에 강력한 필터 `id=933`이 걸려 있으면 역방향이 더 효율적). `lhs_edge_key`/`rhs_edge_key`를 동적으로 swap할 수 있는 인터페이스를 열어둘 것.

#### M26-D: 중복 제거

양방향 스캔 시 동일 edge가 forward/backward 양쪽에서 등장할 수 있음:
- `(a)-[:KNOWS]-(b)`: forward에서 `a→b`, backward에서도 `a←b` (같은 edge)
- **KNOWS처럼 같은 label 양쪽인 경우**: dedup 필요
- **이종 label인 경우**: forward/backward 중 하나만 hit하므로 dedup 불필요

**⚠️ Pitfall — 메모리 폭발:** Hash-set 기반 `seen-set`은 슈퍼 노드나 중간 결과가 클 때 OOM 위험. **Stateless ID 비교 제약** (`src_id < dst_id`일 때만 결과 방출)을 우선 적용할 것. 상태 저장 방식은 가급적 배제.

#### M26-C: VarLen 양방향 지원

**파일:** `src/execution/.../physical_varlen_adjidxjoin.cpp`

Variable-length path에도 동일 dual-scan 적용:
- 각 hop에서 forward + backward 이웃 모두 탐색
- BFS/DFS traversal 시 양방향 확장

**⚠️ Pitfall — Trivial Cycle (가장 위험):** Cypher는 Edge Isomorphism을 강제 (하나의 경로 내에서 동일 edge 재방문 금지). `(a)-[:KNOWS*1..2]-(b)` 양방향 탐색 시, `A→B` 후 2번째 hop에서 `B→A`로 돌아오는 Trivial Cycle(A-B-A)이 발생. 단순히 forward+backward 이웃을 큐에 넣는 것이 아니라, **현재 경로(Path)에 이미 포함된 Edge ID인지 확인하는 로직**이 반드시 필요.

#### M26-E: 테스트

**파일:** `test/query/test_q_bidirectional.cpp` (신규)

| 테스트 | 쿼리 | 검증 |
|--------|------|------|
| BOTH-01 | `(p:Person {id:933})-[:KNOWS]-(f:Person)` | 5명 (directed와 동일) |
| BOTH-02 | `(p:Person {id:933})-[:KNOWS]-(f)-[:KNOWS]-(fof)` | 2-hop 양방향 |
| BOTH-03 | `(c:Comment {id:824635044686})-[:HAS_CREATOR]-(p:Person)` | 이종 label 양방향 |
| BOTH-04 | `(p:Person {id:933})-[:KNOWS*1..2]-(f:Person)` | VarLen 양방향 + cycle 미발생 검증 |
| BOTH-05 | `(m:Comment)-[:REPLY_OF_COMMENT]-(c:Comment)` | 양방향 + 같은 label |

### 구현 순서

**M26-B → M26-A → M26-D → M26-E → M26-C**

1. Physical operator에서 `BOTH` 스캔 가능하게 (B)
2. Converter에서 `BOTH`를 내려보내기 (A)
3. 중복 제거 — stateless ID 비교 우선 (D)
4. 테스트 검증 (E)
5. VarLen 확장 — Edge Isomorphism 보장 (C)

### 주요 파일

| 파일 | 변경 내용 |
|------|-----------|
| `src/converter/cypher2orca_converter.cpp` | `PlanRegularMatch`, `PlanVarLenMatch`에서 `BOTH` direction 전파 |
| `src/storage/graph_storage_wrapper.cpp` | `getAdjListFromVid()` BOTH 분기, D_ASSERT 제거 |
| `src/storage/extent/adjlist_iterator.cpp` | dual-phase iteration (FWD→BWD), mid-batch 전환 |
| `src/include/storage/extent/adjlist_iterator.hpp` | phase state 멤버 추가 |
| `src/execution/.../physical_adjidxjoin.cpp` | `GetNextBatch` forward 소진 → backward 전환 |
| `src/execution/.../physical_varlen_adjidxjoin.cpp` | 양방향 BFS/DFS + Edge Isomorphism 검사 |
| `test/query/test_q_bidirectional.cpp` | 양방향 쿼리 테스트 (신규) |

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
