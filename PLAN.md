# TurboLynx — Execution Plan

## Current Status

343 tests (220 robustness + 123 functional), 952 assertions, all passing.
**IC1~IC14 Neo4j verified (original Cypher queries).** Debug and release builds both fully passing.
Crash-proof signal handler in shell. Query plan cache. UTF-8 rendering.

### Expression Support

| Feature | Status |
|---------|--------|
| Comparison `=, <>, <, >, <=, >=` | Supported (chained comparison included) |
| AND, OR, NOT, XOR | Supported |
| STARTS WITH, ENDS WITH, CONTAINS | Supported |
| CASE (simple + generic) | Supported |
| Math `+, -, *, /, %, ^` | Supported |
| IS NULL / IS NOT NULL | Supported |
| List comprehension, Pattern comprehension | Supported |
| reduce(), coalesce() | Supported |
| Map literal `{key: value}` | Supported |
| `\|\|` string concat | Not supported (ANTLR grammar change needed) |
| List slicing `[1..3]` | Not supported (ANTLR grammar change needed) |
| EXISTS/COUNT/COLLECT subquery | Not supported |

---

## CRUD Design Plan

### 1. Background & Constraints

TurboLynx는 **OLAP 그래프 데이터베이스**로, 읽기 중심 분석 워크로드에 최적화되어 있다.
Storage는 **CGC(Compact Graphlet Clustering)** 기반으로, 같은 property schema를 가진 노드들을
하나의 graphlet(extent)에 압축 저장한다. 이 구조는 벌크 로딩 시 전체 그래프를 보고 최적 배치를
결정하므로, incremental mutation과 본질적으로 충돌한다.

**핵심 설계 원칙:**
- 읽기 성능을 훼손하지 않는다
- 기존 CGC 구조를 최대한 유지한다
- 단순한 것부터 시작하여 점진적으로 확장한다

### 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        Query Pipeline                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  [Parser]  Cypher text → RegularQuery AST                       │
│     ↓         CREATE/SET/DELETE/REMOVE/MERGE 파싱               │
│  [Binder]  AST → BoundMutationStatement                        │
│     ↓         mutation 타입 분류, 대상 label/property 해석       │
│  [Planner] BoundMutation → MutationPlan                        │
│     ↓         ORCA 우회 — mutation은 최적화 불필요               │
│  [Executor] MutationPlan → Storage API 호출                    │
│     ↓                                                           │
│  [Storage] ExtentManager + AdjListManager 수정                 │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Key Decision: Mutation은 ORCA를 통과하지 않는다.**
- ORCA는 읽기 최적화 전용. Mutation은 단순 실행이므로 직접 실행한다.
- `MATCH ... SET ...` 같은 read+write 혼합 쿼리: MATCH는 ORCA, SET은 직접 실행.

### 3. Storage Layer Design

#### 3.1 Design Reference: DuckDB의 Update/Delete 구조

DuckDB는 압축된 columnar store에서 mutation을 다음과 같이 처리한다:

```
RowGroup (~122,880 rows)
├── ColumnData (컬럼별)
│   ├── ColumnSegment  ← 압축된 원본. 수정하지 않음
│   └── UpdateSegment  ← UPDATE 시 (row 위치, 새 값)만 기록
└── RowVersionManager
    ├── inserted_rows  ← 어떤 트랜잭션이 삽입했는지
    └── deleted_rows   ← 삭제 표시 (validity mask)
```

- **UPDATE**: 원본 불변. UpdateSegment에 delta 기록. 읽을 때 merge.
- **DELETE**: 실제 삭제 안 함. RowVersionManager에 삭제 표시. 읽을 때 건너뜀.
- **INSERT**: LocalTableStorage(트랜잭션 로컬 버퍼)에 축적. Commit 시 main에 merge.
- **CHECKPOINT**: UpdateSegment를 base에 통합, 삭제된 row 제거 (인접 row group에서 삭제율 ~25% 넘으면 merge), WAL truncate.

**우리가 DuckDB 패턴을 따르는 이유:**
- TurboLynx도 압축된 columnar store → in-place 수정은 압축 해제/재압축 비용 발생
- DuckDB에서 검증된 패턴: 원본 불변 + delta 기록 + scan-time merge + checkpoint 통합
- 추가 고려: TurboLynx는 **CGC graphlet 배치**가 있으므로 checkpoint 시 re-clustering 필요

#### 3.2 TurboLynx Mutation 구조 (DuckDB 대응)

```
Extent (= DuckDB RowGroup에 대응)
├── Compressed Chunk (= ColumnSegment에 대응)
│   └── 압축된 원본 데이터. 수정하지 않음.
├── UpdateSegment (per chunk, 신규)
│   └── map<offset, Value>: 변경된 row의 (위치, 새 값)
├── DeleteMask (= RowVersionManager.deleted_rows에 대응, 신규)
│   └── bitset<extent_size>: 삭제된 row 표시
└── InsertBuffer (= LocalTableStorage에 대응, 신규)
    └── DataChunk: 새로 삽입된 row들 (비압축)

AdjList Delta (Edge mutation용, DuckDB에 없는 부분)
├── adj_insert: map<VID, vector<{dst, edge_id}>>  — 새 엣지
└── adj_delete: map<VID, set<edge_id>>             — 삭제된 엣지
```

| DuckDB 개념 | TurboLynx 대응 | 비고 |
|-------------|---------------|------|
| RowGroup | Extent | ~수천~수만 rows |
| ColumnSegment | Compressed Chunk | 압축된 컬럼 데이터 |
| UpdateSegment | UpdateSegment | 동일 패턴 |
| RowVersionManager.deleted | DeleteMask (bitset) | 동일 패턴 |
| LocalTableStorage | InsertBuffer | per-partition |
| CHECKPOINT | Compaction | + CGC re-clustering |
| WAL | WAL (Phase 6) | 동일 패턴 |
| — | AdjList Delta | 그래프 특화 (CSR 변경분) |

#### 3.3 Read Merge (Scan-time, DuckDB 방식)

DuckDB와 동일하게 **읽을 때** base + delta를 merge한다. 원본은 건드리지 않는다.

```
Scan 시 처리 순서 (DuckDB ColumnData::ScanVector와 동일):
1. Base Compressed Chunk에서 row 읽기
2. DeleteMask 체크 → 삭제된 row 건너뜀
3. UpdateSegment 체크 → 변경된 값이 있으면 교체
4. InsertBuffer의 추가 row도 scan 대상에 포함

AdjList Scan:
1. Base CSR에서 인접 노드 읽기
2. adj_delete 체크 → 삭제된 엣지 건너뜀
3. adj_insert 체크 → 추가된 엣지 포함
```

**성능 특성:**
- Delta가 비어있으면 overhead 거의 0 (bitset check만)
- Delta가 커지면 merge 비용 증가 → compaction으로 해소

#### 3.4 Compaction (= DuckDB CHECKPOINT에 대응)

**트리거 조건 (DuckDB 참조):**
- **WAL 크기 기반**: WAL 파일이 threshold 초과 시 자동 (DuckDB default: ~16MB)
- **삭제율 기반**: 인접 extent에서 삭제율 ~25% 넘으면 extent merge (DuckDB 방식)
- **명시적 명령**: shell dot command `.checkpoint` 또는 서버 종료 시
- Phase 1에서는 **명시적 명령만** 지원, 이후 자동 트리거 추가
- `CALL` 문법은 Cypher 호환이 필요할 때 추가 (ANTLR 변경 필요). 당분간 dot command로 충분

**Shell 명령:**
```
TurboLynx >> .checkpoint
Compaction completed.
```
- 기존 `.help`, `.mode`, `.output` 등 dot command 체계에 `.checkpoint` 추가
- `tools/shell/include/commands.hpp`에 handler 등록

**Compaction 절차:**
1. **UpdateSegment 통합** — delta 값을 base chunk에 반영, 재압축
2. **DeleteMask 적용** — 삭제된 row 제거, extent 크기 축소
3. **InsertBuffer 통합** — `_ExtractSchemaAndAssign` → 기존 graphlet에 매칭 or 새 extent 생성
4. **CSR 재빌드** — adj_insert/adj_delete 반영하여 새 CSR 생성
5. **CGC re-clustering** (선택적) — `_RebalanceSchemas` 구현. 작은 graphlet 합치기, fragmentation 해소
6. **Delta 초기화** — UpdateSegment, DeleteMask, InsertBuffer, AdjList Delta 클리어
7. **WAL truncate** — compaction 완료 후 WAL 파일 비우기
8. **Catalog 재직렬화** — extent/partition 메타데이터 업데이트

### 4. Implementation Phases

#### Phase 1: CREATE Node (기초)
**목표**: `CREATE (n:Person {id: 1, name: 'John'})` 실행

```
Parser  → UpdatingClause(CREATE) 파싱 (문법 이미 존재)
Binder  → label 해석, property type 검증
Planner → MutationPlan(INSERT_NODE, label, properties)
Executor→ delta_store.insert_buffer에 DataChunk append
         → VID 할당 (partition_id + extent_id + offset)
         → 결과 반환: "Created 1 node"
```

**수정 파일:**
- `src/parser/cypher_transformer.cpp` — `transformUpdatingClause` 구현 (현재 throw)
- `src/binder/binder.cpp` — `BindCreateClause` 추가
- `src/planner/planner.cpp` — mutation plan 경로 추가 (ORCA 우회)
- `src/execution/` — `PhysicalCreateNode` 연산자
- `src/storage/` — `DeltaStore` 클래스 신규

**VID 할당 전략:**
- 기존 partition의 마지막 extent의 다음 offset부터 순차 할당
- Compaction 시 VID 재배치 가능 → external ID(`id` property)는 변하지 않음

#### Phase 2: CREATE Edge
**목표**: `MATCH (a:Person {id: 1}), (b:Person {id: 2}) CREATE (a)-[:KNOWS]->(b)`

```
Executor→ src VID, dst VID 확인
         → delta_store.adj_delta에 edge 추가
         → edge partition의 insert_buffer에 edge property append
         → 양방향 (forward + backward) 모두 기록
```

**난이도: 높음** — CSR 구조 직접 수정 불가, delta에 기록 후 compaction 시 CSR 재빌드

#### Phase 3: SET Property
**목표**: `MATCH (n:Person {id: 1}) SET n.name = 'Jane'`

```
Executor→ MATCH 결과에서 VID 획득 (ORCA 경유)
         → delta_store.update_log[vid][prop_key] = new_value
         → 결과 반환: "Set 1 property"
```

**Read Merge 필요:**
- PropertyLookup 시 update_log 우선 체크
- ExtentIterator에 delta overlay 추가

#### Phase 4: DELETE Node/Edge
**목표**: `MATCH (n:Person {id: 1}) DELETE n` / `DETACH DELETE n`

```
Executor→ MATCH 결과에서 VID 획득
         → DELETE: delta_store.delete_set.insert(vid)
         → DETACH DELETE: 연결된 edge도 모두 delete_set에 추가
         → 결과 반환: "Deleted 1 node, 3 relationships"
```

**Read Merge 필요:**
- NodeScan 시 delete_set 필터링
- AdjListIterator 시 삭제된 edge 건너뛰기

#### Phase 5: REMOVE / MERGE
- `REMOVE n.prop` → `SET n.prop = NULL` 과 동일
- `MERGE` → `EXISTS` 체크 + 조건부 CREATE/SET (Phase 1-3 완료 후)

#### Phase 6: Compaction & Persistence
- Delta store를 base에 통합
- `dev/update-exp`의 `AppendTuplesToExistingExtent` 활용
- CGC re-clustering (`_RebalanceSchemas` 구현)
- CSR 재빌드
- Catalog 업데이트 + 재직렬화

### 5. Transaction Model

**DuckDB 참조:**
- DuckDB는 MVCC (Multi-Version Concurrency Control) 기반
- 각 트랜잭션에 transaction_id 부여, row별 visibility 관리
- LocalTableStorage가 트랜잭션 로컬 버퍼 역할
- Commit 시 main storage에 merge, Rollback 시 LocalTableStorage 폐기

**TurboLynx 적용 (단순화):**

**Phase 1-4: 단일 쿼리 단위 (auto-commit, MVCC 없음)**
- 각 mutation 쿼리가 하나의 "트랜잭션" (DuckDB의 auto-commit 모드와 동일)
- 실행 중 실패 시 해당 쿼리의 delta 변경사항 롤백 (메모리 내이므로 간단)
- 서버 재시작 시 delta 유실 → Phase 6의 WAL로 해결
- **단일 writer** — 동시 mutation은 직렬화 (DuckDB도 write는 단일 트랜잭션 제한)

**Phase 6+: WAL (Write-Ahead Log, DuckDB 방식)**
- Mutation 실행 전 WAL 파일에 변경 내용 기록 (write-ahead)
- 서버 재시작 시 WAL replay → delta 복구
- Compaction(CHECKPOINT) 완료 시 WAL truncate
- WAL 크기 threshold 초과 시 자동 CHECKPOINT 트리거 (DuckDB default: ~16MB)

### 6. Query Pipeline 수정사항

#### Read+Write 혼합 쿼리

```cypher
MATCH (n:Person {id: 1})
SET n.name = 'Updated'
RETURN n.name
```

처리 순서:
1. **MATCH + RETURN**: ORCA를 통한 정상 읽기 쿼리로 컴파일
2. **SET**: mutation plan으로 별도 처리
3. **실행**: MATCH → SET → RETURN (pipeline 내 순서 보장)

#### Parser 변경

현재 `cypher_transformer.cpp`에서 updating clause를 throw하고 있음:
```cpp
// "Updating clauses (SET/DELETE/CREATE) not yet supported"
throw InternalException("...");
```

이걸 실제 구현으로 교체:
- `transformCreateClause` → `CreateStatement`
- `transformSetClause` → `SetStatement`
- `transformDeleteClause` → `DeleteStatement`
- `transformRemoveClause` → `RemoveStatement`
- `transformMergeClause` → `MergeStatement`

### 7. Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Delta merge로 읽기 성능 저하 | 높음 | Delta가 작을 때 overhead 미미; compaction으로 해소 |
| VID 재배치 시 외부 참조 깨짐 | 높음 | External ID(`id` property)는 불변; VID는 내부용 |
| CSR 재빌드 비용 | 중간 | Edge mutation이 적으면 delta만으로 충분; 대량 시에만 재빌드 |
| Compaction 중 읽기 block | 중간 | Copy-on-write: 새 extent 생성 후 atomic swap |
| 메모리 내 delta 유실 | 중간 | Phase 6에서 WAL 추가로 해결 |
| ORCA와 mutation 간 충돌 | 낮음 | Mutation은 ORCA 우회; read path만 ORCA 사용 |

### 8. Implementation Priority & Estimates

```
Phase 1: CREATE Node        — 핵심 인프라 구축 (Parser/Binder/Executor/DeltaStore)
Phase 2: CREATE Edge        — CSR delta 설계
Phase 3: SET Property       — Read merge layer
Phase 4: DELETE             — Delete set + cascade
Phase 5: REMOVE / MERGE     — Phase 1-4 조합
Phase 6: Compaction + WAL   — 영구성 보장
```

### 9. Open Questions

1. **VID 할당**: compaction 시 VID 재배치를 허용할 것인가? → external ID 기반이면 가능
2. **Multi-label**: `SET n:Employee` — 노드에 라벨 추가 시 다른 partition으로 이동?
3. **Schema evolution**: 기존에 없던 property를 SET하면?
   - DuckDB: 컬럼 추가는 ALTER TABLE로 처리, 기존 row는 NULL
   - TurboLynx: graphlet schema 확장 or UpdateSegment에 새 property 보관
4. **Concurrency**: 동시 mutation 허용?
   - DuckDB: write는 단일 트랜잭션, read는 snapshot isolation
   - Phase 1: 단일 writer, read는 delta merge로 최신 상태 반영
5. **Batch mutation**: `UNWIND [1,2,3] AS x CREATE (n:Person {id: x})` 지원 범위
6. **Compaction 트리거**: DuckDB는 WAL ~16MB 기준. 우리는?
   - WAL 크기 기반 + 삭제율 기반 (DuckDB: 인접 row group 삭제율 25%) 조합
7. **Compaction 중 읽기**: DuckDB는 checkpoint 중 다른 읽기 허용 (MVCC). 우리는?
   - Phase 1: compaction 중 read block (단순), Phase 6+: copy-on-write로 무중단

### 10. DuckDB vs TurboLynx 비교 요약

| 측면 | DuckDB | TurboLynx | 비고 |
|------|--------|-----------|------|
| 원본 구조 | RowGroup + ColumnSegment | Extent + Compressed Chunk | 유사 |
| UPDATE 처리 | UpdateSegment (per column) | UpdateSegment (per chunk) | 동일 패턴 |
| DELETE 처리 | RowVersionManager (validity) | DeleteMask (bitset) | 동일 패턴 |
| INSERT 처리 | LocalTableStorage | InsertBuffer (per partition) | 유사 |
| Compaction | CHECKPOINT (WAL 크기 기반) | CHECKPOINT + CGC re-cluster | **확장** |
| Edge mutation | 해당 없음 (관계형 DB) | AdjList Delta (CSR 변경분) | **그래프 특화** |
| Transaction | MVCC, snapshot isolation | Phase 1: auto-commit only | 단순화 |
| WAL | 있음 (기본) | Phase 6에서 추가 | 점진적 |
| Concurrency | 다중 reader + 단일 writer | 동일 | |

---

## Completed Work (Read Path)

### IC Test Coverage (IC1~IC14 Neo4j verified)

| IC | Query | Status |
|----|-------|--------|
| IC1 | Person location | PASS |
| IC2 | Recent messages | PASS |
| IC3 | Friends in cities (chained comparison) | PASS |
| IC4 | Tag co-occurrence | PASS |
| IC5 | Friend posts with tag | PASS |
| IC6 | Tag co-occurrence (VLE+UNWIND) | PASS |
| IC7 | Recent likers (map literal, head/collect, pattern expr) | PASS |
| IC8 | Recent replies | PASS |
| IC9 | Recent messages by friends (collect+UNWIND rewrite) | PASS |
| IC10 | Friend recommendation (list comprehension, datetime, 2-hop pattern) | PASS |
| IC11 | Job referral | PASS |
| IC12 | Trending posts (multi-label VLE *0..) | PASS |
| IC13 | Shortest path (CASE path IS NULL, negate literal) | PASS |
| IC14 | Weighted shortest path (original Cypher, pattern comprehension + reduce) | PASS |

### Crash-Proof Architecture
- Planner::execute → gpos_exec return code check + exception throw
- turbolynx_execute → try/catch wrapping (compile + execute)
- Shell REPL → SIGSEGV/SIGABRT/SIGFPE signal handler + siglongjmp recovery
- ORCA memory pool → recreated per query (state leak prevention)

### Performance Optimizations
- Query plan cache (AST caching, skip ANTLR parse on repeated queries)
- Comparison normalization (const op var → var flipped_op const at parser level)

### Known Limitations
- `CASE WHEN count(f) > 10 THEN ...` — CASE 내부 aggregation (WITH로 분리 필요)
- MPV 출력 컬럼 중복 — message:Message 접근 시 모든 sub-partition property 출력
- List indexing — shell 동작하지만 C API 리턴 타입 문제 (ORCA ANY type)
- `||` string concat, list slicing — ANTLR 문법 변경 필요
