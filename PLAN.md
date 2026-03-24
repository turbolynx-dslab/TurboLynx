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

#### 3.1 Mutation Strategy: Delta Store + Periodic Compaction

```
┌──────────────┐     ┌──────────────┐
│  Base Store   │     │  Delta Store  │
│  (Extents)    │     │  (Append-only)│
│  - 압축됨      │     │  - 비압축      │
│  - CGC 배치    │     │  - INSERT log  │
│  - Read-opt   │     │  - UPDATE log  │
│               │     │  - DELETE set  │
└──────┬───────┘     └──────┬───────┘
       │                     │
       └────────┬────────────┘
                ↓
         [Read Merge Layer]
         base + delta 합쳐서 읽기
                ↓
         [Periodic Compaction]
         delta를 base에 통합 + CGC re-cluster
```

**Why Delta Store?**
- Base extent는 압축 + CGC 최적 배치 → 직접 수정하면 읽기 성능 저하
- Delta store에 변경사항 축적 → 주기적으로 base에 반영
- `dev/update-exp`의 `AppendTuplesToExistingExtent`를 compaction에 활용

#### 3.2 Delta Store 구조

```
Delta Store (per partition):
├── insert_buffer: vector<DataChunk>     — 새 노드/엣지
├── update_log: map<VID, map<PropKey, Value>>  — 프로퍼티 변경
├── delete_set: unordered_set<VID>       — 삭제된 노드/엣지
└── adj_delta: map<VID, vector<Edge>>    — 새 엣지 (인접 리스트 추가분)
```

#### 3.3 Read Merge

기존 읽기 경로에 delta merge를 추가:
- **NodeScan**: base extent scan + insert_buffer scan, delete_set 필터링
- **PropertyLookup**: update_log에 있으면 delta 값 반환, 없으면 base 값
- **AdjListIterator**: base CSR + adj_delta 합산

#### 3.4 Compaction (Background)

일정 조건 충족 시 (delta 크기 > threshold, 유휴 시간 등):
1. Delete set 적용 — tombstone된 tuple 제거
2. Update log 적용 — base extent 값 교체
3. Insert buffer 통합 — `_ExtractSchemaAndAssign` → 기존 graphlet에 매칭 or 새 extent 생성
4. CGC re-clustering (선택적) — `_RebalanceSchemas` 구현
5. CSR 재빌드 — 새 엣지 반영
6. Delta store 초기화

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

**Phase 1-4: 단일 쿼리 단위 (auto-commit)**
- 각 mutation 쿼리가 하나의 "트랜잭션"
- 실패 시 delta store 변경사항 롤백 (메모리 내이므로 간단)
- 서버 재시작 시 delta 유실 → compaction 전까지 비영구

**Phase 6+: WAL (Write-Ahead Log)**
- Delta 변경사항을 WAL 파일에 먼저 기록
- 서버 재시작 시 WAL replay → delta 복구
- Compaction 완료 시 WAL truncate

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
3. **Schema evolution**: 기존에 없던 property를 SET하면? → graphlet schema 확장 or delta store에만 보관
4. **Concurrency**: 동시 mutation 허용? → Phase 1은 단일 writer, Phase 6+에서 확장
5. **Batch mutation**: `UNWIND [1,2,3] AS x CREATE (n:Person {id: x})` 지원 범위
6. **Delta size limit**: compaction 트리거 조건 — delta row count? 비율?

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
