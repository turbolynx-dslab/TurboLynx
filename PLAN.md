# TurboLynx — Execution Plan

## Current Status

Core build is stable. All unit tests (catalog 51, storage 68, common 10) pass.
LDBC SF1 + TPC-H SF1 + DBpedia bulkload E2E tests all passing.

**M19 완료. M20 (Kuzu 제거 — TurboLynx Parser/Binder/Converter) 진행 예정.**

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
| 20 | Kuzu 제거 — TurboLynx Parser/Binder/Converter | Kuzu Parser·Binder 전면 교체, TurboLynx-native 4단계 파이프라인 구현, `optimizer/kuzu/` 삭제 | 🔲 |

---

## Milestone 20 — Kuzu 제거: TurboLynx-native Parser / Binder / Converter

**Goal:** `optimizer/kuzu/` 디렉터리를 완전히 제거하고, Cypher 처리 파이프라인을 TurboLynx 전용으로 재구현.
ORCA 최적화 엔진은 유지. DuckDB·Kuzu 코드가 뒤섞인 planner_logical.cpp 계층을 깔끔하게 교체.

---

### 동기

| 현재 (Kuzu 기반) | 이후 (TurboLynx-native) |
|-----------------|------------------------|
| `optimizer/kuzu/` — 49 파일, 외부 코드베이스 | 삭제 |
| `kuzu::binder::Binder` — TurboLynx 카탈로그 무관 | `turbolynx::Binder` — partition OID, graphlet OID 직접 참조 |
| `kuzu::binder::BoundStatement` — 범용 그래프 IR | `turbolynx::BoundRegularQuery` — TurboLynx 스키마 특화 |
| `planner_logical.cpp` (~2000줄) — Kuzu→ORCA 변환 인라인 | `Cypher2OrcaConverter` 클래스로 분리 |
| `planner_logical_scalar.cpp` (~1500줄) — 스칼라 변환 분산 | ExpressionBinder + Converter로 통합 |

---

### 새 파이프라인

```
Cypher 문자열
    │
    ▼ [Stage 1] Parser
    S62CypherParser (ANTLR4)
    Transformer::transform()
    → ParsedStatement (RegularQuery / SingleQuery / QueryPart / ...)
    │
    ▼ [Stage 2] Binder
    Binder::bind(ParsedStatement)
    ├── BindContext (변수 스코프, 파티션 OID, 그래플릿 OID)
    ├── ExpressionBinder (타입 추론, 카탈로그 조회)
    └── → BoundRegularQuery
        ├── NormalizedSingleQuery
        │   └── NormalizedQueryPart
        │       ├── BoundMatchClause (with QueryGraph)
        │       └── BoundProjectionBody
        └── BoundStatementResult (output schema)
    │
    ▼ [Stage 3] Cypher2OrcaConverter
    Cypher2OrcaConverter::convert(BoundRegularQuery)
    ├── convertMatchClause()   → CLogicalGet / CLogicalSelect
    ├── convertProjection()    → CLogicalProject
    ├── convertGroupBy()       → CLogicalGbAgg
    ├── convertOrderBy()       → CLogicalLimit (sort)
    └── → LogicalPlan (CExpression* root + LogicalSchema)
    │
    ▼ [Stage 4] ORCA (기존 유지)
    CEngine::Optimize() → physical CExpression
    │
    ▼ [Stage 5] Physical Converter (기존 유지)
    pGenPhysicalPlan() → vector<CypherPipeline*>
```

---

### 핵심 타입 설계

#### Parser AST (Stage 1 출력)

```cpp
// 최상위
struct RegularQuery {
    vector<unique_ptr<SingleQuery>> singleQueries;
    // UNION / UNION ALL 연결
};

struct SingleQuery {
    vector<unique_ptr<QueryPart>> queryParts;   // WITH 절로 분리
    unique_ptr<ReturnClause> returnClause;
};

struct QueryPart {
    vector<unique_ptr<ReadingClause>>  readingClauses;  // MATCH, UNWIND
    vector<unique_ptr<UpdatingClause>> updatingClauses; // SET, DELETE, ...
    unique_ptr<WithClause> withClause;
};

// 그래프 패턴
struct NodePattern {
    string name;
    vector<string> labels;
    vector<pair<string,unique_ptr<ParsedExpression>>> properties;
};

struct RelPattern {
    string name;
    vector<string> types;
    RelDirection direction;  // LEFT / RIGHT / BOTH
    RelPatternType patternType;  // SIMPLE / VARIABLE_LENGTH / SHORTEST / ALL_SHORTEST
    unique_ptr<ParsedExpression> lowerBound, upperBound;
};
```

#### Bound AST (Stage 2 출력)

```cpp
// QueryGraph: MATCH 패턴의 그래프 모델
class QueryGraph {
    vector<shared_ptr<BoundNodeExpression>> queryNodes;
    vector<shared_ptr<BoundRelExpression>>  queryRels;
    map<string, idx_t> nodeNameToPos, relNameToPos;
};

// 노드 바인딩: 카탈로그 해석 결과 포함
struct BoundNodeExpression {
    string varName;
    vector<table_oid_t>    partitionOIDs;  // 매칭되는 파티션들
    vector<graphlet_oid_t> graphletOIDs;
    LogicalType uniqueID_type;
    PropertySchema propSchema;
};

// 관계 바인딩
struct BoundRelExpression {
    string varName;
    shared_ptr<BoundNodeExpression> srcNode, dstNode;
    RelDirection direction;
    vector<table_oid_t> edgeTypeOIDs;
    idx_t lowerBound = 1, upperBound = 1;
};
```

---

### 파일 변경

#### 신규 생성

```
src/include/parser/
  query/
    regular_query.hpp
    single_query.hpp
    query_part.hpp
    reading_clause/match_clause.hpp
    reading_clause/unwind_clause.hpp
    updating_clause/set_clause.hpp
    updating_clause/delete_clause.hpp
    updating_clause/insert_clause.hpp
    return_clause.hpp / with_clause.hpp / projection_body.hpp
    graph_pattern/node_pattern.hpp
    graph_pattern/rel_pattern.hpp
    graph_pattern/pattern_element.hpp

src/include/planner/
  binder.hpp
  bind_context.hpp
  binder_scope.hpp
  expression_binder.hpp
  expression_binder_util.hpp
  query/
    bound_regular_query.hpp
    normalized_single_query.hpp
    normalized_query_part.hpp
    graph_pattern/query_graph.hpp
  expression/
    bound_node_expression.hpp
    bound_rel_expression.hpp
    bound_property_expression.hpp

src/include/converter/c2o/
  cypher2orca_converter.hpp

src/planner/
  binder.cpp
  expression_binder.cpp
  expression_binder_util.cpp

src/converter/c2o/
  cypher2orca_converter.cpp
  cypher2orca_scalar.cpp   ← 스칼라 expression 변환 (현 planner_logical_scalar.cpp 대체)
```

#### 수정

```
src/include/planner/planner.hpp     ← kuzu:: include 제거, 새 Binder/Converter 사용
src/planner/planner.cpp             ← 새 파이프라인 연결
src/planner/planner_logical.cpp     ← Cypher2OrcaConverter로 대체 후 삭제
src/planner/planner_logical_scalar.cpp  ← cypher2orca_scalar.cpp으로 대체 후 삭제
src/main/capi/s62-c.cpp            ← kuzu::binder:: → turbolynx::Binder
```

#### 삭제

```
src/optimizer/kuzu/   ← 전체 (49 .cpp + 110개 헤더)
```

---

### 구현 순서

| 단계 | 작업 | 비고 |
|------|------|------|
| 20a | Parser AST 타입 정의 | `src/include/parser/query/` 전체. 빌드만 되면 됨 |
| 20b | Transformer 구현 | ANTLR4 tree → Parser AST. 기존 `src/parser/transformer.cpp` 기반 재작성 |
| 20c | BindContext + BoundExpression 타입 | 카탈로그 연동 없이 타입만 먼저 |
| 20d | Binder 구현 | `bindQueryNode()`, `bindQueryRel()`, `bindMatchClause()`, QueryGraph 구성 |
| 20e | ExpressionBinder 구현 | 프로퍼티, 함수, 상수, 비교, CASE 등 |
| 20f | Cypher2OrcaConverter 구현 | BoundRegularQuery → CExpression. planner_logical.cpp 기능을 클래스로 이식 |
| 20g | Planner 연결 | planner.hpp에서 Kuzu include 제거, 새 Binder·Converter 연결. 빌드 통과 |
| 20h | planner_logical*.cpp 삭제 + optimizer/kuzu/ 삭제 | 빌드 재확인 |
| 20i | LDBC Query 테스트 전체 통과 확인 | Q1-01~21, Q2-01~09 |

---

### 주요 설계 결정

- **Binder는 카탈로그 직접 참조** — `Catalog::GetGraphEntry()`, `GetPartitionEntry()` 등을 통해 partition OID, graphlet OID를 바인딩 타임에 결정. 런타임 lookup 없음.
- **QueryGraph는 join ordering 힌트** — ORCA에 넘기기 전 MATCH 패턴의 연결 구조를 명시적으로 표현. 향후 cost estimation 개선에 활용.
- **Cypher2OrcaConverter는 Visitor 패턴** — `convert(BoundMatchClause&)`, `convert(BoundProjectionBody&)` 등 오버로드로 처리. 현재 planner_logical.cpp의 거대한 if-else 체인 제거.
- **스칼라 expression은 별도 파일** — `cypher2orca_scalar.cpp` 분리로 유지보수성 확보.
- **Kuzu 타입은 단계적으로 제거** — 20g까지 Kuzu 코드 존재 허용. 20h에서 일괄 삭제.

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
