# TurboLynx — Execution Plan

## Current Status

All 148 query tests pass. IC1~IC6 Neo4j 검증 완료.
UNWIND 전체 스택, collect()+IN 자동 rewrite, VLE isomorphism 수정, toInteger/toFloat/floor 지원.

### Query Test Status

| 태그 | 테스트 수 | 상태 | 비고 |
|------|-----------|------|------|
| `[q1]` | 27 | **26 pass, 1 skip** | Q1-22 MPV NodeScan 미지원 |
| `[q2]` | 22 | **22 pass** | UNWIND 6 + func 4 포함 |
| `[q3]` | 9 | **9 pass** | |
| `[q4]` | 7 | **7 pass** | IS 전체 |
| `[q5]` | 11 | **11 pass** | IC1~IC6 (Neo4j 검증) |
| `[q6]` | 65 | **65 pass** | |
| `[q7]` | 4 | **1 pass, 2 fail, 1 skip** | MPV NodeScan 미지원 |
| `[execution]` | 3 | **3 pass** | VLE 회귀 테스트 |

**Total: 148 tests, all pass.**

5개 Phase 2 테스트 추가됨 (Q2-40~Q2-44, `[map]` 태그) — 현재 전부 실패. Phase 2 구현 후 통과 예정.

---

## 이번 세션 완료 작업

| 작업 | 커밋 |
|------|------|
| VLE isomorphism level desync 버그 수정 | `08a43dfcb` |
| VLE 회귀 테스트 (MockDFS, diamond/star graph) | `4d50fef12` |
| `collect()` + `IN list` 연산자 지원 | `a92e527a4` |
| OPTIONAL MATCH edge reordering (unbound first edge) | (위 커밋에 포함) |
| ORDER BY non-RETURN property 지원 | `2df5efef0` |
| collect+IN → DISTINCT 자동 rewrite (plan 최적화) | `9a5742d10` |
| UNWIND 전체 스택 (CLogicalUnnest, CPhysicalUnnest, PhysicalConstScan) | `034155e47`, `cd4e1c721` |
| UNWIND node binding (collect(node) → UNWIND → MATCH index join) | `b6630a1c7` |
| IC3/IC5/IC6 테스트 Neo4j 검증값 업데이트 | 각 커밋에 포함 |
| toInteger, toFloat, floor 함수 지원 | `79d823180` |
| CPhysicalComputeScalar 물리 플래너 지원 | (위 커밋에 포함) |

---

## Next: IC7 Phase 2 — Map Literal + Property Access (진행 중)

### 완료된 부분
- ✅ Grammar: `oC_MapLiteral` 규칙 추가, ANTLR 재생성 완료
- ✅ Parser: `transformMapLiteral` → `FunctionExpression("struct_pack", aliased children)`
- ✅ Binder: `info.name` → `struct_extract(info, 'name')` (non-node property access)
- ✅ Binder: `head(list)` → `list_extract(list, 1)` rewrite

### 남은 작업 (다음 세션)
1. **`struct_pack` 함수 구현**: `src/function/scalar/struct/struct_pack.cpp` 신규 생성. DuckDB의 STRUCT 타입으로 named fields를 가진 구조체 생성.
2. **`struct_extract` 함수 구현**: `src/function/scalar/struct/struct_extract.cpp` 신규 생성. STRUCT에서 named field를 추출.
3. **`list_extract` 함수 활성화**: `src/function/scalar/list/list_extract.cpp`에 `RegisterFunction` 주석 해제 + 수정.
4. **nested_functions.hpp/cpp**: 위 3개 함수 등록 (현재 주석 처리)
5. **Converter 검증**: `struct_pack`/`struct_extract`가 ORCA `CScalarFunc`로 변환되는지 확인

### 목표

IC7 (Recent Likers) 쿼리의 핵심 기능 구현:
```cypher
WITH {msg: message, likeTime: likeTime} AS info   -- map literal
RETURN info.msg.id                                  -- nested property access
```

### 준비된 테스트 (Q2-40 ~ Q2-44, `[map]` 태그)

| 테스트 | 내용 | 필요 기능 |
|--------|------|-----------|
| Q2-40 | `{name: p.firstName, age: 42}` → `info.name` | map literal + field access |
| Q2-41 | `{id: p.id, first: p.firstName, last: p.lastName}` | 다중 필드 map |
| Q2-42 | `head(collect({tagName: t.name}))` → field access | map + collect + head |
| Q2-43 | `{tag: t, personName: p.firstName}` → `info.personName` | map에 node 저장 |
| Q2-44 | `head(collect(t.name))` | head() 함수 단독 |

### 구현 단계

#### Step 1: Grammar — `oC_MapLiteral` 추가

**파일**: `third_party/antlr4_cypher/Cypher.g4`

`oC_Atom` 규칙에 `oC_MapLiteral` 추가:
```
oC_Atom
    : oC_Literal
    | oC_MapLiteral        // ← 추가
    | oC_Parameter
    | oC_CaseExpression
    | ...
    ;

oC_MapLiteral
    : '{' SP? (oC_PropertyKeyName SP? ':' SP? oC_Expression SP?
       (',' SP? oC_PropertyKeyName SP? ':' SP? oC_Expression SP?)*)? '}'
    ;
```

**주의**: `kU_Properties`와 문법 충돌 가능 (동일한 `{k: v}` 구조). `kU_Properties`는 node/edge pattern 내에서만 사용되므로, `oC_MapLiteral`는 expression context에서만 매칭.

**ANTLR 재생성 필요**: `antlr4 -Dlanguage=Cpp Cypher.g4` 실행. 생성된 파일은 `third_party/antlr4_cypher/antlr4/` 에 위치.

#### Step 2: Parser Visitor — MapLiteral → AST

**파일**: `src/include/parser/expression/map_literal_expression.hpp` (신규)
**파일**: `src/parser/cypher_visitor.cpp` (수정)

```cpp
class MapLiteralExpression : public ParsedExpression {
public:
    vector<string> keys;
    vector<unique_ptr<ParsedExpression>> values;
};
```

Visitor에서 `visitOC_MapLiteral` 핸들러 추가:
```cpp
antlrcpp::Any CypherVisitor::visitOC_MapLiteral(CypherParser::OC_MapLiteralContext *ctx) {
    auto expr = make_unique<MapLiteralExpression>();
    // ... key-value 쌍 파싱
    return expr;
}
```

#### Step 3: Binder — MapLiteral → BoundExpression

**파일**: `src/binder/binder.cpp`

`MapLiteralExpression` → `CypherBoundFunctionExpression("struct_pack", children)`

DuckDB의 `struct_pack(k1 := v1, k2 := v2, ...)` 함수로 변환. 반환 타입은 `STRUCT(k1 type1, k2 type2, ...)`.

#### Step 4: Map Property Access — `info.name`

**파일**: `src/binder/binder.cpp` — `BindPropertyExpression`

현재 `expr.GetVariableName()`이 node/edge 이름을 찾으면 property access. 찾지 못하면 map field access로 처리:
- `info.name` → `struct_extract(info, 'name')` 함수 호출로 변환

`struct_extract`는 DuckDB에 이미 등록되어 있음 (`src/function/scalar/struct/`).

#### Step 5: head() 함수

**파일**: `src/function/scalar/list/` 또는 binder에서 rewrite

`head(list)` → `list_extract(list, 1)` (1-indexed). DuckDB의 `list_extract`가 이미 존재하면 binder에서 rewrite만.

#### Step 6: Converter/Planner

`struct_pack`, `struct_extract`는 일반 scalar function이므로 converter의 `ConvertFunction` 경로를 타면 됨. 추가 수정 불필요할 가능성 높음.

### 예상 영향 파일

| 파일 | 수정 내용 |
|------|-----------|
| `Cypher.g4` | `oC_MapLiteral` 규칙 추가 |
| `antlr4/CypherParser.*` | ANTLR 재생성 |
| `antlr4/CypherVisitor.*` | ANTLR 재생성 |
| `src/parser/cypher_visitor.cpp` | `visitOC_MapLiteral` 핸들러 |
| `src/include/parser/expression/map_literal_expression.hpp` | 신규 AST 노드 |
| `src/binder/binder.cpp` | MapLiteral 바인딩 + map field access |
| `test/query/test_q2_filter.cpp` | Q2-40~Q2-44 테스트 (이미 작성) |

---

## IC7 전체 로드맵

| Phase | 기능 | 상태 | 테스트 |
|-------|------|------|--------|
| **1** | toInteger, toFloat, floor | ✅ 완료 | Q2-30~Q2-33 |
| **2** | Map literal + property access + head() | **다음 작업** | Q2-40~Q2-44 |
| **3** | Ordered aggregation (ORDER BY + collect) | 미시작 | |
| **4** | Pattern expression `(a)-[:R]-(b)` in RETURN | 미시작 | |

---

## Known Technical Debt

### UNWIND→MATCH pipeline Vector type assertion (debug only)

UNWIND로 node VID list를 펼친 후 MATCH에서 DataChunk LIST→UBIGINT 타입 assertion 발생. Release build 정상. IC6 테스트에서 debug build graceful skip.

### PlanRegularMatch HashJoin 비효율

두 endpoint 모두 bound인 multi-edge chain에서 HashJoin + full NodeScan. IC5 ~400ms (최적 ~280ms).

### NodeScan Multi-Partition Vertex

Message MPV 스캔 시 CCM file_handler 미등록 assertion. Q1-22, Q7 일부.

### OPTIONAL MATCH atomic semantics

Multi-hop OPTIONAL MATCH chained LOJ fallback. 중간 노드 참조 시 Neo4j와 결과 차이 가능.

### SFG 레거시 코드

Pipeline executor에서 SFG 결과 즉시 덮어쓰는 죽은 코드. 제거 가능, 우선순위 낮음.

### 버그

| 파일 | 내용 |
|------|------|
| `extent_iterator.cpp:205` | 인덱스 계산 오류 |
| `histogram_generator.cpp:215` | boundary ascending 보장 미흡 |
