# TurboLynx — Execution Plan

## Current Status

All 144 query tests pass. IC1~IC6 Neo4j 검증 완료.
UNWIND 전체 스택 구현, collect()+IN 자동 rewrite, VLE isomorphism 버그 수정.

### Query Test Status

| 태그 | 테스트 수 | 상태 | 비고 |
|------|-----------|------|------|
| `[q1]` | 27 | **26 pass, 1 skip** | Q1-22 MPV NodeScan 미지원 |
| `[q2]` | 18 | **18 pass** | UNWIND 6개 포함 |
| `[q3]` | 9 | **9 pass** | |
| `[q4]` | 7 | **7 pass** | IS 전체 |
| `[q5]` | 11 | **11 pass** | IC1~IC6 (Neo4j 검증) |
| `[q6]` | 65 | **65 pass** | |
| `[q7]` | 4 | **1 pass, 2 fail, 1 skip** | MPV NodeScan 미지원 |
| `[execution]` | 3 | **3 pass** | VLE 회귀 테스트 |

**Total: 144 tests, all pass.**

---

## Next: IC7 (Recent Likers) — 미지원 기능 구현 로드맵

IC7 원본 쿼리:
```cypher
MATCH (person:Person {id: ...})<-[:HAS_CREATOR]-(message:Message)<-[like:LIKES]-(liker:Person)
WITH liker, message, like.creationDate AS likeTime, person
ORDER BY likeTime DESC, toInteger(message.id) ASC
WITH liker, head(collect({msg: message, likeTime: likeTime})) AS latestLike, person
RETURN
    liker.id AS personId,
    liker.firstName AS personFirstName,
    liker.lastName AS personLastName,
    latestLike.likeTime AS likeCreationDate,
    latestLike.msg.id AS commentOrPostId,
    coalesce(latestLike.msg.content, latestLike.msg.imageFile) AS commentOrPostContent,
    toInteger(floor(toFloat(latestLike.likeTime - latestLike.msg.creationDate)/1000.0)/60.0) AS minutesLatency,
    not((liker)-[:KNOWS]-(person)) AS isNew
ORDER BY likeCreationDate DESC, toInteger(personId) ASC
LIMIT 20
```

### 미지원 기능 목록 (구현 순서)

| # | 기능 | 예시 | 난이도 | 의존성 |
|---|------|------|--------|--------|
| 1 | `toInteger()` | `toInteger(message.id)` | 하 | 없음 |
| 2 | `toFloat()` | `toFloat(x)` | 하 | 없음 |
| 3 | `floor()` | `floor(x / 1000.0)` | 하 | 없음 |
| 4 | Map literal `{k: v}` | `{msg: message, likeTime: t}` | **상** | 없음 |
| 5 | Map property access | `latestLike.msg` | **상** | #4 |
| 6 | Nested property access | `latestLike.msg.id` | **상** | #5 |
| 7 | `head()` 함수 | `head(collect({...}))` | 중 | #4 |
| 8 | Ordered aggregation | `ORDER BY ... WITH collect(...)` | 중 | 없음 |
| 9 | Pattern expression | `(liker)-[:KNOWS]-(person)` | **상** | 없음 |
| 10 | `not()` on pattern | `not((a)-[:R]-(b))` | **상** | #9 |

### 구현 상세

#### Phase 1: 캐스트/수학 함수 (#1~#3) — 난이도 하

`toInteger()`, `toFloat()`, `floor()` — DuckDB에 이미 등록되어 있을 가능성 높음. Binder에서 함수명 매핑만 추가하면 됨.
- `toInteger` → `CAST(x AS BIGINT)` 또는 DuckDB `cast` 함수
- `toFloat` → `CAST(x AS DOUBLE)`
- `floor` → DuckDB `floor()` (이미 존재)

**영향 파일**: `src/binder/binder.cpp` (함수명 인식)

#### Phase 2: Map literal + property access (#4~#6) — 난이도 상

Cypher의 `{key: value, ...}` map 구조를 DuckDB의 `STRUCT` 타입으로 매핑.

**Parser**: `{k: v}` 구문을 `MapLiteralExpression`으로 파싱 (이미 지원 가능성 있음)
**Binder**: `MapLiteralExpression` → `BoundStructExpression` 바인딩
**Converter**: ORCA `CScalarFunc(struct_pack)` 변환
**Execution**: `StructValue::GetChildren()` 기반 property access

**Nested access** `latestLike.msg.id`:
1. `latestLike` → STRUCT 컬럼
2. `.msg` → `struct_extract(latestLike, 'msg')` → node VID
3. `.id` → node property lookup by VID

이건 node VID를 STRUCT 필드에 저장하고, 나중에 IdSeek으로 property를 조회하는 2-phase access. 가장 복잡한 부분.

#### Phase 3: head() + ordered aggregation (#7~#8) — 난이도 중

`head(list)` → `list[0]` 또는 `list_extract(list, 1)`. DuckDB에 이미 있음.

**Ordered aggregation**: `ORDER BY ... WITH collect(...)` — collect가 ORDER BY 순서를 유지해야 함. 현재 HashAggregate는 순서를 보장하지 않음. StreamAggregate 또는 사전 정렬 필요.

#### Phase 4: Pattern expression (#9~#10) — 난이도 상

`(liker)-[:KNOWS]-(person)` — RETURN 절에서 패턴 존재 여부를 boolean으로 반환.

**구현 방향**:
- Binder: `PatternExpression` → `ExistsSubquery` 변환
- Converter: ORCA `CLogicalCorrelatedLeftSemiApply` (EXISTS) 사용
- `not(pattern)` → `CLogicalCorrelatedLeftAntiSemiApply` (NOT EXISTS)

이건 correlated subquery 지원이 전제. ORCA에는 이미 Apply 연산자가 있으므로 converter 연동이 핵심.

### 대안: IC7 restructure

원본 형태 대신 map literal/pattern expression을 피한 restructured 버전:
```cypher
MATCH (person:Person {id: ...})<-[:HAS_CREATOR]-(message:Message)<-[like:LIKES]-(liker:Person)
WITH liker, message, like.creationDate AS likeTime, person
ORDER BY likeTime DESC, message.id ASC
WITH liker, collect(message)[0] AS latestMsg,
     collect(likeTime)[0] AS latestLikeTime, person
OPTIONAL MATCH (liker)-[:KNOWS]-(person)
WITH liker, latestMsg, latestLikeTime, person,
     CASE WHEN person IS NULL THEN true ELSE false END AS isNew
RETURN liker.id, liker.firstName, liker.lastName,
       latestLikeTime, latestMsg.id,
       coalesce(latestMsg.content, latestMsg.imageFile),
       isNew
ORDER BY latestLikeTime DESC, liker.id ASC
LIMIT 20
```
이 버전은 map literal, nested access, pattern expression을 피하지만, `collect(message)[0]` (list indexing)과 ordered collect가 필요.

---

## Known Technical Debt

### UNWIND→MATCH pipeline Vector type assertion (debug only)

UNWIND로 node VID list를 펼친 후 MATCH에서 사용할 때, DataChunk의 LIST→UBIGINT 타입 전환이 pipeline boundary에서 assertion 발생. Release build에서는 정상 동작. IC6 테스트에서 debug build는 graceful skip.

**영향**: IC6 (debug build에서만)
**근본 원인**: PhysicalUnwind output schema는 element type이지만, 이전 pipeline의 sink가 LIST type으로 데이터 저장

### PlanRegularMatch HashJoin 비효율

두 endpoint 모두 bound인 multi-edge chain에서 HashJoin + full NodeScan 발생. Edge 순서 최적화 또는 ORCA IndexNLJ 선호도 조정 필요.

**영향**: IC5 (~400ms, 최적 시 ~280ms)

### NodeScan Multi-Partition Vertex (CCM file_handler 미등록)

Message MPV (Post + Comment) 스캔 시 assertion 실패.
**영향**: Q1-22, Q7 일부

### OPTIONAL MATCH atomic semantics

Multi-hop OPTIONAL MATCH에서 chained LOJ fallback 사용. 중간 optional 노드가 있는 경우 Neo4j와 결과 다를 수 있음.

### Schema Flow Graph (SFG) 레거시 코드

Pipeline executor에서 SFG 결과를 즉시 UNARY로 덮어쓰고 있어 사실상 죽은 코드. Source의 multi-partition schema 전환에서만 일부 사용. 제거 가능하지만 우선순위 낮음.

### 기타 버그

| 파일 | 내용 |
|------|------|
| `extent_iterator.cpp:205` | 인덱스 계산 오류 |
| `histogram_generator.cpp:215` | boundary ascending 보장 미흡 |
