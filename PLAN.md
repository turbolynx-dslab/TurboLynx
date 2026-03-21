# TurboLynx — Execution Plan

## Current Status

158 query tests pass (726 assertions). IC1~IC9 Neo4j 검증 완료.
UNWIND 전체 스택, collect()+IN/UNWIND 자동 rewrite, VLE isomorphism 수정,
map literal, struct_pack/extract, head/collect/coalesce/size/toInteger/toFloat/floor 지원.

---

## Goal: IC10 원본 쿼리 통과

```cypher
MATCH (person:Person {id: 30786325583618})-[:KNOWS*2..2]-(friend),
      (friend)-[:IS_LOCATED_IN]->(city:City)
WHERE NOT friend=person AND
      NOT (friend)-[:KNOWS]-(person)
WITH person, city, friend, datetime({epochMillis: friend.birthday}) as birthday
WHERE (birthday.month=11 AND birthday.day>=21) OR
      (birthday.month=(11%12)+1 AND birthday.day<22)
WITH DISTINCT friend, city, person
OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post:Post)
WITH friend, city, collect(post) AS posts, person
WITH friend,
     city,
     size(posts) AS postCount,
     size([p IN posts WHERE (p)-[:HAS_TAG]->()<-[:HAS_INTEREST]-(person)]) AS commonPostCount
RETURN friend.id AS personId,
       commonPostCount - (postCount - commonPostCount) AS commonInterestScore,
       friend.gender AS personGender,
       city.name AS personCityName
ORDER BY commonInterestScore DESC, personId ASC
LIMIT 10
```

---

## 미지원 기능 분석

### 이미 지원되는 기능

| 기능 | 상태 | 비고 |
|------|------|------|
| `KNOWS*2..2` (고정 길이 VLE) | ✅ | lower=upper 케이스 |
| 콤마 구분 MATCH `(a)-[]->(b), (b)-[]->(c)` | ✅ | 단일 QueryGraphCollection 내 |
| `NOT friend = person` | ✅ | |
| `WITH ... WHERE` (post-WITH 필터) | ✅ | |
| `WITH DISTINCT` | ✅ | |
| `OPTIONAL MATCH` + `collect()` | ✅ | |
| `size(posts)` — 리스트 길이 | ✅ | list_size 함수 |
| 산술 연산 `a - (b - c)` | ✅ | |
| `%` 모듈로 연산자 | ✅ | |
| `ORDER BY ... DESC, ... ASC` + `LIMIT` | ✅ | |

### 구현 필요한 기능 (4개)

| # | 기능 | 난이도 | 의존성 |
|---|------|--------|--------|
| 1 | Pattern Expression (EXISTS 서브쿼리) | **높음** | 없음 |
| 2 | `datetime({epochMillis: value})` | 중간 | 없음 |
| 3 | Temporal property `.month`, `.day` | 낮음 | #2 |
| 4 | List Comprehension `[x IN list WHERE ...]` | **높음** | #1 (내부에 pattern 사용) |

---

## Milestone 1: Pattern Expression — EXISTS 서브쿼리

### 목표

`(a)-[:REL]-(b)`가 expression context에서 사용될 때 실제 엣지 존재 여부를 확인.

### 현재 상태

```
Parser:     (a)-[:R]-(b) → __pattern_exists(a, 'R', b) ✅
Binder:     __pattern_exists → BOOLEAN 타입 ✅
Converter:  __pattern_exists → constant TRUE ← placeholder, 실제 체크 안 함 ❌
```

### 사용처

- IC7: `not((liker)-[:KNOWS]-(person)) AS isNew` — RETURN에서 사용 (현재 항상 FALSE)
- IC10: `NOT (friend)-[:KNOWS]-(person)` — WHERE에서 사용 (필터링 결과 오류)

### 구현 방안

**옵션 A: OPTIONAL MATCH + IS NULL 리라이트** (권장)

```
WHERE NOT (a)-[:R]-(b)
→ OPTIONAL MATCH (a)-[:R]-(b_check)
  WHERE b_check = b
→ WHERE b_check IS NULL
```

Converter에서 `__pattern_exists`를 감지하면:
1. 해당 패턴을 OPTIONAL MATCH (Left Outer Join)로 변환
2. NULL 체크로 존재 여부 판단
3. `NOT`이면 `IS NULL`, 긍정이면 `IS NOT NULL`

장점: 기존 OPTIONAL MATCH + LOJ 인프라 재활용.

**옵션 B: Semi-Join / Anti-Semi-Join 연산자**

ORCA에 `CLogicalLeftSemiJoin` / `CLogicalLeftAntiSemiJoin`이 존재하면 직접 사용.
더 효율적이지만 ORCA 내부 연산자 활용 검증 필요.

### 수정 파일

| 파일 | 내용 |
|------|------|
| `src/converter/cypher2orca_converter.cpp` | `__pattern_exists` 감지 → LOJ + null check 생성 |
| `src/converter/cypher2orca_scalar.cpp` | constant TRUE placeholder 제거 |

### 검증

- IC7: `isNew` 컬럼이 Neo4j와 일치하는지
- IC10: WHERE 필터가 정확히 동작하는지

---

## Milestone 2: datetime 함수 + Temporal Property Access

### 목표

`datetime({epochMillis: friend.birthday}).month` → 월 추출

### 2-A: `datetime({epochMillis: value})` 함수

**현재**: datetime 함수 미존재. `make_timestamp(y,m,d,h,m,s,ms)` 만 있음.

**구현**:

Binder에서 `datetime({epochMillis: expr})` 감지:
1. `datetime` 함수 호출 + map literal 인자 감지
2. `{epochMillis: expr}`의 expr을 추출
3. → `epoch_ms_to_timestamp(expr)` 내부 함수로 리라이트

`epoch_ms_to_timestamp`: epoch millis (BIGINT) → TIMESTAMP 변환.
DuckDB에 `to_timestamp` 또는 유사 함수가 있을 수 있음. 없으면 신규 생성.

### 2-B: `.month`, `.day` Temporal Property Access

**현재**: `date_part('month', ts)` 함수는 존재. `.month` 문법은 미지원.

**구현**:

Binder의 `BindPropertyExpression`에서:
```
변수가 TIMESTAMP 타입이고, property가 "month"/"day"/"year"/... 이면:
  → date_part('month', expr) 함수 호출로 리라이트
```

이미 alias type tracking (`GetAliasType`)이 있으므로, `birthday`의 타입이 TIMESTAMP임을 알 수 있음.

### 수정 파일

| 파일 | 내용 |
|------|------|
| `src/binder/binder.cpp` | datetime → epoch_ms_to_timestamp 리라이트 |
| `src/binder/binder.cpp` | temporal property → date_part 리라이트 |
| `src/function/scalar/date/` | epoch_ms_to_timestamp 함수 (필요시 신규) |

---

## Milestone 3: List Comprehension — 실행 레벨 구현

### 목표

`[p IN posts WHERE condition]` — 리스트의 각 원소에 predicate를 평가하여 새 리스트 생성.
`[p IN posts | expr]` — 리스트의 각 원소를 매핑하여 새 리스트 생성.
`[p IN posts WHERE cond | expr]` — 필터 + 매핑 조합.

### 설계 결정

**실행 레벨 구현 (PhysicalListComprehension)**을 선택.

Converter 리라이트(UNWIND+collect)나 list_filter 스칼라 함수는 특정 패턴에만 동작하는
workaround. 다른 형태의 list comprehension이 올 때마다 새 리라이트가 필요해짐.
실행 레벨에서 한 번 구현하면 모든 변형을 처리 가능.

### 문법 (이미 존재)

Grammar (`Cypher.g4`)에 정의됨:
```
oC_ListComprehension : '[' oC_FilterExpression ('|' oC_Expression)? ']'
oC_FilterExpression  : oC_IdInColl (SP? oC_Where)?
oC_IdInColl          : oC_Variable SP IN SP oC_Expression
```

Parser transformer에는 미구현 (현재 `oC_Atom`에서 이 경로를 처리하지 않음).

### IC10에서의 사용

```cypher
size([p IN posts WHERE (p)-[:HAS_TAG]->()<-[:HAS_INTEREST]-(person)])
```

WHERE 내부에 **pattern expression**이 있음 → Milestone 1 의존.

### 구현 계획

#### 3-A: Parser — `transformListComprehension`

```
[p IN posts WHERE cond]
→ ListComprehensionExpression {
    variable: "p",
    source:   posts,
    filter:   cond,       // optional
    mapping:  null         // optional, '|' 뒤의 expression
  }
```

`oC_Atom` 에서 `oC_ListComprehension` 분기 추가.

#### 3-B: Binder — ListComprehension 바인딩

1. source expression 바인딩 → LIST 타입 확인, element type 추출
2. loop variable을 임시 BindContext에 등록 (element type으로)
3. filter expression 바인딩 (있으면) — BOOLEAN 반환 확인
4. mapping expression 바인딩 (있으면) — 반환 타입이 새 LIST의 element type
5. 결과 타입: `LIST(mapping_type)` 또는 `LIST(element_type)`

내부에 pattern expression `(p)-[:HAS_TAG]->()...`이 있으면
기존 `__pattern_exists` 바인딩 경로를 탐.

#### 3-C: Converter — ORCA representation

List comprehension을 ORCA scalar expression으로 변환.
`CScalarFunc("list_comprehension", [source, filter_expr, map_expr])` 형태.
filter/map expression은 ORCA scalar tree로 변환되어 children에 포함.

#### 3-D: Physical Planner + Execution — PhysicalListComprehension

새 실행 operator. 핵심 로직:

```cpp
void PhysicalListComprehension::Execute(DataChunk &input, DataChunk &output) {
    for each row:
        auto &source_list = input[source_col].GetValue(row);
        vector<Value> result;
        for each element in source_list:
            // bind element to execution context
            bind_variable(loop_var, element);
            // evaluate filter (if exists)
            if (!filter || evaluate_predicate(filter, element)):
                // evaluate mapping (if exists)
                if (mapping):
                    result.push_back(evaluate_expr(mapping, element))
                else:
                    result.push_back(element)
        output[result_col].SetValue(row, Value::LIST(result));
}
```

**Pattern expression 내부 처리**:
- `(p)-[:HAS_TAG]->()<-[:HAS_INTEREST]-(person)` 평가 시
  엣지 인덱스 조회가 필요 → `AdjacencyIndex::Lookup(p, HAS_TAG)` 호출
- Milestone 1에서 구현하는 pattern expression 평가 로직을 재활용
- 단, 여기서는 plan operator가 아닌 **scalar evaluation context**에서 호출되므로
  execution engine에 그래프 조회 API가 노출되어야 함

### 수정 파일

| 파일 | 내용 |
|------|------|
| `src/parser/cypher_transformer.cpp` | `transformListComprehension` 핸들러 |
| `src/include/parser/expression/` | `ListComprehensionExpression` AST 노드 (신규) |
| `src/binder/binder.cpp` | list comprehension 바인딩 + 임시 변수 스코프 |
| `src/converter/cypher2orca_scalar.cpp` | ORCA scalar 변환 |
| `src/planner/planner_physical.cpp` | PhysicalListComprehension 생성 |
| `src/execution/physical_operator/` | `PhysicalListComprehension` (신규) |

### 난이도 분석

- Parser + Binder: 중간 (문법 이미 존재, 바인딩은 임시 변수 스코프만 추가)
- Converter: 중간 (새 scalar function 등록)
- Execution: **높음** (scalar context에서 그래프 조회 필요, element별 predicate 평가)

---

## Milestone 4: 통합 + IC10 테스트

### 의존성 그래프

```
M1 (Pattern Expression)
  ↓
M3 (List Comprehension) ← pattern이 내부에서 사용됨
  ↓
M4 (IC10 통합 테스트)

M2 (datetime + temporal property) ← M4에서 필요하지만 독립 구현 가능
```

### 구현 순서 (권장)

```
M2 (datetime)  →  독립적, 난이도 낮음
M1 (pattern)   →  IC7 isNew 수정 + IC10 WHERE 필터
M3 (list comp) →  M1 의존, IC10 commonPostCount
M4 (통합)      →  전체 IC10 테스트
```

### IC10 테스트 예상 결과 (Neo4j 검증)

```
| personId       | personFirstName | personLastName | commonInterestScore | personGender | personCityName    |
| 30786325580467 | Michael         | Taylor         | 0                   | female       | Geelong           |
| 30786325582432 | Chris           | Goenka         | 0                   | male         | Sittwe_District   |
| 4398046514484  | Nikhil          | Kumar          | -1                  | female       | Mysore            |
```

---

## Known Technical Debt

### MPV 출력 컬럼 중복

`message:Message` (MPV = Comment + Post) 접근 시 모든 sub-partition property가 출력에 포함됨.
RETURN에서 요청한 컬럼만 출력해야 하는데, IdSeek-MPV가 전체 property를 scan.
IC9에서 15개 컬럼 출력 (기대값 6개). Debug build에서 segfault 원인.

### Debug build Vector type assertion

UNWIND→MATCH, LIST→STRUCT→scalar pipeline boundary에서 Vector type mismatch assertion.
Release build 정상. IC7, IC9 debug build skip.

### PlanRegularMatch HashJoin 비효율

두 endpoint 모두 bound인 multi-edge chain에서 HashJoin + full NodeScan.

### Pattern Expression placeholder

`__pattern_exists` → constant TRUE. Milestone 1에서 해결 예정.

---

## 버그

| 파일 | 내용 |
|------|------|
| `extent_iterator.cpp:205` | 인덱스 계산 오류 |
| `histogram_generator.cpp:215` | boundary ascending 보장 미흡 |
