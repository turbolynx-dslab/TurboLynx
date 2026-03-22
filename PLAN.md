# TurboLynx — Execution Plan

## Current Status

317 tests (220 robustness + 97 functional), 904 assertions, all passing.
IC1~IC13 Neo4j verified. Crash-proof signal handler in shell.

### IC Test Coverage

| IC | Query | Status | Notes |
|----|-------|--------|-------|
| IC1 | Person location | PASS | |
| IC2 | Recent messages | PASS | |
| IC3 | Friends in cities | PASS | |
| IC4 | Tag co-occurrence | PASS | |
| IC5 | Friend posts with tag | PASS | |
| IC6 | Tag co-occurrence (VLE+UNWIND) | PASS | debug build skip |
| IC7 | Recent likers (map literal, head/collect, pattern expr) | PASS | debug build skip |
| IC8 | Recent replies | PASS | |
| IC9 | Recent messages by friends (collect+UNWIND rewrite) | PASS | debug build skip |
| IC10 | Friend recommendation (list comprehension, datetime, 2-hop pattern) | PASS | debug build skip |
| IC11 | Job referral | PASS | |
| IC12 | Trending posts (multi-label VLE *0..) | PASS | |
| IC13 | Shortest path (comma pattern, length(path)) | PASS | |
| **IC14** | **Weighted shortest path** | **NOT STARTED** | **6 missing features** |

---

## Goal: IC14 Original Query

```cypher
MATCH path = allShortestPaths((person1:Person {id: ...})-[:KNOWS*0..]-(person2:Person {id: ...}))
WITH collect(path) as paths
UNWIND paths as path
WITH path, relationships(path) as rels_in_path
WITH
    [n in nodes(path) | n.id] as personIdsInPath,
    [r in rels_in_path |
        reduce(w=0.0, v in [
            (a:Person)<-[:HAS_CREATOR]-(:Comment)-[:REPLY_OF]->(:Post)-[:HAS_CREATOR]->(b:Person)
            WHERE (a.id = startNode(r).id and b.id=endNode(r).id)
               OR (a.id=endNode(r).id and b.id=startNode(r).id)
            | 1.0] | w+v)
    ] as weight1,
    [r in rels_in_path |
        reduce(w=0.0, v in [
            (a:Person)<-[:HAS_CREATOR]-(:Comment)-[:REPLY_OF]->(:Comment)-[:HAS_CREATOR]->(b:Person)
            WHERE (a.id = startNode(r).id and b.id=endNode(r).id)
               OR (a.id=endNode(r).id and b.id=startNode(r).id)
            | 0.5] | w+v)
    ] as weight2
WITH
    personIdsInPath,
    reduce(w=0.0, v in weight1 | w+v) as w1,
    reduce(w=0.0, v in weight2 | w+v) as w2
RETURN
    personIdsInPath,
    (w1+w2) as pathWeight
ORDER BY pathWeight desc
```

---

## Missing Features (6 milestones)

### Dependency Graph

```
M1 (allShortestPaths)
  |
M2 (path functions: nodes/relationships/startNode/endNode)
  |
M3 (reduce — list fold/accumulate)
  |     \
  |      M4 (pattern comprehension with multi-hop MATCH inside)
  |     /
M5 (integration: list comp + reduce + pattern comp in same WITH)
  |
M6 (IC14 test)
```

---

### M1: allShortestPaths

**현재**: `shortestPath` — 단일 최단 경로 반환.
**필요**: `allShortestPaths` — 모든 최단 경로 반환 (LIST of PATH).

**구현 상태**:
- Grammar: `allShortestPaths` 문법 이미 파서에 존재 (ALLSHORTESTPATH 토큰)
- Parser: `PatternPathType::ALL_SHORTEST` 이미 처리
- Binder: `BoundQueryGraph::PathType::ALL_SHORTEST` 존재
- Converter: `CLogicalAllShortestPath` ORCA 연산자 존재
- Physical: `PhysicalAllShortestPathJoin` 실행 연산자 존재
- **실제 테스트 필요** — 동작 여부 미확인

**작업**:
1. `allShortestPaths` 쿼리 테스트
2. 반환 타입 확인: LIST(PATH) 또는 개별 PATH row
3. `collect(path)` 호환성 확인

**난이도**: 낮음 (인프라 이미 존재)

---

### M2: Path Functions — nodes(), relationships(), startNode(), endNode()

**현재**: `length(path)` → `path_length()` 지원. path는 `[node, edge, node, edge, ..., node]` LIST로 저장.

**필요**:
- `nodes(path)` → path에서 node VID만 추출: `[path[0], path[2], path[4], ...]`
- `relationships(path)` → path에서 edge ID만 추출: `[path[1], path[3], ...]`
- `startNode(r)` → edge r의 source node
- `endNode(r)` → edge r의 target node

**구현 방안**:

`nodes(path)`, `relationships(path)`:
- Binder에서 리라이트 또는 DuckDB scalar function
- path LIST에서 짝수 인덱스(nodes) / 홀수 인덱스(rels) 추출
- 반환 타입: `LIST(UBIGINT)`

`startNode(r)`, `endNode(r)`:
- edge ID에서 src/dst node를 resolve해야 함
- 방법 A: edge ID로 edge partition lookup → src_id, dst_id 추출
- 방법 B: path 구조를 활용 — edge 양쪽의 node가 path에 저장되어 있으므로,
  `startNode(rels[i])` = `nodes[i]`, `endNode(rels[i])` = `nodes[i+1]`
  (방법 B가 훨씬 효율적, path context에서만 동작)

**수정 파일**:
- `src/binder/binder.cpp` — nodes/relationships/startNode/endNode 리라이트
- `src/function/scalar/` — path_nodes, path_relationships 함수

**난이도**: 중간

---

### M3: reduce() — List Fold/Accumulate

**현재**: 미지원.

**필요**: `reduce(accumulator = init, variable IN list | expression)`

```cypher
reduce(w=0.0, v in [1.0, 1.0, 1.0] | w+v)  →  3.0
```

이건 함수형 프로그래밍의 `fold` 연산.

**구현 방안**:

Parser:
- Grammar에 `reduce` 규칙이 이미 있을 수 있음 (Cypher 표준). 확인 필요.
- 없으면 `oC_Atom`에 `reduce(...)` 문법 추가

Binder:
- `reduce(acc=init, var IN list | expr)` 바인딩
- `acc`와 `var`를 임시 변수로 등록, `expr`을 바인딩
- 반환 타입은 `init`의 타입 (또는 expr의 타입)

Execution:
- **PhysicalReduce** 또는 scalar function
- 각 list element에 대해 `expr`을 평가하면서 `acc`를 누적
- list comprehension과 유사하지만, 결과가 list가 아닌 단일 값

**난이도**: 중간 (list comprehension 인프라 재활용 가능)

---

### M4: Pattern Comprehension with Multi-hop MATCH

**현재**: list comprehension `[x IN list WHERE cond]` 지원 (converter decorrelation).
         pattern expression `(a)-[:R]-(b)` 지원 (1-hop, 2-hop scalar function).

**필요**:
```cypher
[(a:Person)<-[:HAS_CREATOR]-(:Comment)-[:REPLY_OF]->(:Post)-[:HAS_CREATOR]->(b:Person)
 WHERE a.id = ... AND b.id = ...
 | 1.0]
```

이건 **pattern comprehension** — list comprehension 안에 MATCH 패턴이 source로 사용됨.
현재 list comprehension은 `[x IN existing_list WHERE cond]` 형태만 지원.
Pattern comprehension은 `[pattern WHERE cond | expr]` — pattern 자체가 데이터 소스.

**구현 방안**:

Parser:
- Grammar의 `oC_PatternComprehension` 규칙 활용 (이미 정의됨)
- `[(pattern) WHERE cond | expr]` → 파서에서 변환

Binder:
- 내부 pattern을 별도 BoundQueryGraph로 바인딩
- WHERE 조건을 바인딩
- mapping expression (| 뒤)을 바인딩

Converter — **Decorrelation**:
- pattern comprehension을 OPTIONAL MATCH + collect로 변환
- IC14에서: `[(a)<-[:HAS_CREATOR]-(:Comment)-[:REPLY_OF]->(:Post)-[:HAS_CREATOR]->(b) WHERE ... | 1.0]`
  → `OPTIONAL MATCH (a)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF]->(p:Post)-[:HAS_CREATOR]->(b) WHERE ...`
  → `collect(1.0)` for matching patterns → result is list of 1.0 values
  → `reduce(w=0.0, v IN result | w+v)` → sum = count of matching patterns

이건 M1 (list comprehension decorrelation)의 확장. 차이: source가 기존 리스트가 아니라
**새 MATCH 패턴**.

**난이도**: 높음 (5-hop 패턴을 인라인 서브쿼리로 변환)

---

### M5: 통합 — reduce + pattern comprehension + list comp 조합

IC14의 핵심은 이 모든 것이 **하나의 WITH 안에서 조합**되는 것:

```cypher
WITH
    [n in nodes(path) | n.id] as personIdsInPath,     -- list comp with mapping
    [r in rels_in_path |                                -- list comp over edges
        reduce(w=0.0, v in [                            -- reduce over
            (a)-[:HAS_CREATOR]-(...)                    -- pattern comprehension
            WHERE ...
            | 1.0
        ] | w+v)
    ] as weight1
```

3단 중첩:
1. 외부: list comprehension `[r IN rels | ...]` — edge별 반복
2. 중간: `reduce(w, v IN [...] | w+v)` — 패턴 매칭 결과 합산
3. 내부: pattern comprehension `[(a)-[...]-(b) WHERE ... | 1.0]` — 5-hop 매칭

**구현 방안**:

이 조합을 처리하려면:
- 외부 list comp → UNWIND rels AS r
- 각 r에 대해 pattern comp → OPTIONAL MATCH (5-hop)
- 매칭 수 카운트 → reduce = count 또는 sum
- GROUP BY로 재집계

실질적으로 converter에서 이 중첩 구조를 "풀어서" 관계형 연산으로 변환:

```
UNWIND rels_in_path AS r
OPTIONAL MATCH (a)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF]->(p:Post)-[:HAS_CREATOR]->(b)
WHERE (a.id = startNode(r).id AND b.id = endNode(r).id)
   OR (a.id = endNode(r).id AND b.id = startNode(r).id)
WITH r, count(c) AS weight_for_this_edge
WITH sum(weight_for_this_edge) AS total_weight
```

**난이도**: 높음 (중첩 decorrelation)

---

### M6: IC14 통합 테스트

모든 milestone 완료 후 원본 IC14 쿼리 테스트.

**예상 결과** (Neo4j 검증):
```
| [17592186055119, 4398046515656, 17592186053137, 10995116282665]  | 30.0 |
| [17592186055119, 15393162790796, 17592186053137, 10995116282665] | 28.0 |
```

---

## 권장 구현 순서

```
M1 (allShortestPaths)     — 난이도 낮음, 인프라 존재
  ↓
M2 (path functions)        — 난이도 중간, M1 결과 사용
  ↓
M3 (reduce)                — 난이도 중간, 독립적
  ↓
M4 (pattern comprehension) — 난이도 높음, M3 의존
  ↓
M5 (통합)                  — 난이도 높음, M1-M4 전부 의존
  ↓
M6 (IC14 테스트)
```

**예상 총 작업량**: M1(1일) + M2(1일) + M3(1일) + M4(2일) + M5(2일) + M6(0.5일) ≈ **7-8일**

---

## Known Technical Debt

### Crash-Proof Architecture (구현 완료)
- Planner::execute → gpos_exec 반환값 체크 + exception throw
- turbolynx_execute → try/catch wrapping
- Shell REPL → SIGSEGV/SIGABRT signal handler + siglongjmp recovery
- ORCA memory pool → 매 쿼리마다 재생성

### Debug Build Issues
- UNWIND→MATCH pipeline Vector type assertion (IC6, IC7, IC9, IC10)
- MPV 출력 컬럼 중복 (IC9 등)
- edge property type mismatch (IC11)

### Known Limitations
- `CASE WHEN count(f) > 10 THEN ...` — CASE 내부 aggregation SIGSEGV
- Comma pattern + shortestPath 내부 predicate pushdown 순서 의존
- Unbound variable in WHERE → converter NULL colref (binder에서 throw 필요하지만 기존 코드 호환 문제)
- VarLen `*0..0` — early return 처리 (정확성 미검증)
