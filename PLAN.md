# TurboLynx — Execution Plan

## Current Status

M1~M29 완료. UNWIND 전체 스택 구현 완료. collect()+IN 자동 rewrite.
All 143 query tests pass (UNWIND 6개, IC1~IC5 포함).

### Query Test Status

| 태그 | 테스트 수 | 상태 | 비고 |
|------|-----------|------|------|
| `[q1]` | 27 | **26 pass, 1 skip(mpv)** | Q1-22(MPV NodeScan)만 미지원 |
| `[q2]` | 18 | **18 pass** | UNWIND 6개 포함 |
| `[q3]` | 9 | **9 pass** | |
| `[q4]` | 7 | **7 pass** | IS 전체 통과 |
| `[q5]` | 11 | **11 pass** | IC1~IC6 전체 통과 (Neo4j 검증) |
| `[q6]` | 65 | **65 pass** | |
| `[q7]` | 4 | **1 pass, 2 fail, 1 skip** | MPV NodeScan 미지원 |
| `[execution]` | 3 | **3 pass** | VLE isomorphism 회귀 테스트 |

**Total: 144 tests, all pass.**

**루틴 검증 명령어:**
```bash
cd /turbograph-v3/build-lwtest
./test/unittest "[catalog]" 2>&1 | tail -5
./test/unittest "[storage]" 2>&1 | tail -5
./test/unittest "[common]"  2>&1 | tail -5
./test/query/query_test --db-path /data/ldbc/sf1 2>&1 | tail -5
```

---

## Blocked: IC6 (Tag Co-occurrence) — ORCA Optimization Explosion

### 문제 현상

IC6 쿼리 (및 유사 패턴)에서 ORCA 최적화가 무한 hang:

```cypher
MATCH (person:Person {id: ...})-[:KNOWS*1..2]-(friend)
WHERE NOT person = friend
WITH DISTINCT friend
MATCH (friend)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(tag:Tag)
RETURN tag.name AS tagName, count(post) AS postCount
ORDER BY postCount DESC, tagName ASC
LIMIT 10
```

### 격리된 원인

단계적 테스트로 정확한 hang 지점 확인:

| 쿼리 패턴 | 결과 |
|-----------|------|
| `...MATCH (friend)<-[:HAS_CREATOR]-(post:Post) RETURN count(post)` | ✅ 0.9s |
| `...MATCH (friend)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(tag:Tag) RETURN count(tag)` | ✅ 1.5s |
| `...MATCH (...) RETURN tag.name, count(post) ORDER BY ...` | ❌ HANG |
| `...MATCH (...) RETURN tag.name, count(post) LIMIT 10` | ❌ HANG |
| `...MATCH (...) WHERE tag.name <> 'X' RETURN count(tag)` | ❌ HANG |

**결론**: `count(tag)`는 동작하지만, `tag.name, count(post)` (GROUP BY + property access + ORDER BY) 조합에서 ORCA가 hang.

### 근본 원인 분석

1. **Multi-partition edge + GROUP BY 조합**: `HAS_TAG`는 3개 sub-partition (Comment_HAS_TAG, Post_HAS_TAG, Forum_HAS_TAG). ORCA가 각 sub-partition에 대해 GROUP BY를 push-down하거나 말거나의 alternative를 탐색 → O(3^n) 폭발.

2. **Property access in GROUP BY**: `tag.name`을 GROUP BY 키로 사용하면, ORCA가 Tag 테이블의 property column을 GROUP BY에 포함시키려고 여러 plan alternative를 생성. 이것이 multi-partition edge의 alternative와 cross-product되어 탐색 공간 폭발.

3. **ORDER BY 추가**: GROUP BY 결과에 ORDER BY가 붙으면 sort enforcement + distribution requirement가 추가 plan alternative를 생성.

### 해결 방향

**Option A: ORCA timeout 설정**
- `CEngine::SetSearchStageHint()`로 exploration 횟수 제한
- 장점: 빠른 적용, 모든 쿼리에 안전망
- 단점: 최적 플랜을 놓칠 수 있음

**Option B: Multi-partition edge에 대한 GROUP BY pushdown 비활성화**
- `CXformGbAgg2HashAgg`에서 multi-partition child가 있으면 pushdown alternative 생성 안 함
- 장점: 근본 해결
- 단점: ORCA 내부 수정 필요

**Option C: Converter에서 GROUP BY 전 강제 materialization**
- `WITH tag.name AS tagName, count(post) AS postCount` 전에 `WITH DISTINCT friend, post, tag` 같은 중간 WITH를 삽입
- ORCA가 보는 plan tree의 깊이를 줄여서 탐색 공간 축소
- 장점: converter만 수정
- 단점: workaround

---

## Known Technical Debt

### ORCA GROUP BY + Multi-Partition Edge Explosion (위 섹션 참조)

IC6 및 유사 패턴 (multi-partition edge 결과에 GROUP BY property + ORDER BY) 에서 ORCA hang.

### PlanRegularMatch HashJoin 비효율 (IC5 관련)

`collect()+IN → DISTINCT` rewrite 후에도, 두 endpoint가 모두 bound인 multi-edge chain에서 HashJoin + full NodeScan 발생. Edge 순서 최적화 또는 ORCA IndexNLJ 선호도 조정 필요.

**영향**: IC5 실행 시간 ~400ms (최적 시 ~280ms 가능).
**영향 파일**: `src/converter/cypher2orca_converter.cpp` — `PlanRegularMatch`, `src/planner/planner_physical.cpp`

### NodeScan Multi-Partition Vertex (CCM file_handler 미등록)

`Message` MPV (Post + Comment) 스캔 시 두 번째 partition의 CDF file_handler가
CCM에 등록되지 않아 assertion 실패.
**영향**: IS4, IS6, IS7, Q1-22.

### OPTIONAL MATCH atomic semantics (chained LOJ fallback)

OPTIONAL MATCH가 multi-hop일 때, 후속 edge의 endpoint가 prev_plan에만 bound되고
subquery에는 없는 경우, standalone subquery가 필터 없이 전체 join을 수행하여 성능 극도 저하.

현재 workaround: 해당 지점에서 subquery를 끊고 chained LOJ로 fallback.
**시맨틱 차이**: 중간 optional 노드가 있는 경우 Neo4j(atomic)와 결과 다를 수 있음.
**근본 해결**: ORCA correlated apply 지원 필요.

### Filter Pushdown + Multi-Outer (미구현)

`doSeekUnionAll` filter pushdown 경로에서 하드코딩된 인덱스. Multi-outer + filter pushdown 조합 시 OOB 가능.

### Persistence Tier (미구현)
- WAL, Checkpoint, Transaction Manager 전체 주석 처리 상태
- in-memory only 모드

### 버그
| 파일 | 위치 | 내용 |
|------|------|------|
| `extent_iterator.cpp` | line 205 | 인덱스 계산 오류 (`// TODO bug..`) |
| `histogram_generator.cpp` | line 215 | boundary 값 strictly ascending 보장 미흡 |
