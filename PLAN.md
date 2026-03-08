# TurboLynx — Execution Plan

## Current Status

Core build is stable. Catalog, Storage, Execution layers tested.
Build runs inside `turbograph-s62` Docker container.

LDBC SF1 + TPC-H SF1 + DBpedia bulkload E2E tests all passing.
Milestone 12 (Bulkload Performance Optimization) complete.

**Active milestone: 13 — LDBC Query Test Suite**

---

## Completed Milestones

| # | Milestone | Status |
|---|-----------|--------|
| 1 | Remove Velox dependency | ✅ |
| 2 | Remove Boost dependency | ✅ |
| 3 | Remove Python3 dependency | ✅ |
| 4 | Bundle TBB / libnuma / hwloc | ✅ |
| 5 | Single-file block store (`store.db`) | ✅ |
| 6 | Test suite: catalog, storage, execution | ✅ |
| 7 | Remove libaio-dev system dependency (direct syscalls) | ✅ |
| 8 | Rename library: `libs62gdb.so` → `libturbolynx.so` | ✅ |
| 9 | E2E bulkload test suite (LDBC SF1, TPC-H SF1, DBpedia) | ✅ |
| 10 | Extract `BulkloadPipeline` from `tools/bulkload.cpp` | ✅ |
| 11 | Multi-client support (prototype level) | ✅ |
| 12 | Bulkload performance optimization (12a–12h) | ✅ |

---

## Milestone 13 — LDBC Query Test Suite

### Goal

LDBC SF1을 로드한 DB 위에서 Cypher 쿼리를 실행하고 결과를 검증하는 단위 테스트 스위트.
쿼리 엔진의 정확성과 안정성을 커버리지 있게 검증하는 것이 목표.

---

### LDBC SF1 데이터 요약

| 종류 | Label | 수량(근사) |
|------|-------|----------|
| Vertex | Person | 9,892 |
| Vertex | Comment | 2,052,169 |
| Vertex | Post | 1,003,605 |
| Vertex | Forum | 90,492 |
| Vertex | Place | 1,460 |
| Vertex | Tag | 16,080 |
| Vertex | TagClass | 71 |
| Vertex | Organisation | 7,955 |
| Edge | KNOWS (Person→Person) | 2,889,968 |
| Edge | HAS_CREATOR (Comment→Person) | 2,052,169 |
| Edge | HAS_CREATOR (Post→Person) | 1,003,605 |
| Edge | LIKES (Person→Comment) | 1,438,418 |
| Edge | REPLY_OF (Comment→Post/Comment) | ~2,052,169 |
| Edge | CONTAINER_OF (Forum→Post) | 1,003,605 |

---

### 테스트 구성

테스트는 쿼리 복잡도 기준으로 4단계로 나뉜다.
각 단계는 이전 단계가 모두 통과해야 진행한다.

---

#### Stage 1 — 단순 카운트 및 레이블 검증

DB가 올바르게 로드됐는지 검증하는 smoke test.
결과가 정확한 정수이므로 허용 오차 없이 exact match.

```
Q1-1  전체 Person 수
  MATCH (p:Person) RETURN count(p)
  expected: 9892

Q1-2  전체 Comment 수
  MATCH (c:Comment) RETURN count(c)
  expected: 2052169

Q1-3  전체 Post 수
  MATCH (p:Post) RETURN count(p)
  expected: 1003605

Q1-4  전체 Forum 수
  MATCH (f:Forum) RETURN count(f)
  expected: 90492

Q1-5  전체 Tag 수
  MATCH (t:Tag) RETURN count(t)
  expected: 16080

Q1-6  전체 KNOWS 엣지 수
  MATCH (:Person)-[r:KNOWS]->(:Person) RETURN count(r)
  expected: 2889968

Q1-7  전체 HAS_CREATOR 엣지 수 (Comment 기준)
  MATCH (:Comment)-[r:HAS_CREATOR]->(:Person) RETURN count(r)
  expected: 2052169
```

---

#### Stage 2 — 프로퍼티 필터 및 단일 홉 탐색

특정 정점을 조건으로 필터링하고, 1-hop 이웃을 탐색.

```
Q2-1  특정 Person의 프로퍼티 조회
  MATCH (p:Person) WHERE p.id = 933 RETURN p.firstName, p.lastName
  expected: 결정적 값 (SF1 고정 데이터 기준 generate 모드로 기록)

Q2-2  Person의 직접 친구 수 (아웃고잉 KNOWS)
  MATCH (p:Person {id: 933})-[:KNOWS]->(friend:Person)
  RETURN count(friend)
  expected: generate 모드로 기록

Q2-3  Comment를 작성한 Person 조회 (역방향 탐색)
  MATCH (c:Comment)-[:HAS_CREATOR]->(p:Person)
  WHERE c.id = 1236950581249
  RETURN p.id, p.firstName
  expected: generate 모드로 기록

Q2-4  특정 Forum의 Post 수
  MATCH (f:Forum)-[:CONTAINER_OF]->(p:Post)
  WHERE f.id = 549755813894
  RETURN count(p)
  expected: generate 모드로 기록

Q2-5  특정 Tag를 가진 Post 수
  MATCH (p:Post)-[:HAS_TAG]->(t:Tag)
  WHERE t.name = 'Genghis_Khan'
  RETURN count(p)
  expected: generate 모드로 기록
```

---

#### Stage 3 — 다중 홉 탐색 및 집계

2-hop 이상 탐색, GROUP BY 집계, ORDER BY/LIMIT 포함.

```
Q3-1  친구의 친구 수 (2-hop KNOWS)
  MATCH (p:Person {id: 933})-[:KNOWS]->(f:Person)-[:KNOWS]->(fof:Person)
  WHERE fof <> p
  RETURN count(DISTINCT fof)
  expected: generate 모드로 기록

Q3-2  Person별 작성 Comment 수 Top 10
  MATCH (p:Person)<-[:HAS_CREATOR]-(c:Comment)
  RETURN p.id, count(c) AS cnt
  ORDER BY cnt DESC LIMIT 10
  expected: generate 모드로 기록 (id, count 쌍 목록)

Q3-3  Forum별 Post 수, 내림차순 Top 5
  MATCH (f:Forum)-[:CONTAINER_OF]->(p:Post)
  RETURN f.id, count(p) AS cnt
  ORDER BY cnt DESC LIMIT 5
  expected: generate 모드로 기록

Q3-4  Person이 Like한 Comment의 Creator Person 집합 (2-hop)
  MATCH (p:Person {id: 933})-[:LIKES]->(c:Comment)-[:HAS_CREATOR]->(creator:Person)
  RETURN count(DISTINCT creator)
  expected: generate 모드로 기록

Q3-5  TagClass별 Tag 수
  MATCH (t:Tag)-[:HAS_TYPE]->(tc:TagClass)
  RETURN tc.name, count(t) AS cnt
  ORDER BY cnt DESC LIMIT 5
  expected: generate 모드로 기록
```

---

#### Stage 4 — LDBC Interactive Queries (표준 워크로드 서브셋)

LDBC SNB Interactive Short Query 중 실행 가능한 것들.
완전한 결과 검증보다는 crash 없이 결과 반환을 우선 확인.

```
Q4-1  IS1 — Person 기본 정보 조회
  MATCH (p:Person {id: $personId})
  RETURN p.firstName, p.lastName, p.birthday, p.locationIP, p.browserUsed, p.gender
  (personId = 933)

Q4-2  IS2 — 최근 메시지 10개 (Person이 작성한)
  MATCH (p:Person {id: $personId})<-[:HAS_CREATOR]-(m)
  RETURN m.id, m.content, m.creationDate
  ORDER BY m.creationDate DESC LIMIT 10

Q4-3  IS3 — Person의 친구 목록
  MATCH (p:Person {id: $personId})-[:KNOWS]-(friend:Person)
  RETURN friend.id, friend.firstName, friend.lastName
  ORDER BY friend.lastName, friend.firstName LIMIT 20

Q4-4  IS4 — Message 조회 (Comment or Post)
  MATCH (m {id: $messageId})
  RETURN m.content, m.creationDate

Q4-5  IS5 — Message의 Creator 조회
  MATCH (m {id: $messageId})-[:HAS_CREATOR]->(p:Person)
  RETURN p.id, p.firstName, p.lastName

Q4-6  IS7 — Message의 좋아요 누른 Person 목록
  MATCH (m {id: $messageId})<-[:LIKES]-(p:Person)
  RETURN p.id, p.firstName, p.lastName
  ORDER BY p.lastName
```

---

### 구현 방식

#### 파일 구조

```
test/
└── query/
    ├── query_test_main.cpp       ← main + LDBC DB 공유 픽스처
    ├── helpers/
    │   ├── query_runner.hpp      ← Cypher 실행 헬퍼, 결과 추출
    │   └── expected_results.hpp  ← generate 모드 지원, expected 값 관리
    ├── test_q1_count.cpp         ← Stage 1
    ├── test_q2_filter.cpp        ← Stage 2
    ├── test_q3_multihop.cpp      ← Stage 3
    └── test_q4_interactive.cpp   ← Stage 4
```

#### DB 공유 픽스처

LDBC SF1 DB를 매 TEST_CASE마다 새로 로드하면 시간이 너무 오래 걸린다.
Catch2 세션 단위로 DB를 한 번만 열고 공유하는 방식 사용.

```cpp
// query_test_main.cpp
static std::string g_ldbc_db_path;  // --db-path 로 전달

// 각 테스트에서:
auto conn = open_connection(g_ldbc_db_path);  // 읽기 전용 open, 빠름
auto result = conn.query("MATCH ...");
```

`--db-path`가 없으면 WARN + skip (bulkload 테스트와 동일한 패턴).

#### Generate 모드

```bash
./test/query_test --db-path /path/to/ldbc_sf1_db --generate
```

`--generate` 플래그가 있으면 expected 값을 `test/query/expected_ldbc_sf1.json`에 기록.
이후 일반 실행 시 이 파일을 읽어 검증.

#### CMake 등록

```cmake
add_executable(query_test
    test/query/query_test_main.cpp
    test/query/test_q1_count.cpp
    test/query/test_q2_filter.cpp
    test/query/test_q3_multihop.cpp
    test/query/test_q4_interactive.cpp
)

# ctest 등록
add_test(NAME query_ldbc_sf1_stage1 COMMAND query_test "[q1]" --db-path ...)
add_test(NAME query_ldbc_sf1_stage2 COMMAND query_test "[q2]" --db-path ...)
add_test(NAME query_ldbc_sf1_stage3 COMMAND query_test "[q3]" --db-path ...)
add_test(NAME query_ldbc_sf1_stage4 COMMAND query_test "[q4]" --db-path ...)
```

---

### 구현 순서

| 단계 | 내용 | 상태 |
|------|------|------|
| 1 | `query_runner.hpp` 헬퍼 작성 — DB 오픈, 쿼리 실행, 결과 추출 | ⬜ |
| 2 | Stage 1 (Q1-1~Q1-7) 구현 + generate 모드 | ⬜ |
| 3 | Stage 2 (Q2-1~Q2-5) 구현 | ⬜ |
| 4 | Stage 3 (Q3-1~Q3-5) 구현 | ⬜ |
| 5 | Stage 4 (Q4-1~Q4-6) 구현 | ⬜ |
| 6 | CMake 등록 + ctest 연동 | ⬜ |

---

## Notes

- Keep this file updated at milestone completion.
- One milestone at a time. Validate with `ctest` before closing a milestone.
- Build always in `turbograph-s62` container: `cd /turbograph-v3/build-lwtest && ninja`
- **E2E bulkload 테스트 실행:**
  `ctest --output-on-failure -R "bulkload_ldbc_sf1|bulkload_tpch_sf1"` (staged 제외)
- **DBpedia 테스트** (수 시간 소요, CI 제외):
  `ctest --output-on-failure -R "bulkload_dbpedia"`
  staged subset: `[dbpedia][vertex]`, `[dbpedia][edge-small]`, `[dbpedia][edge-medium]`, `[dbpedia][edge-large]`
