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

LDBC SF1 DB 위에서 Cypher 쿼리를 실행하고 결과를 검증하는 단위 테스트 스위트.
쿼리 엔진의 정확성과 안정성을 커버리지 있게 검증하는 것이 목표.

Expected values below are ground truth from Neo4j 5.24.0 loaded with the same LDBC SF1 dataset.

---

### Edge Type Names (Our System vs Neo4j)

Our system uses fine-grained edge types split by source/target label:

| Our Type | Neo4j | Direction |
|----------|-------|-----------|
| `HAS_CREATOR` | `HAS_CREATOR` | Comment→Person |
| `POST_HAS_CREATOR` | `HAS_CREATOR` | Post→Person |
| `REPLY_OF` | `REPLY_OF` | Comment→Post |
| `REPLY_OF_COMMENT` | `REPLY_OF` | Comment→Comment |
| `LIKES` | `LIKES` | Person→Comment |
| `LIKES_POST` | `LIKES` | Person→Post |
| `HAS_TAG` | `HAS_TAG` | Comment→Tag |
| `POST_HAS_TAG` | `HAS_TAG` | Post→Tag |
| `FORUM_HAS_TAG` | `HAS_TAG` | Forum→Tag |
| `COMMENT_IS_LOCATED_IN` | `IS_LOCATED_IN` | Comment→Place |
| `POST_IS_LOCATED_IN` | `IS_LOCATED_IN` | Post→Place |
| `ORG_IS_LOCATED_IN` | `IS_LOCATED_IN` | Organisation→Place |
| `IS_LOCATED_IN` | `IS_LOCATED_IN` | Person→Place |

---

### Stage 1 — Count / Label Smoke Tests

Node counts:

```
Q1-01  MATCH (p:Person) RETURN count(p)                   → 9892
Q1-02  MATCH (c:Comment) RETURN count(c)                  → 2052169
Q1-03  MATCH (p:Post) RETURN count(p)                     → 1003605
Q1-04  MATCH (f:Forum) RETURN count(f)                    → 90492
Q1-05  MATCH (t:Tag) RETURN count(t)                      → 16080
Q1-06  MATCH (tc:TagClass) RETURN count(tc)               → 71
Q1-07  MATCH (pl:Place) RETURN count(pl)                  → 1460
Q1-08  MATCH (o:Organisation) RETURN count(o)             → 7955
```

Edge counts:

```
Q1-09  MATCH (:Person)-[r:KNOWS]->(:Person) RETURN count(r)                       → 2889968
Q1-10  MATCH (:Comment)-[r:HAS_CREATOR]->(:Person) RETURN count(r)                → 2052169
Q1-11  MATCH (:Post)-[r:POST_HAS_CREATOR]->(:Person) RETURN count(r)              → 1003605
Q1-12  MATCH (:Person)-[r:LIKES]->(:Comment) RETURN count(r)                      → 1438418
Q1-13  MATCH (:Person)-[r:LIKES_POST]->(:Post) RETURN count(r)                    → 751677
Q1-14  MATCH (:Forum)-[r:CONTAINER_OF]->(:Post) RETURN count(r)                   → 1003605
Q1-15  MATCH (:Comment)-[r:REPLY_OF]->(:Post) RETURN count(r)                     → 1011420
Q1-16  MATCH (:Comment)-[r:REPLY_OF_COMMENT]->(:Comment) RETURN count(r)          → 1040749
Q1-17  MATCH (:Comment)-[r:HAS_TAG]->(:Tag) RETURN count(r)                       → 2698393
Q1-18  MATCH (:Post)-[r:POST_HAS_TAG]->(:Tag) RETURN count(r)                     → 713258
Q1-19  MATCH (:Forum)-[r:FORUM_HAS_TAG]->(:Tag) RETURN count(r)                   → 309766
Q1-20  MATCH (:Tag)-[r:HAS_TYPE]->(:TagClass) RETURN count(r)                     → 16080
Q1-21  MATCH (:Place)-[r:IS_PART_OF]->(:Place) RETURN count(r)                    → 1460
```

---

### Stage 2 — Property Filter + 1-hop Traversal

```
Q2-01  MATCH (p:Person) WHERE p.id = 933 RETURN p.firstName, p.lastName
       → "Mahinda", "Perera"

Q2-02  MATCH (p:Person {id: 933})-[:KNOWS]->(friend:Person) RETURN count(friend)
       → 40

Q2-03  MATCH (p:Person {id: 933})-[:IS_LOCATED_IN]->(pl:Place)
       RETURN pl.id, pl.name
       → placeId=1353, placeName="Kelaniya"

Q2-04  MATCH (p:Person {id: 933})
       RETURN p.creationDate, p.gender, p.birthday, p.locationIP, p.browserUsed
       → creationDate=1266161530447, gender="male", birthday=628646400000,
         locationIP="119.235.7.103", browserUsed="Firefox"

Q2-05  MATCH (c:Comment)-[:HAS_CREATOR]->(p:Person) WHERE c.id = 1236950581249
       RETURN p.id, p.firstName
       → personId=10995116284808, firstName="Andrei"

Q2-06  MATCH (f:Forum)-[:CONTAINER_OF]->(p:Post) WHERE f.id = 77644
       RETURN count(p)
       → 1208

Q2-07  MATCH (p:Post)-[:POST_HAS_TAG]->(t:Tag) WHERE t.name = 'Genghis_Khan'
       RETURN count(p)
       → 3715

Q2-08  MATCH (p:Person {id: 933})-[:LIKES]->(c:Comment) RETURN count(c)
       → 12

Q2-09  MATCH (p:Person {id: 933})-[:LIKES_POST]->(po:Post) RETURN count(po)
       → 5
```

---

### Stage 3 — Multi-hop Traversal + Aggregation

```
Q3-01  MATCH (p:Person {id: 933})-[:KNOWS]->(f:Person)-[:KNOWS]->(fof:Person)
       WHERE fof <> p RETURN count(DISTINCT fof)
       → 2087

Q3-02  MATCH (p:Person)<-[:HAS_CREATOR]-(c:Comment)
       RETURN p.id, count(c) AS cnt ORDER BY cnt DESC, p.id ASC LIMIT 10
       → (2199023262543, 8915), (4139, 7896), (2199023259756, 7694),
         (2783, 7530), (4398046513018, 7423), (7725, 6612),
         (6597069777240, 6565), (9116, 6135), (4398046519372, 5894),
         (8796093029267, 5640)

Q3-03  MATCH (f:Forum)-[:CONTAINER_OF]->(p:Post)
       RETURN f.id, count(p) AS cnt ORDER BY cnt DESC, f.id ASC LIMIT 5
       → (77644, 1208), (87312, 1032), (137439023186, 1001),
         (412317916558, 891), (55025, 810)

Q3-04  MATCH (p:Person {id: 933})-[:LIKES]->(c:Comment)-[:HAS_CREATOR]->(creator:Person)
       RETURN count(DISTINCT creator)
       → 12

Q3-05  MATCH (t:Tag)-[:HAS_TYPE]->(tc:TagClass)
       RETURN tc.name, count(t) AS cnt ORDER BY cnt DESC, tc.name ASC LIMIT 5
       → ("Album", 5061), ("Single", 4311), ("Person", 1530),
         ("Country", 1000), ("MusicalArtist", 899)

Q3-06  MATCH (p:Post)-[:POST_HAS_TAG]->(t:Tag)
       RETURN t.name, count(p) AS cnt ORDER BY cnt DESC, t.name ASC LIMIT 5
       → ("Augustine_of_Hippo", 10817), ("Adolf_Hitler", 5227),
         ("Muammar_Gaddafi", 5120), ("Imelda_Marcos", 4412), ("Sammy_Sosa", 4059)
```

---

### Stage 4 — LDBC IS Queries (q1–q7)

These correspond to `queries/ldbc/sf1/q1.cql` through `q7.cql`.

```
Q4-IS1  (q1.cql) IS1 — Person basic info
  MATCH (n:Person {id: 35184372099695})-[:IS_LOCATED_IN]->(p:Place)
  RETURN n.firstName, n.lastName, n.birthday, n.locationIP,
         n.browserUsed, p.id AS cityId, n.gender, n.creationDate
  → firstName="Changpeng", lastName="Wei", birthday=337132800000,
    locationIP="1.1.39.242", browserUsed="Internet Explorer",
    cityId=367, gender="female", creationDate=1347431652132

Q4-IS2  (q2.cql) IS2 — 10 recent messages by Person 933
  MATCH (:Person {id: 933})<-[:HAS_CREATOR]-(message:Comment)
  WITH message ORDER BY message.creationDate DESC, message.id ASC LIMIT 10
  MATCH (message)-[:REPLY_OF*1..8]->(p:Post)-[:POST_HAS_CREATOR]->(person:Person)
  RETURN message.id, message.content, message.creationDate,
         p.id AS postId, person.id AS personId,
         person.firstName, person.lastName
  ORDER BY message.creationDate DESC, message.id ASC
  → (2199027727462, "good", 1347156463979, 2061588773973, 32985348833579, "Otto", "Becker"),
    (2061588773980, "no way!", 1347020779693, 2061588773973, 32985348833579, "Otto", "Becker"),
    (2061584946139, "yes", 1343987237082, 2061584946128, 4139, "Baruch", "Dego"),
    (2061585616327, [content truncated], 1343919397609, 2061585616321, 6597069777240, "Fritz", "Muller"),
    (2061585618894, "thanks", 1343106691147, 2061585618887, 6597069777240, "Fritz", "Muller"),
    (2061585619578, [content truncated], 1342380120292, 2061585619561, 6597069777240, "Fritz", "Muller"),
    (1786707214487, "maybe", 1335712528590, 1786707214481, 10995116284808, "Andrei", "Condariuc"),
    (1786707214957, [content truncated], 1335694024103, 1786707214955, 10995116284808, "Andrei", "Condariuc"),
    (1786707214469, [content truncated], 1335678484226, 1786707214468, 10995116284808, "Andrei", "Condariuc"),
    (1786707216224, "no way!", 1335668918002, 1786707216218, 10995116284808, "Andrei", "Condariuc")

Q4-IS3  (q3.cql) IS3 — Friend list for Person 933
  MATCH (n:Person {id: 933})-[r:KNOWS]-(friend:Person)
  RETURN DISTINCT friend.id, friend.firstName, friend.lastName,
         r.creationDate ORDER BY r.creationDate DESC, friend.id ASC LIMIT 20
  → 5 distinct friends (due to KNOWS CSV containing both directions):
    (32985348833579, "Otto", "Becker", 1346980290195),
    (32985348838375, "Otto", "Richter", 1342512289463),
    (10995116284808, "Andrei", "Condariuc", 1326211584266),
    (6597069777240, "Fritz", "Muller", 1287362460911),
    (4139, "Baruch", "Dego", 1268465841718)

Q4-IS4  (q4.cql) IS4 — Post content
  MATCH (m:Post {id: 2199029886840})
  RETURN m.creationDate, m.imageFile, m.content
  → creationDate=1347463431887, imageFile="photo2199029886840.jpg", content=NULL

Q4-IS5  (q5.cql) IS5 — Comment creator
  MATCH (m:Comment {id: 824635044686})-[:HAS_CREATOR]->(p:Person)
  RETURN p.id, p.firstName, p.lastName
  → personId=933, firstName="Mahinda", lastName="Perera"

Q4-IS6  (q6.cql) IS6 — Forum of message
  MATCH (m:Comment {id: 824635044686})-[:REPLY_OF|REPLY_OF_COMMENT*0..8]->(:Post)
        <-[:CONTAINER_OF]-(f:Forum)-[:HAS_MODERATOR]->(mod:Person)
  RETURN f.id, f.title, mod.id, mod.firstName, mod.lastName
  → forumId=412317916558, title="Wall of Fritz Muller",
    moderatorId=6597069777240, moderatorFirstName="Fritz", moderatorLastName="Muller"

Q4-IS7  (q7.cql) IS7 — Replies to Comment 824635044682
  MATCH (m:Comment {id: 824635044682})<-[:REPLY_OF_COMMENT]-(c:Comment)
        -[:HAS_CREATOR]->(p:Person)
  RETURN c.id, c.content, c.creationDate, p.id, p.firstName, p.lastName
  ORDER BY c.creationDate DESC, p.id ASC
  → (824635044685, "great", 1295218398759, 2738, "Eden", "Atias"),
    (824635044686, "cool", 1295203653676, 933, "Mahinda", "Perera")
```

---

### Stage 5 — LDBC IC Queries (q9–q19 subset)

```
Q5-IC9  (q9.cql) — Friends' messages before date
  MATCH (n:Person {id: 17592186052613})-[:KNOWS]->(friend:Person)
        <-[:HAS_CREATOR]-(message:Comment)
  WHERE message.creationDate <= 1354060800000
  RETURN DISTINCT friend.id, friend.firstName, friend.lastName,
         message.id, message.content, message.creationDate
  ORDER BY message.creationDate DESC, message.id ASC LIMIT 20
  → 20 rows, top result:
    (6597069773262, "Bill", "Moore", 2199023411115, [content], 1347527459367)

Q5-IC11 (q11.cql) — Tag statistics among friends' posts
  MATCH (person:Person {id: 21990232559429})-[:KNOWS]->(friend:Person)
        <-[:POST_HAS_CREATOR]-(post:Post)-[:POST_HAS_TAG]->(tag:Tag)
  WITH DISTINCT tag, post
  WITH tag,
    CASE WHEN post.creationDate >= 1335830400000 AND post.creationDate < 1339027200000 THEN 1 ELSE 0 END AS valid,
    CASE WHEN post.creationDate < 1335830400000 THEN 1 ELSE 0 END AS inValid
  WITH tag.name AS tagName, sum(valid) AS postCount, sum(inValid) AS inValidPostCount
  WHERE postCount > 0 AND inValidPostCount = 0
  RETURN tagName, postCount ORDER BY postCount DESC, tagName ASC LIMIT 10
  → ("Hassan_II_of_Morocco", 2), ("Appeal_to_Reason", 1),
    ("Principality_of_Littoral_Croatia", 1), ("Rivers_of_Babylon", 1),
    ("Van_Morrison", 1)

Q5-IC12 (q12.cql) — Forum posts by 2-hop friends (after join date)
  MATCH (person:Person {id: 28587302325306})-[:KNOWS*1..2]-(friend:Person)
  WHERE NOT person = friend
  WITH DISTINCT friend
  MATCH (friend)<-[membership:HAS_MEMBER]-(forum:Forum)
  WHERE membership.joinDate > 1343088000000
  WITH forum, friend
  MATCH (friend)<-[:POST_HAS_CREATOR]-(post:Post)<-[:CONTAINER_OF]-(forum)
  WITH forum, count(post) AS postCount
  RETURN forum.title, postCount, forum.id
  ORDER BY postCount DESC, forum.id ASC LIMIT 20
  → 20 rows, top result:
    ("Group for She_Blinded_Me_with_Science in Antofagasta", 10, 1236950612644)

Q5-IC13 (q13.cql) — Tag co-occurrence (friends of friends' posts with Angola)
  MATCH (knownTag:Tag {name: 'Angola'})
  WITH knownTag
  MATCH (person:Person {id: 30786325583618})-[:KNOWS*1..2]-(friend:Person)
  WHERE NOT person=friend
  WITH DISTINCT friend, knownTag
  MATCH (friend)<-[:POST_HAS_CREATOR]-(post:Post),
        (post)-[:POST_HAS_TAG]->(knownTag),
        (post)-[:POST_HAS_TAG]->(tag:Tag)
  WHERE NOT knownTag = tag
  WITH tag.name AS tagName, count(post) AS postCount
  RETURN tagName, postCount ORDER BY postCount DESC, tagName ASC LIMIT 10
  → ("Tom_Gehrels", 28), ("Sammy_Sosa", 9), ("Charles_Dickens", 5),
    ("Genghis_Khan", 5), ("Ivan_Ljubičić", 5), ("Marc_Gicquel", 5),
    ("Freddie_Mercury", 4), ("Peter_Hain", 4), ("Robert_Fripp", 4),
    ("Boris_Yeltsin", 3)

Q5-IC15 (q15.cql) — Replies to a person's comment via REPLY_OF_COMMENT
  MATCH (s:Person {id: 24189255818757})<-[:HAS_CREATOR]-(p:Comment)
        <-[:REPLY_OF_COMMENT]-(comment:Comment)-[:HAS_CREATOR]->(person:Person)
  RETURN person.id, person.firstName, person.lastName,
         comment.creationDate, comment.id, comment.content
  ORDER BY comment.creationDate DESC, comment.id ASC LIMIT 20
  → 3 rows:
    (28587302328958, "Jessica", "Castillo", 1341921296480, 2061588598034, [content]),
    (24189255812556, "Naresh", "Sharma", 1341888221696, 2061588598031, "right"),
    (6597069770791, "Roberto", "Acuna y Manrique", 1341854866309, 2061588598026, "yes")

Q5-IC16 (q16.cql) — Friends' recent comments before date
  MATCH (root:Person {id: 13194139542834})-[:KNOWS*1..2]->(friend:Person)
  WHERE NOT friend = root
  WITH DISTINCT friend
  MATCH (friend)<-[:HAS_CREATOR]-(message:Comment)
  WHERE message.creationDate < 1324080000000
  RETURN friend.id, friend.firstName, friend.lastName,
         message.id, message.content, message.creationDate
  ORDER BY message.creationDate DESC, message.id ASC LIMIT 20
  → 20 rows, top result:
    (2199023260919, "Xiaolu", "Wang", 1511829711860, [content], 1324079889425)

Q5-IC18 (q18.cql) — Friends working at companies in a country
  MATCH (person:Person {id: 30786325583618})-[:KNOWS*1..2]->(friend:Person)
  WHERE NOT person = friend
  WITH DISTINCT friend
  MATCH (friend)-[workAt:WORK_AT]->(company:Organisation)
        -[:ORG_IS_LOCATED_IN]->(country:Place {name: 'Laos'})
  WHERE workAt.workFrom < 2010
  RETURN friend.id, friend.firstName, friend.lastName,
         company.name, workAt.workFrom
  ORDER BY workAt.workFrom ASC, friend.id ASC, company.name DESC LIMIT 10
  → (6597069767125, "Eve-Mary Thai", "Pham", "Lao_Airlines", 2002),
    (28587302330691, "Atef", "Hafez", "Lao_Airlines", 2002),
    (5869, "Cy", "Vorachith", "Lao_Airlines", 2004),
    (8796093022909, "Mee", "Vang", "Lao_Air", 2005),
    (10995116285549, "Jetsada", "Charoenpura", "Lao_Airlines", 2005),
    (24189255815555, "A.", "Anwar", "Lao_Airlines", 2006),
    (2199023266276, "Ben", "Li", "Lao_Air", 2007),
    (8796093027636, "Pao", "Sysavanh", "Lao_Airlines", 2007),
    (1259, "Mee", "Vongvichit", "Lao_Air", 2008),
    (2199023258003, "Ali", "Achiou", "Lao_Air", 2009)

Q5-IC19 (q19.cql) — Friends' replies to posts tagged BasketballPlayer
  MATCH (tag:Tag)-[:HAS_TYPE*0..5]->(baseTagClass:TagClass)
  WHERE tag.name = 'BasketballPlayer' OR baseTagClass.name = 'BasketballPlayer'
  WITH DISTINCT tag
  MATCH (person:Person {id: 17592186052613})<-[:KNOWS]-(friend:Person)
        <-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->(post:Post)
        -[:POST_HAS_TAG]->(tag)
  RETURN friend.id, friend.firstName, friend.lastName, count(comment) AS replyCount
  ORDER BY replyCount DESC, friend.id ASC LIMIT 20
  → (8796093029854, "Zaenal", "Budjana", 40),
    (8796093031506, "Gheorghe", "Popescu", 32),
    (13194139535625, "Hamani", "Diori", 16),
    (2199023261325, "Chengdong", "Li", 8),
    (6597069773262, "Bill", "Moore", 8),
    (6597069774392, "Michael", "Yang", 8)
```

---

### Implementation Plan

#### File Structure

```
test/
└── query/
    ├── query_test_main.cpp       ← main + LDBC DB shared fixture, --db-path
    ├── helpers/
    │   └── query_runner.hpp      ← Cypher execution helper, result extraction
    ├── test_q1_count.cpp         ← Stage 1 (Q1-01~Q1-21): exact count match
    ├── test_q2_filter.cpp        ← Stage 2 (Q2-01~Q2-09): property filter + 1-hop
    ├── test_q3_multihop.cpp      ← Stage 3 (Q3-01~Q3-06): multi-hop + aggregation
    ├── test_q4_is.cpp            ← Stage 4 (Q4-IS1~IS7): LDBC IS queries
    └── test_q5_ic.cpp            ← Stage 5 (Q5-IC9~IC19): LDBC IC queries
```

#### DB Shared Fixture

```cpp
// query_test_main.cpp
static std::string g_ldbc_db_path;  // passed via --db-path

// Each test:
auto conn = open_connection(g_ldbc_db_path);  // read-only open, fast
auto result = conn.query("MATCH ...");
```

`--db-path` 없으면 WARN + skip (bulkload 테스트와 동일한 패턴).

#### CMake Registration

```cmake
add_executable(query_test
    test/query/query_test_main.cpp
    test/query/test_q1_count.cpp
    test/query/test_q2_filter.cpp
    test/query/test_q3_multihop.cpp
    test/query/test_q4_is.cpp
    test/query/test_q5_ic.cpp
)

add_test(NAME query_ldbc_sf1_stage1 COMMAND query_test "[q1]" --db-path ...)
add_test(NAME query_ldbc_sf1_stage2 COMMAND query_test "[q2]" --db-path ...)
add_test(NAME query_ldbc_sf1_stage3 COMMAND query_test "[q3]" --db-path ...)
add_test(NAME query_ldbc_sf1_stage4 COMMAND query_test "[q4]" --db-path ...)
add_test(NAME query_ldbc_sf1_stage5 COMMAND query_test "[q5]" --db-path ...)
```

#### Implementation Order

| Step | Content | Status |
|------|---------|--------|
| 1 | `query_runner.hpp` — DB open, query execution, result extraction | ⬜ |
| 2 | Stage 1 (Q1-01~Q1-21) exact count match | ⬜ |
| 3 | Stage 2 (Q2-01~Q2-09) property filter | ⬜ |
| 4 | Stage 3 (Q3-01~Q3-06) multi-hop aggregation | ⬜ |
| 5 | Stage 4 (Q4-IS1~IS7) LDBC IS queries | ⬜ |
| 6 | Stage 5 (Q5-IC9~IC19) LDBC IC queries | ⬜ |
| 7 | CMake registration + ctest integration | ⬜ |

---

---

## Milestone 13 — Stage 1 Failures: Root Cause & Fix Plan

Stage 2–5 (30 tests) pass. Stage 1 (21 tests) has 14 failures — all "Unknown exception":
- Q1-01: `MATCH (p:Person) RETURN count(p)`
- Q1-09~21: all edge full-scan count queries

### Root Cause

**`ExtentIterator::Initialize()` — undefined behavior on empty `extent_ids`**

File: `src/storage/extent/extent_iterator.cpp` (overloads at line 51 and 115)

```cpp
max_idx = property_schema_cat_entry->extent_ids.size();
ext_ids_to_iterate.reserve(property_schema_cat_entry->extent_ids.size());
for (...) ext_ids_to_iterate.push_back(extent_ids[i]);

// BUG: crashes if ext_ids_to_iterate is empty (size() == 0)
if (!ObtainFromCache(ext_ids_to_iterate[current_idx], toggle)) { ... }
```

When `extent_ids` is empty, `ext_ids_to_iterate[0]` is out-of-bounds access → UB (memory corruption).
The crash is caught by `catch (...)` in `execution_task.cpp:22` → "Unknown exception in Finalize!".

**Why does this happen for Person but not Forum/Tag/Place/Organisation?**

The logical planner chooses between two scan paths (in `planner_logical.cpp:1497`):
```cpp
if (pruned_table_oids.size() > 1) {
    return lPlanNodeOrRelExprWithDSI(...);   // DSI path → works
} else {
    return lPlanNodeOrRelExprWithoutDSI(...); // Non-DSI path → crashes if extent_ids empty
}
```

Person's `PropertySchemaCatalogEntry` has empty `extent_ids` when accessed via the non-DSI
full-scan path. Filtered queries (e.g., `MATCH (p:Person {id: 933})`) use an index seek that
bypasses `ExtentIterator::Initialize()` entirely — hence they work.

Why Person's catalog entry has empty `extent_ids` in the full-scan OID path needs further
investigation, but the defensive fix below is independently correct.

**Edge full-scans fail for the same reason** — edge `PropertySchemaCatalogEntry` also has
empty `extent_ids` when scanned without a src-node filter. Filtered traversals
(`MATCH (p:Person {id: 933})-[:KNOWS]->(f:Person)`) use adjacency index instead of extent
full-scan, so they work.

### Fix Plan

#### Fix 1 — Guard in `ExtentIterator::Initialize()` (defensive, quick)

Add an early-return in both 2-arg (line 51) and 4-arg (line 115) overloads of
`ExtentIterator::Initialize()`:

```cpp
// After building ext_ids_to_iterate:
if (ext_ids_to_iterate.empty()) {
    is_initialized = true;
    return;  // nothing to scan; GetNextExtent() returns false immediately
}
// ... existing ObtainFromCache() call
```

Also fix `PhysicalNodeScan::GetData()` — the `D_ASSERT(state.ext_its.size() > 0)` at line 207
will fire if all iterators are empty. Change to a conditional return:

```cpp
// In PhysicalNodeScan::GetData, after InitializeScan:
if (state.ext_its.empty()) return;  // nothing to scan
```

**Effect of Fix 1 alone:** UB/crash is eliminated. However, full-scan counts may return 0
instead of the correct value if the root cause (wrong OID or missing extent registration) is
not also fixed.

#### Fix 2 — Root cause: why extent_ids is empty for Person/edge full-scan

Investigate why `PropertySchemaCatalogEntry` returned by
`cat_instance.GetEntry(client, DEFAULT_SCHEMA, oids[i])` in `InitializeScan()`
has empty `extent_ids` for Person and KNOWS (etc.) but not for Forum/Tag/Place/Organisation.

Hypothesis: The OID passed from `pTransformEopNormalTableScan` may correspond to a vertex
partition catalog entry (`vpart_Person`) rather than a property-schema entry (`vps_Person`).
Casting a partition entry to `PropertySchemaCatalogEntry*` gives an object whose `extent_ids`
field is uninitialized/empty. For Forum/Tag etc. the OIDs may happen to point to the correct
entry layout by luck.

Investigation steps:
1. Add a debug log in `InitializeScan()` printing the OID and
   `ps_cat_entry->extent_ids.size()`.
2. Check what catalog entry type is returned for Person OID vs Forum OID.
3. Trace the OID from `node_expr->getTableIDs()` in the binder to understand what it represents.
4. Compare with the OID used by the ID-seek path (which works for Person).

#### Recommended Implementation Order

1. Apply Fix 1 (guard) — eliminates crash, tests fail with count mismatch instead
2. Add debug logging to identify exact OID mismatch for Person
3. Apply Fix 2 — correct OID → `extent_ids` populated → correct counts → Stage 1 passes

### Files to Modify

| File | Change |
|------|--------|
| `src/storage/extent/extent_iterator.cpp` | Fix 1: guard for empty ext_ids_to_iterate (2 overloads) |
| `src/execution/execution/physical_operator/physical_node_scan.cpp` | Fix 1: handle empty ext_its in GetData |
| `src/storage/graph_storage_wrapper.cpp` | Fix 2: verify correct OID in InitializeScan |
| `src/planner/planner_logical.cpp` | Fix 2: check OID passed to scan for Person/edge types |

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
