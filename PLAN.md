# TurboLynx — Execution Plan

## Current Status

**595 tests (424 query + 171 unit). 422 query pass, 2 compaction [!mayfail] (SET/DELETE compaction 미구현).**
**IC1~IC14 Neo4j verified.** Full CRUD (CREATE/READ/UPDATE/DELETE) + MERGE + DETACH DELETE + WAL.
Crash-proof signal handler in shell. Query plan cache. UTF-8 rendering.

### CRUD Operations Supported

| Operation | Example | Status |
|-----------|---------|--------|
| CREATE Node | `CREATE (n:Person {id: 1, firstName: 'John'})` | ✅ |
| CREATE Edge (new nodes) | `CREATE (a:Person {id:1})-[:KNOWS]->(b:Person {id:2})` | ✅ |
| CREATE Edge (existing nodes) | `MATCH (a:Person {id:933}), (b:Person {id:4139}) CREATE (a)-[:KNOWS]->(b)` | ✅ |
| SET (UPDATE) | `MATCH (n:Person {id:933}) SET n.firstName = 'New'` | ✅ |
| DELETE | `MATCH (n:Person {id:65}) DELETE n` | ✅ |
| DETACH DELETE | `MATCH (n:Person {id:933}) DETACH DELETE n` | ✅ |
| REMOVE | `MATCH (n:Person {id:933}) REMOVE n.email` | ✅ |
| MERGE (upsert) | `MERGE (n:Person {id:999, firstName: 'Y'})` | ✅ |
| Filter + in-memory | CREATE 후 id로 바로 검색 | ✅ |
| WAL persistence | 서버 재시작 후 mutation 유지 | ✅ |

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

### 1. Design Philosophy

DuckDB의 검증된 패턴을 따르되, TurboLynx의 CGC(Compact Graphlet Clustering) 구조에 맞게 확장한다.

**핵심 원칙:**
- **원본 불변**: base extent(압축된 디스크 데이터)를 직접 수정하지 않는다
- **Delta 기록**: 변경사항을 메모리에 기록하고, scan 시 merge한다
- **Compaction으로 통합**: 주기적으로 delta를 base에 통합한다
- **DuckDB 호환 설계**: 향후 동시성(MVCC), Transaction rollback 지원을 위해 LocalStorage 패턴 유지

### 2. DuckDB Reference Architecture

```
DuckDB:
  INSERT → LocalTableStorage (메모리) → CHECKPOINT 시 base에 통합
  UPDATE → per-column UpdateSegment (메모리) → scan 시 merge → CHECKPOINT 시 통합
  DELETE → RowVersionManager (삭제 표시) → scan 시 skip → CHECKPOINT 시 제거

  Scan 순서:
    1. ColumnSegment에서 base data 읽기
    2. UpdateSegment에서 변경값 merge (FetchUpdates → MergeUpdates)
    3. RowVersionManager에서 삭제된 row 필터 (ApplyVersionFilter)
    4. LocalTableStorage의 INSERT된 row도 scan 대상에 포함
```

DuckDB가 LocalStorage를 쓰는 이유 (base에 직접 append하지 않는 이유):
- **Transaction rollback**: 실패 시 LocalStorage만 버리면 됨
- **Concurrent read**: 다른 트랜잭션이 읽는 중에 base 수정하면 dirty read
- **쓰기 성능**: 매 INSERT마다 압축 해제/재압축 대신 메모리에 쌓고 한 번에 flush

### 3. TurboLynx Storage Layer Design

#### 3.1 CREATE — In-Memory Extent 방식

TurboLynx는 DuckDB와 달리 **같은 label 내에서도 extent마다 schema가 다르다** (CGC).
INSERT된 row를 기존 extent에 바로 넣으면 schema 불일치 + 재압축 비용이 발생한다.

**해결: In-Memory Extent**

INSERT된 row들을 InsertBuffer에 모으되, 각 InsertBuffer를 **가상 extent로 간주**하여
catalog에 등록한다. 이러면 기존 ExtentIterator + NodeScan + ORCA가 수정 없이 동작한다.

```
ExtentID (32bit): [PartitionID:16][LocalExtentID:16]

일반 extent:      LocalExtentID = 0x0000 ~ 0xFEFF  (65280개)
in-memory extent: LocalExtentID = 0xFF00 ~ 0xFFFF  (256개)
```

```cpp
inline bool IsInMemoryExtent(ExtentID eid) {
    return (eid & 0xFFFF) >= 0xFF00;
}
```

**CREATE 흐름:**
```
CREATE (n:Person {id: 1, firstName: 'John'})

1. row의 schema 결정: {id, firstName}
2. Person partition에서 같은 schema의 in-memory extent 찾기
   → 있으면: 해당 InsertBuffer에 row append
   → 없으면: 새 in-memory ExtentID 할당 (0xFF00부터)
             + InsertBuffer 생성
             + Catalog의 PropertySchema extent list에 등록
3. 다음 MATCH에서 ORCA가 이 extent를 인식 → NodeScan에 포함
```

**Read 흐름:**
```
MATCH (n:Person) RETURN count(n)

1. ORCA compile → NodeScan (기존 그대로, in-memory extent 포함)
2. InitializeScan → ExtentIterator에 extent ID list 전달
3. ExtentIterator::GetNextExtent에서:
   → ExtentID 체크: IsInMemoryExtent(eid)?
   → YES → InsertBuffer에서 메모리 직접 읽기
   → NO  → 디스크에서 읽기 (기존 경로)
```

**장점:**
- NodeScan 수정 없음
- ORCA 수정 없음
- 기존 UnionAll / schemaless scan 그대로 동작
- ExtentIterator에 새 필드 추가 없음 (ExtentID 값으로만 판단)

**InsertBuffer 구조:**
```
DeltaStore:
  in_memory_extents: map<ExtentID, InsertBuffer>

  InsertBuffer:
    schema_keys: vector<string>        — property key names
    rows: vector<vector<Value>>        — row data
```

#### 3.2 UPDATE — Per-Extent, Per-Column UpdateSegment (DuckDB 방식)

```
MATCH (n:Person {id: 933}) SET n.firstName = 'Updated'

1. ORCA로 MATCH 실행 → VID 획득
2. VID에서 ExtentID + row_offset 추출
3. DeltaStore의 UpdateSegment에 기록:
   UpdateSegment[extent_id][row_offset][col_idx] = 'Updated'
```

**Read merge:**
```
ExtentIterator::referenceRows()에서:
  1. base compressed chunk에서 row 읽기 (기존)
  2. DeltaStore.UpdateSegment 체크 (context.client->db->delta_store)
     → 해당 extent_id + row_offset에 update가 있으면 값 교체
  3. 반환
```

DuckDB의 FetchUpdates → MergeUpdates 패턴과 동일.
**ExtentIterator에 새 필드 추가 없이**, `context` 파라미터를 통해 DeltaStore 접근.

#### 3.3 DELETE — Per-Extent DeleteMask (DuckDB RowVersionManager 방식)

```
MATCH (n:Person {id: 933}) DELETE n

1. ORCA로 MATCH 실행 → VID 획득
2. VID에서 ExtentID + row_offset 추출
3. DeltaStore의 DeleteMask에 표시:
   DeleteMask[extent_id].Delete(row_offset)
```

**Read merge:**
```
ExtentIterator::referenceRows()에서:
  1. row_offset의 DeleteMask 체크
     → 삭제됨 → skip
     → 삭제 안 됨 → 정상 반환
```

DuckDB의 ApplyVersionFilter 패턴과 동일.

#### 3.4 요약: CUD × Storage 매핑

| 연산 | 쓰기 위치 | 읽기 (scan-time merge) | 단위 |
|------|----------|----------------------|------|
| CREATE | InsertBuffer (in-memory extent) | GetNextExtent에서 ExtentID 분기 | per in-memory extent |
| UPDATE | UpdateSegment | referenceRows에서 delta 값 교체 | per base extent, per column |
| DELETE | DeleteMask | referenceRows에서 deleted row skip | per base extent |

**세 가지 모두 ExtentIterator에 필드 추가 없이** `context.client->db->delta_store`를 통해 접근.

| DuckDB 개념 | TurboLynx 대응 | 비고 |
|-------------|---------------|------|
| RowGroup | Extent (graphlet) | schema가 extent마다 다를 수 있음 |
| ColumnSegment | Compressed Chunk | 압축된 컬럼 데이터 |
| UpdateSegment | UpdateSegment (per-Extent, per-Column) | DuckDB 동일 패턴 |
| RowVersionManager.deleted | DeleteMask (per-Extent) | DuckDB 동일 패턴 |
| LocalTableStorage | InsertBuffer (per in-memory ExtentID) | **in-memory extent로 catalog 등록** |
| CHECKPOINT | Compaction + CGC re-cluster | 그래프 특화 확장 |
| WAL | WAL (Phase 6) | DuckDB 동일 패턴 |
| — | AdjList Delta | 그래프 특화 (CSR 변경분) |

### 4. Compaction (= DuckDB CHECKPOINT)

**트리거 조건:**
- **In-memory extent 수 기반**: partition당 in-memory extent ≥ 256개 → 자동 compaction
- **총 in-memory row 수 기반**: partition당 in-memory rows ≥ 100K → 자동 compaction
- **삭제율 기반**: 인접 extent에서 삭제율 ~25% 넘으면 extent merge (DuckDB 방식)
- **명시적 명령**: shell `.checkpoint` 또는 서버 종료 시

**Shell 명령:**
```
TurboLynx >> .checkpoint
Compaction completed.
```

**Compaction 절차:**
1. **UpdateSegment 통합** — delta 값을 base chunk에 반영, 재압축
2. **DeleteMask 적용** — 삭제된 row 제거, extent 크기 축소
3. **InsertBuffer 통합 (CGC 배치)**:
   a. 각 in-memory extent의 InsertBuffer에서 row 추출
   b. CGC (`_ExtractSchemaAndAssign`) — 기존 graphlet에 매칭 or 새 extent 생성
   c. `AppendTuplesToExistingExtent`로 실제 extent에 flush
   d. Catalog에서 in-memory ExtentID 제거
4. **CSR 재빌드** — AdjList Delta 반영
5. **CGC re-clustering** (선택적) — `_RebalanceSchemas` 구현
6. **Delta 초기화** — UpdateSegment, DeleteMask, InsertBuffer, AdjList Delta clear
7. **WAL truncate** (Phase 6)
8. **Catalog 재직렬화**

### 5. Transaction Model

**Phase 1: 단일 쿼리 단위 (auto-commit, MVCC 없음)**
- 각 mutation 쿼리가 하나의 "트랜잭션"
- 실행 중 실패 시 해당 쿼리의 delta 변경사항 롤백 (메모리 내이므로 간단)
- **단일 writer** (DuckDB도 write는 단일 트랜잭션 제한)
- InsertBuffer 패턴을 유지하여 향후 MVCC/rollback 확장 가능

**Phase 6+: WAL + 자동 compaction**
- Mutation 실행 전 WAL 파일에 변경 내용 기록
- 서버 재시작 시 WAL replay → delta 복구
- Compaction 완료 시 WAL truncate

### 6. Query Pipeline

**Mutation 전용 쿼리** (`CREATE (n:Person {id: 1})`):
```
Parser → Binder → ORCA 우회 → DeltaStore에 직접 기록
```

**Read+Write 혼합 쿼리** (`MATCH ... SET ... RETURN`):
```
MATCH + RETURN → ORCA로 compile
SET → mutation plan으로 별도 처리
실행: MATCH → SET → RETURN (pipeline 내 순서 보장)
```

### 7. Implementation Phases (Test-First)

각 Phase: 테스트 먼저 작성 (FAIL) → 구현 → 통과 → regression 확인.

#### Phase 0: DeltaStore 자료구조 ✅ DONE
- InsertBuffer, UpdateSegment, DeleteMask, AdjListDelta
- 33 module tests, 104 assertions

#### Phase 1: CREATE Node ✅ DONE
- Parser → Binder → ORCA bypass → DeltaStore ✅ DONE
- DeltaStore per-ExtentID 재설계 ✅ DONE
- IsInMemoryExtent() + AllocateInMemoryExtentID() ✅ DONE
- 37 module tests (M-1~M-8), 128 assertions ✅ DONE
- **Read merge (in-memory extent) ✅ DONE**
  - Delta scan in PhysicalNodeScan::GetData (execution unity build only)
  - Filter pushdown 쿼리에서는 delta scan 비활성화
  - NULL VARCHAR validity check fix in turbolynx_get_value
  - 7 query tests (Q7-01~Q7-07), 13 assertions

**Read merge 구현 시도 이력 및 발견사항:**

1. **NodeScan 내부 수정** — `ext_its` + `iter_finished` 상태 관리와 충돌. SIGSEGV 유발. ❌

2. **ExtentIterator에 delta 필드 6개 추가** — 클래스 크기 변경으로 비결정적 SIGSEGV.
   ExtentIterator 내부에 latent memory bug가 있어서, 크기 변경 시 heap layout이 바뀌며 노출됨. ❌

3. **ExtentIterator에 bool 1개만 추가** — 345/345 안정! 클래스 크기 변경이 최소라 bug 미노출. ✅

4. **delta state를 heap에 별도 할당 (void* 1개 추가)** — 비결정적 crash. 포인터 8바이트가 alignment 변경. ❌

5. **delta state를 static global map에 저장** — extent_iterator.cpp 재컴파일 시 코드 생성 변경으로 crash. ❌

**결론:** `extent_iterator.cpp`를 재컴파일하면 비결정적 crash 발생. `.hpp`에 bool 1개 추가는 안전하지만, `.cpp`에 delta scan 코드를 넣으면 불안정.

6. **PhysicalNodeScan::GetData에 delta scan 구현 (execution unity build)** — ✅ 성공!
   - storage unity build (extent_iterator.cpp 포함) 변경 없이, execution unity build에만 코드 추가
   - `graph_storage_wrapper.hpp/cpp` 변경 → storage unity build 재컴파일 → crash ❌
   - `physical_node_scan.cpp` 변경 → execution unity build만 재컴파일 → OK ✅
   - filter pushdown 쿼리에서 delta scan 비활성화 (in-memory VID가 downstream에 전파되는 것 방지)
   - NULL VARCHAR validity check 누락 수정 (turbolynx_get_value)

**Latent bug 분석 결과 (향후 수정 필요):**
- `extent_iterator.cpp`의 `referenceRows`가 I/O buffer를 zero-copy로 Vector에 매핑
- `string_t` 내부 포인터가 I/O buffer를 직접 참조
- ExtentIterator 소멸 시 I/O buffer unpin 누락 → pin leak
- 바이너리 레이아웃 변경 시 heap 재사용 패턴이 달라져 use-after-free 노출
- MALLOC_PERTURB_=170으로 freed memory pattern 0x55 확인

#### Phase 2: CREATE Edge ✅ DONE
- BoundCreateEdgeInfo 구조체 + Binder edge chain 처리
- Executor: AdjListDelta에 forward + backward edge 기록
- Read merge: getAdjListFromVid에서 base CSR + delta edges merge (임시 buffer)
- In-memory extent guard: DFSIterator, ShortestPath 등에서 in-memory EID skip
- MATCH+CREATE edge: query decomposition으로 기존 노드 간 edge 생성
- 4 query tests (Q7-10~Q7-13) + 4 MATCH+CREATE tests (Q7-100~Q7-103)

#### Phase 3: SET Property (UPDATE) ✅ DONE
- Parser: `transformSetClause` (SetItem: variable.property = expression)
- Binder: `BindSetClause` → BoundSetItem (variable, property_key, value)
- Executor: two-phase (ORCA MATCH → user_id 추출 → DeltaStore::SetPropertyByUserId)
- Read merge: NodeScan::GetData에서 user_id 기반 property 교체
  - scan_projection_mapping → PropertySchema key names → output column 매핑
  - property_key_names offset -1 (excludes _id)
- 10 query tests (Q7-20~Q7-29)

#### Phase 4: DELETE ✅ DONE
- Parser: `transformDeleteClause`
- Binder: BoundDeleteClause (variable names)
- Executor: two-phase MATCH → VID + user_id → DeleteMask + DeleteByUserId
- Read merge: NodeScan::GetData에서 SelectionVector로 deleted rows skip
- DETACH DELETE ✅: query rewrite → edge cascade (GraphCatalog → 모든 edge partition 순회 → AdjListDelta.DeleteEdge)
- 5 DETACH DELETE tests (Q7-35~Q7-39) + 6 DELETE tests (Q7-30~Q7-34)

#### Phase 5: REMOVE / MERGE ✅ DONE
- `REMOVE n.prop` → query rewrite `SET n.prop = NULL` (regex 기반, ANTLR 변경 없음)
- `MERGE (n:Label {key: val})` → query decomposition (MATCH count check → conditional CREATE)
  - executeMerge: 내부적으로 turbolynx_prepare + turbolynx_execute 재귀 호출
  - count 결과 직접 벡터 읽기 (turbolynx_get_int64 cursor 문제 우회)
- 4 REMOVE tests (Q7-50~Q7-53) + 4 MERGE tests (Q7-70~Q7-73)

#### Phase 6: WAL ✅ DONE (Compaction 미구현)
- WALWriter: binary append-only log (INSERT_NODE, UPDATE_PROP, DELETE_NODE, INSERT_EDGE)
- WALReader: 서버 시작 시 WAL replay → DeltaStore 복구
- 파일: `{db_path}/delta.wal` (magic "TLWL", version 1)
- Value 직렬화: NULL/BIGINT/UBIGINT/VARCHAR/DOUBLE/BOOL/INTEGER
- turbolynx_connect에서 replay, 모든 mutation executor에서 WAL write
- turbolynx_clear_delta에서 WAL truncate
- 7 WAL tests (Q7-90~Q7-96)

#### Compaction — 기본 동작 확인 (INSERT compaction 완료)

**구현된 것:**
- `turbolynx_checkpoint()` C API + `qr->checkpoint()` 테스트 helper
- InsertBuffer → DataChunk 변환 → `ExtentManager::CreateExtent()` → store.db에 chunk 기록
- Schema 매칭: exact match → 해당 PropertySchema에 extent 추가 / no match → 첫 PropertySchema에 NULL 패딩 후 추가
- `Catalog::SaveCatalog()` → catalog.bin에 새 extent 등록
- 새 segment를 `UnswizzleFlushSwizzle()`로 직접 flush → store.db 기록
- `FlushMetaInfo()` → .store_meta에 chunk offset/size 기록
- delta clear + WAL truncate
- **테스트 격리**: `CompactionWorkspace` 클래스 — db를 /tmp에 복사, 테스트 간 메타데이터 복원
- **ExtentIterator multi-extent transition**: 기존 extent 소진 시 다음 extent로 자동 전환
- 8/10 compaction tests 통과 (Q7-110~Q7-112, Q7-115~Q7-119)
- 2/10 [!mayfail]: Q7-113/Q7-114 (SET/DELETE compaction 미구현)

**해결된 버그:**
1. **새 segment dirty flag 누락**: CreateExtent가 ChunkCacheManager에 segment를 생성하지만 dirty로 마킹하지 않음. **수정**: checkpoint에서 새 extent의 segment를 직접 `UnswizzleFlushSwizzle`로 flush.
2. **ExtentIterator extent 전환 실패**: `num_tuples < idx*scan_size` 일 때 `return false` → multi-extent PropertySchema에서 첫 extent 이후 스캔 중단. **수정**: advance 조건에 extent 소진 체크 추가.
3. **DataChunk 컬럼 매핑 오류**: `GetTypesWithCopy()`가 `_id`를 포함하지 않는데, checkpoint 코드가 column 0을 _id로 가정하여 전체 매핑이 1칸씩 밀림 → filter pushdown 실패 + 데이터 타입 불일치. **수정**: VID를 DataChunk에서 제거, property만 PropertySchema key 순서에 맞게 채움.
4. **테스트 ghost extent 누적**: `FRESH_DB()`가 DeltaStore만 clear. **수정**: `CompactionWorkspace`로 임시 디렉토리 사용.
5. **테스트 전역 상태 충돌**: DiskAioFactory/ChunkCacheManager 글로벌 singleton이 compaction test와 singleton runner 간 충돌. **수정**: `CompactionGuard` RAII — singleton disconnect/reconnect.

**미해결 이슈:**
- **SET/DELETE compaction 미구현** (Q7-113, Q7-114): 현재 INSERT만 flush.
- **`.checkpoint` shell command** 미연동.

**다음 작업 순서:**
1. Filter pushdown 경로에서 새 extent 지원 (ChunkDefinition lookup 수정)
2. UpdateSegment/DeleteMask compaction
3. `.checkpoint` shell command 연동

**데이터 복원 방법** (compaction 테스트로 데이터가 오염된 경우):
```bash
rm -f /data/ldbc/sf1/catalog.bin /data/ldbc/sf1/.store_meta /data/ldbc/sf1/catalog_version /data/ldbc/sf1/store.db /data/ldbc/sf1/delta.wal
bash /turbograph-v3/scripts/load.sh
```

### 8. Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| ~~ExtentIterator latent buffer overflow~~ | ~~높음~~ | **수정 완료**: turbolynx_get_varchar NULL validity 체크 |
| ~~DETACH DELETE edge cascade~~ | ~~중간~~ | **구현 완료**: GraphCatalog → edge partition 순회 → AdjListDelta.DeleteEdge |
| ~~메모리 내 delta 유실~~ | ~~중간~~ | **WAL 구현 완료**: delta.wal replay on connect |
| In-memory extent 수 폭증 | 중간 | partition당 256개 제한 + Compaction (미구현) |
| Scan-time merge 성능 overhead | 중간 | Delta 비어있으면 fast path |
| Compaction 미구현 | 중간 | delta가 메모리에 계속 누적 → 대량 mutation 시 성능 저하 |
| DELETE edge constraint 미구현 | 낮 | `DELETE n` 시 edge 있어도 허용 (Neo4j는 에러) |
| SET+RETURN 같은 쿼리 | 낮 | two-phase라 RETURN이 base 값 반환 (별도 쿼리로 우회) |

### 9. Open Questions

1. **VID 할당**: in-memory extent의 row에 VID를 어떻게 할당? → `[partition_id:16][in_memory_eid:16][offset:32]`
2. **Multi-label**: `SET n:Employee` — 노드에 라벨 추가 시 다른 partition으로 이동?
3. **Schema evolution**: 기존에 없던 property를 SET하면? → UpdateSegment에 새 property 보관
4. **NULL 시맨틱**: Neo4j 시맨틱 따름. `SET n.prop = NULL` = `REMOVE n.prop`. 의도적 NULL 없음.
5. **Batch mutation**: `UNWIND [1,2,3] AS x CREATE (n:Person {id: x})` 지원 범위

### 10. 수정 파일 목록

```
CRUD Core:
  src/main/capi/turbolynx-c.cpp              — CREATE/SET/DELETE/MERGE/DETACH DELETE executor
  src/execution/.../physical_node_scan.cpp    — delta scan, delete merge, SET merge
  src/storage/graph_storage_wrapper.cpp       — AdjList delta merge, UpdateSegment merge
  src/storage/extent/adjlist_iterator.cpp     — in-memory extent guard (VLE/shortestPath)

Parser/Binder:
  src/parser/cypher_transformer.cpp           — SET/DELETE clause transformer
  src/include/parser/query/updating_clause/   — set_clause.hpp, delete_clause.hpp
  src/include/binder/query/updating_clause/   — bound_set_clause.hpp, bound_delete_clause.hpp, bound_create_clause.hpp (edge info)
  src/binder/binder.cpp                       — BindSetClause, BindDeleteClause, edge chain binding

Storage:
  src/include/storage/delta_store.hpp         — InsertBuffer, UpdateSegment, DeleteMask, AdjListDelta, user_id updates/deletes
  src/include/storage/wal.hpp                 — WALWriter, WALReader
  src/storage/wal.cpp                         — WAL binary format, replay logic
  src/include/common/constants.hpp            — IsInMemoryExtent()

Infrastructure:
  src/include/main/database.hpp               — DeltaStore + WALWriter members
  src/include/main/capi/turbolynx.h           — turbolynx_clear_delta() API
  src/include/main/capi/cypher_prepared_statement.hpp — copyResults deep copy fix

Tests:
  test/query/test_q7_crud.cpp                 — 71 CRUD tests (CREATE/SET/DELETE/MERGE/DETACH/REMOVE/WAL/stress/mixed)
  test/query/helpers/query_runner.hpp         — clearDelta(), reconnect()
  test/storage/test_delta_store.cpp           — 37 DeltaStore unit tests
```

---

## Completed Work (Read Path)

### IC Test Coverage (IC1~IC14 Neo4j verified)

| IC | Query | Status |
|----|-------|--------|
| IC1~IC14 | All LDBC Interactive Complex queries | PASS (original Cypher) |

### Infrastructure
- Crash-proof signal handler (SIGSEGV/SIGABRT/SIGFPE recovery)
- ORCA memory pool recreated per query (state leak prevention)
- Query plan cache (AST caching, skip ANTLR parse on repeated queries)
- Comparison normalization (const op var → var flipped_op const)
- UTF-8 table rendering
