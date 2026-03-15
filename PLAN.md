# TurboLynx — Execution Plan

## Current Status

M1~M29 완료. Load script가 Neo4j LDBC import와 동일한 통합 edge type 사용.
(HAS_CREATOR ×2, LIKES ×2, HAS_TAG ×3, IS_LOCATED_IN ×4, REPLY_OF ×2)

Binder graphlet 필터링 버그 수정 완료: partition_ids 필터링 시 graphlet_ids도 함께 필터링.
→ 단일 label로 좁혀지는 쿼리 (e.g., `(a:Post)-[:HAS_CREATOR]->(b:Person)`)는 정상 동작.

**M29 완료**: IdSeek multi-outer-schema 지원. `num_outer_schemas`를 planner에서 올바르게 전달.
11개 MPE 테스트 모두 통과.

### Query Test Status

| 태그 | 테스트 수 | 상태 | 비고 |
|------|-----------|------|------|
| `[q1]` | 27 | **26 pass, 1 skip(mpv)** | Q1-22(MPV NodeScan)만 미지원 |
| `[q2]` | 12 | **12 pass** | Q2-10~12 MPE 테스트 포함 |
| `[q3]` | 9 | **9 pass** | Q3-07~09 MPE 테스트 포함 |
| `[q4]` | 7 | **4 pass, 1 fail, 2 skip** | IS2: REPLY_OF VarLen 결과 수 변경, IS6/IS7: mayfail |
| `[q5]` | 8 | **0 pass, 8 fail** | IC: unified edge type DB reload 이후 검증 필요 |
| `[q7]` | 4 | **1 pass, 2 fail, 1 skip** | MPV NodeScan 미지원 |
| `[mpe]` | 11 | **11 pass** | ✅ M29로 해결 |

**루틴 검증 명령어:**
```bash
cd /turbograph-v3/build-lwtest
./test/unittest "[catalog]" 2>&1 | tail -5
./test/unittest "[storage]" 2>&1 | tail -5
./test/unittest "[common]"  2>&1 | tail -5
./test/query/query_test "[q1]" --db-path /data/ldbc/sf1 2>&1 | tail -5
./test/query/query_test "[q2]" --db-path /data/ldbc/sf1 2>&1 | tail -5
./test/query/query_test "[q3]" --db-path /data/ldbc/sf1 2>&1 | tail -5
./test/query/query_test "[mpe]" --db-path /data/ldbc/sf1 2>&1 | tail -5
```

---

## M29 — IdSeek Multi-Outer-Schema 지원 (완료)

### 문제

multi-partition edge(예: 통합 HAS_CREATOR)를 사용하는 쿼리에서 ORCA가
`SerialUnionAll`을 생성하면, 각 child의 output chunk에 `schema_idx = 0, 1, ...`이
설정된다. IdSeek는 `num_outer_schemas = 1`을 하드코딩하고 있어 assertion 실패.

### 수정 내용

1. **M29-A**: `PhysicalIdSeek` 두 생성자에 `size_t num_outer_schemas = 1` 파라미터 추가.
   생성자 내부에서 `num_total_schemas = num_outer_schemas * num_inner_schemas`로 계산.

2. **M29-B**: `planner_physical.cpp`의 8개 IdSeek 생성 위치에서 `pGetNumOuterSchemas()` 전달.

3. **M29-C**: `referInputChunkLeft`의 `inner_col_maps[schema_idx]` 버그 수정 —
   outer schema index를 inner index로 사용하는 대신, 모든 inner schema를 순회하여 nullify.

### 수정 파일

| 파일 | 변경 |
|------|------|
| `physical_id_seek.hpp` | 생성자에 `num_outer_schemas` 파라미터 추가 |
| `physical_id_seek.cpp` | 생성자, referInputChunkLeft 수정 |
| `planner_physical.cpp` | 8개 IdSeek 생성 시 `pGetNumOuterSchemas()` 전달 |
| `test_q1_count.cpp` | Q1-23~27: `[!mayfail]` 제거 |
| `test_q2_filter.cpp` | Q2-10~12: `[!mayfail]` 제거 |
| `test_q3_multihop.cpp` | Q3-07~09: `[!mayfail]` 제거 |

---

## Known Technical Debt

### IC 테스트 실패 (q5)

통합 edge type DB reload 이후 IC 테스트 8개 모두 실패. 원인:
- 기대값이 이전 DB(별도 edge type) 기준으로 작성됨
- REPLY_OF 통합으로 VarLen 경로 결과 변경 가능
- 재검증 + 기대값 업데이트 필요

### ORCA Xform 버그 (multi-label 양끝점)

`CreateHomogeneousIndexApplyAlternativesUnionAll` xform이 양쪽 endpoint가 모두
multi-labeled인 경우 dangling column reference로 SEGFAULT.
임시 조치로 해당 xform을 비활성화.
영향받는 테스트: Q1-15, Q1-16, IS7.

### NodeScan Multi-Partition Vertex

`MATCH (m:Message) RETURN count(m)` — primary partition만 스캔.
Q1-22에서 2,052,169 반환 (기대: 3,055,774).

### Filter Pushdown + Multi-Outer (미구현)

`doSeekUnionAll` filter pushdown 경로에서 `executors[0]`, `non_pred_col_idxs_per_schema[0]`
하드코딩. multi-outer + filter pushdown 조합 시 OOB 가능. 현재 해당 조합 테스트 없음.

### Persistence Tier (미구현)
- WAL, Checkpoint, Transaction Manager 전체 주석 처리 상태
- in-memory only 모드

### 버그
| 파일 | 위치 | 내용 |
|------|------|------|
| `extent_iterator.cpp` | line 205 | 인덱스 계산 오류 (`// TODO bug..`) |
| `histogram_generator.cpp` | line 215 | boundary 값 strictly ascending 보장 미흡 |
