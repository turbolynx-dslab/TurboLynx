# TurboLynx — Execution Plan

## Current Status

M1~M29 완료. OPTIONAL MATCH (LOJ), multi-partition edge direction 수정 완료.
IS 테스트를 Neo4j 쿼리 기준으로 전면 업데이트 (Message MPV 라벨, OPTIONAL MATCH, CASE 포함).

### Query Test Status

| 태그 | 테스트 수 | 상태 | 비고 |
|------|-----------|------|------|
| `[q1]` | 27 | **26 pass, 1 skip(mpv)** | Q1-22(MPV NodeScan)만 미지원 |
| `[q2]` | 12 | **12 pass** | Q2-10~12 MPE 테스트 포함 |
| `[q3]` | 9 | **9 pass** | Q3-07~09 MPE 테스트 포함 |
| `[q4]` | 7 | **4 pass, 3 fail** | IS4/IS6/IS7: Message MPV → CCM file_handler 미등록 |
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
영향받는 테스트: Q1-15, Q1-16.

### NodeScan Multi-Partition Vertex (CCM file_handler 미등록)

`Message` MPV (Post + Comment) 스캔 시 두 번째 partition의 CDF file_handler가
CCM에 등록되지 않아 assertion 실패. → **M30에서 수정 예정.**
영향: IS4, IS6, IS7, Q1-22.

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
