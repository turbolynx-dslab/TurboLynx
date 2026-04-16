# TurboLynx — CRUD 설계

## §1. 설계 요약

### 1.1 큰 그림: OLAP 지향 Base/Delta 구조

이 구조는 OLAP 환경의 그래프 저장소를 위한 CRUD 설계이다. 목표는 scan과 traversal에 유리한 Base storage의 읽기 레이아웃을 유지하면서 create, update, delete를 수용하는 것이다. 이를 위해 질의가 주로 읽는 `Base storage`와 최근 변경을 먼저 기록하는 `Delta storage`를 분리한다.

- **쓰기 경로**: 모든 create, update, delete는 Base를 직접 수정하지 않고 먼저 Delta에 기록한다.
- **읽기 경로**: 질의는 Base를 읽고, Delta를 함께 반영해 최신 상태를 본다.
- **정리 경로**: compaction이 누적된 Delta를 Base에 흡수해 읽기 최적화된 형태를 회복한다.

이 설계에서 update는 in-place mutation이 아니라 delete + insert로 처리한다. property 추가나 삭제로 schema가 바뀌면 기존 record를 같은 위치에서 직접 갱신할 수 없기 때문이다. delete + insert의 문제는 record의 물리 위치가 바뀐다는 점이다. 이를 피하기 위해 live node/edge record에는 안정적인 논리 ID인 `LID`를 부여하고, `LidPidTable`이 `LID`를 현재 물리 위치인 `PID`에 매핑한다. record가 이동하면 `LidPidTable`만 새 `PID`로 갱신한다.

### 1.2 Base storage

Base storage는 시스템이 장기적으로 유지하는 정리된 데이터이다. 내부 구조는 다음과 같다.

```
Partition
  └── Graphlet
        └── Extent
              ├── Column chunks
              └── Adjacency chunks (CSR)
```

- **Partition**: 가장 큰 논리 단위이다. 노드는 label별, 엣지는 `(src label, edge type, dst label)`별로 나뉜다.
- **Graphlet**: 한 Partition 안에서 같은 컬럼 배치를 공유할 수 있는 row들을 다시 묶은 그룹이다.
- **Extent**: 실제로 읽고 쓰는 물리 블록이다. scan의 기본 단위가 된다.
- **Column chunks**: 속성 값을 컬럼 형태로 저장하는 영역이다.
- **Adjacency chunks (CSR)**: 노드에서 연결된 엣지를 빠르게 따라가기 위한 인접 리스트 구조이다. 각 `(edge partition, direction)` 조합별로 따로 유지된다.

### 1.3 Delta storage

Delta storage는 아직 Base에 흡수되지 않은 최신 변경을 담는다. 구성은 `Graphlet Delta`와 `CSR Delta`의 두 부분으로 나뉜다.
질의 시점에는 Base와 Delta를 함께 읽어 최신 상태를 구성한다.

- **Graphlet Delta**: partition마다 하나씩 있으며, Graphlet에 저장되는 node/edge record의 변경을 담는다.
  - `GraphletInserts`: `map<schema, record list>`
  - `GraphletDeletes`: `map<extent_id, bitmap>`
- **CSR Delta**: `(edge partition, direction)`마다 하나씩 있으며, CSR index의 변경을 담는다.
  - `CsrInserts`: `map<src_lid, edge list>`
  - `CsrDeletes`: `map<extent_id, bitmap>`

node의 create/update/delete는 `Graphlet Delta`에만 반영된다. node는 Graphlet에 record만 저장되기 때문이다.

edge는 먼저 Graphlet에 edge record를 저장하고, 그 edge를 따라가기 위한 CSR index를 함께 갱신해야 한다. 따라서 edge의 create/delete는 `Graphlet Delta`와 `CSR Delta`를 함께 갱신한다. 반면 edge property update는 edge record 본문만 바꾸므로 `Graphlet Delta`에만 반영된다.

### 1.4 CRUD 처리 방식

각 연산은 다음과 같이 처리된다.

| 연산 | 반영되는 자료구조 | 의미 |
|---|---|---|
| **CREATE node** | `GraphletInserts` | 새 node record를 해당 partition의 schema별 record list에 추가한다. |
| **CREATE edge** | `GraphletInserts` + `CsrInserts` | edge record는 Graphlet에 추가하고, 이를 따라가기 위한 CSR index entry를 edge list에 추가한다. |
| **UPDATE node** | `GraphletDeletes` + `GraphletInserts` | 기존 node record는 extent별 bitmap에 표시하고, 변경된 node record를 새 schema별 record list에 추가한다. |
| **UPDATE edge property** | `GraphletDeletes` + `GraphletInserts` | 기존 edge record는 extent별 bitmap에 표시하고, 변경된 edge record를 새 schema별 record list에 추가한다. |
| **DELETE node** | `GraphletDeletes` | 기존 node record를 extent별 bitmap에 표시한다. |
| **DELETE edge** | `GraphletDeletes` + `CSR Delta` | edge record는 Graphlet에서 삭제 표시한다. CSR index entry가 Base CSR에 있으면 `CsrDeletes`에 표시하고, CSR Delta에만 있으면 `CsrInserts`에서 제거한다. |
| **READ** | Base + Delta | 질의는 Base를 읽고, Delta의 삽입/삭제를 함께 반영해 최신 상태를 본다. |

핵심은 update를 포함한 모든 쓰기가 in-place overwrite가 아니라는 점이다. 기존 상태는 무효화하고, 새 상태는 Delta에 추가한다. 이 방식은 쓰기 경로를 단순하게 유지하면서 Base의 읽기 레이아웃을 보존한다.

### 1.5 Compaction

Compaction은 누적된 Delta를 Base에 반영해 읽기 비용을 낮추는 단계이다.

- **Minor compaction**
  - **의미**: Delta를 Base로 합치는 compaction이다.
  - **조건**: 한 partition의 `Graphlet Delta` 또는 `CSR Delta`가 일정 크기 이상 쌓이거나, delete bitmap 누적이 커지거나, 주기적 정리 시점이 되면 실행한다.
  - **영향 범위**: 하나의 partition과 그 partition에 대응하는 `CSR Delta`만 대상으로 한다.
  - **동작**: `GraphletInserts`의 record를 새 Base extent로 반영하고, `GraphletDeletes`를 대응하는 Base extent의 delete bitmap에 반영한다. CSR 쪽은 delete되지 않은 기존 Base CSR entry와 `CsrInserts`의 새 entry를 합쳐 새 CSR index로 다시 기록한다. 반영이 끝난 `Graphlet Delta`와 `CSR Delta`는 비운다.
- **Major compaction**
  - **의미**: Base data files 자체를 다시 정리하는 compaction이다.
  - **조건**: 작은 extent가 많이 쌓였거나, delete가 많이 누적되었거나, schema가 많이 흩어졌거나, 장기 정리 시점이 되면 실행한다.
  - **영향 범위**: 여러 partition 또는 전체 Base를 대상으로 한다.
  - **동작**: delete bitmap에 표시되지 않은 record만 다시 모아 새 Base extent를 만든다. 이때 Cost-based Graphlet-Chunking을 다시 적용해, 함께 저장하는 것이 유리한 record들을 같은 graphlet으로 다시 묶고 새 extent들로 다시 나눈다. 새 배치에 맞춰 CSR index를 다시 만들고, `LidPidTable`의 `LID -> PID` 매핑도 함께 다시 쓴다.

---

## §2. Storage Layout

이 절은 §1에서 소개한 구조를 실제 저장 레이아웃 기준으로 풀어 쓴다.

### 2.1 LID, PID, LidPidTable

`LidPidTable`은 live node/edge record의 `LID -> location`을 유지하는 매핑이다.

- `LID`는 안정적인 node/edge 식별자이다.
- `PID`는 row 위치를 가리키는 payload이다.
- `LidPidTable` 엔트리는 `is_delta` bit와 `PID`로 구성된다.
- node LID와 edge LID는 서로 충돌하지 않는 독립 namespace를 가진다.
- `is_delta = 0`이면 `PID`는 Base row를 가리키고, `PID = (extent_id, row_offset)`이다.
- `is_delta = 1`이면 `PID`는 `GraphletInserts` 안의 row를 가리키고, `PID = (schema_id, row_index)`이다.

삭제된 record는 `LidPidTable` 엔트리를 제거하거나 `invalid` 상태로 둔다. 별도의 deleted bit는 두지 않는다.

node reference와 edge CRUD는 물리 위치 대신 `LID`를 기준으로 동작한다. record가 update나 compaction으로 이동하면 `LidPidTable`만 새 위치로 갱신한다.

### 2.2 Base storage

Base storage는 질의가 주로 읽는 본체이다.

- record 본문은 `Partition -> Graphlet -> Extent` 구조 안의 column chunks에 저장된다.
- CSR index는 node extent에 붙은 CSR chunks에 저장된다.
- 각 CSR chunk는 하나의 `(edge partition, direction)`에 대응한다.
- Base는 가능한 한 읽기 전용 포맷을 유지하고, 삭제만 bitmap으로 표현한다.

Base의 삭제 상태는 두 종류이다.
- **row delete bitmap**: extent 안의 row와 1:1로 대응한다.
- **CSR delete bitmap**: CSR chunk 안의 edge slot과 1:1로 대응한다.

### 2.3 Graphlet Delta

`Graphlet Delta`는 partition마다 하나씩 있으며, Graphlet에 저장되는 node/edge record의 최신 변경을 담는다.

- `GraphletInserts`: `map<schema, record list>`
- `GraphletDeletes`: `map<extent_id, bitmap>`

`GraphletInserts`는 schema별 신규 row 목록이다. 각 row는 `(schema_id, row_index)`로 식별된다.

`GraphletDeletes`는 Base에 있던 record를 읽기에서 제외하기 위한 구조이다. 어떤 row를 물리적으로 바로 지우지 않고, 대응하는 extent의 bitmap에 표시한다.

### 2.4 CSR Delta

`CSR Delta`는 `(edge partition, direction)`마다 하나씩 있으며, CSR index의 최신 변경을 담는다.

- `CsrInserts`: `map<src_lid, edge list>`
- `CsrDeletes`: `map<extent_id, bitmap>`

`CsrInserts`는 source node별 신규 edge 목록이다. 각 entry는 `(dst_lid, edge_lid)`를 가진다.

`CsrDeletes`는 Base CSR에 있던 entry를 읽기에서 제외하기 위한 구조이다. 삭제 대상이 속한 extent의 bitmap에 해당 slot을 표시한다.

---

## §3. 쓰기 경로

모든 쓰기는 다음 순서를 따른다.

1. 대상 partition과 현재 위치를 찾는다.
2. WAL에 변경 내용을 기록한다.
3. `Graphlet Delta` 또는 `CSR Delta`를 갱신한다.
4. node 또는 edge 위치가 바뀌면 `LidPidTable`을 새 `PID`로 갱신한다.

### 3.1 Node create/update/delete

**CREATE node**

```text
1. 대상 partition과 schema를 정한다.
2. WAL에 create를 기록한다.
3. record를 GraphletInserts[schema]의 끝에 추가한다.
4. 새 `node_lid`를 할당한다.
5. LidPidTable[node_lid] = { is_delta = 1, pid = (schema_id, row_index) } 로 기록한다.
```

**UPDATE node**

```text
1. entry = LidPidTable[node_lid]를 읽는다.
2. WAL에 update를 기록한다.
3. if entry.is_delta == 0:
     GraphletDeletes[extent_id]의 row_offset bit를 set한다.
4. else:
     GraphletInserts[schema_id][row_index]를 무효화한다.
5. 변경된 record를 GraphletInserts[new_schema]의 끝에 추가한다.
6. LidPidTable[node_lid] = { is_delta = 1, pid = (new_schema_id, new_row_index) } 로 갱신한다.
```

**DELETE node**

```text
1. entry = LidPidTable[node_lid]를 읽는다.
2. WAL에 delete를 기록한다.
3. if entry.is_delta == 0:
     GraphletDeletes[extent_id]의 row_offset bit를 set한다.
4. else:
     GraphletInserts[schema_id][row_index]를 무효화한다.
5. LidPidTable[node_lid]를 제거하거나 invalid 상태로 바꾼다.
```

### 3.2 Edge create/update/delete

**CREATE edge**

```text
1. 대상 edge partition과 schema를 정한다.
2. 새 `edge_lid`를 할당한다.
3. WAL에 create를 기록한다.
4. edge record를 GraphletInserts[schema]에 추가한다.
5. LidPidTable[edge_lid] = { is_delta = 1, pid = (schema_id, row_index) } 로 기록한다.
6. forward CSR entry `(dst_lid, edge_lid)`를 CsrInserts[src_lid]에 추가한다.
7. backward CSR entry `(src_lid, edge_lid)`를 CsrInserts[dst_lid]에 추가한다.
```

**UPDATE edge property**

```text
1. entry = LidPidTable[edge_lid]를 읽는다.
2. WAL에 update를 기록한다.
3. if entry.is_delta == 0:
     GraphletDeletes[extent_id]의 row_offset bit를 set한다.
4. else:
     GraphletInserts[schema_id][row_index]를 무효화한다.
5. 변경된 edge record를 GraphletInserts[new_schema]에 추가한다.
6. LidPidTable[edge_lid] = { is_delta = 1, pid = (new_schema_id, new_row_index) } 로 갱신한다.
7. source와 destination이 바뀌지 않으므로 CSR index는 그대로 둔다.
```

**DELETE edge**

```text
1. entry = LidPidTable[edge_lid]를 읽고, source/destination을 확인한다.
2. WAL에 delete를 기록한다.
3. if entry.is_delta == 0:
     GraphletDeletes[extent_id]의 row_offset bit를 set한다.
4. else:
     GraphletInserts[schema_id][row_index]를 무효화한다.
5. if entry.is_delta == 0:
     forward/backward CSR slot을 찾아 각 CsrDeletes bitmap에 표시한다.
6. else:
     CsrInserts[src_lid]와 CsrInserts[dst_lid] 안의 해당 entry를 무효화한다.
7. LidPidTable[edge_lid]를 제거하거나 invalid 상태로 바꾼다.
```

source 또는 destination이 바뀌는 edge update는 CSR index의 key 자체가 바뀌므로, `DELETE edge + CREATE edge`로 처리한다.

### 3.3 Base 항목과 Delta 항목의 차이

`GraphletDeletes`와 `CsrDeletes`는 Base에 있던 항목만 가리킨다. 이미 Delta 안에만 있는 record나 CSR entry를 다시 update/delete할 때는, 대응하는 `record list`나 `edge list` 안에서 직접 무효화한다.

```text
1. 대상이 Base에 있으면:
   GraphletDeletes 또는 CsrDeletes bitmap에 표시한다.
2. 대상이 Delta에 있으면:
   GraphletInserts 또는 CsrInserts 안의 entry를 직접 무효화한다.
```

---

## §4. 읽기 경로

### 4.1 Node scan

```text
1. partition의 Base extent를 순서대로 읽는다.
2. 각 extent에서 Base row delete bitmap을 적용한다.
3. 같은 extent에 대해 GraphletDeletes[extent_id]를 추가로 적용한다.
4. 삭제되지 않은 Base row를 출력한다.
5. 같은 partition의 GraphletInserts를 읽는다.
6. 무효화되지 않은 Delta row를 출력한다.
```

즉 node scan은 Base row와 Delta row를 차례로 읽어 최신 상태를 만든다.

### 4.2 IdSeek

```text
1. entry = LidPidTable[node_lid]를 읽는다.
2. entry가 없으면 삭제된 node로 처리한다.
3. if entry.is_delta == 0:
     Base의 (extent_id, row_offset)를 바로 읽는다.
4. else:
     GraphletInserts[schema_id][row_index]를 바로 읽는다.
```

`IdSeek`은 Base 전체와 Delta 전체를 합치지 않고, `LidPidTable`이 가리키는 현재 위치만 읽는다.

### 4.3 Edge traversal

```text
1. (edge partition, direction)을 결정한다.
2. Base CSR chunk에서 source의 adjacency range를 읽는다.
3. Base CSR delete bitmap을 적용한다.
4. CsrDeletes[extent_id]를 추가로 적용한다.
5. 삭제되지 않은 Base CSR entry를 출력한다.
6. CsrInserts[src_lid]의 유효한 entry를 이어붙인다.
7. edge property가 필요하면 대응하는 edge record를 Graphlet 쪽에서 읽는다.
```

### 4.4 Fast path

```text
1. 관련 Graphlet Delta와 CSR Delta가 모두 비어 있으면:
   Base만 읽는다.
2. 하나라도 비어 있지 않으면:
   Base와 Delta를 함께 읽는다.
```

---

## §5. Compaction

### 5.1 Minor compaction

Minor compaction은 한 범위의 Delta를 같은 범위의 Base에 흡수하는 단계이다.

- 입력은 하나의 partition의 Base record, 그 partition의 `Graphlet Delta`, 그리고 대응하는 `(edge partition, direction)`들의 `CSR Delta`이다.
- Base record 쪽은 삭제되지 않은 row와 아직 유효한 `GraphletInserts`를 합쳐 새 Base extent를 만든다.
- CSR 쪽은 삭제되지 않은 Base CSR entry와 아직 유효한 `CsrInserts`를 합쳐 새 CSR chunks를 만든다.
- 새 Base extent로 내려간 node와 edge에 맞춰 `LidPidTable` 엔트리를 `{ is_delta = 0, pid = (extent_id, row_offset) }`로 다시 쓴다.
- 반영이 끝난 `Graphlet Delta`와 `CSR Delta`는 비운다.

Minor compaction은 graphlet 경계를 크게 바꾸지 않고, 현재 partition 안에서 Delta를 정리하는 단계이다.

### 5.2 Major compaction

Major compaction은 Base data files 자체를 다시 배치하는 단계이다.

- 입력은 여러 partition 또는 전체 Base의 현재 유효 상태이다.
- 대상 범위의 Delta는 먼저 minor compaction으로 비우거나, 동일한 유효 상태를 만들어 입력으로 사용한다.
- delete bitmap에 걸리지 않은 record만 모아 다시 배치한다.
- Cost-based Graphlet-Chunking을 적용해 어떤 record를 같은 graphlet과 extent에 둘지 다시 정한다.
- 새 배치에 맞춰 Base extent와 CSR chunks를 함께 다시 만든다.
- 이동한 모든 node와 edge에 맞춰 `LidPidTable` 엔트리를 `{ is_delta = 0, pid = (extent_id, row_offset) }`로 다시 쓴다.

Minor compaction이 Delta를 Base로 합치는 단계라면, Major compaction은 Base 전체 배치를 다시 잡는 단계이다.

---

## §6. WAL과 복구

### 6.1 Write ordering

- 모든 write는 `WAL append + flush`가 먼저 일어난다.
- 그 다음 `Graphlet Delta` 또는 `CSR Delta`가 갱신된다.
- node나 edge의 위치가 바뀌면 마지막에 `LidPidTable`에 새 `PID`를 반영한다.

이 순서를 지키면 reader가 새 `PID`를 보았을 때, 그 위치의 record는 이미 WAL과 Delta에 반영된 상태가 된다.

### 6.2 Checkpoint와 recovery

- checkpoint는 현재 Base files와 `LidPidTable`의 일관된 상태를 저장한다.
- restart 시에는 마지막 checkpoint를 읽고, 그 이후의 WAL을 replay해 `Graphlet Delta`, `CSR Delta`, `LidPidTable`을 복구한다.
- compaction 결과는 새 Base files와 `LidPidTable` 갱신이 모두 끝난 뒤에만 반영한다.

## 부록 A — 참고

| # | 출처 | 차용 포인트 |
|---|---|---|
| [1] | Revisiting DGS (PACMMOD 2025) | 공통 추상화, coarse versioning, CSR 비교 |
| [2] | RadixGraph (SIGMOD 2026) | snapshot+log (참고만, 채택 X), NULL-weight delete, SORT |
| [3] | DuckDB CHECKPOINT | WAL/checkpoint 시맨틱 |
| [4] | Iceberg | positional deletion bitmap, 2-tier rewrite |
| [5] | Delta Lake | 트랜잭션 로그 |
| [6] | ClickHouse | background merge scheduler |
| [7] | Druid | segment publish-then-merge |
| [8] | S62 | delete-and-reinsert, periodic rebalance |
| [9] | Sortledton / Teseo / LiveGraph / Aspen | neighbor layout 설계 공간 |

## 부록 B — 테스트

```bash
cd /turbograph-v3/build-lwtest && ninja
./test/unittest "[execution]" --case "CRUD*"
./test/query/query_test --ldbc-path /data/ldbc/sf1
```
