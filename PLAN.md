# TurboLynx — Execution Plan

## Current Status

Core build is stable. Catalog, Storage, Execution layers tested.
Build runs inside `turbograph-s62` Docker container.

LDBC SF1 + TPC-H SF1 + DBpedia bulkload E2E tests all passing.
Milestone 12 (Bulkload Performance Optimization) complete. Next milestone TBD.

## Completed Milestones

| # | Milestone | Status |
|---|-----------|--------|
| 1 | Remove Velox dependency | ✅ Done |
| 2 | Remove Boost dependency | ✅ Done |
| 3 | Remove Python3 dependency | ✅ Done |
| 4 | Bundle TBB / libnuma / hwloc | ✅ Done |
| 5 | Single-file block store (`store.db`) | ✅ Done |
| 6 | Test suite: catalog, storage, execution | ✅ Done |
| 7 | Remove libaio-dev system dependency (direct syscalls) | ✅ Done |
| 8 | Rename library: `libs62gdb.so` → `libturbolynx.so` | ✅ Done |
| 9 | E2E bulkload test suite (LDBC SF1, TPC-H SF1, DBpedia) | ✅ Done |
| 10 | Extract `BulkloadPipeline` from `tools/bulkload.cpp` | ✅ Done |
| 11 | Multi-client support (prototype level) | ✅ Done |


---

## Milestone 12 — Bulkload Performance Optimization

### Background: Architecture Analysis

Bulkload pipeline consists of five sequential phases.
All timing observations are relative; absolute numbers depend on dataset and hardware.

```
BulkloadPipeline::Run()
 ├── LoadVertices()           CSV parse → extents + LID-to-PID map
 ├── ReconstructIDMappings()  incremental mode only: MATCH queries to rebuild maps
 ├── LoadForwardEdges()       CSV parse → edge extents + fwd adjlist CSR
 ├── LoadBackwardEdges()      CSV parse → bwd adjlist CSR
 └── RunPostProcessing()
      ├── HistogramGenerator::CreateHistogram()   ← MAJOR BOTTLENECK
      ├── ChunkCacheManager::FlushMetaInfo()
      └── Catalog::SaveCatalog()
```

---

### Bottleneck Analysis (코드 기반)

#### BN-1: Histogram — Two Full Storage Scans Per Partition  ★★★★★
**File:** `src/storage/statistics/histogram_generator.cpp`

`_create_histogram()` performs **two independent full scans** of every partition:

- **Scan 1** (lines 124–130): accumulates every value into unbounded `vector<int64_t> accms[i]`
  and tracks NDV via `unordered_set<uint64_t>` with `GetValue()` virtual dispatch per cell.
- Between scans: `std::sort(accms[i])` on the full accumulated vector → O(N log N), up to
  hundreds of MB of in-memory data per column for large datasets.
- **Scan 2** (lines 239–244): re-reads the same data to count bucket frequencies.

For N partitions (e.g., 10 in LDBC SF1), total I/O = N × 2 full sequential reads of all extents.
The sort is bounded by available memory and degrades to virtual memory thrashing at scale.

Additionally, `_accumulate_data_for_ndv()` (line 527) calls `target_vec.GetValue(i)` — a slow
virtual-dispatch value boxing call — for **every cell in every row**. By contrast, `_accumulate_data_for_hist()`
correctly uses typed raw pointer access. The NDV path does O(N × C) virtual calls.

**Root cause summary:**
1. Sort of the full accumulated column data — O(N log N), O(N) memory
2. Two full storage scans per partition
3. `GetValue()` hot path in NDV accumulation
4. Partitions processed serially with no intra-partition parallelism

---

#### BN-2: Adjacency List Buffer — vector<vector<idx_t>> per Extent  ★★★★
**File:** `src/loader/bulkload_pipeline.cpp`, lines 141–351

The adj list buffer type is:
```cpp
unordered_map<ExtentID, pair<pair<uint64_t,uint64_t>, vector<vector<idx_t>>>> adj_list_buffers
```

Problems:
- **Allocation on first use** (`FillAdjListBuffer`, line 365–373): upon first encounter of an
  extent, allocates `empty_adj_list.resize(STORAGE_STANDARD_VECTOR_SIZE)` = 2048 empty
  `vector<idx_t>` objects. For a graph with 1M extents, this creates 2B vector objects total.
- **AppendAdjListChunk** (lines 329–342): for every extent, materializes a flat
  `tmp_adj_list_buffer` by copying out of the `vector<vector<idx_t>>`, then discards it.
  This copy is O(sum of out-degrees in that extent) — repeated for every extent.
- **ClearAdjListBuffers** (lines 141–162): called once per edge file. Iterates all extents,
  calling `.clear()` on each of the 2048 inner vectors. The OMP parallel loop collects
  iterators into a temporary vector (heap allocation) before parallelizing.
- Across both forward and backward passes, `adj_list_buffers` is cleared but **not freed**:
  the hash map grows monotonically to cover all extents and is never shrunk.

---

#### BN-3: Edge Processing Fully Single-Threaded  ★★★
**File:** `src/loader/bulkload_pipeline.cpp`, lines 924–962, 1139–1175

The inner edge loop processes one row at a time:
```cpp
while (src_seqno < max_seqno) {
    src_key.first = src_key_columns[0][src_seqno];
    cur_src_pid = src_lid_to_pid_map_instance.at(src_key);   // hash lookup per row
    ...
}
```
- No parallelism across CSV chunks within a single edge file.
- No parallelism across different edge files (called one at a time in a for-loop).
- A comment in the file references a multi-threaded version at commit `6fdb44c` — this
  path was removed and not re-integrated after the pipeline refactor.
- Different edge files are fully independent; their loading could overlap.

---

#### BN-4: Synchronous Cache Flush After Every File  ★★★
**File:** `src/loader/bulkload_pipeline.cpp`, lines 618, 648, 992, 1200

```cpp
ChunkCacheManager::ccm->FlushDirtySegmentsAndDeleteFromcache(false);
```

Called once after all vertex files (line 618/648) and once after each of the forward and
backward edge file batches (lines 992, 1200). Each call flushes all dirty cache segments
to disk synchronously, then evicts them from cache, before moving to the next phase.
The `false` argument disables async mode. This creates a hard pipeline stall between phases.

---

#### BN-5: `unordered_map` in Per-Row Hot Loop  ★★
**File:** `src/loader/bulkload_pipeline.cpp`, `FillAdjListBuffer` / `FillBwdAdjListBuffer`

`lid_to_pid_map` is `unordered_map<LidPair, idx_t, LidPairHash>`. The `.at()` call is in the
innermost row-level loop (once per edge row for src, once for dst). For LDBC SF1 ≈ 17M "knows"
edges: 34M+ hash map lookups. `std::unordered_map` has high constant overhead due to
open addressing with pointer chasing and per-bucket allocation. A cache-friendly flat hash map
(e.g., `robin_hood::unordered_flat_map`) typically gives 2–5× better lookup throughput.

---

#### BN-6: Linear Search for Vertex Label Lookup  ★
**File:** `src/loader/bulkload_pipeline.cpp`, lines 1057–1070

```cpp
auto src_it = std::find_if(bulkload_ctx.lid_to_pid_map.begin(), ...,
    [&src_vertex_label](const auto &e) {
        return e.first.find(src_vertex_label) != string::npos;
    });
```

Called once per edge file (not per row), so impact is minor unless hundreds of edge types exist.
The `string::find` substring match is fragile (could match partial label names) and should be
an exact match lookup. Replacing with `unordered_map<string, ...>` indexed by label name
eliminates the linear scan and fixes the correctness risk.

---

### Optimization Plan

Prioritized by expected impact / effort ratio:

| Step  | What | Files | Expected Gain |
|-------|------|-------|---------------|
| **12a** | Fix NDV hot path: replace `GetValue()` with typed pointer loop | `histogram_generator.cpp` | 10–20% histogram time |
| **12b** | Merge histogram scans 1+2 into single pass using reservoir sampling for quantile boundaries | `histogram_generator.cpp` | ~50% histogram I/O |
| **12c** | Parallelize histogram generation across partitions with `std::thread` / OMP | `histogram_generator.cpp` | linear speedup vs. partition count |
| **12d** | Flatten adj list buffer: replace `vector<vector<idx_t>>` with per-extent CSR flat arena | `bulkload_pipeline.cpp` | 20–40% edge load time |
| **12e** | Batch cache flush: remove mid-pipeline `FlushDirtySegmentsAndDeleteFromcache` calls; flush once before `FlushMetaInfo` | `bulkload_pipeline.cpp` | 10–20% total time |
| **12f** | Parallel edge file loading: process independent edge files concurrently using a thread pool | `bulkload_pipeline.cpp` | near-linear speedup vs. file count |
| **12g** | Replace `lid_to_pid_map unordered_map` with flat hash map in hot loop | `bulkload_pipeline.cpp` | 5–15% edge load time |
| **12h** | Replace linear `find_if` label lookup with `unordered_map<string,...>` index | `bulkload_pipeline.cpp` | minor, correctness fix |

---

### 상세 구현 명세

#### Step 12a — NDV 핫패스 수정 (`GetValue()` → 타입별 raw pointer)

**현재 코드** (`histogram_generator.cpp:521–551`):
```cpp
for (auto i = 0; i < chunk.size(); i++) {
    for (auto j = 0; j < chunk.ColumnCount(); j++) {
        auto target_value = target_vec.GetValue(i);   // ← virtual dispatch, heap alloc
        switch(types[j].id()) {
            case INTEGER: target_set.insert(target_value.GetValue<int32_t>()); ...
```

**변경 후**:
```cpp
for (auto j = 0; j < chunk.ColumnCount(); j++) {
    auto &target_set = ndv_counters[j];
    auto &validity = chunk.data[j].GetValidity();   // ← NULL 체크 필수
    switch (types[j].id()) {
        case INTEGER: {
            auto *ptr = (int32_t *)chunk.data[j].GetData();
            for (idx_t i = 0; i < chunk.size(); i++) {
                if (validity.RowIsValid(i)) target_set.insert(ptr[i]);
            }
            break;
        }
        case BIGINT: {
            auto *ptr = (int64_t *)chunk.data[j].GetData();
            for (idx_t i = 0; i < chunk.size(); i++) {
                if (validity.RowIsValid(i)) target_set.insert((uint64_t)ptr[i]);
            }
            break;
        }
        // ... UINTEGER, UBIGINT, DATE 동일 패턴
    }
}
```
열 기준 outer loop → row 기준 inner loop으로 바꿔 캐시 친화적으로 변경.

> ⚠️ **NULL 유효성 검사 필수:**
> `validity.RowIsValid(i)` 를 빠뜨리면 NULL 셀의 raw 메모리 쓰레기 값이 NDV set에
> 삽입된다. 실제 고유값 수가 부풀려지고 Query Planner가 selectivity를 과소평가한다.

---

#### Step 12b — 히스토그램 단일 패스 (Reservoir Sampling for Quantile Boundaries)

**현재 문제**: Pass 1에서 모든 값을 `accms[i]`에 축적 → `std::sort` O(N log N) → Pass 2에서 bucket fill.

**변경 목표**: `accms[i]` 대신 고정 크기 reservoir (예: 4096개)를 유지.
  1. Pass 1을 streaming reservoir 샘플링으로 대체 — O(N) time, O(R) memory (R=4096 fixed)
  2. 샘플에 대해 정렬 → O(R log R) ≈ O(1) 상수
  3. 샘플에서 quantile 추출 → bucket boundaries 결정
  4. Pass 2: 기존과 동일하게 bucket fill (여전히 스토리지 재읽기 필요)

→ Pass 1의 메모리 사용 수백 MB → 수십 KB로 감소.
→ Sort 시간 O(N log N) → O(R log R) ≈ 상수.
→ 대용량 데이터셋에서 Pass 1 I/O는 동일하지만 메모리 압박으로 인한 paging/thrashing 제거.

> ⚠️ **Power-law 왜곡 주의 (LDBC 그래프 데이터):**
> LDBC 소셜 그래프는 극단적인 멱법칙(Power-law) 분포를 따른다. 슈퍼노드에 엣지가 수백만 개
> 몰려 있을 경우 R=4096 고정 샘플로는 긴 꼬리(Long-tail) 영역의 quantile 경계가 심하게
> 왜곡될 수 있다. Query Planner가 잘못된 조인 순서를 선택할 위험이 있다.
>
> **대응 방안**: 데이터를 1-pass 스트리밍하면서 `n_seen`에 비례하여 R을 동적으로 확장하거나,
> 분포 왜도(skewness)가 임계값(예: max/median > 1000)을 초과하면 해당 컬럼은 전체 축적
> 경로(기존 방식)로 fallback하는 adaptive 전략을 사용한다.
> 테스트 Level 3 (T3-2)의 오차 허용 범위 ±5%는 균등분포 기준이며,
> power-law 컬럼에 대해서는 별도 허용 범위 or fallback 여부를 판단하는 케이스를 추가한다.

Reservoir sampling 구현 (Vitter's Algorithm R):
```cpp
struct ReservoirSampler {
    static constexpr size_t R = 4096;
    std::vector<int64_t> reservoir;
    size_t n_seen = 0;
    std::mt19937_64 rng;

    // seed를 명시적으로 받아야 한다 — 기본 생성자는 OMP 병렬 환경에서 금지.
    // 동일한 default seed로 모든 스레드를 생성하면 동일한 random sequence →
    // 샘플이 편향되어 일부 partition의 quantile 계산이 왜곡된다.
    explicit ReservoirSampler(uint64_t seed = std::random_device{}()) : rng(seed) {}

    void add(int64_t val) {
        n_seen++;
        if (reservoir.size() < R) {
            reservoir.push_back(val);
        } else {
            size_t j = rng() % n_seen;
            if (j < R) reservoir[j] = val;
        }
    }
    std::vector<int64_t> sorted_sample() {
        auto s = reservoir;
        std::sort(s.begin(), s.end());
        return s;
    }
};
```

`accms` 멤버 타입을 `vector<int64_t>*` → `ReservoirSampler*`로 교체.
`_init_accumulators`, `_accumulate_data_for_hist`, `compute_quantile` 함께 수정.

---

#### Step 12c — 히스토그램 병렬화 (파티션 간 독립 실행)

**현재 코드** (`histogram_generator.cpp:33–35`):
```cpp
for (auto &part_oid : part_oids) {
    CreateHistogram(client, part_oid);   // serial
}
```

각 파티션의 히스토그램은 완전히 독립적.

**변경 후**:
```cpp
#pragma omp parallel for schedule(dynamic,1) num_threads(N_HIST_THREADS)
for (size_t i = 0; i < part_oids.size(); i++) {
    // HistogramGenerator는 thread-local 상태(accms, ext_its)를 가지므로 per-thread 인스턴스 생성.
    // ReservoirSampler seed는 partition OID 또는 omp_get_thread_num()을 사용해 고유하게 지정.
    // 동일 seed 금지: 모든 스레드가 같은 random sequence를 생성해 샘플이 편향된다.
    // omp_get_thread_num()을 시드에 섞으면 schedule(dynamic)에서 스레드 배정이
    // 매 실행마다 달라져 재현성이 깨진다. partition OID만으로 시드를 고정한다.
    uint64_t seed = std::hash<uint64_t>{}(static_cast<uint64_t>(part_oids[i]));
    HistogramGenerator local_gen(seed);
    local_gen.CreateHistogram(client, part_oids[i]);
}
```

주의사항:
- `HistogramGenerator`의 `accms`, `ext_its`, `target_cols` 는 이미 인스턴스 멤버 → thread-local 인스턴스 사용 시 safe.
- `client` (shared_ptr<ClientContext>) 의 스토리지 scan path가 thread-safe한지 확인 필요.
  → `ExtentIterator` 는 scan 상태를 각자 가지고 있으므로 개별 인스턴스로 생성하면 safe.
  → `Catalog::GetEntry`는 `shared_lock` 으로 보호되어 있으므로 safe (Milestone 11d 적용).

N_HIST_THREADS = min(partition_count, hardware_concurrency / 2) 권장.

---

#### Step 12d — Adj List Buffer 평탄화 (flat CSR arena)

**현재**: `vector<vector<idx_t>>` (2048개 per extent). 각 inner vector는 동적 증가.

**변경 목표**: per-extent flat arena buffer로 교체.

> ⚠️ **1-Pass Dynamic Append로는 올바른 CSR이 만들어지지 않는다:**
> CSV를 청크 단위로 읽으면 한 청크 안에 서로 다른 src vertex의 엣지가 섞여서 들어온다
> (파일이 src 기준으로 정렬되어 있어도 청크 경계에서 두 src vertex가 공존할 수 있다).
> 따라서 단일 `flat_neighbors` vector에 `push_back`만 해서는 vertex별 offset을 확정할 수 없다.
> 진정한 CSR 포맷(offset 배열 + 연속 data 배열)을 만들려면 **반드시 2-pass**가 필요하다:
>
> **Pass A**: 각 src_seqno의 degree를 카운트 → `degree[seqno]` 배열 완성
> **Pass B**: prefix-sum으로 `offset[seqno]` 확정 → 미리 할당된 `data` 배열에 직접 삽입
>
> Pass A에서 엣지 파일을 다시 읽거나 degree를 첫 번째 스캔에서 집계해야 한다.
> 만약 2-pass 재읽기를 피하고 싶다면, **Memory-Pool 기반 Linked-Chunk 리스트**
> (per-seqno small arena block → flush 시 flatten)를 사용한다.

**선택된 설계 — 2-Pass with degree accumulation**:

```cpp
struct ExtentAdjBuffer {
    // Pass A: degree counting
    std::vector<uint32_t> degree;       // degree[seqno] = edge count for this src vertex
    uint64_t max_seqno = 0;

    // Pass B: flat CSR data (populated after prefix-sum)
    std::vector<idx_t>    data;         // [dst_pid, epid, dst_pid, epid, ...]
    std::vector<uint32_t> offset;       // offset[seqno] = write cursor for Pass B
    bool offsets_ready = false;

    void count(uint32_t src_seqno, uint32_t n_edges) {
        if (src_seqno >= degree.size()) degree.resize(src_seqno + 1, 0);
        degree[src_seqno] += n_edges;
        max_seqno = std::max(max_seqno, (uint64_t)src_seqno);
    }
    void build_offsets() {   // called once between Pass A and Pass B
        offset.resize(degree.size() + 1, 0);
        uint32_t cur = 0;
        for (size_t i = 0; i < degree.size(); i++) {
            offset[i] = cur;
            cur += degree[i] * 2;   // each edge = (dst_pid, epid)
        }
        // resize()는 zero-fill을 수행한다. Phase 3에서 모든 슬롯을 덮어쓰므로
        // 초기화 비용은 낭비다. 수억 원소 수준에서는 측정 후 필요 시
        // reserve() + custom allocator 또는 uninitialized 버퍼로 교체를 고려한다.
        data.resize(cur);
        offsets_ready = true;
    }
    void fill(uint32_t src_seqno, idx_t dst_pid, idx_t epid) {
        auto &pos = offset[src_seqno];
        data[pos++] = dst_pid;
        data[pos++] = epid;
    }
};
```

**2-pass의 데이터 소스는 CSV가 아니라 이미 쓰여진 바이너리 Extent여야 한다.**

> ⚠️ **CSV 2회 파싱은 기존보다 느려진다:**
> 수십~수백 GB의 CSV를 텍스트로 두 번 파싱하면 CPU·디스크 I/O 비용이 두 배가 된다.
> 이는 최적화가 아니라 퇴보다. **CSV는 반드시 한 번만 파싱한다.**
>
> **올바른 파이프라인 구조**:
> 1. **CSV 단 1회 파싱** → 엣지 데이터를 바이너리 Extent로 스토리지에 기록
>    + 동시에 degree 카운트만 집계 (`ExtentAdjBuffer::count()`)
> 2. **모든 CSV 파싱 완료 후**: `build_offsets()` 호출 (메모리 내 prefix-sum, 매우 빠름)
> 3. **바이너리 Extent를 순차 스캔** (Pass B): 이미 정수로 변환된 데이터를 읽어 CSR 채우기
>    → 텍스트 파싱 없음. `doScan()` 기반의 binary sequential read는 CSV 파싱 대비 수십 배 빠름.
>
> 이 방식은 히스토그램이 `_create_histogram`에서 Extent를 2회 스캔하는 것과 동일한 패턴이다.

**수정된 파이프라인**:

```
현재:
  while (CSV 청크) { CSV 파싱 → DataChunk → CreateExtent + FillAdjListBuffer }

변경 후:
  Phase 1 — CSV 단 1회 파싱:
    while (CSV 청크) { CSV 파싱 → DataChunk → CreateExtent + CountPass(degree++) }

  Phase 2 — prefix-sum (메모리, O(V)):
    build_offsets() for all ExtentAdjBuffers

  Phase 3 — 바이너리 Extent 스캔 (FillPass):
    while (doScan(extent) → DataChunk) { FillPass(src_pid, dst_pid, epid) }
```

Phase 3의 `doScan`은 이미 `HistogramGenerator`가 사용하는 동일한 스토리지 API.
CSV 텍스트 파싱 비용 없이 정수 배열만 순회하므로 Phase 1 대비 I/O는 비슷하나
CPU 비용이 대폭 감소한다.

> ⚠️ **OOM 방지를 위한 Phase 분리 — Phase 1은 반드시 Global이어야 한다:**
>
> **메모리 규모 팩트 체크:**
> - Degree 카운트 배열(Phase 1): 정점 10억 개 × 4B = ~4 GB → 전체 글로벌 보유 무방
> - CSR Data 배열(Phase 3): 간선 100억 개 × 8B = ~80 GB → 글로벌 보유 시 즉시 OOM
>
> **따라서 루프 구조는 아래와 같이 분리해야 한다:**
>
> ```
> // Phase 1 (Global) — CSV 단 1회 파싱
> while (CSV 청크) {
>     CSV 파싱 → DataChunk → CreateExtent
>     fwd_adj_buffer[src_partition].count(src_seqno)  // Out-degree (Forward CSR용)
>     bwd_adj_buffer[dst_partition].count(dst_seqno)  // In-degree  (Backward CSR용)
> }
> // ⚠️ src의 Out-degree만 세면 Backward CSR 구축 시 dst의 offset을 계산할 수 없다.
> // fwd/bwd 두 배열을 동시에 카운트해야 한다.
>
> // Phase 2 & 3 (Per-Partition) — OOM의 주범인 data 배열을 격리
> // fwd / bwd 각각 동일한 루프를 실행한다 (fwd 완료 후 bwd 순차, 또는 동시 병렬)
> #pragma omp parallel for   // 파티션 간 독립적이므로 병렬화 가능
> for each partition P {
>     Phase 2: build_offsets() for fwd_adj_buffer[P]  // offset 배열 + data 배열 할당
>     Phase 3: P 소속 바이너리 Extent만 스캔 → Forward CSR fill → 디스크 flush
>     fwd_adj_buffer[P].data.clear();
>     fwd_adj_buffer[P].data.shrink_to_fit();
> }
> // (bwd_adj_buffer도 동일한 Phase 2 & 3 루프 수행)
> ```
>
> Phase 1을 파티션 루프 안에 넣으면 파티션 수(P)만큼 CSV를 반복 읽게 된다.
> 파티션 10개면 수백 GB의 CSV를 10회 파싱 — "CSV 1회 파싱" 원칙을 위반하는 대참사.
> **Phase 2 & 3만 파티션 단위 루프로 격리**하면 CSV 1회 파싱 + OOM 방지를 동시에 달성한다.

> ⚠️ **Phase 3 병렬 스캔 전 `ChunkCacheManager` Lock 구조 확인 필수:**
> Phase 3에서 여러 스레드가 동시에 `doScan(extent)`을 호출하면 모두 `ChunkCacheManager`의
> 페이지 lookup/pinning 경로로 몰린다. CCM 내부에 전역 뮤텍스(Global Mutex)가 있다면
> 스레드 16개를 띄워도 순차 처리와 다를 바 없고, 컨텍스트 스위칭 비용만 추가된다.
>
> **병렬화 효과를 보려면 다음 중 하나여야 한다:**
> 1. CCM page lookup이 Lock-free (Concurrent Hash Map 기반)
> 2. Fine-grained latch (페이지/버킷 단위 락) 로 구현
>
> 12d Phase 3 병렬화 적용 전, CCM의 `GetFrame()`/`PinPage()` 경로의 락 범위를 확인한다.
> 전역 락이 확인되면 Phase 3 병렬화는 보류하고 순차 실행 후 프로파일링으로 재평가한다.

구현 복잡도가 높으므로 12d는 이전 단계 완료 후 진행.

---

#### Step 12e — 캐시 플러시 배치 처리 (trivial)

**현재**: 각 파일 배치(vertex, fwd edge, bwd edge) 완료 후 `FlushDirtySegmentsAndDeleteFromcache(false)` 호출.

**변경**: 3곳의 동기 flush(lines 618/648, 992, 1200)를 **단순 제거하지 않고**,
워터마크를 두 단계로 나눠 파이프라인을 제어한다.

```cpp
// 기존 (동기, stall 발생):
ChunkCacheManager::ccm->FlushDirtySegmentsAndDeleteFromcache(false);

// 변경 후: 파이프라인 내 주기적 호출 (청크 처리 루프마다 또는 파일 단위)
ChunkCacheManager::ccm->ThrottleIfNeeded();
```

`ThrottleIfNeeded()`는 `ChunkCacheManager`에 추가할 메서드. 두 단계 워터마크로 동작:

```
dirty_ratio = dirty_frames / total_frames

dirty_ratio < HIGH_WATERMARK (75%):
  → no-op. 파이프라인 계속 진행.

HIGH_WATERMARK (75%) ≤ dirty_ratio < CRITICAL_WATERMARK (95%):
  → 비동기 flush 스레드 wake-up.
  → 파이프라인은 블로킹 없이 계속 진행 (데이터 로딩 멈추지 않음).

dirty_ratio ≥ CRITICAL_WATERMARK (95%):
  → Backpressure 발동. 파이프라인을 일시 정지(sleep/condvar wait).
  → flush 스레드가 dirty_ratio를 HIGH_WATERMARK 이하로 낮출 때까지 대기.
  → 대기 후 재개. OOM 방지.
```

> ⚠️ **비동기 flush만으로는 Backpressure가 없다:**
> 디스크 쓰기 속도 < 벌크로드 데이터 생성 속도인 경우, 비동기 flush가 진행 중이어도
> 캐시가 100%에 도달하여 OOM 또는 엔진 패닉이 발생할 수 있다.
> **High Watermark(비동기 시작) + Critical Watermark(강제 일시 정지)** 두 단계가 반드시 필요하다.

> ⚠️ **백그라운드 flush 스레드 장애 시 데드락 방지:**
> 메인 파이프라인이 `condition_variable::wait()`로 무한 대기 중에 flush 스레드가
> 디스크 I/O 에러나 OS 인터럽트로 멈추면 파이프라인이 영원히 깨어나지 못한다.
> **`wait_for`로 타임아웃을 두고 주기적으로 상태를 재확인**해야 한다:
> ```cpp
> // 무한 대기 대신 100ms 단위로 깨어나 dirty_ratio_ 재확인
> while (dirty_ratio_ >= CRITICAL_WATERMARK) {
>     flush_cv_.wait_for(lock, std::chrono::milliseconds(100));
> }
> ```

`ChunkCacheManager`에 추가할 멤버:
- `std::thread flush_thread_` — background flush worker
- `std::condition_variable flush_cv_`, `std::mutex flush_mu_`
- `std::atomic<bool> stop_flush_` — 종료 플래그
- `std::atomic<float> dirty_ratio_` — 주기적으로 업데이트
- `static constexpr float HIGH_WATERMARK = 0.75f`
- `static constexpr float CRITICAL_WATERMARK = 0.95f`

**Graceful Shutdown 필수 — 스레드 좀비화 방지:**
`flush_thread_`를 명시적으로 종료하지 않으면 소멸자에서 `std::terminate` 호출 → 엔진 비정상 종료.
```cpp
~ChunkCacheManager() {
    stop_flush_.store(true);
    flush_cv_.notify_all();   // wait_for에서 깨어나 루프 탈출
    if (flush_thread_.joinable()) flush_thread_.join();
}
```
`RunPostProcessing()` 또는 소멸자 중 먼저 호출되는 쪽에서 위 시퀀스를 실행한다.
중복 join을 막기 위해 `joinable()` 체크가 필수다.

`RunPostProcessing()` 직전에는 기존과 동일하게 동기 flush를 1회 수행(모든 dirty page 보장):
```cpp
ChunkCacheManager::ccm->FlushDirtySegmentsAndDeleteFromcache(false); // final sync flush
ChunkCacheManager::ccm->FlushMetaInfo(...);
```

테스트: LDBC SF1 — OOM 없이 완료 확인. DBpedia(대용량) — Critical Watermark 실제 발동 여부 로그 확인.

---

#### Step 12f — 엣지 파일 병렬 로딩

**현재**: `ReadFwdEdgeCSVFilesAndCreateEdgeExtents`에서 파일을 순차 처리.

> ⚠️ **Backward Edge Race Condition — src_label 그룹핑만으로는 불충분:**
> Forward Adj Buffer는 src vertex partition에 쓰므로 src_label 기준 그룹핑이 안전하다.
> 그러나 Backward Adj Buffer는 **dst vertex partition에 쓴다**.
> 파일 A(`Person_knows_Person`)와 파일 B(`Comment_hasCreator_Person`)는 서로 다른
> src_label을 가져 병렬 실행 후보가 되지만, 둘 다 dst=`Person` 파티션의 Backward Buffer에
> 동시에 write → **Race Condition 발생**.
>
> **해결**: Forward 패스와 Backward 패스를 명확히 분리하고, 각각 다른 축으로 그룹핑한다.
> - **Forward 패스 병렬화**: `src_label` 기준 그룹핑 (기존 설계 유지)
> - **Backward 패스 병렬화**: `dst_label` 기준 그룹핑

**수정된 설계**:

```
Forward pass (병렬):
  그룹 기준: src_vertex_label
  각 그룹 독립 adj_list_buffers (fwd) + lid_pair_to_epid_map

Backward pass (별도 순차 단계 → 이후 dst_label로 병렬화):
  그룹 기준: dst_vertex_label  ← src_label이 아님
  각 그룹 독립 adj_list_buffers (bwd)
```

```cpp
// Forward: src_label 기준 그룹핑
unordered_map<string, vector<LabeledFile>> fwd_groups;
for (auto &f : csv_edge_files)
    fwd_groups[get_src_label(f)].push_back(f);

// Backward: dst_label 기준 그룹핑
unordered_map<string, vector<LabeledFile>> bwd_groups;
for (auto &f : csv_edge_files)
    bwd_groups[get_dst_label(f)].push_back(f);

// 각각 독립 adj_list_buffers per group, catalog write는 mutex 보호
```

Forward/Backward 패스 모두 파일 내 `lid_pair_to_epid_map` 접근은 해당 파일 전용이므로 safe.
`BulkloadContext`의 catalog write (`CreateEdgeCatalogInfos`)는 mutex로 보호 필요.

> ⚠️ **12f(CSV 병렬화)와 12d(CSR 2-pass)의 라이프사이클 충돌:**
> 12f는 **Phase 1 (CSV→Extent 기록 + degree 카운트)** 를 파일 label 기준으로 병렬화한다.
> 12d의 **Phase 3 (바이너리 Extent 재스캔 → CSR fill)** 은 파일 label이 아닌
> **파티션 기준으로 병렬화**해야 한다.
> 이유: Phase 3 시점에는 CSV 파일이 이미 소비되어 존재하지 않는다.
> 데이터는 파티션/Extent 단위로 스토리지에 기록되어 있으므로, 파티션 단위로 병렬 스캔이 자연스럽다.
>
> 따라서 12d+12f를 함께 구현 시:
> - Phase 1 병렬화 축: `src_label` / `dst_label` 기준 파일 그룹 (12f)
> - Phase 3 병렬화 축: `partition_oid` 기준 (12d)

---

#### Step 12g — LID-to-PID Hash Map 교체

**현재**: `std::unordered_map<LidPair, idx_t, LidPairHash>`

LidPair 비교가 간단 (두 uint64 비교)하므로 open-addressing flat hash map 적합.

**옵션 1**: `robin_hood::unordered_flat_map` (header-only, MIT) — 외부 의존성 없이 단일 .hpp 파일.
**옵션 2**: 직접 구현 — linear probing with power-of-2 table, `uint64_t` key로 `LidPair` 인코딩.

`LidPair` = `{uint64_t first, uint64_t second}`. first만 사용하는 경우(단일 키) second=0 → `uint128_t`로 단순 인코딩 가능:
```cpp
using FlatKey = __uint128_t;
inline FlatKey to_key(LidPair p) { return (FlatKey)p.first << 64 | p.second; }
```

> ⚠️ **`__uint128_t` 기본 해시 함수 없음:**
> `std::hash<__uint128_t>`는 C++ 표준에 없으며, robin_hood 등 외부 라이브러리도 이를 제공하지
> 않는다. 컴파일 에러가 발생한다. Bit-mixing 커스텀 해시 구조체를 반드시 함께 정의해야 한다:
>
> ```cpp
> struct UInt128Hash {
>     size_t operator()(__uint128_t v) const noexcept {
>         // MurmurHash3 finalizer style mix
>         uint64_t lo = (uint64_t)v;
>         uint64_t hi = (uint64_t)(v >> 64);
>         lo ^= lo >> 33; lo *= 0xff51afd7ed558ccdULL; lo ^= lo >> 33;
>         hi ^= hi >> 33; hi *= 0xc4ceb9fe1a85ec53ULL; hi ^= hi >> 33;
>         return lo ^ (hi * 0x9e3779b97f4a7c15ULL);
>     }
> };
> // 사용:
> robin_hood::unordered_flat_map<FlatKey, idx_t, UInt128Hash> lid_to_pid;
> ```
>
> 또는 `__uint128_t` 대신 `LidPair` 구조체를 그대로 키로 쓰고 기존 `LidPairHash`를 유지하면
> 이 문제를 완전히 회피할 수 있다. 이 경우 robin_hood에 커스텀 해시 전달:
> ```cpp
> robin_hood::unordered_flat_map<LidPair, idx_t, LidPairHash> lid_to_pid;
> ```

표준 `std::unordered_map` 대비 flat hash map은 lookup throughput 2–5× 향상 (benchmark 기준).
`reserve()` 호출이 필수. 현재 코드에 `reserve(approximated_num_rows)` 이미 있으므로 효과적.
**권장**: `__uint128_t` 인코딩보다 `LidPair` + `LidPairHash` 그대로 robin_hood에 전달하는 것이 간단.

> ⚠️ **robin_hood flat map은 해시 품질에 민감하다 (Avalanche Effect 필수):**
> `robin_hood::unordered_flat_map`은 open-addressing 방식으로 해시 충돌 시
> Primary Clustering이 발생하면 `std::unordered_map`보다 수십 배 느려질 수 있다.
> 기존 `LidPairHash`가 단순 XOR(`hash = first ^ second`)로 구현되어 있다면,
> 상위 비트가 분산되지 않아 버킷 충돌이 집중된다.
>
> **적용 전 반드시 확인**: `LidPairHash`가 MurmurHash3 finalizer 또는 CityHash 수준의
> bit-mixing(Avalanche Effect)을 수행하는지 점검한다. XOR 단순 조합이라면
> 아래와 같이 교체한다:
> ```cpp
> struct LidPairHash {
>     size_t operator()(const LidPair &p) const noexcept {
>         uint64_t h = p.first * 0x9e3779b97f4a7c15ULL + p.second;
>         h ^= h >> 30; h *= 0xbf58476d1ce4e5b9ULL;
>         h ^= h >> 27; h *= 0x94d049bb133111ebULL;
>         return h ^ (h >> 31);   // splitmix64 finalizer
>     }
> };
> ```

---

#### Step 12h — 레이블 룩업 선형 검색 제거 (correctness + minor perf)

**현재** (lines 1057–1069):
```cpp
auto src_it = std::find_if(bulkload_ctx.lid_to_pid_map.begin(), ...,
    [&](const auto &e) { return e.first.find(src_vertex_label) != string::npos; });
```

`string::find` 부분 매칭은 "Person"이 "VIPPerson" 에도 매칭될 수 있어 잠재적 버그.

**변경**:
```cpp
// BulkloadContext에 index 추가:
unordered_map<string, size_t> lid_to_pid_map_index;  // label → vector index

// emplace 시 동시 등록:
bulkload_ctx.lid_to_pid_map.emplace_back(vertex_labelset, ...);
bulkload_ctx.lid_to_pid_map_index[vertex_labelset] = bulkload_ctx.lid_to_pid_map.size() - 1;

// 검색:
auto &src_map = bulkload_ctx.lid_to_pid_map[bulkload_ctx.lid_to_pid_map_index.at(src_vertex_label)].second;
```

---

### 예상 종합 효과

| Phase | 현재 | 최적화 후 |
|-------|------|----------|
| Histogram (per partition) | 2× full scan + O(N log N) sort | 1× full scan (Pass 2 유지) + O(R log R) ≈ 상수 |
| Histogram (전체) | N partitions serial | N partitions parallel (12c) |
| NDV accumulation | O(N×C) virtual calls | O(N×C) direct pointer reads |
| AdjList buffer | 2048 vector<idx_t>/extent + copy | flat CSR (2-pass: degree count → prefix-sum fill) |
| Cache flush | 3× sync stall (mid-pipeline) | watermark-triggered async flush + 1× final sync |
| Edge loading (fwd) | serial | parallel by src-label group |
| Edge loading (bwd) | serial | parallel by **dst-label** group (src-label 기준 시 race condition) |
| LID→PID lookup | std::unordered_map | flat hash map 2–5× |

가장 임팩트 큰 순서: 12c (hist parallel) > 12b (hist single-pass) > 12f (edge parallel) > 12a (NDV fix) > 12d (flat buffer) > 12e (flush batch) > 12g (hashmap) > 12h (label index).

구현 난이도 낮은 순서: 12e < 12h < 12a < 12g < 12b < 12c < 12f < 12d.

**권장 구현 순서**: 12e → 12h → 12a → 12b → 12c → 12g → 12f → 12d

---

### 구현 단계 및 상태

| 단계 | 내용 | 선행 | 상태 |
|------|------|------|------|
| **12e** | 캐시 플러시 배치 처리 — 중간 flush 제거, RunPostProcessing 직전으로 통합 | — | ✅ |
| **12h** | 레이블 룩업 인덱스 — `lid_to_pid_map_index` unordered_map 추가, find_if 제거 | — | ✅ |
| **12a** | NDV 핫패스 수정 — `GetValue()` → 타입별 raw pointer loop | — | ✅ |
| **12b** | 히스토그램 단일 패스 — reservoir sampling (R=4096)으로 quantile 계산 | 12a | ✅ |
| **12c** | 히스토그램 병렬화 — OMP parallel for across partitions | 12b | ✅ |
| **12g** | Flat hash map — LID-to-PID에 robin_hood 또는 직접 구현 도입 | — | ✅ |
| **12f** | 엣지 파일 병렬 로딩 — src partition 기준 그룹화, OMP parallel for | 12g, 12h | ✅ |
| **12d** | Flat CSR adj buffer — vector<vector<idx_t>> 제거, flat arena 교체 | 12f | ✅ |

---

### 테스트 전략

히스토그램 최적화(12a/12b/12c)는 결과가 바뀌어도 ctest가 통과할 수 있다.
Query planner가 히스토그램을 읽어 Plan이 달라지더라도 쿼리 *결과*는 여전히 맞기 때문이다.
따라서 히스토그램 결과 자체의 정합성을 검증하는 별도 테스트가 필요하다.

테스트는 3개 레벨로 구성한다:

---

#### Test Level 1 — 순수 단위 테스트 (스토리지 없음)

**대상 파일:** `test/common/test_histogram.cpp` 에 추가
**Tag:** `[common][histogram][reservoir]`

12b 구현 시 추가할 새 컴포넌트 `ReservoirSampler`를 직접 테스트한다.
`HistogramGenerator`의 private 경로를 우회하기 위해 `ReservoirSampler`는
`histogram_generator.hpp` (또는 별도 `reservoir_sampler.hpp`)에 `public` struct로 선언한다.

```
T1-1  균등분포 quantile 정확도
  - 입력: 0..9999 (N=10000) 순서대로 삽입
  - 검증: p=0.1 quantile → 약 1000 ± 500 (허용 오차 ±5% of N)
          p=0.5 quantile → 약 5000 ± 500
          p=0.9 quantile → 약 9000 ± 500

T1-2  단조 분포 (모든 값이 같음)
  - 입력: 42를 5000번 삽입
  - 검증: 어떤 p에 대해서도 quantile == 42

T1-3  작은 입력 (N < reservoir 크기 R)
  - 입력: 10개 삽입 (R=4096 >> 10)
  - 검증: sorted_sample() 크기 == 10, 정렬 상태 확인

T1-4  결과 정렬 보장
  - 임의 순서로 1000개 삽입 후 sorted_sample() 호출
  - 검증: sorted_sample()[i] <= sorted_sample()[i+1] (모든 i)

T1-5  큰 입력에서 reservoir 크기 고정
  - 1,000,000개 삽입
  - 검증: reservoir 내부 size() == R (메모리 bounded 확인)
```

---

#### Test Level 2 — 히스토그램 구조 불변식 (통합, 스토리지 필요)

**대상 파일:** `test/storage/test_histogram_generator.cpp` (신규)
**Tag:** `[storage][histogram]`

`ScopedTempDir` + 디스크 기반 `TestDB`에 알려진 데이터를 쓰고
`HistogramGenerator::CreateHistogram` 실행 후 결과를 검증한다.

테스트 픽스처는 기존 `build_vertex_schema` 헬퍼를 재활용하되,
실제 스토리지 쓰기(`ExtentManager::CreateExtent`)와 `ChunkCacheManager` 초기화가
필요하므로 `BulkloadPipeline` 없이는 실행이 어렵다.

→ **대안**: LDBC SF1 E2E 테스트 완료 후 DB를 재오픈하여 파티션 카탈로그를 읽고 검증.
  기존 `test/bulkload/test_ldbc.cpp` 의 PASSED 직후에 섹션 추가.

```
T2-1  offset_infos 크기 = 컬럼 수
  - partition_cat->GetOffsetInfos()->size() == partition_cat->GetTypes().size()

T2-2  boundary_values 단조 비감소 (컬럼 내)
  - 컬럼 i의 경계값 구간 [begin_offset, end_offset) 내에서
    boundary_values[j] <= boundary_values[j+1] (모든 j)

T2-3  frequency_values 합 == 전체 row 수 (가장 중요)
  - ps_cat->GetFrequencyValues() 합산 == ps_cat->GetNumberOfRowsApproximately()
  - 이 조건이 깨지면 Pass 2 스캔 또는 bucket fill에 버그가 있음

T2-4  NDV 범위 유효
  - ndvs->at(0) == num_total_tuples  (첫 번째 항목은 항상 총 row 수)
  - 모든 ndvs->at(i) >= 1 (빈 파티션 아니면 NDV는 최소 1)
  - 모든 ndvs->at(i) <= ndvs->at(0)

T2-5  히스토그램 있는 파티션에서 bucket count 총합 == row 수
  - T2-3과 동일 조건을 LDBC의 'knows' 엣지 파티션에 대해 명시적으로 검증
```

---

#### Test Level 3 — 알고리즘 동등성 회귀 테스트 (parity)

**대상 파일:** `test/storage/test_histogram_generator.cpp` 에 추가
**Tag:** `[storage][histogram][parity]`

12a/12b 최적화 전에 현재 알고리즘으로 **골든 레퍼런스** 출력을 생성하여 저장하고,
최적화 후 동일 입력에 대해 결과를 비교한다.

```
T3-1  NDV 정확성 — 최적화 전후 동일
  - 방법: 12a 구현 전에 합성 DataChunk (INT/BIGINT 각 1000행, 알려진 고유값 수)에
    대해 _accumulate_data_for_ndv 경로가 올바른 NDV를 반환함을 검증.
  - 합성 데이터: INT 컬럼 [1,2,3,...,100] × 10회 반복 → NDV=100, total=1000
  - 검증: ndvs[INT_col] == 100
  - 주의: _accumulate_data_for_ndv은 private이므로 테스트는
    CreateHistogram을 통해 ps_cat->GetNDVs()로 검증.

T3-2  Quantile boundary 근사 오차 허용 범위
  - 방법: 알려진 균등분포 데이터 (e.g., 1..1000, BIGINT) 로 히스토그램 생성.
  - 현재 알고리즘(전체 정렬)의 정확한 10-quantile 경계를 레퍼런스로 기록.
  - 12b 이후: 동일 데이터에서 reservoir 기반 경계 계산.
  - 허용 오차: 각 quantile 경계 ±(N * 0.05) 이내 (5th percentile tolerance).
    → N=1000이면 ±50 범위.
  - CONST 방식(bin=10) 사용 시 10개 경계값 각각 확인.

T3-3  frequency_values 동일성 (Pass 2는 변경 없음)
  - 경계값이 동일하다면(T3-2 통과) frequency_values는 반드시 동일.
  - 경계값이 근사 범위 내라면 frequency_values 차이는 경계 근처 행 수 이하.
  - 검증: sum(|new_freq[i] - ref_freq[i]|) <= boundary_error × num_bins
```

---

#### 테스트 추가 위치 및 CMake 등록

```
test/
├── common/
│   └── test_histogram.cpp         ← T1 추가 (기존 파일에 append)
├── storage/
│   └── test_histogram_generator.cpp  ← T2, T3 신규 파일
└── bulkload/
    └── test_ldbc.cpp              ← T2-3~T2-5 assertion 추가 (기존 파일)
```

`test_histogram_generator.cpp`는 `[storage]` 태그로 등록:
```cmake
# test/CMakeLists.txt 에 추가
target_sources(unittest PRIVATE storage/test_histogram_generator.cpp)
```

실행:
```bash
./test/unittest "[histogram]"      # Level 1+2+3 전체
./test/unittest "[reservoir]"      # Level 1만
./test/unittest "[histogram][parity]"  # Level 3만
ctest --output-on-failure -E "bulkload_dbpedia"  # E2E 포함 전체
```

---

#### 구현 순서와 테스트 연동

| 구현 단계 | 먼저 작성할 테스트 | 통과 조건 |
|-----------|-------------------|----------|
| 12a 전 | T3-1 골든 NDV 기록 | 기존 코드로 PASS |
| 12a 후 | T3-1 재실행 | NDV 동일 → PASS |
| 12b 전 | T1-1~T1-5 작성 | ReservoirSampler 신규 구현 검증 |
| 12b 후 | T3-2, T3-3 실행 | boundary 근사 오차 허용, freq 동일 |
| 12c 후 | T2-1~T2-5 전체 실행 | 구조 불변식 모두 통과 |
| 최종 | LDBC + TPC-H E2E | 쿼리 결과 동일 |

---

## Notes

- Keep this file updated at milestone completion.
- One milestone at a time. Validate with `ctest` before closing a milestone.
- Build always in `turbograph-s62` container: `cd /turbograph-v3/build-lwtest && ninja`
- **The DBpedia test (`bulkload_dbpedia`) is excluded from CI and routine development runs
  due to its very long runtime (several hours). To run it explicitly:**
  `ctest --output-on-failure -R "bulkload_dbpedia"`
  Use the staged subset tags (`[dbpedia][vertex]`, `[dbpedia][edge-small]`, `[dbpedia][edge-medium]`,
  `[dbpedia][edge-large]`) in `bulkload_test` to diagnose specific stages without running the full load.
