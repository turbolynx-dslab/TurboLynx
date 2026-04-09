# TurboLynx — Execution Plan

## Current Status (2026-04-09)

**LDBC: 464/464 pass. TPC-H: 22/22 pass. IC1~IC14 Neo4j verified.**
**NL2Cypher: S0–S3 pipeline complete (zero-shot, schema linker, multi-candidate).**
**Intra-pipeline parallel execution: complete for all TPC-H/LDBC operators.**
**Parallel speedup (TPC-H sf1 16t): TOTAL 1.37x. Q1 5.70x, Q6 4.17x,
Q14 3.51x, Q21 2.75x, Q12 2.55x.**

---

## Test Commands

```bash
# Build (debug/test)
cd /turbograph-v3/build-lwtest && ninja

# Build (release — use for bulkload)
cd /turbograph-v3/build-release && ninja

# LDBC query tests (464 tests)
cd /turbograph-v3/build-lwtest
./test/query/query_test --db-path /data/ldbc/sf1 "~[tpch]"

# TPC-H query tests (22 tests)
./test/query/query_test --db-path /data/tpch/sf1 "[tpch]"

# Unit tests
./test/unittest "[catalog]" && ./test/unittest "[storage]" && ./test/unittest "[common]"

# Restore LDBC data (if corrupted by mutation tests)
rm -rf /data/ldbc/sf1 && bash /turbograph-v3/scripts/load-ldbc.sh

# Load TPC-H data
rm -rf /data/tpch/sf1 && bash /turbograph-v3/scripts/load-tpch.sh /turbograph-v3/build-release
```

---

## What Works

### Query Features
| Feature | Status | Notes |
|---------|--------|-------|
| IC1~IC14 (LDBC Interactive Complex) | ✅ | Neo4j verified |
| Comparison `=, <>, <, >, <=, >=` | ✅ | |
| AND, OR, NOT, XOR | ✅ | |
| STARTS WITH, ENDS WITH, CONTAINS | ✅ | |
| `=~` regex operator | ✅ | Rewrites to `regexp_full_match()` |
| CASE (simple + generic) | ✅ | |
| Math `+, -, *, /, %, ^` | ✅ | |
| String `+` concatenation | ✅ | Rewrites to `\|\|` (ConcatFun) |
| IS NULL / IS NOT NULL | ✅ | |
| List comprehension, Pattern comprehension | ✅ | |
| reduce(), coalesce() | ✅ | |
| Map literal `{key: value}` | ✅ | struct_pack |
| List slicing `[begin:end]` | ✅ | DuckDB 1-based inclusive |
| EXISTS { MATCH ... WHERE ... } | ✅ | Single outer-bound node |
| NOT EXISTS { ... } | ✅ | Single outer-bound node |
| labels(n), type(r), keys(n/r) | ✅ | Bind-time constant resolution |
| properties(n/r) | ✅ | struct_pack rewrite |
| date(), datetime() | ✅ | Literal → DATE, non-literal passthrough |
| toUpper(), toLower() | ✅ | Maps to DuckDB upper/lower |
| toInteger(), toFloat() | ✅ | Cast functions |
| count(*) | ✅ | count_star in AGG_FUNCS |
| Temporal property access (n.date.year) | ✅ | date_part rewrite for chained properties |

### CRUD Operations
| Operation | Status |
|-----------|--------|
| CREATE Node / Edge | ✅ |
| SET property | ✅ (existing properties only) |
| DELETE / DETACH DELETE | ✅ (edge constraint enforced) |
| REMOVE property | ✅ (rewrite to SET NULL) |
| MERGE (upsert) | ✅ |
| UNWIND + CREATE (batch) | ✅ (two-phase decomposition) |
| WAL persistence | ✅ (replay on connect) |
| Auto compaction | ✅ (threshold-based, configurable) |
| Crash-safe checkpoint | ✅ (WAL CHECKPOINT_BEGIN/END markers) |

### TPC-H SF1 Results

| Query | Status | Notes |
|-------|--------|-------|
| Q1 | ✅ | Aggregation with date filter |
| Q2 | ✅ | Multi-hop join + subquery |
| Q3 | ✅ | 3-hop join + aggregation |
| Q4 | ✅ | EXISTS subquery |
| Q5 | ✅ | Multi-hop join + aggregation |
| Q6 | ✅ | Simple scan + aggregation |
| Q7 | ✅ | date_part + GROUP BY computed key |
| Q8 | ✅ | Two-stage aggregation (WITH → RETURN) |
| Q9 | ✅ | Multi-hop + date_part |
| Q10 | ✅ | 4-hop join + aggregation |
| Q11 | ✅ | PARTSUPP edge properties |
| Q12 | ✅ | toUpper + date filter |
| Q13 | ✅ | OPTIONAL MATCH + CASE WHEN + count (rewritten for LEFT JOIN semantics) |
| Q14 | ✅ | CASE inside SUM (func-over-agg) |
| Q15 | ✅ | Two-stage aggregation |
| Q16 | ✅ | list_contains Orrify fix |
| Q17 | ✅ | PlanGroupBy pruning + AGG_FUNCTION TryGenScalarIdent skip + pass-through ScalarAgg |
| Q18 | ✅ | Nested aggregation with LIMIT |
| Q19 | ✅ | Same as Q16 (list_contains Orrify fix) |
| Q20 | ✅ | PlanGroupBy _id pruning for edge variables |
| Q21 | ✅ | HashJoin condition sort + IdSeek filter BoundRef index remap |
| Q22 | ✅ | CScalarSubqueryNotExists + RHS outer-bound correlation + substring 0→1 based |

---

## Parallel Execution Status

Intra-pipeline parallelism: each pipeline can be executed by N PipelineTasks
running concurrently, dispatched per-extent from the source's GlobalSourceState
and combined into a shared GlobalSinkState before Finalize. Thread count is
controlled by `PRAGMA threads = N` (default = `hardware_concurrency`).

### Parallel-safe operators

| Operator role | Status | Notes |
|---|---|---|
| **NodeScan** (source) | ✅ | Per-extent dispatch, multi-oid support, in-memory delta extents handled via `NodeScanGlobalState::TryClaimDeltaPhase()` (one thread claims) |
| NodeScan **with filter pushdown** | ✅ | Enabled. Required four fixes: output mapping + FP_COMPLEX init (`a151b565a`), `ChunkCacheManager::PinSegment` race serialization, parallel `MergeUserIdPropertyUpdates` overlay, `PipelineTask` using operator's `InitializeOutputChunks` for empty-type chunks |
| HashAggregate (sink + Combine) | ✅ | |
| HashJoin (sink) | ✅ | Build side parallel; per-thread local HT → Combine into global |
| HashJoin (probe / mid-pipe) | ✅ | `deps` map allowed when every dep op is `HASH_JOIN`; `HASH_JOIN` also in the per-operator allowlist. Per-thread state on `PhysicalHashJoinState`; finalized HT read-only at probe time. Other dep types still gated |
| Sort (sink) | ✅ | |
| TopNSort (sink) | ✅ | |
| ProduceResults (sink) | ✅ | |
| BlockwiseNLJoin (sink) | ✅ | |
| CrossProduct (sink) | ✅ | |
| Filter / Projection / Unwind / Top | ✅ | Stateless (per-thread checkpoint) |
| **IdSeek** (mid-pipe) | ✅ | Filter-pushdown scratch (`tmp_chunks`, `executors`, `is_tmp_chunk_initialized_per_schema`) moved to per-thread `IdSeekState` |
| **AdjIdxJoin** (mid-pipe) | ✅ | All state per-thread in `AdjIdxJoinState` |
| **VarlenAdjIdxJoin** (VLE, mid-pipe) | ✅ | All state per-thread in `VarlenAdjIdxJoinState` |
| **HashAggregate (source)** | ✅ | Final-projection / post-agg pipelines run in parallel. Atomic per-partition claim cursor in `HashAggregateParallelGlobalSourceState`; per-thread scratch in `HashAggregateParallelLocalSourceState`; reads finalized HTs through new `RadixPartitionedHashTable::ScanFinalizedPartition` accessor without exposing `RadixHTGlobalState` |
| Multi-group / multi-source pipelines | ✅ | `ExecutePipelineParallel` loops over `pipeline->AdvanceGroup()` to scan e.g. `Place = City + Country` variants |
| PipelineTask HMO drain | ✅ | Stack-based, mirrors sequential `ExecutePipe` (commit `01b610d74`) |
| `graph_storage_wrapper` mutable state | ✅ | Refactored into caller-owned `IndexSeekScratch` |
| CrossProduct (mid-pipe dep) | ✅ | rhs_materialized read-only after TransferGlobalToLocal; per-thread position in CrossProductOperatorState |
| BlockwiseNLJoin (mid-pipe dep) | ✅ | right_chunks read-only; per-thread position/executor in BlockwiseNLJoinState; rhs_found_match null (no RIGHT OUTER) |
| PiecewiseMergeJoin (sink) | ❌ | Not parallel — complex two-side merge, low ROI |

### Gates remaining in `CanParallelize()`

The following pipeline shapes are **silently sequential** today:

| Gate | Means | Why it matters | Path to unblock |
|---|---|---|---|
| `childs.size() > 1` | Pipelines with multiple child feeds (rare) | None observed in TPC-H/LDBC | Multi-bridge wiring in PipelineTask |
| Source `!ParallelSource()` with childs | Pipelines whose **source** is `Sort`/`TopNSort` output | Post-sort projection pipelines | Sort-as-source is intentionally **not** parallelized — `PayloadScanner` is order-sensitive by design and parallel scan would break ORDER BY |
| Operator type not in allowlist | Any unknown mid-pipe op | Future operators | Audit + add case |

### Open follow-ups (priority order)

> **Where we are now (post-`ecb2618c7`):** TPC-H 22/22 correctness, bench TOTAL
> 1.37x at 16t. All TPC-H/LDBC operators parallelized. Remaining slow queries
> are extent-bound (single storage extent → 1 thread). Reproduce:
> ```bash
> cd /turbograph-v3/build-lwtest
> ./test/query/query_test --db-path /data/tpch/sf1 "[bench][tpch]"
> ```

#### 1. Slow queries with extent-bound parallelism  *(MEDIUM)*

After dep-gate and partition-cap fixes, remaining slow queries are bound
by **max_extents=1** on their bottleneck pipelines (single storage extent
for that table means only 1 thread regardless of parallelism support):

| Q | 1t (ms) | 16t (ms) | 16t/1t | Bottleneck |
|---|---------|----------|--------|------------|
| **Q13** | 18966 | 18375 | 1.03x | ORDERS NodeScan, 1 extent → 18124ms single-thread |
| **Q15** |  6888 |  6054 | 1.14x | LINEITEM-related scan, 1 extent → 6220ms |
| **Q9**  |  5292 |  4818 | 1.10x | Multiple 1-2 extent scans |

**Root cause:** source-level parallelism is extent-granular. Tables with
fewer rows than one extent (or loaded as a single extent) cannot be split.
**Fix path:** intra-extent row-range partitioning in NodeScan — significant
change, deferred for now.

#### 2. Small-query regression from parallel overhead  *(LOW — easy)*

- Q11: 552 → 551 ms at 16t (1.00x — improved from 0.79x via partition cap)
- Q2:  1231 → 1217 ms at 16t (1.01x — improved from 0.92x)

Partition cap largely fixed the small-query overhead. Remaining cost is
GlobalSinkState/Combine overhead; consider a cardinality threshold in
`CanParallelize` / `ExecutePipeline` if it resurfaces.

#### 3. Static gates that remain  *(documented, not active work)*

| Gate | Means | Status |
|---|---|---|
| Source is `Sort` / `TopNSort` | `PayloadScanner` is order-sensitive | **NOT PLANNED** — would break `ORDER BY` |
| `childs.size() > 1` | multi-child feed pipelines | Not observed in TPC-H/LDBC |
| `PiecewiseMergeJoin` sink | two-side merge complexity | Low ROI; revisit only if a target query needs it |

#### 4. Inter-pipeline parallelism  *(LOW priority)*

We do intra-pipeline only. Independent pipelines could run concurrently.
Most TPC-H/LDBC queries have linear pipeline DAGs so the upside is bounded.

### Parallel work — commit log

| Commit | What |
|---|---|
| `01b610d74` | `PipelineTask::ProcessChunk` HMO drain — stack-based, mirrors sequential `ExecutePipe` |
| `f23e0398b` | Enable AdjIdxJoin in CanParallelize + multi-group / delta gating (gates later removed) |
| `fa5d34051` | `ExecutePipelineParallel` multi-group loop — runs each child variant in turn into shared sink |
| `cae3c95c4` | Enable VarlenAdjIdxJoin in CanParallelize |
| `462ec71b0` | `PhysicalIdSeek` filter-pushdown parallel-safe refactor — `tmp_chunks` / `executors` / init-flags moved to `IdSeekState` |
| `cf1cc6075` | Parallel NodeScan reads in-memory delta extents via `TryClaimDeltaPhase()` |
| `56f5fc62a` | Allow `HASH_JOIN`-deps probe pipelines in CanParallelize |
| `9abaf3674` | Doc-only — initial filter-pushdown parallel symptoms |
| `a151b565a` | Filter-pushdown parallel: `output_column_idxs` mapping fix + FP_COMPLEX init OOB fix (still gated by `ParallelSource()`) |
| `4387f0701` | Filter-pushdown parallel NodeScan **enabled**: `ChunkCacheManager::PinSegment` race serialized; parallel `NodeScan::GetData` calls `MergeUserIdPropertyUpdates` so post-SET reads see new values; `PipelineTask` now uses `op->InitializeOutputChunks` so empty-type IdSeek chunks are tolerated. LDBC 464/464, TPC-H 23/23 |
| `bba077d0d` | **HashAggregate-as-source parallel enabled**: new `GetData(GlobalSource, LocalSource, LocalSinkState&)` virtual on `CypherPhysicalOperator` for parallel non-leaf sources; `HashAggregateParallelGlobalSourceState` (atomic per-partition `next_ht_idx` + `empty_emitted` per radix table); `HashAggregateParallelLocalSourceState` (per-thread scratch chunks); `RadixPartitionedHashTable::ScanFinalizedPartition` accessor avoids exposing private sink state; `PipelineTask` takes optional `child_sink_state` and uses `GetLocalSourceStateParallel` virtual when bridging; `CanParallelize()` `!childs.empty()` gate replaced with `childs.size() > 1`. LDBC 464/464, TPC-H 23/23 |
| `443effb05` | **HASH_JOIN added to `CanParallelize()` per-operator allowlist**: closes the gap left by `56f5fc62a` (which only fixed the `deps` map check). Now that filter-pushdown NodeScan is parallel (`4387f0701`), HashJoin probe pipelines actually go parallel. Per-thread state lives on `PhysicalHashJoinState` (`join_keys`, `probe_executor`, `scan_structure`); finalized HT is read-only at probe time via the bridged dep sink state. The `mutable num_loops` profiling counter is a benign race (same pattern as `AdjIdxJoin` / `HashAggregate`, both already allowlisted). LDBC 464/464, TPC-H 23/23 |
| `836bdc687` | **diskaio core_id slot recycling**: `Turbo_bin_aio_handler` allocates a per-thread `my_core_id_` from a monotonic `core_counts_` capped at `MAX_NUM_PER_THREAD_DATASTRUCTURE = 256`. `ExecutePipelineParallel` spawns fresh `std::thread`s per call, so after ~256 thread starts the counter overflows and `WaitForResponses` SEGVs. Fixed with a `thread_local CoreIdReleaser` whose dtor returns the slot to a `free_core_ids_` vector; new threads pop from the free list before incrementing. Stress: Q21@16t × 30 iters and Q12@64t × 30 iters both clean. |
| `ecb2618c7` | **CrossProduct + BlockwiseNLJoin dep allowlist + partition cap**: `CanParallelize()` now allows CROSS_PRODUCT and BLOCKWISE_NL_JOIN as dep operators (probe-side read-only after finalization). Mid-pipe operator allowlist also extended. `ResolveNPartitions` capped at 16 to reduce 64t partition overhead. Q22 64t regression resolved (0.64x → 1.02x); Q1 16t 5.04x → 5.70x; Q6 3.58x → 4.17x. LDBC 464/464, TPC-H 22/22. |
| `0078fbf68` | **Parallel HashAggregate serialization fix (Q1 16t 0.47x → 5.04x)**: `RadixHTGlobalState` was sized via `TaskScheduler::NumberOfThreads()`, which returns 1 in turbolynx (the DuckDB background pool is empty — intra-pipeline parallelism uses per-pipeline `std::thread`s). With `n_partitions = 1`, `ForceSingleHT()` was true and every parallel HashAgg worker took `gstate.lock` on every `Sink`/`Combine` call. Now resolved from `ClientConfig::maximum_threads` with `hardware_concurrency()` fallback. Enabling `n_partitions > 1` exposed an unimplemented finalize-event scheduling path (`physical_hash_aggregate.cpp:311` assertion); avoided by forcing `do_partition = false` in `Sink` and skipping `Partition()` in `Combine`, so `intermediate_hts` stay unpartitioned and `Finalize` takes the existing simple-merge path while threads still build per-thread local HTs in parallel. Bonus: `ChunkCacheManager::PinSegment` cache-hit fast path bypasses `pin_mu_` (BufferPool::Get is internally synchronized), with double-checked locking on the miss path. TPC-H 22/22, bench: Q1 5.04x · Q6 3.58x · Q12 2.81x · Q14 3.41x · Q21 2.58x at 16t. |

---

## Architecture Reference

### Query Pipeline
```
Cypher text → ANTLR Parser → CypherTransformer → AST
    → Binder (BindContext, BoundExpressions)
    → Converter (Cypher2OrcaConverter → ORCA CExpression tree)
    → ORCA Optimizer (cost-based optimization)
    → Physical Planner (planner_physical.cpp → DuckDB operators)
    → Execution Engine (DuckDB-based)
```

### Key Source Files

| Component | File | Purpose |
|-----------|------|---------|
| Parser | `src/parser/cypher_transformer.cpp` | ANTLR → AST |
| Binder | `src/binder/binder.cpp` | Name resolution, type inference, function rewriting |
| Converter | `src/converter/cypher2orca_converter.cpp` | AST → ORCA logical plan |
| Converter (scalar) | `src/converter/cypher2orca_scalar.cpp` | Expression conversion |
| ORCA metadata | `src/optimizer/orca/gpopt/translate/CTranslatorTBGPPToDXL.cpp` | Type casts, comparisons |
| Physical planner | `src/planner/planner_physical.cpp` | ORCA plan → DuckDB operators |
| Physical planner (scalar) | `src/planner/planner_physical_scalar.cpp` | Scalar expression translation |
| Schema | `src/include/planner/logical_schema.hpp` | Column tracking through plan stages |
| IdSeek | `src/execution/.../physical_id_seek.cpp` | Vertex lookup by physical ID |
| Catalog wrapper | `src/include/catalog/catalog_wrapper.hpp` | ORCA ↔ DuckDB catalog bridge |
| CSV parser | `src/include/common/graph_simdcsv_parser.hpp` | Bulkload CSV parsing |
| Bulkload | `src/loader/bulkload_pipeline.cpp` | Vertex/edge loading pipeline |

### Converter Design Decisions

**PlanProjectionBody** (`cypher2orca_converter.cpp:~1417`):
- Detects aggregation in RETURN/WITH projections
- Splits expressions into GROUP BY keys vs aggregates
- **Pre-projects computed GROUP BY keys** (e.g., `date_part('year', col)`) via CLogicalProject before PlanGroupBy — ORCA's GbAgg requires key columns to be simple CScalarIdent
- **Pre-projects complex aggregate inputs** (e.g., `SUM(a * (1-b))`) — DuckDB hash aggregate requires aggregate children to be BOUND_REF
- **Recursively extracts nested aggregates** from expressions like `100 * SUM(...) / SUM(...)` — splits into aggregation step + post-aggregation projection
- **Inherits alias_map** in PlanGroupBy to preserve WITH aliases through aggregation stages

**PlanGroupBy** (`cypher2orca_converter.cpp:~2080`):
- `const` → non-const parameter to allow pre-projection replacement of aggregate children
- `new_schema.inheritAliases(*prev_plan->getSchema())` preserves WITH aliases
- Registers aliases for all output columns (agg results, GROUP BY keys, general expressions)

### EXISTS Subquery Implementation

**Single outer-bound node** pattern:
```
EXISTS { MATCH (n)-[:EDGE]->(m) WHERE pred }
```
- Inner plan: edge scan + target scan (outer node NOT re-scanned)
- Correlation: `outer.n._id = inner.edge._sid` via CLogicalSelect
- ORCA decorrelates to LeftSemiHashJoin (EXISTS) or LeftAntiSemiHashJoin (NOT EXISTS)
- UBIGINT↔ID binary-coercible cast enables decorrelation (edge _sid is UBIGINT, node _id is ID)

**NOT EXISTS**: `ConvertBoolOp` detects `NOT + EXISTENTIAL` pattern and generates `CScalarSubqueryNotExists` directly (instead of `NOT(CScalarSubqueryExists)`), enabling ORCA to decorrelate to `LeftAntiSemiHashJoin`.

**RHS outer-bound**: `PlanRegularMatch` now checks `rhs_is_subquery_outer` in addition to `lhs_is_subquery_outer`. When the target node of an edge is outer-bound (e.g., `(o:ORDERS)-[:MADE_BY]->(c2)` where `c2` is outer), the RHS scan is skipped and correlation key is recorded.

**Outer property references**: `ConvertExistsSubquery` registers `outer_plan_` before converting inner predicates, so `TryGenScalarIdent` can resolve outer property references (e.g., `s.S_SUPPKEY` in inner WHERE).

**Limitation**: Q21 pattern with inequality correlation (`<>`) in join predicate not yet supported in HashJoin execution. See Q21 analysis above.

### PlanGroupBy Variable Key Pruning

When a node/edge variable appears as GROUP BY key (e.g., `WITH lineitem, avg(...)`), only `_id` + downstream-referenced properties are included as GROUP BY keys, instead of all properties. This prevents:
- Bloated GROUP BY key sets (17 cols → 3 cols for LINEITEM)
- Internal types leaking to output schema (edge _sid/_tid)
- `_id` is included only when downstream actually references it (via MATCH re-binding, property access, or variable identity comparison)

**Key files**: `CollectDownstreamPropertyRefs` and `CollectPropertyRefsFromExpr` in `cypher2orca_converter.cpp`

### Regex → LIKE Optimization

Parser rewrites simple `=~` regex patterns to LIKE for ~40% performance improvement:
- `.*X.*Y.*` → `%X%Y%` (LIKE)
- Complex regex (character classes, alternation, etc.) falls through to `regexp_full_match`
- Implemented in `TryRegexToLike()` in `cypher_transformer.cpp`

### Cypher substring 0-based → DuckDB 1-based

Parser converts `substring(str, start, len)` start index from Cypher 0-based to DuckDB 1-based by adding 1 to the start argument. Handles both constant and non-constant start expressions.

### Bulkload

- **LDBC**: `scripts/load-ldbc.sh` — uses `turbolynx import`
- **TPC-H**: `scripts/load-tpch.sh` — same format
- **Multi-key vertex** (LINEITEM): composite key `(L_ORDERKEY, L_LINENUMBER)` → combined single key via `key1 * multiplier + key2` registered alongside original pair in `PopulateLidToPidMap`
- **DOUBLE/FLOAT types**: added to `graph_simdcsv_parser.hpp` type map
- **ConcatFun**: was commented out in `string_functions.cpp` → re-enabled for `||` operator support

### Data Restoration
```bash
# LDBC
rm -rf /data/ldbc/sf1 && bash /turbograph-v3/scripts/load-ldbc.sh

# TPC-H
rm -rf /data/tpch/sf1 && bash /turbograph-v3/scripts/load-tpch.sh /turbograph-v3/build-release
```

⚠️ **CRUD tests (Q7) modify LDBC data**. If Person count ≠ 9892, reload. Use release build for bulkload (debug is 12x slower).
