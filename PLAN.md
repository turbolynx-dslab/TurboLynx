# TurboLynx — Execution Plan

## Current Status (2026-04-07)

**LDBC: 464/464 pass. TPC-H: 22/22 pass. IC1~IC14 Neo4j verified.**
**Intra-pipeline parallel execution: enabled for most query shapes.**

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

# TPC-H query tests (22 tests, 17 pass)
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
| PiecewiseMergeJoin (sink) | ❌ | Not parallel — complex two-side merge, low ROI |

### Gates remaining in `CanParallelize()`

The following pipeline shapes are **silently sequential** today:

| Gate | Means | Why it matters | Path to unblock |
|---|---|---|---|
| `childs.size() > 1` | Pipelines with multiple child feeds (rare) | None observed in TPC-H/LDBC | Multi-bridge wiring in PipelineTask |
| Source `!ParallelSource()` with childs | Pipelines whose **source** is `Sort` output (HashJoin is not a source in this codebase; HashAgg is now parallel) | Post-sort projection pipelines | Sort-as-source is intentionally **not** parallelized — `PayloadScanner` is order-sensitive by design and parallel scan would break ORDER BY |
| `dep.type != HASH_JOIN` | Pipelines where a mid-op references e.g. a CrossProduct/BlockwiseNLJoin sink | Rare in TPC-H/LDBC but blocks any future query that puts those sinks mid-pipeline | Verify those operators' Execute is thread-safe given a finalized shared sink state, then add to the allowlist |
| Operator type not in allowlist | Any unknown mid-pipe op | Future operators | Audit + add case |

### Open follow-ups (priority order)

#### 1. Parallel sources from Sort  *(NOT PLANNED)*

`PhysicalSort` is the only remaining non-leaf source after HashAggregate. It is
**not** a parallelization candidate: `PayloadScanner` reads sorted blocks
sequentially by design, and a parallel claim would break ORDER BY semantics.
`PhysicalHashJoin` is not a source in this codebase (no `IsSource()` override),
so there is no post-join source pipeline to parallelize.

#### 2. Other dep-op types  *(LOW impact)*

Audit `BlockwiseNLJoin` / `CrossProduct` / etc. as mid-pipe operators with
`deps`, verify their Execute paths are read-only against a finalized shared
sink state, and add to the `CanParallelize()` allowlist.

#### 3. Pipeline-level parallelism  *(LOW priority)*

We do intra-pipeline only. Independent pipelines could run concurrently. Most
TPC-H/LDBC queries have linear pipeline DAGs so the win is bounded.

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
| (this commit) | **HASH_JOIN added to `CanParallelize()` per-operator allowlist**: closes the gap left by `56f5fc62a` (which only fixed the `deps` map check). Now that filter-pushdown NodeScan is parallel (`4387f0701`), HashJoin probe pipelines actually go parallel. Per-thread state lives on `PhysicalHashJoinState` (`join_keys`, `probe_executor`, `scan_structure`); finalized HT is read-only at probe time via the bridged dep sink state. The `mutable num_loops` profiling counter is a benign race (same pattern as `AdjIdxJoin` / `HashAggregate`, both already allowlisted). LDBC 464/464, TPC-H 23/23 |

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
