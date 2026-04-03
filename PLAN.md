# TurboLynx — Execution Plan

## Current Status (2026-04-03)

**LDBC: 464/464 pass. TPC-H: 17/22 pass. IC1~IC14 Neo4j verified.**

Full CRUD + MERGE + DETACH DELETE + WAL + Auto Compaction + Crash-safe WAL checkpoint.
UNWIND+CREATE batch mutation. List slicing `[begin:end]`. EXISTS / NOT EXISTS subquery.
Meta functions (labels, type, keys, properties). String `+` concat. `=~` regex. date().
TPC-H SF1 bulkload + query tests (DOUBLE/FLOAT types, combined multi-key support).

---

## What Works

### Query Features
| Feature | Status |
|---------|--------|
| IC1~IC14 (LDBC Interactive Complex) | ✅ All pass, Neo4j verified |
| Comparison `=, <>, <, >, <=, >=` | ✅ |
| AND, OR, NOT, XOR | ✅ |
| STARTS WITH, ENDS WITH, CONTAINS | ✅ |
| CASE (simple + generic) | ✅ |
| Math `+, -, *, /, %, ^` | ✅ |
| IS NULL / IS NOT NULL | ✅ |
| List comprehension, Pattern comprehension | ✅ |
| reduce(), coalesce() | ✅ |
| Map literal `{key: value}` | ✅ |
| List slicing `[begin:end]` | ✅ (DuckDB 1-based inclusive) |
| EXISTS { MATCH ... WHERE ... } | ✅ |
| NOT EXISTS { ... } | ✅ |
| `\|\|` string concat | N/A (Neo4j uses `+`, not `\|\|`) |

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

### Unsupported Mutations (guarded with clear error messages)
| Operation | Error Message |
|-----------|--------------|
| `SET n:Label` (multi-label) | "Unsupported: adding/changing labels is not yet supported" |
| `SET n.newProp = 'val'` (schema evolution) | "schema evolution not yet supported" |
| `REMOVE n.nonExistent` | Silent no-op (Neo4j semantics) |

### Storage Bug Fixes (2026-03-31)
- **ExtentIterator pin leak**: destructor now unpins all pinned I/O buffers. MALLOC_PERTURB_ sweep (0/42/85/127/170/200/255) all pass.
- **DiskAIO WaitKernel**: `ret` check before `events_[0].res2` access (uninitialized memory read).
- **FinalizeIO null guard**: `file_handlers.find()` before access.
- **RetrieveCast**: binary-coercible cast for ID/BIGINT/INTEGER types.

---

## Completed: EXISTS Subquery

### Architecture (5-layer pipeline)

```
Layer         | Status | Notes
--------------+--------+------
ANTLR grammar | ✅     | EXISTS { MATCH pattern [WHERE expr] } already defined
Parser        | ✅     | ExistsSubqueryExpression — extracts pattern + WHERE
Binder        | ✅     | BoundExistsSubqueryExpression with inner BoundMatchClause
Converter     | ✅     | Inner plan via PlanRegularMatch + CScalarSubqueryExists
ORCA          | ✅     | CSubqueryHandler decorrelation (EXISTS → semi-join, NOT EXISTS → anti-semi-join)
Phys. planner | ✅     | CScalarCast handling + IdSeek fallback for no-ComputeScalar plans
```

### Test Results

| Test | Query | Status |
|------|-------|--------|
| Q7-160 | `MATCH (n) WHERE EXISTS { MATCH (n)-[:KNOWS]->(:Person) } RETURN DISTINCT n.id` | ✅ |
| Q7-161 | `MATCH (n) WHERE NOT EXISTS { MATCH (n)-[:KNOWS]->(:Person) } RETURN n.firstName` | ✅ |
| Q7-162 | `MATCH (n) WHERE EXISTS { MATCH (n)-[:KNOWS]->(m) WHERE m.firstName = 'X' } RETURN n.id` | ✅ |
| Q7-163 | `MATCH (n) WHERE EXISTS { MATCH (n)-[:KNOWS]->(:Person) } RETURN count(n)` | ✅ |

### Key Implementation Details

**Inner plan construction** (`ConvertExistsSubquery` in `cypher2orca_scalar.cpp`):
- Outer-bound nodes (e.g., `n`) are NOT re-scanned in inner plan
- `PlanRegularMatch` with `subquery_outer_nodes` param skips the outer node, only scans edge + target
- Correlation predicate: `outer.n._id = inner.edge._sid` added via `CLogicalSelect`
- `SubqueryCorrelation` struct records which edge key (SID/TID) to use

**RetrieveCast** (`CTranslatorTBGPPToDXL.cpp`):
- Binary-coercible cast registered for ID(108) ↔ BIGINT(14) ↔ INTEGER(13) ↔ UBIGINT(31)
- UBIGINT needed for EXISTS/NOT EXISTS decorrelation (edge `_sid`/`_tid` are UBIGINT, node `_id` is ID)

**Physical planner fixes for UBIGINT cast**:

1. **CScalarCast in join predicates** (`planner_physical.cpp`):
   - `pTranslatePredicateToJoinCondition` used to assert both sides are `CScalarIdent`. Now unwraps `CScalarCast` to find the underlying ident for lhs/rhs side determination.

2. **CScalarCast in scalar expressions** (`planner_physical_scalar.cpp`):
   - Added `pTransformScalarCast` to `pTransformScalarExpr` switch. Binary-coercible casts return the child directly; non-binary casts wrap in `BoundCastExpression`.

3. **IdSeek column mapping fallback** (`planner_physical.cpp`):
   - The IdSeek handler (`pTransformEopPhysicalInnerIndexNLJoinToIdSeekNormal`) assumed the inner plan always contains `CPhysicalComputeScalarColumnar`, which is where `inner_col_maps`, `outer_col_map`, and `scan_types` were constructed. The converter always wraps scans in `CLogicalProjectColumnar` → `CPhysicalComputeScalarColumnar`, but ORCA may eliminate this projection during optimization (e.g., EXISTS decorrelation produces `Filter → IndexScan` without an intermediate projection). Added a fallback after the inner-plan traversal loop that constructs these mappings from `inner_cols` directly when they remain empty.

### Files Modified for EXISTS

```
Parser:
  src/parser/cypher_transformer.cpp                    — transformExistentialSubquery
  src/include/parser/expression/exists_subquery_expression.hpp  — NEW

Binder:
  src/binder/binder.cpp                                — BindExistsSubquery
  src/include/binder/binder.hpp                        — BindExistsSubquery decl + exist_counter_
  src/include/binder/expression/bound_exists_subquery_expression.hpp — NEW

Converter:
  src/converter/cypher2orca_scalar.cpp                 — ConvertExistsSubquery
  src/converter/cypher2orca_converter.cpp              — PlanRegularMatch subquery_outer_nodes
  src/include/converter/cypher2orca_converter.hpp      — SubqueryCorrelation, PlanRegularMatch sig

ORCA:
  src/optimizer/orca/gpopt/translate/CTranslatorTBGPPToDXL.cpp — RetrieveCast

Physical planner:
  src/planner/planner_physical.cpp                     — correlated NL join error msg
  src/execution/.../physical_id_seek.cpp               — empty output schema dummy column

Tests:
  test/query/test_q7_crud.cpp                          — Q7-160..Q7-163
```

---

## Completed Work Reference

### CRUD Design (DeltaStore architecture)

- **CREATE**: InsertBuffer (in-memory extent, per partition)
- **UPDATE**: SetPropertyByUserId (user_id → property map)
- **DELETE**: DeleteMask (per extent) + DeleteByUserId
- **Edge mutation**: AdjListDelta (InsertEdge/DeleteEdge per partition)
- **Compaction**: InsertBuffer → CreateExtent → disk flush → SaveCatalog → WAL truncate
- **WAL**: binary append-only log (INSERT_NODE, UPDATE_PROP, DELETE_NODE, INSERT_EDGE, CHECKPOINT_BEGIN/END)

### Data Restoration (if compaction tests corrupt data)
```bash
rm -f /data/ldbc/sf1/catalog.bin /data/ldbc/sf1/.store_meta /data/ldbc/sf1/catalog_version /data/ldbc/sf1/store.db /data/ldbc/sf1/delta.wal
bash /turbograph-v3/scripts/load-ldbc.sh
```

### IdSeek Refactoring (2026-04-01)
- Renamed: `doSeekSchemaless` → `doSeekRowMajor`, `doSeekUnionAll` → `doSeekColumnar`
- Removed 9 dead methods + 1 dead static variable (354 lines deleted)
