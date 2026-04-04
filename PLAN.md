# TurboLynx — Execution Plan

## Current Status (2026-04-04)

**LDBC: 464/464 pass. TPC-H: 17/22 pass. IC1~IC14 Neo4j verified.**

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
| Q13 | ✅ | OPTIONAL MATCH + count |
| Q14 | ✅ | CASE inside SUM (func-over-agg) |
| Q15 | ✅ | Two-stage aggregation |
| Q16 | ❌ | `count(distinct)` + list_value → vector type assertion |
| Q17 | ❌ | Multi-part correlated subquery → empty types |
| Q18 | ✅ | Nested aggregation with LIMIT |
| Q19 | ❌ | Large OR + list_value in IdSeek filter → vector assertion |
| Q20 | ❌ | Edge property variable in WITH → planner assertion |
| Q21 | ❌ | Multi-variable NOT EXISTS → correlated NLJ unsupported |
| Q22 | ✅ | NOT EXISTS + substring + IN list |

---

## Remaining TPC-H Failures — Root Cause Analysis

### Q16: count(distinct) + list_value vector type

**Query**: `count(distinct s.S_SUPPKEY)` with `NOT s.S_COMMENT =~ ".*Customer.*Complaints.*"`

**Error**: `vector.GetVectorType() == VectorType::CONSTANT_VECTOR || FLAT_VECTOR || ROW_VECTOR` (execution)

**Root cause**: `list_value()` function returns a LIST-typed vector. When this is used inside an IdSeek filter expression, the execution engine encounters an unexpected vector type (likely DICTIONARY_VECTOR or LIST_VECTOR) during filter evaluation. The hash aggregate for `count(distinct)` may also contribute.

**Fix direction**: Check how `list_value()` results are consumed in IdSeek's `ExpressionExecutor`. May need to flatten the vector before filter evaluation. Also verify `count(distinct)` aggregate path in physical planner.

### Q17: Multi-part correlated subquery

**Query**:
```cypher
MATCH (lineitem:LINEITEM)-[:COMPOSED_BY]->(part:PART) WHERE ...
WITH lineitem, part
MATCH (lineitem2:LINEITEM)-[:COMPOSED_BY]->(part)
WITH lineitem, 0.2 * avg(lineitem2.L_QUANTITY) as avg_quantity
WHERE lineitem.L_QUANTITY < avg_quantity
RETURN sum(lineitem.L_EXTENDEDPRICE) / 7.0 as avg_yearly
```

**Error**: `data_chunk.cpp line 40: !types.empty()` (prepare)

**Root cause**: This query has a correlated subquery pattern — the second MATCH re-joins `part` from the first MATCH, then aggregates `lineitem2.L_QUANTITY`. The physical planner generates a plan with empty output types somewhere in the pipeline. Likely the `WITH lineitem, part` → second MATCH re-binding creates a plan where the GROUP BY key set is too large (all LINEITEM columns as keys).

**Fix direction**: Investigate the physical planner's handling of GROUP BY with many key columns. The `PlanGroupBy` output schema may not properly propagate types for all columns when the key set is large (17 columns for LINEITEM). Also check if the second MATCH's re-binding of `part` is handled correctly.

### Q19: Large OR + list_value in filter

**Query**: Three-way OR with `part.P_CONTAINER in ["SM CASE", "SM BOX", ...]`

**Error**: Same vector type assertion as Q16

**Root cause**: `IN ["SM CASE", ...]` is rewritten to `list_contains(list_value(...), val)`. The `list_value()` function creates a LIST vector. When this expression is pushed down into an IdSeek filter, the filter evaluation encounters the LIST vector type which isn't handled by the vector flattening logic.

**Fix direction**: Same as Q16 — the `list_value()` function needs special handling in IdSeek filter expressions, or the filter should be evaluated on a FLAT_VECTOR.

### Q20: Edge property variable in WITH

**Query**: `WITH ps, s, 0.5 * sum(li.L_QUANTITY) as quantity_sum`

**Error**: `helper-c.cpp line 118: 0` (prepare)

**Root cause**: `ps` is an edge variable (PARTSUPP edge with properties). When `ps` appears in a WITH clause alongside aggregation, the converter passes it through as a GROUP BY key. The physical planner's handling of edge property variables as GROUP BY keys may not construct the output schema correctly, leading to an assertion in the C API helper.

**Fix direction**: Check how `PlanGroupBy` handles edge variable expressions when they're GROUP BY keys. The edge's property columns may not be properly mapped in the output schema. Also check the `helper-c.cpp:118` assertion context.

### Q21: Multi-variable NOT EXISTS

**Query**:
```cypher
AND NOT EXISTS {
    MATCH (l3:LINEITEM)-[:IS_PART_OF]->(o)
    WHERE s.S_SUPPKEY <> l3.L_SUPPKEY AND l3.L_RECEIPTDATE > l3.L_COMMITDATE
}
```

**Error**: `NOT EXISTS subquery is not yet supported. Use EXISTS with negation instead.` (planner_physical.cpp:4608)

**Root cause**: The EXISTS implementation only supports **single outer-bound node** correlation. Q21's NOT EXISTS references both `o` (ORDERS) and `s` (SUPPLIER) from the outer scope, creating multi-variable correlation. ORCA cannot decorrelate this to a hash anti-semi-join, so it falls back to `CorrelatedLeftAntiSemiNLJoin`. The physical planner explicitly rejects correlated NL joins.

**Fix direction**: Two options:
1. **Extend EXISTS converter** to support multiple outer-bound nodes — add correlation predicates for each outer variable, allowing ORCA to decorrelate
2. **Support correlated NL join execution** — implement `PhysicalBlockwiseNLJoin` for correlated subqueries (re-execute inner pipeline per outer row)

Option 1 is preferred as it produces better plans (hash join vs nested loop).

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

**Limitation**: Only single outer-bound node. Multi-variable (Q21 pattern) falls back to correlated NLJ which is unsupported.

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
