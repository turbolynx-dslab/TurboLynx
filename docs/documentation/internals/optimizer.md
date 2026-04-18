# Query Optimizer

TurboLynx uses **ORCA**, a Cascades-framework cost-based optimizer originally developed at Pivotal for Greenplum/HAWQ, adapted for graph queries.

## Pipeline

```
Cypher String
     │
     ▼
  ANTLR4 Parser  →  RegularQuery AST
     │
     ▼
  TurboLynx Binder  →  BoundRegularQuery
     (type resolution, catalog lookup, property binding)
     │
     ▼
  Cypher2OrcaConverter  →  ORCA Logical Plan (CExpression tree)
     │
     ▼
  ORCA Optimizer  →  Physical Plan
     (cost-based join ordering, operator selection, GEM)
     │
     ▼
  Pipeline Executor
```

## Binder

The TurboLynx Binder (`src/binder/binder.cpp`) resolves Cypher AST nodes against the graph catalog:

- Maps node/rel variables to partitions and graphlets (via catalog OIDs)
- Resolves property names to `key_id` values
- Populates `BoundNodeExpression` and `BoundRelExpression` with all property columns needed for execution
- Synthesizes inline node/rel property filters (`{k:v}` → equality predicates)

## Cypher2OrcaConverter

The converter (`src/converter/cypher2orca_converter.cpp`) translates `BoundRegularQuery` into an ORCA `CExpression` logical tree:

- Each label scan becomes a `CLogicalGet` wrapped in a `CLogicalProjectColumnar`
- Multi-graphlet labels use `CLogicalUnionAll` to combine per-graphlet scans
- Edge traversals are modeled as joins: `CLogicalInnerJoin` (index join) or `CLogicalNAryJoin`
- WHERE predicates become `CLogicalSelect`
- Projections and aggregates map to `CLogicalProjectColumnar` and `CLogicalGbAgg`

## ORCA Overview

ORCA uses the **Cascades** optimization framework:

1. **Memo** — A compact representation of the space of equivalent logical plans (expression groups)
2. **Exploration** — Transformation rules enumerate alternative logical plans
3. **Implementation** — Logical operators are mapped to physical operators
4. **Optimization** — Cost-based selection of the best physical plan given statistics

## GEM — Graphlet Early Merge

**GEM** (Graphlet Early Merge) is the TurboLynx-specific optimizer extension introduced in the VLDB paper. It manages the plan search space that arises when a single label scan expands to a `UNION ALL` over many graphlets.

A naive optimizer treats the union as one relation and enumerates a single join order above it, leaving each graphlet to inherit the same plan. GEM instead groups graphlets into coarse-granular virtual graphlets before join enumeration, explores alternatives through the `PushJoinBelowUnionAll` rule, and uses heuristics with a time limit plus Greedy Operator Ordering to keep the search space manageable.

## Join Strategies

| Strategy | Trigger | Description |
|---|---|---|
| Index join | Selective edge traversal | Uses forward/backward adjacency list; fast for low-fanout patterns |
| Hash join | Large intermediate results | General-purpose; ORCA default fallback |
| Merge join | Ordered inputs | Can be disabled with `--disable-merge-join` |

## Statistics

ORCA uses column-level statistics for cost estimation. Each graphlet carries per-column equal-depth numeric histograms (plus min/max, NDV, and null fraction) built from a reservoir sample. Statistics are (re)built with the `analyze` command:

```bash
./tools/turbolynx --workspace /path/to/db
TurboLynx >> .analyze
TurboLynx >> .exit
```

Without statistics, ORCA falls back to heuristic cardinality estimates.

## Shell Flags

```bash
# Run a single query
./tools/turbolynx --workspace /path/to/db --query "MATCH (n:Person) RETURN n.firstName LIMIT 10;"

# Force join strategy
./tools/turbolynx --workspace /path/to/db --index-join-only --query "..."
./tools/turbolynx --workspace /path/to/db --hash-join-only  --query "..."

# Disable merge join
./tools/turbolynx --workspace /path/to/db --disable-merge-join --query "..."

# Print ORCA internal trace
./tools/turbolynx --workspace /path/to/db --debug-orca --query "..."

# Print selected physical plan without executing
./tools/turbolynx --workspace /path/to/db --explain --query "..."

# Compile only (no execution)
./tools/turbolynx --workspace /path/to/db --compile-only --query "..."
```
