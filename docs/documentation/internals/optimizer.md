# Query Optimizer

TurboLynx uses **ORCA**, the cost-based query optimizer originally developed at Pivotal for Greenplum/HAWQ, adapted for graph queries.

## Pipeline

```
Cypher String
     │
     ▼
  ANTLR4 Parser  →  AST
     │
     ▼
  Binder (kuzu-style)  →  Bound AST  (type resolution, schema lookup)
     │
     ▼
  Logical Planner  →  Logical Plan
     │
     ▼
  ORCA Optimizer  →  Physical Plan  (cost-based join ordering, operator selection)
     │
     ▼
  Pipeline Executor
```

## ORCA Overview

ORCA is a Cascades-framework optimizer. It works by:

1. **Memo** — A compact representation of the space of equivalent logical plans
2. **Exploration** — Applying transformation rules to enumerate alternatives
3. **Implementation** — Mapping logical operators to physical operators
4. **Optimization** — Cost-based selection of the best physical plan

ORCA was designed for large-scale parallel databases and handles complex multi-way joins efficiently.

## Join Strategies

| Strategy | When used | CLI flag |
|---|---|---|
| Index join | Selective edge traversals with adjacency list index | `--join-order-optimizer query` |
| Hash join | Larger intermediate result sets | (default fallback) |
| Merge join | Ordered input streams | `--disable-merge-join` to disable |

## Statistics

ORCA relies on column-level statistics for cost estimation.
Statistics must be rebuilt after loading a dataset:

```bash
./tools/client --workspace /path/to/db
TurboLynx >> analyze
TurboLynx >> :exit
```

Without statistics, ORCA falls back to heuristic estimates.

## Debug Output

```bash
./tools/client --workspace /path/to/db \
               --query "MATCH (n:Person) RETURN n.firstName LIMIT 10;" \
               --debug-orca
```

Prints ORCA's internal plan search trace.

## Explain Plan

```bash
./tools/client --workspace /path/to/db \
               --query "MATCH (n:Person) RETURN n.firstName LIMIT 10;" \
               --explain
```

Prints the selected physical plan without executing the query.
