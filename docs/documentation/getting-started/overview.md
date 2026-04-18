# What is TurboLynx?

TurboLynx is a fast, scalable **OLAP graph database** written in C++17.

## Key Characteristics

| Property | Value |
|---|---|
| Query language | Cypher — the dialect defined by the openCypher specification, with a subset of clauses currently implemented (see [Cypher Query Language](../cypher/overview.md)) |
| Architecture | Single-process embedded library |
| Storage model | Extent-based columnar (Graphlet partitioning) |
| Optimizer | ORCA cost-based (Cascades framework, ported from Greenplum) |
| Graph model | Property graph (vertices + directed edges + properties) |

## Architecture at a Glance

```
Cypher Query
     │
     ▼
  ANTLR4 Parser  →  RegularQuery AST
     │
     ▼
  TurboLynx Binder  →  BoundRegularQuery  (type resolution, schema lookup)
     │
     ▼
  Cypher2OrcaConverter  →  ORCA Logical Plan
     │
     ▼
  ORCA Optimizer  →  Physical Plan  (cost-based join ordering, Graphlet Early Merge)
     │
     ▼
  Pipeline Executor
     │
     ▼
  Storage  (BufferPool → store.db via the TurboLynx storage backend)
```

## Graphlets — Schema-Clustered Columnar Storage

TurboLynx organizes graph data into cost-based clusters, called **Graphlets**, and stores them in a columnar format. **Cost-based Graphlet Chunking (CGC)** chooses the clustering granularity by balancing graphlet count, null entries, and vectorization overhead on heterogeneous schemaless inputs.

## GEM — Per-Graphlet Join Ordering

**Graphlet Early Merge (GEM)** is the TurboLynx optimizer extension that manages the plan search space when a label scan expands to a `UNION ALL` over many graphlets. It forms coarse-granular virtual graphlets before join enumeration, explores alternatives via `PushJoinBelowUnionAll`, and uses heuristics plus Greedy Operator Ordering to keep optimization tractable.

## Single-Process Embedded

Unlike traditional graph databases that run as a separate server process, TurboLynx embeds directly into the calling process.

- No daemon to start or stop
- No IPC or shared memory
- Catalog persisted as `catalog.bin`; graph data as `store.db`
- C API via `libturbolynx.so` on Linux or `libturbolynx.dylib` on macOS

## No External Runtime Dependencies

All dependencies are compiled into the binary from bundled source.
Only the standard C/C++ runtime libraries provided by the host toolchain are required at runtime.

---

## Next Steps

- [Quickstart](quickstart.md) — load a dataset and run your first Cypher query
- [Installation](../../installation/overview.md) — build from source
