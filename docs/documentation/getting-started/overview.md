# What is TurboLynx?

TurboLynx is a fast, scalable **OLAP graph database** written in C++17.

## Key Characteristics

| Property | Value |
|---|---|
| Query language | Cypher (openCypher subset) |
| Architecture | Single-process embedded (DuckDB-style) |
| Storage model | Extent-based columnar (Graphlet partitioning) |
| Optimizer | ORCA cost-based (Cascades framework, ported from Greenplum) |
| Graph model | Property graph (vertices + directed edges + properties) |
| Primary use case | Read-heavy analytical graph workloads |

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
  ORCA Optimizer  →  Physical Plan  (cost-based join ordering, GEM algorithm)
     │
     ▼
  Pipeline Executor
     │
     ▼
  Storage  (BufferPool → store.db via Linux Kernel AIO)
```

## Graphlets — NULL-free Columnar Storage

Nodes with the same label but different attribute sets are grouped into **Graphlets** — compact columnar tables with a uniform schema and no NULL columns. The CGC algorithm selects the cost-optimal grouping automatically.

This eliminates the NULL inflation that occurs when storing schemaless graphs in a single wide table, and makes every graphlet SIMD-vectorizable.

## GEM — Per-Graphlet Join Ordering

The **GEM** (Graph-aware Enumeration via Memo) optimizer extension pushes joins below `UNION ALL` nodes, allowing each graphlet group to receive its own cost-optimal join order. This is particularly effective on schemaless graphs where different graphlets have very different cardinalities.

## Single-Process Embedded

Unlike traditional graph databases that run as a separate server process, TurboLynx embeds directly into the calling process — similar to DuckDB or SQLite.

- No daemon to start or stop
- No IPC or shared memory
- Catalog persisted as `catalog.bin`; graph data as `store.db`
- C API via `libturbolynx.so`

## No External Runtime Dependencies

All dependencies are compiled into the binary from bundled source.
Only standard C/C++ runtime libraries (`libstdc++`, `libc`, `libpthread`) are required at runtime.

---

## Next Steps

- [Quickstart](quickstart.md) — load a dataset and run your first Cypher query
- [Installation](../../installation/overview.md) — build from source
