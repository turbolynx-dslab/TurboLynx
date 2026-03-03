# What is TurboLynx?

TurboLynx is a fast, scalable **OLAP graph database** written in C++17.

## Key Characteristics

| Property | Value |
|---|---|
| Query language | Cypher |
| Architecture | Single-process embedded (DuckDB-style) |
| Storage model | Extent-based columnar |
| Optimizer | ORCA cost-based (ported from Greenplum) |
| Graph model | Property graph (vertices + directed edges + properties) |
| Primary use case | Read-heavy analytical graph workloads |

## Architecture at a Glance

```
Cypher Query
     │
     ▼
  Parser (ANTLR4)
     │
     ▼
  Binder / Planner (kuzu-style)
     │
     ▼
  Optimizer (ORCA)  ←─ Statistics
     │
     ▼
  Physical Plan
     │
     ▼
  Pipeline Executor
     │
     ▼
  Storage (Extent-based columnar AIO)
```

## Single-Process Embedded

Unlike traditional graph databases (Neo4j, JanusGraph) that run as a separate server process, TurboLynx embeds directly into the calling process — similar to DuckDB or SQLite.

- No daemon to start or stop
- No IPC or shared memory
- No store server
- Catalog persisted as `catalog.bin` in the workspace directory

## No External Runtime Dependencies

TurboLynx requires no special system libraries at runtime.
All dependencies are compiled into the binary from bundled source.
Only standard C/C++ runtime libraries (`libstdc++`, `libc`, `libpthread`) are required.

---

## Next Steps

- [Quickstart](quickstart.md) — load a dataset and run your first Cypher query
- [Installation](../../installation/overview.md) — build from source
