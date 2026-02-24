# Storage Engine

TurboLynx uses an **extent-based columnar storage** model optimized for OLAP graph workloads.

## Concepts

### Extent

The fundamental unit of storage. An extent stores a fixed-size block of tuples for one partition (vertex type or edge type).

- Fixed number of tuples per extent
- Columnar layout within each extent (one file per property column)
- Extents are immutable after bulk load; delta extents handle updates

### Partition

A partition corresponds to a vertex or edge type (e.g., `Person`, `KNOWS`).
Each partition owns a sequence of extents and tracks:

- ExtentID counter (globally unique: `partition_id << 16 | local_id`)
- Number of rows per extent
- Property schema (column types)

### Catalog

The catalog stores the schema in memory and persists it to `catalog.bin` via binary serialization (`CatalogSerializer`).
On startup, the catalog is restored from `catalog.bin` — no re-scan of data files is required.

## Async I/O

TurboLynx uses Linux Kernel AIO for non-blocking disk reads/writes via `disk_aio/`.

- **`DiskAioThread`** — dedicated I/O thread per NUMA socket
- **`DiskAioRequest`** — wraps a kernel `iocb` (I/O control block)
- Syscalls used: `io_setup`, `io_submit`, `io_getevents` (via `libaio_shim.hpp` — no external library)

## Adjacency Lists

Edges are stored in two directions:
- Forward adjacency list (`:START_ID → :END_ID`)
- Backward adjacency list (`:END_ID → :START_ID`)

Both are stored as separate chunk definitions within the edge partition's extents.

## Cache

A buffer pool cache sits between the execution engine and disk I/O.
Cache entries are pinned during query execution and released when no longer needed.

## Directory Layout on Disk

```
<workspace>/
├── catalog.bin          ← serialized catalog (schema + extent metadata)
├── person/              ← vertex partition directory
│   ├── 0.col            ← column 0 (e.g., firstName)
│   ├── 1.col            ← column 1 (e.g., lastName)
│   └── adj/             ← adjacency list chunks
└── knows/               ← edge partition directory
    ├── 0.col
    └── adj/
```

> Exact layout may vary — consult `src/storage/` for current implementation.
