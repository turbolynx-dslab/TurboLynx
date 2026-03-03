# Storage Engine

TurboLynx uses an **extent-based columnar storage** model optimized for OLAP graph workloads.

## Concepts

### Extent

The fundamental unit of storage. An extent stores a fixed-size block of tuples for one partition (vertex type or edge type).

- Fixed number of tuples per extent
- Columnar layout within each extent (one chunk per property column)
- Extents are immutable after bulk load

### Partition

A partition corresponds to a vertex or edge type (e.g., `Person`, `KNOWS`).
Each partition owns a sequence of extents and tracks:

- ExtentID counter (globally unique: `partition_id << 16 | local_id`)
- Number of rows per extent
- Property schema (column types)

### Catalog

The catalog stores the schema in memory and persists it to `catalog.bin` via binary serialization.
On startup, the catalog is restored from `catalog.bin` — no re-scan of data files is required.

## Single-File Block Store

All chunk data is packed into a single file (`store.db`) in the workspace directory.

```
workspace/
├── catalog.bin     ← schema and extent metadata
├── store.db        ← all chunk data (binary packed, 512B-aligned blocks)
└── .store_meta     ← chunk offset table: [chunk_id, offset, alloc_size, requested_size]
```

Chunks are allocated sequentially within `store.db`. Each chunk occupies a contiguous region starting at a 512-byte-aligned offset. The offset table in `.store_meta` is written atomically via a `.tmp` → `rename` pattern on shutdown.

## Async I/O

TurboLynx uses Linux Kernel AIO for non-blocking disk reads and writes.

- One open file descriptor for `store.db` (shared across all chunks)
- All reads and writes are issued as `io_submit` requests with absolute byte offsets into `store.db`
- A dedicated I/O thread per NUMA socket handles completion events

## Adjacency Lists

Edges are stored in two directions:

- Forward adjacency list (`:START_ID → :END_ID`)
- Backward adjacency list (`:END_ID → :START_ID`)

Both are stored as separate chunk definitions within the edge partition's extents, packed alongside property chunks in `store.db`.

## Buffer Pool Cache

A buffer pool (Lightning) sits between the execution engine and disk I/O. Chunks are pinned into memory on first access, swizzled (disk offsets → memory pointers), and released when no longer needed. Dirty chunks are unswizzled and flushed to `store.db` on shutdown.
