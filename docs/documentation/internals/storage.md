# Storage Engine

TurboLynx uses an **extent-based columnar storage** model optimized for OLAP graph workloads.

## Core Concepts

### Graphlet

A **Graphlet** is a set of nodes (or edges) with the same label and the same property schema. It is the fundamental unit of columnar storage — each graphlet maps to a sequence of extents in `store.db`.

TurboLynx automatically groups nodes with the same label into graphlets using the **CGC** (Compact Graphlet Clustering) algorithm during bulk load. Graphlets contain no NULL columns, making them SIMD-vectorizable.

### Partition

A **Partition** corresponds to a vertex type or edge type (e.g., `Person`, `KNOWS`). Each partition owns one or more graphlets and tracks:

- A set of graphlet IDs (graphlet catalog entries)
- Extent ID counter (globally unique: `partition_id << 16 | local_extent_id`)
- Property schema (column types per graphlet)

### Extent

An **Extent** is a fixed-size block of tuples for one graphlet. Extents are immutable after bulk load. Each extent is represented in the catalog by an `ExtentCatalogEntry`.

### Chunk

A **Chunk** stores one column's data for one extent. Each chunk occupies a contiguous 512B-aligned region in `store.db`. Chunks are identified globally by `chunk_id` and addressed via the `.store_meta` offset table.

### Catalog

The catalog stores all schema and extent metadata in memory and persists it to `catalog.bin` via binary serialization (`CatalogSerializer`). On startup, the full catalog is restored from `catalog.bin` — no re-scan of data files is needed.

---

## Single-File Block Store

All chunk data is packed into a single file (`store.db`) in the workspace directory.

```
workspace/
├── catalog.bin     ← schema and extent metadata (binary serialized)
├── store.db        ← all chunk data (512B-aligned, packed sequentially)
└── .store_meta     ← chunk offset table: [chunk_id, offset, alloc_size, requested_size]
```

Chunks are allocated sequentially. The offset table in `.store_meta` is written atomically via a `.tmp` → `rename` pattern on shutdown.

---

## Async I/O

TurboLynx issues all disk I/O through **Linux Kernel AIO** (`io_setup` / `io_submit` / `io_getevents`) directly — no `libaio-dev` system library is required.

- One shared file descriptor for `store.db` across all chunks
- All reads and writes are absolute-offset `io_submit` requests
- A dedicated I/O thread per NUMA socket handles completion events

---

## Adjacency Lists

Each edge type stores **two adjacency lists**:

- **Forward**: start-node → end-node (`SID → TID`)
- **Backward**: end-node → start-node (`TID → SID`)

Both are stored as separate chunk definitions within the edge partition's extents, packed alongside property chunks in `store.db`. The backward adjacency list is pre-sorted during bulk load for efficient reverse traversal.

---

## Buffer Pool

The **BufferPool** (`src/storage/cache/buffer_pool.cc`) is an in-process page cache between the execution engine and disk.

- Chunks are loaded on first access, pinned in memory, and released when done
- Dirty chunks are flushed to `store.db` when unpinned or on shutdown
- Eviction uses the **Second-Chance Clock** algorithm
- All allocations are 512B-aligned (`posix_memalign`)
- The pool tracks pin counts and dirty state per frame in an `unordered_map<chunk_id, Frame>`

### Chunk Cache Manager (CCM)

The **Chunk Cache Manager** (`src/storage/cache/chunk_cache_manager.cc`) wraps the BufferPool and adds:

- **ThrottleIfNeeded()**: async wake-up at 75% dirty capacity; blocking at 95%
- **Background flush thread**: drains dirty chunks continuously during bulk load
- **Multi-process locking**: `fcntl` `F_WRLCK` / `F_RDLCK` on `store.db` — one writer or N readers

---

## Multi-Process Access

TurboLynx supports concurrent multi-process access to the same workspace:

| Mode | Lock |
|------|------|
| Read-write (default) | Exclusive write lock (`F_WRLCK`) — blocks all readers |
| Read-only | Shared read lock (`F_RDLCK`) — multiple readers allowed |

Multiple read-only connections can run concurrently. A write-lock holder blocks all readers and vice versa. Lock state is enforced via `fcntl` on `store.db`.

---

## Statistics

Column-level statistics are stored per-graphlet and used by the ORCA optimizer for cost estimation:

- **Numeric histograms**: equal-width / equal-depth; min/max/NDV
- **String histograms**: length distribution; prefix sampling
- **Validity histograms**: null fraction per column

Statistics are built with the `analyze` command after bulk loading. Without statistics, ORCA falls back to heuristic cardinality estimates.

Zone-map pruning uses extent-level min/max boundaries to skip extents that cannot satisfy a filter predicate.
