# TurboLynx — CRUD Design

## Overview

TurboLynx uses a read-optimized **Base/Delta** layout for OLAP-style graph workloads.

- **Base storage** keeps the long-lived columnar layout used by scans and traversals.
- **Delta storage** captures recent inserts, updates, and deletes without rewriting Base in place.
- **Compaction** periodically folds Delta back into Base.

Updates are handled as **delete + insert**, not in-place mutation. A record may move when its schema changes, so TurboLynx uses a stable logical identifier:

- **LID**: stable logical ID for a node or edge
- **PID**: current physical location
- **LidPidTable**: maps `LID -> PID`

When a record moves, only `LidPidTable` is updated.

## Storage Layout

### Base Storage

Base storage is organized as:

```text
Partition
  -> Graphlet
     -> Extent
        -> Column chunks
        -> Adjacency chunks (CSR)
```

- **Partition**: top-level logical unit; nodes are grouped by label, edges by relation shape.
- **Graphlet**: a graphlet-local schema group inside a partition.
- **Extent**: the physical unit read by scans.
- **Column chunks**: property columns.
- **Adjacency chunks (CSR)**: traversal indexes for each `(edge partition, direction)`.

### Delta Storage

Delta storage contains recent changes that are not yet absorbed into Base.

- **Graphlet Delta**
  - `GraphletInserts: map<schema, record list>`
  - `GraphletDeletes: map<extent_id, bitmap>`
- **CSR Delta**
  - `CsrInserts: map<src_lid, edge list>`
  - `CsrDeletes: map<src_lid, set<edge_lid>>` in the current V1 design

Node writes affect only `Graphlet Delta`. Edge writes may also affect `CSR Delta`, because edge visibility and traversal indexes must stay in sync.

## CRUD Semantics

| Operation | Main structures touched | Behavior |
|---|---|---|
| `CREATE node` | `GraphletInserts`, `LidPidTable` | Append a new node record to Delta and assign a new `node_lid`. |
| `CREATE edge` | `GraphletInserts`, `CsrInserts`, `LidPidTable` | Append the edge record and add forward/backward adjacency entries. |
| `UPDATE node` | `GraphletDeletes`, `GraphletInserts`, `LidPidTable` | Invalidate the old version and append a new one. |
| `UPDATE edge property` | `GraphletDeletes`, `GraphletInserts`, `LidPidTable` | Replace the edge record; adjacency stays unchanged if endpoints do not change. |
| `DELETE node` | `GraphletDeletes`, `CSR Delta`, `LidPidTable` | Invalidate the node and remove incident edge visibility. |
| `DETACH DELETE node` | same as `DELETE node` | Current query surface uses cascade semantics. |
| `DELETE edge` | `CSR Delta`, `LidPidTable` | Mark the edge invisible through adjacency tombstones; invalidate the logical record. |
| `READ` | Base + Delta | Merge Base data with Delta inserts/deletes to expose the latest state. |

## Write Path

All writes follow the same high-level order:

1. Resolve the target partition and current location.
2. Append the change to the WAL.
3. Update `Graphlet Delta` and/or `CSR Delta`.
4. Update `LidPidTable` if the record moved or became invalid.

### Node Writes

- **Create**: append a new row to `GraphletInserts`, allocate a new `node_lid`, and record a Delta PID.
- **Update**: invalidate the old version, append the new version, and repoint `LidPidTable`.
- **Delete**: invalidate the node and remove incident edge visibility from traversal state.

### Edge Writes

- **Create**: append the edge record and add adjacency entries to both traversal directions.
- **Property update**: replace the edge record in Delta; adjacency is unchanged if endpoints stay the same.
- **Endpoint update**: conceptually treated as `DELETE edge + CREATE edge`.
- **Delete**: record source-based tombstones so traversals filter the edge out.

## Read Path

### Node Scan

Node scan merges:

1. Base extents
2. Base delete state
3. `GraphletDeletes`
4. live rows from `GraphletInserts`

### ID Seek

`IdSeek` does not scan Base and Delta. It reads `LidPidTable` and jumps directly to the current location.

### Edge Traversal

Traversal merges:

1. Base CSR adjacency
2. `CsrDeletes` tombstones
3. live entries from `CsrInserts`

If edge properties are needed, the engine follows the corresponding `edge_lid` back to the current edge record.

## Compaction

### Minor Compaction

Minor compaction folds one partition's Delta state into new Base extents and CSR chunks.

- Rebuilds Base rows from live Base rows plus live Delta rows
- Rebuilds CSR from surviving Base adjacency plus live Delta adjacency
- Rewrites `LidPidTable` entries to Base PIDs
- Clears the consumed Delta state

### Major Compaction

Major compaction reorganizes Base itself.

- Re-clusters live records
- Recomputes graphlet placement
- Rebuilds extents and CSR chunks
- Rewrites `LidPidTable` for moved records

In practice, the current V1 checkpoint path is closer to **flush Delta-backed inserts to disk and preserve remaining delete/adjacency state via WAL** than to a fully general compaction pipeline.

## WAL and Recovery

TurboLynx relies on WAL-first ordering:

1. append and flush WAL
2. update Delta structures
3. publish the new PID in `LidPidTable`

Recovery restores the latest checkpoint and replays WAL to reconstruct:

- `Graphlet Delta`
- `CSR Delta`
- `LidPidTable`

This keeps Base read-optimized while preserving the latest logical state across restarts.
