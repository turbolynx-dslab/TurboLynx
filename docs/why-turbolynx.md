# TurboLynx

## What is TurboLynx?

**TurboLynx is an embedded analytical graph database** built for a specific gap in the database landscape: high-throughput analytical queries over **schemaless property graphs**, where labels are reused across nodes that carry wildly different attribute sets.

It speaks Cypher, ships as a single shared library (no server, no daemon), stores data in extent-based columnar files, and runs queries through a Cascades-style cost-based optimizer that has been taught to reason about graphs.

If your data looks like a knowledge graph, a heterogeneous social graph, or a billion-edge ETL output where every entity has its own quirks, TurboLynx is built for you. If you need an OLTP graph store, full transactions, or write-heavy workloads — pick a different tool.

## Why TurboLynx?

There is no one-size-fits-all database. Every system is a set of design trade-offs — choosing it means committing to its strengths and accepting its limits. TurboLynx makes those trade-offs explicit: it sacrifices write throughput, transactional isolation, and schema enforcement to gain something the rest of the graph database market doesn't offer — **fast analytical queries on schemaless property graphs without ETL.**

---

## Key Characteristics of TurboLynx

### Schemaless

Real-world property graphs are messy. DBpedia has 2,796 unique attribute keys for `Person`. LDBC SNB nodes share labels but not columns. Loading these into a row-table forces you to either pre-ETL into a fixed schema (lossy) or accept that 60–80% of your storage is `NULL` (wasteful).

TurboLynx's storage layer groups nodes with similar attribute sets into **Graphlets** — compact columnar tables with no `NULL`s, picked automatically by a cost-driven partitioner (CGC). One label can be backed by many graphlets; the engine never asks you to declare a schema up front.

### Fast

TurboLynx is built on three fast-by-default ideas:

- **Extent-based columnar storage** with zone-map pruning, modeled on DuckDB's vectorized execution layer.
- **The GEM optimizer**, which pushes joins *below* `UNION ALL` so each graphlet group gets its own cost-optimal join order. One Cypher query can produce many physical plans, one per graphlet partition.
- **SIMD vectorized operators** for filters, hashes, joins, and aggregations.

The result: graph analytical workloads that outperform Neo4j on multi-hop traversal and DuckDB on graph-shaped joins, without giving up Cypher expressivity.

### Embedded

Like DuckDB and Kuzu, TurboLynx ships as a single shared library — no server, no daemon, no IPC. Link `libturbolynx.so` into your application, point it at a workspace directory, and start querying. The C API surface is small enough to wrap from any language with FFI.

There is no separate "client" and "server" concept. There is no port to open. Your query runs in the same address space as your application, and result vectors land directly in your buffers.

### Cypher-Native

TurboLynx speaks Cypher as its primary query language — not a SQL dialect with graph extensions, not a custom DSL. Variable-length paths (`*1..3`), `OPTIONAL MATCH`, `WITH ... ORDER BY ... LIMIT`, `UNWIND`, `collect()`, aggregations, `CASE` expressions, and the standard string/numeric/temporal function library are all supported.

The query frontend is built on the ANTLR4 Cypher grammar; the optimizer is a fork of Greenplum's ORCA Cascades framework, taught to reason about graph operators.

### Analytical

Most graph databases optimize for transactional pattern matching: "find this exact subgraph and return it." TurboLynx is optimized for the next step: **group, aggregate, and analyze the result**.

You can write a single Cypher query that does a 4-hop traversal, filters on a property predicate, groups by a label attribute, and returns top-k counts — and the optimizer will plan it as a single pipeline rather than materializing intermediate result sets. This is the workload graph databases historically push to a separate OLAP system; TurboLynx absorbs it.

### Open

TurboLynx is MIT-licensed and source-available. There are no enterprise tiers, no usage telemetry, and no paid features. Build it from source, link it into anything, fork it if you need to.

The project lives on [GitHub](https://github.com/turbolynx-dslab/TurboLynx); contributions and bug reports are welcome.

### Tested

Correctness is tracked through three layers:

- **Unit tests** (Catch2) cover the catalog, storage engine, common utilities, and execution operators.
- **Query regression tests** run against [LDBC Social Network Benchmark](https://ldbcouncil.org/benchmarks/snb/) interactive queries (IC1–IC14) and the full [TPC-H](https://www.tpc.org/tpch/) benchmark (Q1–Q22), with reference results cross-validated against Neo4j and DuckDB respectively.
- **Continuous integration** rebuilds and re-tests every commit on Linux.

The current status: **LDBC 464/464 pass · TPC-H 22/22 pass · IC1–IC14 Neo4j-verified.**

---

## Peer-Reviewed Publications

TurboLynx grew out of research at POSTECH on storage-optimal column grouping (CGC), graph-aware join enumeration (GEM), and schemaless property graph systems. Publications and citations are listed on the project's [GitHub README](https://github.com/turbolynx-dslab/TurboLynx).

---

## Standing on the Shoulders of Giants

TurboLynx would not exist without the open-source database research community. In particular:

- **[DuckDB](https://duckdb.org/)** — for the embedded distribution model, the vectorized execution architecture, and large parts of the expression evaluation layer (which TurboLynx integrates directly).
- **[Greenplum's ORCA](https://github.com/greenplum-db/gporca)** — for the Cascades-style cost-based optimizer that TurboLynx forks and extends with graph operators.
- **[Kùzu](https://kuzudb.com/)** — for proving that an embedded analytical graph database is a viable category and a worthwhile research direction.
- **[Neo4j](https://neo4j.com/)** — for designing and stewarding the Cypher language, and for setting the bar that an analytical graph engine must clear.
- **[ANTLR4 Cypher grammar](https://github.com/openCypher/openCypher)** — for the openCypher reference grammar that the TurboLynx parser is built on.
- **[LDBC Council](https://ldbcouncil.org/)** — for the SNB benchmark suite that drives our correctness and performance regression testing.

Many other papers and projects shaped specific design decisions; the source tree credits them inline.
