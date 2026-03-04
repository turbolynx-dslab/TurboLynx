---
hide:
  - navigation
  - toc
---

<!-- ══════════════════════════════════════════════════════
     HERO — 2-column: left=text+CTA, right=code window
══════════════════════════════════════════════════════ -->

<div class="tl-hero">
<div class="tl-hero-text">

<img src="assets/logo.png" alt="TurboLynx" class="tl-hero-logo">

<h1>Turbo<span>Lynx</span></h1>

<p class="tl-hero-tagline">
  A fast in-process analytical graph database.<br>
  Up to <strong style="color:#FFCA28">183.9×</strong> faster than leading graph database systems.
</p>

<div class="tl-hero-meta">
  <span class="tl-pill">Cypher</span>
  <span class="tl-pill">C++17</span>
  <span class="tl-pill">Embedded · No Daemon</span>
  <span class="tl-pill">Zero Dependencies</span>
</div>

<div class="tl-hero-cta">
  <a href="documentation/getting-started/quickstart/" class="md-button md-button--primary tl-btn-primary">Get Started</a>
  <a href="installation/overview/" class="md-button tl-btn-ghost">Installation</a>
  <a href="https://github.com/your-org/turbograph-v3" class="md-button tl-btn-ghost">GitHub</a>
</div>

</div>
<div class="tl-hero-code">

<div class="tl-code-window">
<div class="tl-code-win-header">
  <span class="tl-dot tl-dot--r"></span>
  <span class="tl-dot tl-dot--y"></span>
  <span class="tl-dot tl-dot--g"></span>
  <span class="tl-win-title">query.cypher</span>
</div>
<code class="tl-code-win-body"><span class="tl-cmt">-- Find top-connected friends of Alice (up to 3 hops)</span>
<span class="tl-kw">MATCH</span> (p:<span class="tl-var">Person</span>)-[:<span class="tl-rel">KNOWS*1..3</span>]->(friend:<span class="tl-var">Person</span>)
<span class="tl-kw">WHERE</span>  p.<span class="tl-prop">firstName</span> = <span class="tl-str">'Alice'</span>
  <span class="tl-kw">AND</span>  friend.<span class="tl-prop">birthday</span> > <span class="tl-fn">date</span>(<span class="tl-str">'1990-01-01'</span>)
<span class="tl-kw">RETURN</span> friend.<span class="tl-prop">firstName</span> <span class="tl-kw">AS</span> name,
       <span class="tl-fn">count</span>(*) <span class="tl-kw">AS</span> connections
<span class="tl-kw">ORDER BY</span> connections <span class="tl-kw">DESC  LIMIT</span> <span class="tl-num">5</span>
</code>
<div class="tl-code-win-result">
  <div class="tl-result-label">Result — 5 rows &nbsp;·&nbsp; 2 ms</div>
  <table class="tl-result-table">
    <thead><tr><th>name</th><th>connections</th></tr></thead>
    <tbody>
      <tr><td>Bob</td><td class="tl-val-num">31</td></tr>
      <tr><td>Carol</td><td class="tl-val-num">17</td></tr>
      <tr><td>Dave</td><td class="tl-val-num">9</td></tr>
    </tbody>
  </table>
</div>
</div>

</div>
</div>

<!-- ══════════════════════════════════════════════════════
     STATS BAR — real benchmark numbers
══════════════════════════════════════════════════════ -->

<div class="tl-stats">

<div class="tl-stat">
<span class="tl-stat-num">183.9×</span>
<span class="tl-stat-label">Max speedup · LDBC SNB SF100</span>
</div>

<div class="tl-stat">
<span class="tl-stat-num">86.1×</span>
<span class="tl-stat-label">Max speedup · DBpedia KG</span>
</div>

<div class="tl-stat">
<span class="tl-stat-num">29.8×</span>
<span class="tl-stat-label">Avg speedup · LDBC SF100</span>
</div>

<div class="tl-stat">
<span class="tl-stat-num">7</span>
<span class="tl-stat-label">Systems benchmarked</span>
</div>

</div>

<!-- ══════════════════════════════════════════════════════
     FEATURES
══════════════════════════════════════════════════════ -->

<div class="tl-section-label">FEATURES</div>

## Why TurboLynx?

<div class="tl-feature-grid">

<div class="tl-feature-card">
<span class="tl-cat tl-cat-blue">Optimizer</span>

### ORCA + GEM Join Ordering

Cost-based query optimizer ported from Greenplum. The **GEM** algorithm selects optimal multi-hop traversal strategies automatically — index join, hash join, or merge join.
</div>

<div class="tl-feature-card">
<span class="tl-cat tl-cat-purple">Storage</span>

### Columnar Extent Storage

Extent-based columnar layout with zone-map pruning and per-column compression. Entire extents are skipped at scan time — designed for read-heavy analytical workloads.
</div>

<div class="tl-feature-card">
<span class="tl-cat tl-cat-green">Embedded</span>

### Single-Process, No Daemon

Zero IPC, zero shared memory. Integrates directly into your application via a clean C API — like DuckDB or SQLite. No separate server process to manage.
</div>

<div class="tl-feature-card">
<span class="tl-cat tl-cat-orange">Graph Model</span>

### Schema-flexible Property Graph

Handles both fixed-schema graphs (LDBC, TPC-H) and highly heterogeneous schemaless graphs (DBpedia: 2,796 unique attributes). One engine, any structure.
</div>

<div class="tl-feature-card">
<span class="tl-cat tl-cat-teal">Portability</span>

### Zero System Dependencies

TBB, NUMA, and hwloc are bundled via CMake FetchContent. Builds on any Ubuntu 22.04 host with only `build-essential` — no apt-get maze, no Docker required.
</div>

<div class="tl-feature-card">
<span class="tl-cat tl-cat-rose">Catalog</span>

### Persistent Binary Catalog

Schema serialized to `catalog.bin` via atomic rename. Your billion-edge graph is fully accessible milliseconds after restart — zero re-scan on startup.
</div>

</div>

<!-- ══════════════════════════════════════════════════════
     QUICK START
══════════════════════════════════════════════════════ -->

<div class="tl-section-label">QUICK START</div>

## Run in 3 Steps

=== "1 · Build"

    ```bash
    git clone <repo-url> turbograph-v3 && cd turbograph-v3
    mkdir build && cd build
    cmake -GNinja -DCMAKE_BUILD_TYPE=Release \
          -DENABLE_TCMALLOC=OFF \
          -DBUILD_UNITTESTS=OFF \
          -DTBB_TEST=OFF ..
    ninja   # → libturbolynx.so  client  bulkload
    ```

    All dependencies auto-fetched at configure time. No extra `apt install` needed.

=== "2 · Load Data"

    ```bash
    # Vertices: id,firstName,lastName,birthday
    # Edges:    srcId,dstId,creationDate
    ./tools/bulkload \
      --workspace  /path/to/mydb \
      --vertices   Person    data/person.csv \
      --edges      KNOWS     data/knows.csv \
      --src srcId  --dst dstId
    ```

    See [Data Import → File Formats](documentation/data-import/formats.md) for details.

=== "3 · Query"

    ```bash
    ./tools/client --workspace /path/to/mydb
    ```

    ```cypher
    TurboLynx >> MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
                 WHERE a.firstName = 'Alice'
                 RETURN b.firstName, count(*) AS hops
                 ORDER BY hops ASC LIMIT 10;
    ```

[Full Quickstart Guide →](documentation/getting-started/quickstart.md){ .md-button .md-button--primary }

---

<!-- ══════════════════════════════════════════════════════
     ARCHITECTURE
══════════════════════════════════════════════════════ -->

<div class="tl-section-label">INTERNALS</div>

## How a Query Runs

<div class="tl-pipeline">
  <div class="tl-pipe-step tl-pipe-step--parser">
    <strong>Parser</strong>
    <small>ANTLR4 Cypher</small>
  </div>
  <span class="tl-pipe-arrow">→</span>
  <div class="tl-pipe-step tl-pipe-step--optimizer">
    <strong>Optimizer</strong>
    <small>ORCA · GEM</small>
  </div>
  <span class="tl-pipe-arrow">→</span>
  <div class="tl-pipe-step tl-pipe-step--executor">
    <strong>Executor</strong>
    <small>Vectorized</small>
  </div>
  <span class="tl-pipe-arrow">→</span>
  <div class="tl-pipe-step tl-pipe-step--cache">
    <strong>Cache</strong>
    <small>Lightning CCM</small>
  </div>
  <span class="tl-pipe-arrow">→</span>
  <div class="tl-pipe-step tl-pipe-step--storage">
    <strong>Storage</strong>
    <small>store.db · AIO</small>
  </div>
</div>

<div class="tl-arch-grid">

<div class="tl-arch-card">

**Workspace layout**

```
WORKSPACE/
├── catalog.bin    ← schema (binary, atomic write)
├── store.db       ← all chunks (512 B-aligned)
└── .store_meta    ← offset table per chunk
```

</div>

<div class="tl-arch-card">

**Key subsystems**

| Module | Role |
|--------|------|
| `Catalog` | Schema registry · binary persistence |
| `StorageManager` | Block pool · buffer management |
| `ChunkCacheManager` | In-process Lightning cache |
| `ORCA / GEM` | Cost-based join ordering |
| `Extents` | Columnar layout · predicates |

</div>

</div>

<!-- ══════════════════════════════════════════════════════
     BENCHMARK RESULTS
══════════════════════════════════════════════════════ -->

<div class="tl-section-label">BENCHMARKS</div>

## Evaluated at Scale

Benchmarked against **7 systems** across 3 workloads on a dual-socket Intel Xeon Gold 6130 @ 2.1 GHz, 512 GB RAM.
All results are **geometric mean** of 5 runs after 3 warm-up runs. Timeout: 1 hour.

<div class="tl-bench-grid">

<div class="tl-bench-card tl-bench-card--ldbc">
<span class="tl-bench-label">LDBC SNB Interactive</span>
<span class="tl-bench-title">Social Network Benchmark · 14 complex queries</span>

<div class="tl-bench-stats">
  <div class="tl-bench-stat">
    <span>183.9×</span>
    <small>max speedup</small>
  </div>
  <div class="tl-bench-stat">
    <span>29.78×</span>
    <small>avg · SF100</small>
  </div>
  <div class="tl-bench-stat">
    <span>1.78B</span>
    <small>edges · SF100</small>
  </div>
</div>

Multi-step graph pattern matches across people, posts, and forums with multi-hop traversals evaluated at SF1 / SF10 / SF100.
</div>

<div class="tl-bench-card tl-bench-card--tpch">
<span class="tl-bench-label">TPC-H</span>
<span class="tl-bench-title">Decision Support Benchmark · 22 queries</span>

<div class="tl-bench-stats">
  <div class="tl-bench-stat">
    <span>44.6×</span>
    <small>max speedup</small>
  </div>
  <div class="tl-bench-stat">
    <span>5.13×</span>
    <small>avg · SF10</small>
  </div>
  <div class="tl-bench-stat">
    <span>866M</span>
    <small>vertices · SF100</small>
  </div>
</div>

Classic OLAP workload converted to Cypher via `SqlTranslator`. PK-FK relationships become graph edges. All 22 queries translated and executed natively.
</div>

<div class="tl-bench-card tl-bench-card--dbpedia">
<span class="tl-bench-label">DBpedia</span>
<span class="tl-bench-title">Schemaless Knowledge Graph · 20 queries</span>

<div class="tl-bench-stats">
  <div class="tl-bench-stat">
    <span>86.1×</span>
    <small>max speedup</small>
  </div>
  <div class="tl-bench-stat">
    <span>27.4×</span>
    <small>avg speedup</small>
  </div>
  <div class="tl-bench-stat">
    <span>227M</span>
    <small>edges</small>
  </div>
</div>

Real-world knowledge graph with 2,796 unique attributes and 282,764 unique attribute sets. Queries converted from SPARQL to Cypher.
</div>

</div>

### Average Speedup over All Competitors

<div class="tl-table-wrap">
<table class="tl-table">
<thead>
<tr>
  <th>Benchmark</th>
  <th>Scale Factor</th>
  <th>Avg Speedup (geomean)</th>
  <th>Max Speedup</th>
</tr>
</thead>
<tbody>
<tr><td rowspan="3"><strong>LDBC SNB</strong></td><td>SF1</td><td>4.07×</td><td>12.27×</td></tr>
<tr><td>SF10</td><td>11.81×</td><td>45.9×</td></tr>
<tr><td>SF100</td><td>29.78×</td><td>183.9×</td></tr>
<tr><td rowspan="3"><strong>TPC-H</strong></td><td>SF1</td><td>3.21×</td><td>20.3×</td></tr>
<tr><td>SF10</td><td>5.13×</td><td>44.6×</td></tr>
<tr><td>SF100</td><td>3.15×</td><td>15.7×</td></tr>
<tr><td><strong>DBpedia</strong></td><td>—</td><td>27.4×</td><td>86.1×</td></tr>
</tbody>
</table>
</div>

### Dataset Statistics

<div class="tl-table-wrap">
<table class="tl-table">
<thead>
<tr>
  <th>Dataset</th><th>SF</th><th>|V|</th><th>|E|</th><th>Relational</th><th>PGM</th>
</tr>
</thead>
<tbody>
<tr><td rowspan="3"><strong>LDBC SNB</strong></td><td>1</td><td>3.2M</td><td>17.3M</td><td>0.7 GB</td><td>1.3 GB</td></tr>
<tr><td>10</td><td>30.0M</td><td>177M</td><td>7.2 GB</td><td>14 GB</td></tr>
<tr><td>100</td><td>283M</td><td>1.78B</td><td>75 GB</td><td>140 GB</td></tr>
<tr><td rowspan="3"><strong>TPC-H</strong></td><td>1</td><td>8.66M</td><td>—</td><td>1.1 GB</td><td>1.7 GB</td></tr>
<tr><td>10</td><td>86.6M</td><td>—</td><td>11 GB</td><td>14 GB</td></tr>
<tr><td>100</td><td>866M</td><td>—</td><td>106 GB</td><td>141 GB</td></tr>
<tr><td><strong>DBpedia</strong></td><td>—</td><td>77M</td><td>227M</td><td>213 GB</td><td>19 GB</td></tr>
</tbody>
</table>
</div>

### Compared Systems

<div class="tl-table-wrap">
<table class="tl-table">
<thead>
<tr>
  <th>System</th><th>Type</th><th>Storage</th><th>Query Language</th><th>Version</th>
</tr>
</thead>
<tbody>
<tr class="tl-highlight">
  <td><strong>TurboLynx ★</strong></td>
  <td>Graph DB · OLAP</td>
  <td>Columnar</td>
  <td>Cypher</td>
  <td>—</td>
</tr>
<tr><td>GraphScope</td><td>Graph Engine · OLAP</td><td>Columnar</td><td>Cypher</td><td>v0.31.0</td></tr>
<tr><td>Memgraph</td><td>Graph DB</td><td>Row-based</td><td>Cypher</td><td>v3.0</td></tr>
<tr><td>Neo4j CE</td><td>Graph DB</td><td>Row-based</td><td>Cypher</td><td>v2025.03.0</td></tr>
<tr><td>Kuzu</td><td>Graph DB · OLAP</td><td>Columnar</td><td>Cypher</td><td>v0.8.2</td></tr>
<tr><td>DuckPGQ</td><td>RDBMS + PGQ ext.</td><td>Columnar</td><td>SQL/PGQ</td><td>947eb8d</td></tr>
<tr><td>Umbra</td><td>RDBMS · HTAP</td><td>Columnar</td><td>SQL</td><td>v25.07.1</td></tr>
<tr><td>DuckDB</td><td>RDBMS · OLAP</td><td>Columnar</td><td>SQL</td><td>v1.2.0</td></tr>
</tbody>
</table>
</div>

---

<!-- ══════════════════════════════════════════════════════
     FOOTER CTA
══════════════════════════════════════════════════════ -->

<div class="tl-footer-cta">
<h2>Ready to run at scale?</h2>
<p>Build TurboLynx in minutes on any Ubuntu 22.04 host. No Docker. No extra services.</p>
<div class="tl-footer-btns">
  <a href="documentation/getting-started/quickstart/" class="md-button md-button--primary tl-btn-primary">Get Started →</a>
  <a href="installation/overview/" class="md-button tl-btn-ghost">Installation Guide</a>
  <a href="documentation/cypher/overview/" class="md-button tl-btn-ghost">Cypher Reference</a>
</div>
</div>
