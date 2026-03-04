---
hide:
  - navigation
  - toc
---

<!-- ══════════════════════════════════════════════════════
     HERO
══════════════════════════════════════════════════════ -->

<div class="tl-hero">
<div class="tl-hero-text">

<img src="assets/logo.png" alt="TurboLynx" class="tl-hero-logo">

<h1>Turbo<span>Lynx</span></h1>

<p class="tl-hero-tagline">
  Schemaless graph analytics,<br>
  at every layer.
</p>

<p class="tl-hero-sub">
  Real-world graphs are heterogeneous. TurboLynx treats schemalessness
  as a first-class requirement — across storage, execution, and optimization —
  delivering up to <strong>183.9×</strong> faster analytics than leading graph databases.
</p>

<div class="tl-hero-cta">
  <a href="documentation/getting-started/quickstart/" class="md-button md-button--primary tl-btn-primary">Get Started</a>
  <a href="https://github.com/postechdblab/TurboLynx" class="md-button tl-btn-ghost">GitHub</a>
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
<code class="tl-code-win-body"><span class="tl-cmt">-- DBpedia: 2,796 unique attribute types per entity</span>
<span class="tl-kw">MATCH</span> (s:<span class="tl-var">Entity</span>)-[:<span class="tl-rel">RELATED_TO*1..3</span>]->(t:<span class="tl-var">Entity</span>)
<span class="tl-kw">WHERE</span>  s.<span class="tl-prop">type</span> = <span class="tl-str">'Organization'</span>
<span class="tl-kw">RETURN</span> t.<span class="tl-prop">label</span>,
       <span class="tl-fn">count</span>(*) <span class="tl-kw">AS</span> connections
<span class="tl-kw">ORDER BY</span> connections <span class="tl-kw">DESC  LIMIT</span> <span class="tl-num">10</span>
</code>
<div class="tl-code-win-result">
  <div class="tl-result-label">77M nodes · 227M edges · schemaless knowledge graph</div>
  <div class="tl-result-perf">
    <span class="tl-perf-num">86.1×</span>
    <span class="tl-perf-label">faster than Neo4j</span>
  </div>
</div>
</div>

</div>
</div>

<!-- ══════════════════════════════════════════════════════
     STATS BAR
══════════════════════════════════════════════════════ -->

<div class="tl-stats">

<div class="tl-stat">
<span class="tl-stat-num">183.9×</span>
<span class="tl-stat-label">Max speedup over graph databases · LDBC SNB SF100</span>
</div>

<div class="tl-stat">
<span class="tl-stat-num">41.3×</span>
<span class="tl-stat-label">Max speedup over DuckDB and Umbra · TPC-H</span>
</div>

<div class="tl-stat">
<span class="tl-stat-num">7</span>
<span class="tl-stat-label">Systems benchmarked · VLDB 2026</span>
</div>

</div>

<!-- ══════════════════════════════════════════════════════
     CORE DIFFERENTIATORS
══════════════════════════════════════════════════════ -->

<div class="tl-feature-grid">

<div class="tl-feature-card">
<span class="tl-cat tl-cat-orange">Storage</span>

### Graphlets — Columnar, Schemaless

Most graph databases embed schema into each row, making vectorization impossible. TurboLynx groups vertices and edges with similar schemas into **graphlets** — columnar units that enable SIMD vectorization while remaining fully schemaless. No ETL, no schema migration.

</div>

<div class="tl-feature-card">
<span class="tl-cat tl-cat-blue">Execution</span>

### Multi-Schema Operators + SSRF

When schemas diverge across graphlets, intermediate results would normally explode in schema count. **SSRF (Shared Schema Row Format)** collapses this overhead with a unified header scheme — storing schema definitions once, referenced by rows — enabling efficient vectorized joins across heterogeneous data.

</div>

<div class="tl-feature-card">
<span class="tl-cat tl-cat-green">Optimizer</span>

### Orca + GEM Join Ordering

A cost-based optimizer ported from Greenplum RDBMS, augmented with graph-specific rules and the **GEM (Graphlet Early Merge)** strategy. Graphlets are merged before join enumeration — keeping the plan search space tractable while preserving schemaless flexibility.

</div>

</div>

<!-- ══════════════════════════════════════════════════════
     QUICK START
══════════════════════════════════════════════════════ -->

<div class="tl-section-label">QUICK START</div>

## Get Running in 3 Steps

=== "1 · Build"

    ```bash
    git clone https://github.com/postechdblab/TurboLynx turbograph-v3
    cd turbograph-v3
    cmake -GNinja -DCMAKE_BUILD_TYPE=Release \
          -DENABLE_TCMALLOC=OFF -DBUILD_UNITTESTS=OFF -DTBB_TEST=OFF \
          -B build
    ninja -C build
    ```

    All dependencies are fetched automatically at configure time.

=== "2 · Load"

    ```bash
    ./build/tools/bulkload \
      --workspace /path/to/mydb \
      --vertices  Person  data/person.csv \
      --edges     KNOWS   data/knows.csv \
      --src srcId --dst dstId
    ```

=== "3 · Query"

    ```bash
    ./build/tools/client --workspace /path/to/mydb
    ```

    ```cypher
    MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
    WHERE a.firstName = 'Alice'
    RETURN b.firstName, count(*) AS hops
    ORDER BY hops ASC LIMIT 10;
    ```

[Full Quickstart Guide →](documentation/getting-started/quickstart.md){ .md-button .md-button--primary }

---

<!-- ══════════════════════════════════════════════════════
     FOOTER CTA
══════════════════════════════════════════════════════ -->

<div class="tl-footer-cta">
<h2>Ready to run at scale?</h2>
<p>Build from source in minutes on any Ubuntu 22.04 host. Published at VLDB 2026.</p>
<div class="tl-footer-btns">
  <a href="documentation/getting-started/quickstart/" class="md-button md-button--primary tl-btn-primary">Get Started</a>
  <a href="https://github.com/postechdblab/TurboLynx" class="md-button tl-btn-ghost">GitHub</a>
</div>
</div>
