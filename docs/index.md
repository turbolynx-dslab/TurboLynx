---
hide:
  - navigation
  - toc
---

<div class="tl-home">

<!-- ══════════════════════════════════════════════════════
     HERO
══════════════════════════════════════════════════════ -->

<section class="tl-welcome">
<div class="tl-welcome-text">

<h1 class="tl-headline">
  TurboLynx is a fast<br>
  <span class="tl-type" data-strings="analytical|schemaless|in-process|open-source">analytical</span><br>
  graph database
</h1>

<p class="tl-subline">
  Query and analyze property graphs with Cypher —<br>
  from schemaless knowledge graphs to billion-edge social networks.
</p>

<div class="tl-actions">
  <a href="installation/overview/" class="tl-btn tl-btn--ghost tl-btn--arrow">Installation</a>
  <a href="documentation/getting-started/quickstart/" class="tl-btn tl-btn--yellow">Documentation</a>
</div>

</div>
<div class="tl-welcome-demo">

<div class="tl-window">
<div class="tl-window-bar">
  <span class="tl-dot tl-dot--r"></span>
  <span class="tl-dot tl-dot--y"></span>
  <span class="tl-dot tl-dot--g"></span>
  <span class="tl-window-title">query.cypher</span>
</div>
<div class="tl-window-code"><code><span class="tl-cmt">-- Multi-hop traversal on a schemaless knowledge graph</span>
<span class="tl-kw">MATCH</span>  (s:<span class="tl-var">Entity</span>)-[:<span class="tl-rel">RELATED*1..3</span>]->(t:<span class="tl-var">Entity</span>)
<span class="tl-kw">WHERE</span>  s.<span class="tl-prop">type</span> = <span class="tl-str">'Organization'</span>
<span class="tl-kw">RETURN</span> t.<span class="tl-prop">label</span>, <span class="tl-fn">count</span>(*) <span class="tl-kw">AS</span> n
<span class="tl-kw">ORDER BY</span> n <span class="tl-kw">DESC LIMIT</span> <span class="tl-num">10</span></code></div>
<div class="tl-window-footer">
  <span class="tl-window-meta">DBpedia · 227M edges · 2,796 unique attribute types</span>
  <a class="tl-btn tl-btn--sm tl-btn--yellow" href="live-demo/">Live demo</a>
</div>
</div>

</div>
</section>

<!-- ══════════════════════════════════════════════════════
     METRICS
══════════════════════════════════════════════════════ -->

<section class="tl-metrics">
<div class="tl-metric">
  <div class="tl-metric-n">183.9×</div>
  <div class="tl-metric-l">faster than Neo4j<br><small>LDBC SNB · SF100 · 1.78B edges</small></div>
</div>
<div class="tl-metric">
  <div class="tl-metric-n">41.3×</div>
  <div class="tl-metric-l">faster than DuckDB<br><small>graph analytical workloads</small></div>
</div>
<div class="tl-metric">
  <div class="tl-metric-n">VLDB 2026</div>
  <div class="tl-metric-l">peer-reviewed<br><small>Vol. 19, No. 6, pp. 1250–1263</small></div>
</div>
</section>

<!-- ══════════════════════════════════════════════════════
     WHY
══════════════════════════════════════════════════════ -->

<section class="tl-why">
<h2 class="tl-section-h">Why TurboLynx?</h2>
<div class="tl-pillars">

<div class="tl-pillar">
<div class="tl-pillar-icon">
<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="12" cy="12" r="3"/><circle cx="4" cy="6" r="2"/><circle cx="20" cy="6" r="2"/><circle cx="4" cy="18" r="2"/><circle cx="20" cy="18" r="2"/><line x1="6" y1="7" x2="10" y2="11"/><line x1="18" y1="7" x2="14" y2="11"/><line x1="6" y1="17" x2="10" y2="13"/><line x1="18" y1="17" x2="14" y2="13"/></svg>
</div>
<h3>Schemaless</h3>
<p>Nodes and edges carry different attributes with no predefined schema. Handles DBpedia's 2,796 unique attribute types without ETL or schema migration.</p>
<a href="documentation/getting-started/overview/">Read more</a>
</div>

<div class="tl-pillar">
<div class="tl-pillar-icon">
<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><polyline points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg>
</div>
<h3>Fast</h3>
<p>Extent-based columnar storage with zone-map pruning and SIMD vectorized execution. The GEM optimizer selects optimal join strategies automatically.</p>
<a href="documentation/internals/storage/">Read more</a>
</div>

<div class="tl-pillar">
<div class="tl-pillar-icon">
<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><rect x="3" y="3" width="18" height="18" rx="2"/><line x1="3" y1="9" x2="21" y2="9"/><line x1="9" y1="21" x2="9" y2="9"/></svg>
</div>
<h3>Analytical</h3>
<p>Group-by, aggregation, and multi-hop graph traversal in a single Cypher query. Outperforms both graph databases and RDBMSes on analytical workloads.</p>
<a href="documentation/cypher/overview/">Read more</a>
</div>

<div class="tl-pillar">
<div class="tl-pillar-icon">
<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"/></svg>
</div>
<h3>In-Process</h3>
<p>No daemon, no IPC. Embed TurboLynx directly in your application via C API — like DuckDB for graphs. Your billion-edge graph loads in milliseconds.</p>
<a href="installation/overview/">Read more</a>
</div>

</div>
</section>

<!-- ══════════════════════════════════════════════════════
     QUICK INSTALL
══════════════════════════════════════════════════════ -->

<section class="tl-install" id="quickinstall">
<h2 class="tl-section-h">Installation</h2>

<div class="tl-install-tabs">
<div class="tl-tab-bar">
  <button class="tl-tab active" data-tab="build">Build from source</button>
  <button class="tl-tab" data-tab="load">Load data</button>
  <button class="tl-tab" data-tab="query">Query</button>
</div>

<div class="tl-tab-content active" id="tab-build">

```bash
git clone https://github.com/postechdblab/TurboLynx turbograph-v3
cd turbograph-v3
cmake -GNinja -DCMAKE_BUILD_TYPE=Release \
      -DENABLE_TCMALLOC=OFF -DBUILD_UNITTESTS=OFF \
      -B build
ninja -C build
```

</div>
<div class="tl-tab-content" id="tab-load">

```bash
./build/tools/bulkload \
  --workspace /path/to/mydb \
  --vertices  Person  data/person.csv \
  --edges     KNOWS   data/knows.csv \
  --src srcId --dst dstId
```

</div>
<div class="tl-tab-content" id="tab-query">

```cypher
./build/tools/client --workspace /path/to/mydb

MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
WHERE a.firstName = 'Alice'
RETURN b.firstName, count(*) AS hops
ORDER BY hops ASC LIMIT 10;
```

</div>
</div>

<p class="tl-install-note">
  All dependencies are fetched automatically. Only <code>build-essential</code> and <code>cmake</code> required.
  &nbsp;<a href="installation/overview/">Full installation guide →</a>
</p>
</section>

</div>

<script>
/* Typewriter effect — smooth, no shared-variable stutter */
(function() {
  var el = document.querySelector('.tl-type');
  if (!el) return;
  var strings = el.getAttribute('data-strings').split('|');
  var idx = 0, pos = 0, deleting = false;

  var TYPE_MS   = 115;   /* ms per character while typing   */
  var DELETE_MS = 55;    /* ms per character while deleting */
  var HOLD_MS   = 2200;  /* pause at end of word            */
  var NEXT_MS   = 350;   /* pause before typing next word   */

  function type() {
    var word = strings[idx];
    if (!deleting) {
      pos++;
      el.textContent = word.slice(0, pos);
      if (pos === word.length) {
        deleting = true;
        setTimeout(erase, HOLD_MS);
      } else {
        setTimeout(type, TYPE_MS);
      }
    }
  }

  function erase() {
    var word = strings[idx];
    pos--;
    el.textContent = word.slice(0, pos);
    if (pos === 0) {
      deleting = false;
      idx = (idx + 1) % strings.length;
      setTimeout(type, NEXT_MS);
    } else {
      setTimeout(erase, DELETE_MS);
    }
  }

  setTimeout(type, 1400);
})();

/* Tab switcher */
(function() {
  document.querySelectorAll('.tl-tab').forEach(function(btn) {
    btn.addEventListener('click', function() {
      var tab = this.getAttribute('data-tab');
      this.closest('.tl-install-tabs').querySelectorAll('.tl-tab').forEach(function(b) { b.classList.remove('active'); });
      this.closest('.tl-install-tabs').querySelectorAll('.tl-tab-content').forEach(function(c) { c.classList.remove('active'); });
      this.classList.add('active');
      document.getElementById('tab-' + tab).classList.add('active');
    });
  });
})();
</script>
