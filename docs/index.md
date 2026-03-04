---
hide:
  - navigation
  - toc
---

<div class="tl-home">

<!-- HERO -->
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
  <a href="documentation/getting-started/quickstart/" class="tl-btn tl-btn--primary">Documentation</a>
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
  <a class="tl-btn tl-btn--sm tl-btn--primary" href="live-demo/">Live demo</a>
</div>
</div>

</div>
</section>

<!-- METRICS -->
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

<!-- HOW IT WORKS — scroll-driven -->
<section class="tl-story">
<div class="tl-story-sticky">

  <!-- Left: text steps -->
  <div class="tl-story-text">

    <div class="tl-step active" data-step="0">
      <span class="tl-step-over">The Problem</span>
      <h3>Real-world graphs are heterogeneous</h3>
      <p>Nodes carry wildly different attributes. DBpedia has 2,796 unique attribute types and 282,764 unique attribute combinations. Traditional row-based graph databases embed schema into every row — making vectorization impossible and analytics slow.</p>
      <div class="tl-step-dots">
        <span class="tl-step-dot active"></span>
        <span class="tl-step-dot"></span>
        <span class="tl-step-dot"></span>
        <span class="tl-step-dot"></span>
      </div>
    </div>

    <div class="tl-step" data-step="1">
      <span class="tl-step-over">Storage Layer</span>
      <h3>Graphlets cluster similar schemas together</h3>
      <p>TurboLynx groups nodes with similar attribute sets into <strong>Graphlets</strong> — cost-based clusters stored in columnar format. Each Graphlet is independently SIMD-vectorizable. No per-row schema overhead. No null columns.</p>
      <div class="tl-step-dots">
        <span class="tl-step-dot"></span>
        <span class="tl-step-dot active"></span>
        <span class="tl-step-dot"></span>
        <span class="tl-step-dot"></span>
      </div>
    </div>

    <div class="tl-step" data-step="2">
      <span class="tl-step-over">Query Optimizer</span>
      <h3>One Cypher query becomes a multi-Graphlet plan</h3>
      <p>The <strong>GEM optimizer</strong> (Graphlet Early Merge) rewrites a single Cypher query into an efficient execution plan across all relevant Graphlets — automatically choosing index join, hash join, or merge join per Graphlet.</p>
      <div class="tl-step-dots">
        <span class="tl-step-dot"></span>
        <span class="tl-step-dot"></span>
        <span class="tl-step-dot active"></span>
        <span class="tl-step-dot"></span>
      </div>
    </div>

    <div class="tl-step" data-step="3">
      <span class="tl-step-over">Result</span>
      <h3>Up to 183.9× faster than the competition</h3>
      <p>Results stream from all Graphlets in parallel through the vectorized execution engine. TurboLynx outperforms Neo4j, Kuzu, Memgraph, GraphScope, DuckPGQ, Umbra, and DuckDB on LDBC SNB, TPC-H, and DBpedia benchmarks.</p>
      <div class="tl-step-dots">
        <span class="tl-step-dot"></span>
        <span class="tl-step-dot"></span>
        <span class="tl-step-dot"></span>
        <span class="tl-step-dot active"></span>
      </div>
    </div>

  </div>

  <!-- Right: SVG visualization -->
  <div class="tl-story-visual">
    <svg id="tl-story-svg" viewBox="0 0 520 440" fill="none" xmlns="http://www.w3.org/2000/svg">

      <!-- EDGES (drawn first, behind nodes) -->
      <g id="svg-edges" opacity="1">
        <line id="e0" x1="160" y1="100" x2="300" y2="180" stroke="rgba(255,255,255,0.12)" stroke-width="1.5"/>
        <line id="e1" x1="300" y1="180" x2="420" y2="130" stroke="rgba(255,255,255,0.12)" stroke-width="1.5"/>
        <line id="e2" x1="160" y1="100" x2="80"  y2="230" stroke="rgba(255,255,255,0.12)" stroke-width="1.5"/>
        <line id="e3" x1="80"  y1="230" x2="200" y2="320" stroke="rgba(255,255,255,0.12)" stroke-width="1.5"/>
        <line id="e4" x1="300" y1="180" x2="200" y2="320" stroke="rgba(255,255,255,0.12)" stroke-width="1.5"/>
        <line id="e5" x1="420" y1="130" x2="440" y2="280" stroke="rgba(255,255,255,0.12)" stroke-width="1.5"/>
        <line id="e6" x1="200" y1="320" x2="360" y2="370" stroke="rgba(255,255,255,0.12)" stroke-width="1.5"/>
        <line id="e7" x1="440" y1="280" x2="360" y2="370" stroke="rgba(255,255,255,0.12)" stroke-width="1.5"/>
      </g>

      <!-- NODES -->
      <g id="svg-nodes">
        <!-- Group A nodes (Person-like): red -->
        <circle id="n0" class="tl-node" cx="160" cy="100" r="18" fill="#C8102E" fill-opacity="0.9"/>
        <circle id="n1" class="tl-node" cx="80"  cy="230" r="14" fill="#C8102E" fill-opacity="0.7"/>
        <circle id="n2" class="tl-node" cx="200" cy="320" r="16" fill="#C8102E" fill-opacity="0.8"/>

        <!-- Group B nodes (Course-like): blue -->
        <circle id="n3" class="tl-node" cx="300" cy="180" r="18" fill="#5B8DEF" fill-opacity="0.9"/>
        <circle id="n4" class="tl-node" cx="420" cy="130" r="14" fill="#5B8DEF" fill-opacity="0.7"/>
        <circle id="n5" class="tl-node" cx="360" cy="370" r="15" fill="#5B8DEF" fill-opacity="0.8"/>

        <!-- Group C nodes (Event-like): green -->
        <circle id="n6" class="tl-node" cx="440" cy="280" r="16" fill="#50C878" fill-opacity="0.85"/>
        <circle id="n7" class="tl-node" cx="260" cy="60"  r="12" fill="#50C878" fill-opacity="0.7"/>
        <circle id="n8" class="tl-node" cx="100" cy="370" r="13" fill="#50C878" fill-opacity="0.75"/>
      </g>

      <!-- NODE ATTRIBUTE LABELS (Act 1: chaos) -->
      <g id="svg-attr-labels" opacity="1">
        <text x="132" y="92"  font-size="8" fill="rgba(245,240,240,0.65)" font-family="monospace">name</text>
        <text x="132" y="102" font-size="8" fill="rgba(245,240,240,0.65)" font-family="monospace">age</text>
        <text x="132" y="112" font-size="8" fill="rgba(245,240,240,0.65)" font-family="monospace">email</text>

        <text x="270" y="172" font-size="8" fill="rgba(245,240,240,0.65)" font-family="monospace">subject</text>
        <text x="270" y="182" font-size="8" fill="rgba(245,240,240,0.65)" font-family="monospace">year</text>
        <text x="270" y="192" font-size="8" fill="rgba(245,240,240,0.65)" font-family="monospace">room</text>

        <text x="410" y="272" font-size="8" fill="rgba(245,240,240,0.65)" font-family="monospace">timestamp</text>
        <text x="410" y="282" font-size="8" fill="rgba(245,240,240,0.65)" font-family="monospace">location</text>
      </g>

      <!-- GRAPHLET BOXES (Act 2: hidden initially) -->
      <g id="svg-graphlets" opacity="0">
        <!-- Graphlet A (red) -->
        <rect x="20" y="60" width="145" height="120" rx="8"
              fill="rgba(200,16,46,0.08)" stroke="#C8102E" stroke-width="1.5" stroke-opacity="0.7"/>
        <text x="30" y="78" font-size="9" fill="#C8102E" font-family="monospace" font-weight="700">Graphlet A</text>
        <line x1="20" y1="84" x2="165" y2="84" stroke="#C8102E" stroke-width="0.8" stroke-opacity="0.4"/>
        <text x="30" y="97"  font-size="8" fill="rgba(245,240,240,0.7)" font-family="monospace">name  │ age │ email</text>
        <text x="30" y="110" font-size="8" fill="rgba(245,240,240,0.5)" font-family="monospace">Alice │ 25  │ a@..</text>
        <text x="30" y="121" font-size="8" fill="rgba(245,240,240,0.5)" font-family="monospace">Bob   │ 31  │ b@..</text>
        <text x="30" y="132" font-size="8" fill="rgba(245,240,240,0.5)" font-family="monospace">Carol │ 28  │ c@..</text>
        <text x="30" y="143" font-size="8" fill="rgba(245,240,240,0.5)" font-family="monospace">Dave  │ 19  │ d@..</text>
        <text x="30" y="154" font-size="8" fill="rgba(245,240,240,0.5)" font-family="monospace">Eve   │ 34  │ e@..</text>
        <text x="30" y="167" font-size="7" fill="rgba(200,16,46,0.7)" font-family="monospace">⚡ columnar · SIMD ready</text>

        <!-- Graphlet B (blue) -->
        <rect x="185" y="60" width="160" height="130" rx="8"
              fill="rgba(91,141,239,0.08)" stroke="#5B8DEF" stroke-width="1.5" stroke-opacity="0.7"/>
        <text x="195" y="78" font-size="9" fill="#5B8DEF" font-family="monospace" font-weight="700">Graphlet B</text>
        <line x1="185" y1="84" x2="345" y2="84" stroke="#5B8DEF" stroke-width="0.8" stroke-opacity="0.4"/>
        <text x="195" y="97"  font-size="8" fill="rgba(245,240,240,0.7)" font-family="monospace">subject │ year │ room</text>
        <text x="195" y="110" font-size="8" fill="rgba(245,240,240,0.5)" font-family="monospace">Math    │ 2024 │  G4</text>
        <text x="195" y="121" font-size="8" fill="rgba(245,240,240,0.5)" font-family="monospace">CS      │ 2024 │  B1</text>
        <text x="195" y="132" font-size="8" fill="rgba(245,240,240,0.5)" font-family="monospace">History │ 2023 │  A2</text>
        <text x="195" y="143" font-size="8" fill="rgba(245,240,240,0.5)" font-family="monospace">Physics │ 2025 │  C3</text>
        <text x="195" y="167" font-size="7" fill="rgba(91,141,239,0.7)" font-family="monospace">⚡ columnar · SIMD ready</text>

        <!-- Graphlet C (green) -->
        <rect x="360" y="60" width="145" height="110" rx="8"
              fill="rgba(80,200,120,0.08)" stroke="#50C878" stroke-width="1.5" stroke-opacity="0.7"/>
        <text x="370" y="78" font-size="9" fill="#50C878" font-family="monospace" font-weight="700">Graphlet C</text>
        <line x1="360" y1="84" x2="505" y2="84" stroke="#50C878" stroke-width="0.8" stroke-opacity="0.4"/>
        <text x="370" y="97"  font-size="8" fill="rgba(245,240,240,0.7)" font-family="monospace">timestamp  │ location</text>
        <text x="370" y="110" font-size="8" fill="rgba(245,240,240,0.5)" font-family="monospace">2024-01-15 │ Atlanta</text>
        <text x="370" y="121" font-size="8" fill="rgba(245,240,240,0.5)" font-family="monospace">2024-02-03 │ Seoul</text>
        <text x="370" y="132" font-size="8" fill="rgba(245,240,240,0.5)" font-family="monospace">2024-03-22 │ Tokyo</text>
        <text x="370" y="154" font-size="7" fill="rgba(80,200,120,0.7)" font-family="monospace">⚡ columnar · SIMD ready</text>
      </g>

      <!-- QUERY PLAN (Act 3: hidden initially) -->
      <g id="svg-query" opacity="0">
        <!-- Cypher input -->
        <rect x="20" y="220" width="230" height="70" rx="6"
              fill="rgba(255,255,255,0.04)" stroke="rgba(255,255,255,0.12)" stroke-width="1"/>
        <text x="30" y="237" font-size="8" fill="rgba(245,240,240,0.5)" font-family="monospace">Cypher Query</text>
        <text x="30" y="251" font-size="8" fill="#ff79c6"  font-family="monospace">MATCH</text>
        <text x="60" y="251" font-size="8" fill="#e6edf3"  font-family="monospace">(n:Entity)</text>
        <text x="30" y="263" font-size="8" fill="#ff79c6"  font-family="monospace">WHERE</text>
        <text x="62" y="263" font-size="8" fill="#bd93f9"  font-family="monospace">n.name</text>
        <text x="100" y="263" font-size="8" fill="#e6edf3" font-family="monospace">= </text>
        <text x="108" y="263" font-size="8" fill="#f1fa8c" font-family="monospace">'Alice'</text>
        <text x="30" y="275" font-size="8" fill="#ff79c6"  font-family="monospace">RETURN</text>
        <text x="66" y="275" font-size="8" fill="#bd93f9"  font-family="monospace">n.name, n.age</text>

        <!-- GEM arrow -->
        <text x="128" y="308" font-size="8" fill="rgba(200,16,46,0.9)" font-family="monospace" text-anchor="middle">GEM Optimizer ↓</text>
        <line x1="128" y1="292" x2="128" y2="315" stroke="#C8102E" stroke-width="1.5" stroke-opacity="0.8"/>

        <!-- UNION ALL plan -->
        <rect x="20" y="318" width="230" height="100" rx="6"
              fill="rgba(200,16,46,0.06)" stroke="rgba(200,16,46,0.3)" stroke-width="1"/>
        <text x="30" y="333" font-size="7.5" fill="rgba(245,240,240,0.5)" font-family="monospace">Execution Plan</text>
        <text x="30" y="347" font-size="7.5" fill="#50fa7b" font-family="monospace">SELECT</text>
        <text x="62" y="347" font-size="7.5" fill="#e6edf3" font-family="monospace">name, age</text>
        <text x="30" y="358" font-size="7.5" fill="#50fa7b" font-family="monospace">FROM</text>
        <text x="55" y="358" font-size="7.5" fill="#C8102E" font-family="monospace">graphlet_A</text>
        <text x="115" y="358" font-size="7.5" fill="#e6edf3" font-family="monospace">WHERE ...</text>
        <text x="30" y="370" font-size="7.5" fill="#ff79c6" font-family="monospace">  UNION ALL</text>
        <text x="30" y="381" font-size="7.5" fill="#50fa7b" font-family="monospace">SELECT</text>
        <text x="62" y="381" font-size="7.5" fill="#e6edf3" font-family="monospace">name, NULL</text>
        <text x="30" y="392" font-size="7.5" fill="#50fa7b" font-family="monospace">FROM</text>
        <text x="55" y="392" font-size="7.5" fill="#5B8DEF" font-family="monospace">graphlet_B</text>
        <text x="115" y="392" font-size="7.5" fill="#e6edf3" font-family="monospace">WHERE ...</text>
        <text x="30" y="404" font-size="7.5" fill="#ff79c6" font-family="monospace">  UNION ALL  ...</text>

        <!-- Graphlet highlight boxes (referencing Act 2 boxes) -->
        <rect x="270" y="220" width="235" height="200" rx="8"
              fill="rgba(200,16,46,0.05)" stroke="rgba(200,16,46,0.2)" stroke-width="1" stroke-dasharray="4 3"/>
        <text x="387" y="315" font-size="9" fill="rgba(245,240,240,0.4)" text-anchor="middle" font-family="monospace">scanning all graphlets</text>
        <!-- Animated scan lines on graphlets -->
        <line x1="270" y1="270" x2="505" y2="270" stroke="#C8102E" stroke-width="1" stroke-opacity="0.4" stroke-dasharray="3 4"/>
        <line x1="270" y1="300" x2="505" y2="300" stroke="#C8102E" stroke-width="1" stroke-opacity="0.3" stroke-dasharray="3 4"/>
        <line x1="270" y1="330" x2="505" y2="330" stroke="#C8102E" stroke-width="1" stroke-opacity="0.2" stroke-dasharray="3 4"/>
      </g>

      <!-- RESULT (Act 4: hidden initially) -->
      <g id="svg-result" opacity="0">
        <!-- Converging arrows from graphlets -->
        <line x1="165" y1="200" x2="255" y2="310" stroke="#C8102E" stroke-width="1.5" stroke-opacity="0.6"/>
        <line x1="345" y1="200" x2="290" y2="310" stroke="#5B8DEF" stroke-width="1.5" stroke-opacity="0.6"/>
        <line x1="505" y1="200" x2="340" y2="310" stroke="#50C878" stroke-width="1.5" stroke-opacity="0.6"/>

        <!-- Result table -->
        <rect x="200" y="310" width="180" height="110" rx="8"
              fill="rgba(255,255,255,0.04)" stroke="rgba(255,255,255,0.15)" stroke-width="1.5"/>
        <text x="215" y="326" font-size="8" fill="rgba(245,240,240,0.5)" font-family="monospace">Result</text>
        <line x1="200" y1="332" x2="380" y2="332" stroke="rgba(255,255,255,0.1)" stroke-width="1"/>
        <text x="215" y="344" font-size="8" fill="rgba(245,240,240,0.6)" font-family="monospace">name    │ age</text>
        <line x1="200" y1="349" x2="380" y2="349" stroke="rgba(255,255,255,0.07)" stroke-width="1"/>
        <text x="215" y="362" font-size="8" fill="#50fa7b" font-family="monospace" id="r0" opacity="0">Alice   │  25</text>
        <text x="215" y="374" font-size="8" fill="#50fa7b" font-family="monospace" id="r1" opacity="0">Alice_2 │  32</text>
        <text x="215" y="386" font-size="8" fill="#50fa7b" font-family="monospace" id="r2" opacity="0">Alice_3 │  19</text>

        <!-- Timing badge -->
        <rect x="300" y="388" width="75" height="22" rx="5"
              fill="rgba(200,16,46,0.15)" stroke="#C8102E" stroke-width="1" stroke-opacity="0.5"/>
        <text x="312" y="402" font-size="8.5" fill="#E8193A" font-family="monospace" font-weight="700" id="perf-badge" opacity="0">✓ 2 ms</text>

        <!-- Big performance number -->
        <text x="290" y="290" font-size="28" font-weight="900" fill="#C8102E"
              text-anchor="middle" font-family="Inter, sans-serif"
              id="perf-num" opacity="0">183.9×</text>
        <text x="290" y="305" font-size="10" fill="rgba(245,240,240,0.45)"
              text-anchor="middle" font-family="monospace"
              id="perf-label" opacity="0">faster than Neo4j</text>
      </g>

    </svg>
  </div>

</div>
</section>

<!-- divider -->
<div class="tl-divider"></div>

<!-- WHY -->
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

<!-- INSTALL -->
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
git clone https://github.com/postech-dblab-iitp/turbograph-v3
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
/* ── Typewriter ────────────────────────────────────────────────── */
(function() {
  var el = document.querySelector('.tl-type');
  if (!el) return;
  var strings = el.getAttribute('data-strings').split('|');
  var idx = 0, pos = 0;
  var TYPE_MS = 115, DELETE_MS = 55, HOLD_MS = 2200, NEXT_MS = 350;
  function type() {
    var word = strings[idx];
    pos++;
    el.textContent = word.slice(0, pos);
    if (pos === word.length) { setTimeout(erase, HOLD_MS); }
    else { setTimeout(type, TYPE_MS); }
  }
  function erase() {
    var word = strings[idx];
    pos--;
    el.textContent = word.slice(0, pos);
    if (pos === 0) { idx = (idx + 1) % strings.length; setTimeout(type, NEXT_MS); }
    else { setTimeout(erase, DELETE_MS); }
  }
  setTimeout(type, 1400);
})();

/* ── Tab switcher ──────────────────────────────────────────────── */
(function() {
  document.querySelectorAll('.tl-tab').forEach(function(btn) {
    btn.addEventListener('click', function() {
      var tab = this.getAttribute('data-tab');
      var wrap = this.closest('.tl-install-tabs');
      wrap.querySelectorAll('.tl-tab').forEach(function(b) { b.classList.remove('active'); });
      wrap.querySelectorAll('.tl-tab-content').forEach(function(c) { c.classList.remove('active'); });
      this.classList.add('active');
      document.getElementById('tab-' + tab).classList.add('active');
    });
  });
})();

/* ── Scroll-driven story ───────────────────────────────────────── */
(function() {
  var story   = document.querySelector('.tl-story');
  var steps   = document.querySelectorAll('.tl-step');
  var dots    = document.querySelectorAll('.tl-step-dots');
  if (!story || !steps.length) return;

  /* SVG element references */
  var edges     = document.getElementById('svg-edges');
  var attrLbls  = document.getElementById('svg-attr-labels');
  var graphlets = document.getElementById('svg-graphlets');
  var query     = document.getElementById('svg-query');
  var result    = document.getElementById('svg-result');
  var nodes     = [
    document.getElementById('n0'), document.getElementById('n1'), document.getElementById('n2'),
    document.getElementById('n3'), document.getElementById('n4'), document.getElementById('n5'),
    document.getElementById('n6'), document.getElementById('n7'), document.getElementById('n8')
  ];

  /* Node target positions per act */
  var positions = {
    0: [ /* Act 1: scattered */
      [160,100],[80,230],[200,320],
      [300,180],[420,130],[360,370],
      [440,280],[260,60],[100,370]
    ],
    1: [ /* Act 2: clustered by group (inside graphlet boxes) */
      [65,115],[90,128],[65,141],      /* Group A → left box */
      [230,115],[255,128],[230,141],   /* Group B → middle box */
      [405,115],[430,128],[405,141]    /* Group C → right box */
    ],
    2: [ /* Act 3: stay clustered, fade */
      [65,115],[90,128],[65,141],
      [230,115],[255,128],[230,141],
      [405,115],[430,128],[405,141]
    ],
    3: [ /* Act 4: converge toward result */
      [230,280],[230,290],[230,300],
      [290,280],[290,290],[290,300],
      [350,280],[350,290],[350,300]
    ]
  };

  var current = -1;

  function setStep(s) {
    if (s === current) return;
    current = s;

    /* Text panels */
    steps.forEach(function(el, i) {
      el.classList.toggle('active', i === s);
    });

    /* Move nodes */
    var pos = positions[s] || positions[0];
    nodes.forEach(function(n, i) {
      if (!n || !pos[i]) return;
      n.setAttribute('cx', pos[i][0]);
      n.setAttribute('cy', pos[i][1]);
    });

    /* Layer visibility */
    if (s === 0) {
      fade(edges,     1); fade(attrLbls,  1);
      fade(graphlets, 0); fade(query,     0); fade(result, 0);
      nodes.forEach(function(n) { if(n) n.setAttribute('opacity','1'); });
    } else if (s === 1) {
      fade(edges,     0); fade(attrLbls,  0);
      fade(graphlets, 1); fade(query,     0); fade(result, 0);
      nodes.forEach(function(n) { if(n) n.setAttribute('opacity','0.6'); });
    } else if (s === 2) {
      fade(edges,     0); fade(attrLbls,  0);
      fade(graphlets, 0.3); fade(query,   1); fade(result, 0);
      nodes.forEach(function(n) { if(n) n.setAttribute('opacity','0.3'); });
    } else if (s === 3) {
      fade(edges,     0); fade(attrLbls,  0);
      fade(graphlets, 0); fade(query,     0); fade(result, 1);
      nodes.forEach(function(n) { if(n) n.setAttribute('opacity','0.5'); });
      /* Cascade result rows */
      setTimeout(function() { setOpacity('r0', 1); }, 300);
      setTimeout(function() { setOpacity('r1', 1); }, 600);
      setTimeout(function() { setOpacity('r2', 1); }, 900);
      setTimeout(function() { setOpacity('perf-badge', 1); }, 1200);
      setTimeout(function() { setOpacity('perf-num', 1); setOpacity('perf-label', 1); }, 1600);
    }
  }

  function fade(el, val) {
    if (el) el.style.transition = 'opacity 0.55s ease';
    if (el) el.style.opacity = val;
  }

  function setOpacity(id, val) {
    var el = document.getElementById(id);
    if (el) { el.style.transition = 'opacity 0.4s ease'; el.style.opacity = val; }
  }

  /* Scroll handler */
  var STEPS = [0, 0.22, 0.50, 0.78];

  window.addEventListener('scroll', function() {
    var rect     = story.getBoundingClientRect();
    var scrollH  = story.offsetHeight - window.innerHeight;
    if (scrollH <= 0) return;
    var progress = Math.max(0, Math.min(1, -rect.top / scrollH));

    var s = 0;
    for (var i = STEPS.length - 1; i >= 0; i--) {
      if (progress >= STEPS[i]) { s = i; break; }
    }
    setStep(s);
  }, { passive: true });

  /* Init */
  setStep(0);
})();
</script>
