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
  <span class="tl-type" data-strings="analytical|schemaless|embedded|open-source">analytical</span><br>
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
      <h3>Same label. Different schemas.</h3>
      <p>All these nodes are labeled <code>Person</code> and connected by <code>:knows</code> edges. But each carries a different attribute set. Store them in one row-table and <strong id="tl-null-count">54 of 80 cells</strong> become NULLs — 67% wasted.</p>
      <div class="tl-step-dots">
        <span class="tl-step-dot active"></span>
        <span class="tl-step-dot"></span>
        <span class="tl-step-dot"></span>
      </div>
    </div>

    <div class="tl-step" data-step="1">
      <span class="tl-step-over">Storage Layer</span>
      <h3>Graphlets eliminate NULLs</h3>
      <p>TurboLynx groups nodes with similar attribute sets into <strong>Graphlets</strong> — compact columnar tables with no NULLs, SIMD-vectorizable. The CGC algorithm picks the cost-optimal partition automatically.</p>
      <div class="tl-null-badge">54 NULLs → <span id="tl-null-after">4</span></div>
      <div class="tl-step-dots">
        <span class="tl-step-dot"></span>
        <span class="tl-step-dot active"></span>
        <span class="tl-step-dot"></span>
      </div>
    </div>

    <div class="tl-step" data-step="2">
      <span class="tl-step-over">Query Engine</span>
      <h3>One Cypher. Per-graphlet join orders.</h3>
      <p>TurboLynx compiles Cypher to an operator tree where each label scan becomes a <code>UNION ALL</code> across matching graphlets. Then <strong>GEM</strong> pushes the join <em>below</em> the <code>UNION ALL</code> — each graphlet group gets its own cost-optimal join order.</p>
      <div class="tl-step-dots">
        <span class="tl-step-dot"></span>
        <span class="tl-step-dot"></span>
        <span class="tl-step-dot active"></span>
      </div>
    </div>

  </div>

  <!-- Right: SVG visualization -->
  <div class="tl-story-visual">
    <svg id="tl-story-svg" viewBox="0 0 520 430" fill="none" xmlns="http://www.w3.org/2000/svg">

      <!-- ══════════════════════════════════════════════════
           ACT 1: Person-knows-Person graph
           ══════════════════════════════════════════════════ -->
      <g id="act1">

        <!-- :knows edges -->
        <g id="a1-edges">
          <line x1="90"  y1="75"  x2="240" y2="52"  stroke="rgba(255,255,255,0.13)" stroke-width="1.5"/>
          <line x1="240" y1="52"  x2="400" y2="85"  stroke="rgba(255,255,255,0.13)" stroke-width="1.5"/>
          <line x1="90"  y1="75"  x2="185" y2="168" stroke="rgba(255,255,255,0.13)" stroke-width="1.5"/>
          <line x1="240" y1="52"  x2="185" y2="168" stroke="rgba(255,255,255,0.13)" stroke-width="1.5"/>
          <line x1="50"  y1="188" x2="185" y2="168" stroke="rgba(255,255,255,0.13)" stroke-width="1.5"/>
          <line x1="185" y1="168" x2="355" y2="183" stroke="rgba(255,255,255,0.13)" stroke-width="1.5"/>
          <line x1="400" y1="85"  x2="355" y2="183" stroke="rgba(255,255,255,0.13)" stroke-width="1.5"/>
          <line x1="50"  y1="188" x2="118" y2="308" stroke="rgba(255,255,255,0.13)" stroke-width="1.5"/>
          <line x1="118" y1="308" x2="295" y2="298" stroke="rgba(255,255,255,0.13)" stroke-width="1.5"/>
          <line x1="185" y1="168" x2="295" y2="298" stroke="rgba(255,255,255,0.13)" stroke-width="1.5"/>
          <!-- :knows label on one visible edge -->
          <text x="128" y="116" font-size="8" fill="rgba(255,180,100,0.7)" font-family="monospace" transform="rotate(-22,128,116)">:knows</text>
        </g>

        <!-- Attribute badges (small labels near nodes) -->
        <g id="a1-attrs" font-size="7.5" font-family="monospace" fill="rgba(245,240,240,0.55)">
          <!-- n0 -->
          <text x="108" y="67">FN, LN, age</text>
          <!-- n1 -->
          <text x="255" y="43">FN, gender</text>
          <!-- n2 -->
          <text x="415" y="78">major, name</text>
          <!-- n3 -->
          <text x="12"  y="192">FN, LN</text>
          <!-- n4 -->
          <text x="198" y="162">FN, LN, gender</text>
          <!-- n5 -->
          <text x="368" y="177">name, bday</text>
          <!-- n6 -->
          <text x="130" y="320">gender, major</text>
          <!-- n7 -->
          <text x="308" y="290">FN, age</text>
        </g>

        <!-- "Person" label boxes above a few nodes -->
        <g id="a1-labels" font-size="7" font-family="monospace" font-weight="700">
          <rect x="64"  y="52" width="44" height="13" rx="3" fill="rgba(200,16,46,0.25)" stroke="rgba(200,16,46,0.5)" stroke-width="0.8"/>
          <text x="68"  y="62" fill="#E8193A">Person</text>
          <rect x="214" y="29" width="44" height="13" rx="3" fill="rgba(200,16,46,0.25)" stroke="rgba(200,16,46,0.5)" stroke-width="0.8"/>
          <text x="218" y="39" fill="#E8193A">Person</text>
          <rect x="374" y="62" width="44" height="13" rx="3" fill="rgba(200,16,46,0.25)" stroke="rgba(200,16,46,0.5)" stroke-width="0.8"/>
          <text x="378" y="72" fill="#E8193A">Person</text>
        </g>

      </g>

      <!-- ══════════════════════════════════════════════════
           ACT 2: Graphlet boxes (hidden initially)
           ══════════════════════════════════════════════════ -->
      <g id="act2" opacity="0">

        <!-- gl₁ (blue) — FN, LN, age -->
        <rect x="8"  y="22" width="118" height="185" rx="8"
              fill="rgba(91,141,239,0.07)" stroke="#5B8DEF" stroke-width="1.5" stroke-opacity="0.7"/>
        <text x="18" y="40" font-size="9" fill="#5B8DEF" font-family="monospace" font-weight="700">gl₁</text>
        <line x1="8" y1="46" x2="126" y2="46" stroke="#5B8DEF" stroke-width="0.8" stroke-opacity="0.4"/>
        <text x="14" y="58" font-size="7.5" fill="rgba(245,240,240,0.65)" font-family="monospace">FN   │ LN   │age</text>
        <line x1="8" y1="63" x2="126" y2="63" stroke="rgba(255,255,255,0.08)" stroke-width="0.8"/>
        <text x="14" y="75"  font-size="7.5" fill="rgba(245,240,240,0.5)" font-family="monospace">Ana  │ Kim  │ 25</text>
        <text x="14" y="87"  font-size="7.5" fill="rgba(245,240,240,0.5)" font-family="monospace">Eve  │ Park │ 31</text>
        <text x="14" y="99"  font-size="7.5" fill="rgba(245,240,240,0.5)" font-family="monospace">Min  │ Lee  │ 28</text>
        <text x="14" y="195" font-size="7" fill="rgba(91,141,239,0.7)" font-family="monospace">⚡ columnar · no NULLs</text>

        <!-- gl₂ (purple) — FN, LN, gender -->
        <rect x="140" y="22" width="118" height="185" rx="8"
              fill="rgba(155,114,207,0.07)" stroke="#9B72CF" stroke-width="1.5" stroke-opacity="0.7"/>
        <text x="150" y="40" font-size="9" fill="#9B72CF" font-family="monospace" font-weight="700">gl₂</text>
        <line x1="140" y1="46" x2="258" y2="46" stroke="#9B72CF" stroke-width="0.8" stroke-opacity="0.4"/>
        <text x="146" y="58" font-size="7.5" fill="rgba(245,240,240,0.65)" font-family="monospace">FN   │ LN  │gender</text>
        <line x1="140" y1="63" x2="258" y2="63" stroke="rgba(255,255,255,0.08)" stroke-width="0.8"/>
        <text x="146" y="75"  font-size="7.5" fill="rgba(245,240,240,0.5)" font-family="monospace">Bob  │Chen │  M</text>
        <text x="146" y="87"  font-size="7.5" fill="rgba(245,240,240,0.5)" font-family="monospace">Tom  │ Wu  │  M</text>
        <text x="146" y="195" font-size="7" fill="rgba(155,114,207,0.7)" font-family="monospace">⚡ columnar · no NULLs</text>

        <!-- gl₃ (amber) — gender, major, name -->
        <rect x="272" y="22" width="118" height="185" rx="8"
              fill="rgba(245,166,35,0.07)" stroke="#F5A623" stroke-width="1.5" stroke-opacity="0.7"/>
        <text x="282" y="40" font-size="9" fill="#F5A623" font-family="monospace" font-weight="700">gl₃</text>
        <line x1="272" y1="46" x2="390" y2="46" stroke="#F5A623" stroke-width="0.8" stroke-opacity="0.4"/>
        <text x="278" y="58" font-size="7.5" fill="rgba(245,240,240,0.65)" font-family="monospace">gender│major│name</text>
        <line x1="272" y1="63" x2="390" y2="63" stroke="rgba(255,255,255,0.08)" stroke-width="0.8"/>
        <text x="278" y="75"  font-size="7.5" fill="rgba(245,240,240,0.5)" font-family="monospace">  F   │ CS  │ Eva</text>
        <text x="278" y="87"  font-size="7.5" fill="rgba(245,240,240,0.5)" font-family="monospace">  M   │ EE  │ Jin</text>
        <text x="278" y="195" font-size="7" fill="rgba(245,166,35,0.7)" font-family="monospace">⚡ columnar · no NULLs</text>

        <!-- gl₄ (green) — name, birthday -->
        <rect x="404" y="22" width="108" height="185" rx="8"
              fill="rgba(80,200,120,0.07)" stroke="#50C878" stroke-width="1.5" stroke-opacity="0.7"/>
        <text x="414" y="40" font-size="9" fill="#50C878" font-family="monospace" font-weight="700">gl₄</text>
        <line x1="404" y1="46" x2="512" y2="46" stroke="#50C878" stroke-width="0.8" stroke-opacity="0.4"/>
        <text x="410" y="58" font-size="7.5" fill="rgba(245,240,240,0.65)" font-family="monospace">name │ bday</text>
        <line x1="404" y1="63" x2="512" y2="63" stroke="rgba(255,255,255,0.08)" stroke-width="0.8"/>
        <text x="410" y="75"  font-size="7.5" fill="rgba(245,240,240,0.5)" font-family="monospace">Eva  │ 1998</text>
        <text x="410" y="87"  font-size="7.5" fill="rgba(245,240,240,0.5)" font-family="monospace">Mia  │ 2001</text>
        <text x="410" y="195" font-size="7" fill="rgba(80,200,120,0.7)" font-family="monospace">⚡ no NULLs</text>

        <!-- NULL counter -->
        <text x="260" y="250" font-size="13" font-weight="700" fill="rgba(245,240,240,0.85)"
              text-anchor="middle" font-family="Inter, sans-serif">
          <tspan fill="#C8102E" id="a2-null-from">54</tspan>
          <tspan fill="rgba(245,240,240,0.5)"> NULLs  →  </tspan>
          <tspan fill="#50C878" id="a2-null-to">4</tspan>
          <tspan fill="rgba(245,240,240,0.5)"> NULLs</tspan>
        </text>
        <text x="260" y="268" font-size="9" fill="rgba(245,240,240,0.4)"
              text-anchor="middle" font-family="monospace">93% storage efficiency</text>

      </g>

      <!-- ══════════════════════════════════════════════════
           ACT 3: Operator tree with GEM
           ══════════════════════════════════════════════════ -->
      <g id="act3" opacity="0">

        <!-- Cypher query box -->
        <rect x="8" y="8" width="504" height="68" rx="7"
              fill="rgba(255,255,255,0.03)" stroke="rgba(255,255,255,0.1)" stroke-width="1"/>
        <text x="18" y="23" font-size="7.5" fill="rgba(245,240,240,0.4)" font-family="monospace">query.cypher</text>
        <text x="18" y="37" font-size="9" font-family="monospace">
          <tspan fill="#ff79c6">MATCH </tspan>
          <tspan fill="#e6edf3">(p:</tspan><tspan fill="#8be9fd">Person</tspan><tspan fill="#e6edf3">)-[:</tspan><tspan fill="#ffb86c">knows</tspan><tspan fill="#e6edf3">]-&gt;(q:</tspan><tspan fill="#8be9fd">Person</tspan><tspan fill="#e6edf3">)</tspan>
        </text>
        <text x="18" y="52" font-size="9" font-family="monospace">
          <tspan fill="#ff79c6">RETURN </tspan><tspan fill="#bd93f9">p.FN</tspan><tspan fill="#e6edf3">, </tspan><tspan fill="#bd93f9">q.FN</tspan>
        </text>

        <!-- Arrow + label -->
        <line x1="260" y1="78" x2="260" y2="100" stroke="#C8102E" stroke-width="1.5" stroke-opacity="0.7"/>
        <polygon points="255,100 265,100 260,108" fill="#C8102E" fill-opacity="0.7"/>
        <text x="260" y="97" font-size="8" fill="rgba(200,16,46,0.8)" text-anchor="middle" font-family="monospace">TurboLynx Optimizer</text>

        <!-- ── Operator tree ── -->

        <!-- π node -->
        <circle cx="260" cy="128" r="20" fill="rgba(200,16,46,0.12)" stroke="#C8102E" stroke-width="1.5"/>
        <text x="260" y="132" font-size="11" fill="#E8193A" text-anchor="middle" font-family="monospace">π</text>

        <!-- π → UNION ALL line -->
        <line x1="260" y1="148" x2="260" y2="172" stroke="rgba(255,255,255,0.2)" stroke-width="1.5"/>

        <!-- UNION ALL node (GEM splits here) -->
        <rect x="185" y="172" width="150" height="26" rx="5"
              fill="rgba(200,16,46,0.1)" stroke="#C8102E" stroke-width="1.5" stroke-opacity="0.8"/>
        <text x="260" y="189" font-size="8.5" fill="#E8193A" text-anchor="middle" font-family="monospace" font-weight="700">UNION ALL</text>

        <!-- GEM badge -->
        <rect x="344" y="172" width="40" height="16" rx="4" fill="rgba(200,16,46,0.2)" stroke="#C8102E" stroke-width="0.8"/>
        <text x="364" y="183" font-size="7" fill="#E8193A" text-anchor="middle" font-family="monospace" font-weight="700">GEM</text>

        <!-- UNION ALL → left ⋈ -->
        <line x1="218" y1="198" x2="145" y2="228" stroke="rgba(255,255,255,0.2)" stroke-width="1.5"/>
        <!-- UNION ALL → right ⋈ -->
        <line x1="302" y1="198" x2="375" y2="228" stroke="rgba(255,255,255,0.2)" stroke-width="1.5"/>

        <!-- Left ⋈ (gl₁, gl₂ group) -->
        <circle cx="140" cy="244" r="20" fill="rgba(91,141,239,0.12)" stroke="#5B8DEF" stroke-width="1.5"/>
        <text x="140" y="249" font-size="12" fill="#5B8DEF" text-anchor="middle" font-family="monospace">⋈</text>

        <!-- Right ⋈ (gl₃, gl₄ group) -->
        <circle cx="380" cy="244" r="20" fill="rgba(245,166,35,0.12)" stroke="#F5A623" stroke-width="1.5"/>
        <text x="380" y="249" font-size="12" fill="#F5A623" text-anchor="middle" font-family="monospace">⋈</text>

        <!-- Left ⋈ branches: p / :knows / q -->
        <line x1="124" y1="262" x2="80"  y2="300" stroke="rgba(255,255,255,0.15)" stroke-width="1.2"/>
        <line x1="140" y1="264" x2="140" y2="300" stroke="rgba(255,255,255,0.15)" stroke-width="1.2"/>
        <line x1="156" y1="262" x2="200" y2="300" stroke="rgba(255,255,255,0.15)" stroke-width="1.2"/>

        <!-- Left leaf labels -->
        <rect x="48"  y="300" width="64" height="38" rx="5" fill="rgba(91,141,239,0.1)" stroke="#5B8DEF" stroke-width="1" stroke-opacity="0.6"/>
        <text x="80"  y="314" font-size="7.5" fill="#5B8DEF" text-anchor="middle" font-family="monospace" font-weight="700">p:Person</text>
        <text x="80"  y="328" font-size="7"   fill="rgba(245,240,240,0.45)" text-anchor="middle" font-family="monospace">gl₁, gl₂</text>

        <rect x="108" y="300" width="64" height="38" rx="5" fill="rgba(255,255,255,0.04)" stroke="rgba(255,255,255,0.15)" stroke-width="1"/>
        <text x="140" y="314" font-size="7.5" fill="rgba(255,180,100,0.85)" text-anchor="middle" font-family="monospace" font-weight="700">:knows</text>
        <text x="140" y="328" font-size="7"   fill="rgba(245,240,240,0.4)"  text-anchor="middle" font-family="monospace">Scan</text>

        <rect x="168" y="300" width="64" height="38" rx="5" fill="rgba(91,141,239,0.1)" stroke="#5B8DEF" stroke-width="1" stroke-opacity="0.6"/>
        <text x="200" y="314" font-size="7.5" fill="#5B8DEF" text-anchor="middle" font-family="monospace" font-weight="700">q:Person</text>
        <text x="200" y="328" font-size="7"   fill="rgba(245,240,240,0.45)" text-anchor="middle" font-family="monospace">gl₁, gl₂</text>

        <!-- Right ⋈ branches: p / :knows / q -->
        <line x1="364" y1="262" x2="320" y2="300" stroke="rgba(255,255,255,0.15)" stroke-width="1.2"/>
        <line x1="380" y1="264" x2="380" y2="300" stroke="rgba(255,255,255,0.15)" stroke-width="1.2"/>
        <line x1="396" y1="262" x2="440" y2="300" stroke="rgba(255,255,255,0.15)" stroke-width="1.2"/>

        <!-- Right leaf labels -->
        <rect x="288" y="300" width="64" height="38" rx="5" fill="rgba(245,166,35,0.1)" stroke="#F5A623" stroke-width="1" stroke-opacity="0.6"/>
        <text x="320" y="314" font-size="7.5" fill="#F5A623" text-anchor="middle" font-family="monospace" font-weight="700">p:Person</text>
        <text x="320" y="328" font-size="7"   fill="rgba(245,240,240,0.45)" text-anchor="middle" font-family="monospace">gl₃, gl₄</text>

        <rect x="348" y="300" width="64" height="38" rx="5" fill="rgba(255,255,255,0.04)" stroke="rgba(255,255,255,0.15)" stroke-width="1"/>
        <text x="380" y="314" font-size="7.5" fill="rgba(255,180,100,0.85)" text-anchor="middle" font-family="monospace" font-weight="700">:knows</text>
        <text x="380" y="328" font-size="7"   fill="rgba(245,240,240,0.4)"  text-anchor="middle" font-family="monospace">Scan</text>

        <rect x="408" y="300" width="64" height="38" rx="5" fill="rgba(245,166,35,0.1)" stroke="#F5A623" stroke-width="1" stroke-opacity="0.6"/>
        <text x="440" y="314" font-size="7.5" fill="#F5A623" text-anchor="middle" font-family="monospace" font-weight="700">q:Person</text>
        <text x="440" y="328" font-size="7"   fill="rgba(245,240,240,0.45)" text-anchor="middle" font-family="monospace">gl₃, gl₄</text>

        <!-- Annotation -->
        <text x="260" y="390" font-size="8.5" fill="rgba(245,240,240,0.4)" text-anchor="middle" font-family="monospace">each graphlet group: own cost-optimal join order</text>

      </g>

      <!-- ══════════════════════════════════════════════════
           SHARED NODES — transition between acts via JS
           ══════════════════════════════════════════════════ -->
      <g id="svg-nodes">
        <!-- All Person — same color since same label -->
        <circle id="n0" class="tl-node" cx="90"  cy="75"  r="16" fill="#C8102E" fill-opacity="0.85"/>
        <circle id="n1" class="tl-node" cx="240" cy="52"  r="16" fill="#C8102E" fill-opacity="0.85"/>
        <circle id="n2" class="tl-node" cx="400" cy="85"  r="16" fill="#C8102E" fill-opacity="0.85"/>
        <circle id="n3" class="tl-node" cx="50"  cy="188" r="16" fill="#C8102E" fill-opacity="0.85"/>
        <circle id="n4" class="tl-node" cx="185" cy="168" r="16" fill="#C8102E" fill-opacity="0.85"/>
        <circle id="n5" class="tl-node" cx="355" cy="183" r="16" fill="#C8102E" fill-opacity="0.85"/>
        <circle id="n6" class="tl-node" cx="118" cy="308" r="16" fill="#C8102E" fill-opacity="0.85"/>
        <circle id="n7" class="tl-node" cx="295" cy="298" r="16" fill="#C8102E" fill-opacity="0.85"/>
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
<p>Extent-based columnar storage with zone-map pruning and SIMD vectorized execution. The GEM optimizer selects per-graphlet join strategies automatically.</p>
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
<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M12 2L2 7l10 5 10-5-10-5z"/><path d="M2 17l10 5 10-5"/><path d="M2 12l10 5 10-5"/></svg>
</div>
<h3>Embedded</h3>
<p>Like DuckDB and Kuzu — import the library, start querying. No server, no daemon, no IPC. Embed TurboLynx directly in your application via C API.</p>
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
  var story  = document.querySelector('.tl-story');
  var steps  = document.querySelectorAll('.tl-step');
  if (!story || !steps.length) return;

  /* Layer refs */
  var act1   = document.getElementById('act1');
  var act2   = document.getElementById('act2');
  var act3   = document.getElementById('act3');
  var nodes  = [];
  for (var i = 0; i < 8; i++) nodes.push(document.getElementById('n' + i));

  /* Node colors per act */
  var COLORS_ACT1 = ['#C8102E','#C8102E','#C8102E','#C8102E','#C8102E','#C8102E','#C8102E','#C8102E'];
  /* Act 2: color matches graphlet */
  /* n0,n3,n7 → gl₁ blue; n1,n4 → gl₂ purple; n2,n6 → gl₃ amber; n5 → gl₄ green */
  var COLORS_ACT2 = ['#5B8DEF','#9B72CF','#F5A623','#5B8DEF','#9B72CF','#50C878','#F5A623','#5B8DEF'];

  /* Node positions per act */
  var POS = {
    0: [ /* Act 1: social graph */
      [90,75],[240,52],[400,85],
      [50,188],[185,168],[355,183],
      [118,308],[295,298]
    ],
    1: [ /* Act 2: inside graphlet boxes */
      [52, 92],[180, 92],[312, 92],   /* gl₁: n0; gl₂: n1; gl₃: n2 */
      [52,108],[180,108],[445, 92],   /* gl₁: n3; gl₂: n4; gl₄: n5 */
      [312,108],[52,124]              /* gl₃: n6; gl₁: n7 */
    ],
    2: [ /* Act 3: collapse off-screen (fade out via opacity) */
      [260,20],[260,20],[260,20],
      [260,20],[260,20],[260,20],
      [260,20],[260,20]
    ]
  };

  var current = -1;
  var nullAnimated = false;

  function animateCounter(elId, from, to, dur) {
    var el = document.getElementById(elId);
    if (!el) return;
    var start = performance.now();
    (function step(now) {
      var t = Math.min((now - start) / dur, 1);
      var ease = 1 - Math.pow(1 - t, 3);
      el.textContent = Math.round(from + (to - from) * ease);
      if (t < 1) requestAnimationFrame(step);
    })(start);
  }

  function setColors(colorArr, opacity) {
    nodes.forEach(function(n, i) {
      if (!n) return;
      n.setAttribute('fill', colorArr[i]);
      n.style.opacity = opacity;
    });
  }

  function fade(el, val, dur) {
    if (!el) return;
    el.style.transition = 'opacity ' + (dur || 0.55) + 's ease';
    el.style.opacity = val;
  }

  function moveNodes(actKey) {
    var pos = POS[actKey];
    nodes.forEach(function(n, i) {
      if (!n || !pos[i]) return;
      n.setAttribute('cx', pos[i][0]);
      n.setAttribute('cy', pos[i][1]);
    });
  }

  function setStep(s) {
    if (s === current) return;
    current = s;

    /* Update text panels */
    steps.forEach(function(el, i) { el.classList.toggle('active', i === s); });

    if (s === 0) {
      fade(act1, 1);
      fade(act2, 0);
      fade(act3, 0);
      setColors(COLORS_ACT1, 1);
      moveNodes(0);

    } else if (s === 1) {
      fade(act1, 0, 0.4);
      fade(act2, 1);
      fade(act3, 0);
      /* nodes transition colors + positions */
      setTimeout(function() {
        setColors(COLORS_ACT2, 0.7);
        moveNodes(1);
      }, 200);
      /* animate null counter once */
      if (!nullAnimated) {
        nullAnimated = true;
        setTimeout(function() {
          animateCounter('a2-null-from', 54, 4, 900);
          animateCounter('a2-null-to', 54, 4, 900);
        }, 600);
      }

    } else if (s === 2) {
      fade(act1, 0, 0.3);
      fade(act2, 0, 0.4);
      fade(act3, 1);
      /* fade nodes out */
      nodes.forEach(function(n) { if (n) { n.style.transition = 'opacity 0.4s'; n.style.opacity = 0; } });
      moveNodes(2);
    }
  }

  /* Scroll handler — 3 steps at [0, 0.35, 0.70] */
  var BREAKS = [0, 0.35, 0.70];

  window.addEventListener('scroll', function() {
    var rect    = story.getBoundingClientRect();
    var scrollH = story.offsetHeight - window.innerHeight;
    if (scrollH <= 0) return;
    var p = Math.max(0, Math.min(1, -rect.top / scrollH));
    var s = 0;
    for (var i = BREAKS.length - 1; i >= 0; i--) {
      if (p >= BREAKS[i]) { s = i; break; }
    }
    setStep(s);
  }, { passive: true });

  setStep(0);
})();
</script>
