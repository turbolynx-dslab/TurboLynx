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

<div class="tl-actions">
  <a href="installation/overview/" class="tl-btn tl-btn--ghost tl-btn--arrow">Installation</a>
  <a href="documentation/getting-started/quickstart/" class="tl-btn tl-btn--primary">Documentation</a>
</div>

</div>
<div class="tl-welcome-demo">

<div class="tl-intent">
  <div class="tl-intent-head">
    <svg class="tl-intent-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="7"/><line x1="16" y1="16" x2="21" y2="21"/></svg>
    <span class="tl-intent-label">The Question</span>
  </div>
  <p class="tl-intent-text">Across a knowledge graph of organizations and their related entities, find the <strong>10 most-connected entities</strong> reachable within <strong>three hops</strong> of any organization.</p>
</div>

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
</div>

</div>
</section>

<!-- WHY — pillar cards -->
<section class="tl-why">
<h2 class="tl-section-h">Why TurboLynx?</h2>

<div class="tl-pillars">

<div class="tl-pillar">
<div class="tl-pillar-icon">
<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="12" cy="12" r="3"/><circle cx="4" cy="6" r="2"/><circle cx="20" cy="6" r="2"/><circle cx="4" cy="18" r="2"/><circle cx="20" cy="18" r="2"/><line x1="6" y1="7" x2="10" y2="11"/><line x1="18" y1="7" x2="14" y2="11"/><line x1="6" y1="17" x2="10" y2="13"/><line x1="18" y1="17" x2="14" y2="13"/></svg>
</div>
<h3>Schemaless</h3>
<p>Nodes and edges carry different attributes with no predefined schema. Handles DBpedia's 2,796 unique attribute types without ETL or migration.</p>
<a href="why-turbolynx/#schemaless">Read more</a>
</div>

<div class="tl-pillar">
<div class="tl-pillar-icon">
<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg>
</div>
<h3>Fast</h3>
<p>Extent-based columnar storage, SIMD-vectorized operators, and a graph-aware Cascades optimizer that pushes joins below <code>UNION ALL</code> per graphlet.</p>
<a href="why-turbolynx/#fast">Read more</a>
</div>

<div class="tl-pillar">
<div class="tl-pillar-icon">
<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M3 3h7v7H3z"/><path d="M14 3h7v7h-7z"/><path d="M14 14h7v7h-7z"/><path d="M3 14h7v7H3z"/></svg>
</div>
<h3>Analytical</h3>
<p>Group-by, aggregation, and multi-hop traversal in a single Cypher query. Outperforms graph databases on traversal and RDBMSes on graph-shaped joins.</p>
<a href="why-turbolynx/#analytical">Read more</a>
</div>

<div class="tl-pillar">
<div class="tl-pillar-icon">
<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2L2 7l10 5 10-5-10-5z"/><path d="M2 17l10 5 10-5"/><path d="M2 12l10 5 10-5"/></svg>
</div>
<h3>Embedded</h3>
<p>Like DuckDB and Kuzu — link the library, start querying. No server, no daemon, no IPC. Embed TurboLynx directly via the C API.</p>
<a href="why-turbolynx/#embedded">Read more</a>
</div>

</div>
</section>

<!-- INSTALL — vertical: heading on top, card below -->
<section class="tl-install" id="quickinstall">
<div class="tl-install-stack">

  <div class="tl-install-cta">
    <h2 class="tl-install-cta-h">Install TurboLynx</h2>
    <p class="tl-install-cta-p">Start using TurboLynx in your environment.</p>
  </div>

  <div class="tl-install-card">

    <div class="tl-install-tabs" role="tablist">
      <button class="tl-install-tab active" data-tab="cli" type="button">CLI</button>
      <button class="tl-install-tab" data-tab="capi" type="button">C / C++</button>
    </div>

    <div class="tl-install-codebox" data-tab-content="cli">
      <pre><code>git clone https://github.com/turbolynx-dslab/TurboLynx &amp;&amp; cd TurboLynx &amp;&amp; cmake -B build &amp;&amp; cmake --build build</code></pre>
      <button class="tl-install-copy" type="button" aria-label="Copy to clipboard" title="Copy">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="11" height="11" rx="2"/><path d="M5 15V5a2 2 0 0 1 2-2h10"/></svg>
      </button>
    </div>

    <div class="tl-install-codebox" data-tab-content="capi" hidden>
      <pre><code>#include "main/capi/turbolynx.h"
int64_t conn = turbolynx_connect("/path/to/workspace");
if (conn &lt; 0) { /* handle error */ }</code></pre>
      <button class="tl-install-copy" type="button" aria-label="Copy to clipboard" title="Copy">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="11" height="11" rx="2"/><path d="M5 15V5a2 2 0 0 1 2-2h10"/></svg>
      </button>
    </div>

    <div class="tl-install-meta">
      <span>Version: main</span>
      <span>System detected: Linux</span>
    </div>

  </div>

  <a class="tl-install-cta-btn" href="installation/overview/">
    More clients and platforms
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="5" y1="12" x2="19" y2="12"/><polyline points="12 5 19 12 12 19"/></svg>
  </a>

</div>
</section>

</div>

<script>
/* ── Typewriter ────────────────────────────────────────────────── */
(function() {
  var el = document.querySelector('.tl-type');
  if (!el) return;
  var strings = el.getAttribute('data-strings').split('|');
  var idx = 0, pos = 0, deleting = false;
  function tick() {
    var word = strings[idx];
    if (!deleting) {
      pos++;
      el.textContent = word.slice(0, pos);
      if (pos === word.length) { deleting = true; setTimeout(tick, 1800); return; }
    } else {
      pos--;
      el.textContent = word.slice(0, pos);
      if (pos === 0) { deleting = false; idx = (idx + 1) % strings.length; }
    }
    setTimeout(tick, deleting ? 35 : 75);
  }
  setTimeout(tick, 800);
})();

/* ── Install card: tab switcher + copy button ─────────────────── */
(function() {
  document.querySelectorAll('.tl-install-tab').forEach(function(tab) {
    tab.addEventListener('click', function() {
      var card = tab.closest('.tl-install-card');
      var name = tab.dataset.tab;
      card.querySelectorAll('.tl-install-tab').forEach(function(t) {
        t.classList.toggle('active', t === tab);
      });
      card.querySelectorAll('.tl-install-codebox').forEach(function(box) {
        box.hidden = (box.dataset.tabContent !== name);
      });
    });
  });
  document.querySelectorAll('.tl-install-copy').forEach(function(btn) {
    btn.addEventListener('click', function() {
      var code = btn.parentElement.querySelector('code');
      if (!code) return;
      navigator.clipboard.writeText(code.textContent).then(function() {
        var orig = btn.innerHTML;
        btn.innerHTML = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg>';
        setTimeout(function() { btn.innerHTML = orig; }, 1400);
      });
    });
  });
})();
</script>
</content>
</invoke>