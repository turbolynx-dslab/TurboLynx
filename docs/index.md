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
  TurboLynx is<br>
  the fastest<br>
  <span class="tl-type" data-strings="analytical|schemaless|embedded|open-source">analytical</span><br>
  graph database
</h1>

<div class="tl-cta-grid">
  <a href="installation/overview/" class="tl-btn tl-btn--ghost tl-btn--arrow tl-cta-grid__a">Installation</a>
  <a href="documentation/getting-started/quickstart/" class="tl-btn tl-btn--primary tl-btn--arrow-r tl-cta-grid__b">Documentation</a>

  <a class="tl-vldb-box tl-cta-grid__vldb" href="assets/p1605-han.pdf" target="_blank" rel="noopener">
    <span class="tl-vldb-trophy" aria-hidden="true">🏆</span>
    <span class="tl-vldb-text"><strong>Accepted at VLDB 2026</strong> — Read the paper</span>
    <span class="tl-vldb-arrow" aria-hidden="true">↗</span>
  </a>
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
<div class="tl-window-code"><code><span class="tl-kw">MATCH</span>  (s:<span class="tl-var">Entity</span>)-[:<span class="tl-rel">RELATED*1..3</span>]->(t:<span class="tl-var">Entity</span>)
<span class="tl-kw">WHERE</span>  s.<span class="tl-prop">type</span> = <span class="tl-str">'Organization'</span>
<span class="tl-kw">RETURN</span> t.<span class="tl-prop">label</span>, <span class="tl-fn">count</span>(*) <span class="tl-kw">AS</span> n
<span class="tl-kw">ORDER BY</span> n <span class="tl-kw">DESC LIMIT</span> <span class="tl-num">10</span></code></div>
</div>

</div>
</section>

<!-- TIMELINE — full-bleed horizontal-scroll research journey -->
<section class="tl-road" id="tl-road">
  <div class="tl-road-head">
    <h2 class="tl-road-section-h">The Graph Research Road to TurboLynx</h2>
  </div>

  <div class="tl-road-stage" id="tl-road-stage">
    <div class="tl-road-track" id="tl-road-track"></div>
  </div>

  <div class="tl-road-modal-bd" id="tl-road-modal-bd">
    <div class="tl-road-modal" id="tl-road-modal">
      <button class="tl-road-modal-close" id="tl-road-close" aria-label="Close">×</button>
      <span class="tl-road-m-year" id="tl-road-m-year"></span>
      <h3 id="tl-road-m-title"></h3>
      <div class="tl-road-m-venue" id="tl-road-m-venue"></div>
      <p id="tl-road-m-desc"></p>
      <div class="tl-road-m-tag" id="tl-road-m-tag"></div>
    </div>
  </div>
</section>

<!-- INSTALL — vertical: heading on top, card below -->
<section class="tl-install" id="quickinstall">
<div class="tl-install-stack">

  <div class="tl-install-cta">
    <h2 class="tl-install-cta-h">Install TurboLynx</h2>
  </div>

  <div class="tl-install-card">

    <div class="tl-install-tabs" role="tablist">
      <button class="tl-install-tab active" data-tab="cli" type="button">CLI</button>
      <button class="tl-install-tab" data-tab="python" type="button">Python</button>
    </div>

    <div class="tl-install-codebox" data-tab-content="cli">
      <button class="tl-copy-btn" type="button" aria-label="Copy" data-copy="git clone https://github.com/turbolynx-dslab/TurboLynx&#10;cd TurboLynx&#10;cmake -GNinja -DCMAKE_BUILD_TYPE=Release -DENABLE_TCMALLOC=OFF -DBUILD_UNITTESTS=OFF -DTBB_TEST=OFF -B build&#10;cmake --build build">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>
      </button>
<pre class="tl-code"><code><span class="tl-cmd">git</span> <span class="tl-sub">clone</span> <span class="tl-str">https://github.com/turbolynx-dslab/TurboLynx</span>
<span class="tl-cmd">cd</span> <span class="tl-str">TurboLynx</span>
<span class="tl-cmd">cmake</span> <span class="tl-flag">-GNinja</span> <span class="tl-flag">-DCMAKE_BUILD_TYPE</span>=<span class="tl-val">Release</span> <span class="tl-cont">\</span>
      <span class="tl-flag">-DENABLE_TCMALLOC</span>=<span class="tl-val">OFF</span> <span class="tl-flag">-DBUILD_UNITTESTS</span>=<span class="tl-val">OFF</span> <span class="tl-cont">\</span>
      <span class="tl-flag">-DTBB_TEST</span>=<span class="tl-val">OFF</span> <span class="tl-flag">-B</span> <span class="tl-str">build</span>
<span class="tl-cmd">cmake</span> <span class="tl-flag">--build</span> <span class="tl-str">build</span></code></pre>
    </div>

    <div class="tl-install-codebox tl-install-codebox--soon" data-tab-content="python" hidden>
      <button class="tl-copy-btn" type="button" aria-label="Copy" data-copy="pip install turbolynx">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>
      </button>
<pre class="tl-code"><code><span class="tl-cmd">pip</span> <span class="tl-sub">install</span> <span class="tl-str">turbolynx</span>   <span class="tl-cmt"># coming soon</span></code></pre>
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
  document.querySelectorAll('.tl-copy-btn').forEach(function(btn) {
    btn.addEventListener('click', function() {
      var txt = btn.getAttribute('data-copy') || '';
      if (navigator.clipboard) navigator.clipboard.writeText(txt);
      btn.classList.add('copied');
      setTimeout(function() { btn.classList.remove('copied'); }, 1200);
    });
  });
})();
</script>
</content>
</invoke>