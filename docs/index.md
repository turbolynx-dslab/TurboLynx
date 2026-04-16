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

<div class="tl-playground" id="tl-playground">
  <div class="tl-terminal" id="tl-terminal">
    <div class="tl-terminal-bar">
      <span class="tl-dot tl-dot--r"></span>
      <span class="tl-dot tl-dot--y"></span>
      <span class="tl-dot tl-dot--g"></span>
      <span class="tl-terminal-title">turbolynx &mdash; LDBC</span>
      <span class="tl-playground-badge" id="tl-wasm-status">Loading...</span>
    </div>
    <div class="tl-terminal-body" id="tl-terminal-body">
      <div class="tl-terminal-history" id="tl-terminal-history"></div>
      <div class="tl-terminal-input-line" id="tl-terminal-input-line">
        <span class="tl-prompt">turbolynx&gt;&nbsp;</span>
        <div class="tl-terminal-input" id="tl-terminal-input" contenteditable="true" spellcheck="false" role="textbox" aria-label="Cypher query input"></div>
      </div>
    </div>
  </div>

  <div class="tl-preset-queries">
    <span class="tl-preset-label">Preset</span>
    <button class="tl-preset-btn" data-query="MATCH (n:Person) RETURN n.firstName, n.lastName, n.gender LIMIT 10">People</button>
    <button class="tl-preset-btn" data-query="MATCH (n:Person)-[:KNOWS]->(m:Person) RETURN n.firstName, m.firstName LIMIT 10">Friends</button>
    <button class="tl-preset-btn" data-query="MATCH (n:Person)-[:KNOWS]->(m:Person)-[:KNOWS]->(o:Person) RETURN n.firstName, m.firstName, o.firstName LIMIT 10">2-Hop</button>
    <button class="tl-preset-btn" data-query="MATCH (p:Post)-[:HAS_CREATOR]->(n:Person) RETURN n.firstName, count(p) AS posts ORDER BY posts DESC LIMIT 10">Top Posters</button>
    <button class="tl-preset-btn" data-query="MATCH (n:Person)-[:KNOWS]->(m:Person) RETURN count(*) AS friendships">Count</button>
    <button class="tl-preset-btn" data-query="MATCH (n:Person)-[:IS_LOCATED_IN]->(c:Place) RETURN c.name, count(n) AS residents ORDER BY residents DESC LIMIT 10">Cities</button>
  </div>
</div>

</div>
</section>

<!-- TIMELINE — full-bleed horizontal-scroll research journey -->
<section class="tl-road" id="tl-road">
  <div class="tl-road-head">
    <div class="tl-road-kicker">We are graph gurus.</div>
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
      <button class="tl-install-tab" data-tab="node" type="button">Node.js</button>
    </div>

    <p>Build native artifacts on the machine that will run them. The snippets below use the portable build so the same steps work on Linux and macOS.</p>

    <div class="tl-install-codebox" data-tab-content="cli">
      <button class="tl-copy-btn" type="button" aria-label="Copy" data-copy="git clone https://github.com/turbolynx-dslab/TurboLynx&#10;cd TurboLynx&#10;cmake -GNinja -DCMAKE_BUILD_TYPE=Release -DTURBOLYNX_PORTABLE_DISK_IO=ON -DENABLE_TCMALLOC=OFF -DBUILD_UNITTESTS=OFF -DTBB_TEST=OFF -B build-portable&#10;cmake --build build-portable">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>
      </button>
<pre class="tl-code"><code><span class="tl-cmd">git</span> <span class="tl-sub">clone</span> <span class="tl-str">https://github.com/turbolynx-dslab/TurboLynx</span>
<span class="tl-cmd">cd</span> <span class="tl-str">TurboLynx</span>
<span class="tl-cmd">cmake</span> <span class="tl-flag">-GNinja</span> <span class="tl-flag">-DCMAKE_BUILD_TYPE</span>=<span class="tl-val">Release</span> <span class="tl-cont">\</span>
      <span class="tl-flag">-DTURBOLYNX_PORTABLE_DISK_IO</span>=<span class="tl-val">ON</span> <span class="tl-flag">-DENABLE_TCMALLOC</span>=<span class="tl-val">OFF</span> <span class="tl-cont">\</span>
      <span class="tl-flag">-DBUILD_UNITTESTS</span>=<span class="tl-val">OFF</span> <span class="tl-flag">-DTBB_TEST</span>=<span class="tl-val">OFF</span> <span class="tl-flag">-B</span> <span class="tl-str">build-portable</span>
<span class="tl-cmd">cmake</span> <span class="tl-flag">--build</span> <span class="tl-str">build-portable</span></code></pre>
    </div>

    <div class="tl-install-codebox" data-tab-content="python" hidden>
      <button class="tl-copy-btn" type="button" aria-label="Copy" data-copy="git clone https://github.com/turbolynx-dslab/TurboLynx&#10;cd TurboLynx&#10;python3 -m pip install pybind11 wheel&#10;cmake -GNinja -DCMAKE_BUILD_TYPE=Release -DBUILD_PYTHON=ON -DTURBOLYNX_PORTABLE_DISK_IO=ON -DENABLE_TCMALLOC=OFF -DBUILD_UNITTESTS=OFF -DTBB_TEST=OFF -B build-portable&#10;cmake --build build-portable&#10;tools/pythonpkg/scripts/build_wheel.sh build-portable&#10;python3 -m pip install tools/pythonpkg/dist/turbolynx-*.whl">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>
      </button>
<pre class="tl-code"><code><span class="tl-cmd">git</span> <span class="tl-sub">clone</span> <span class="tl-str">https://github.com/turbolynx-dslab/TurboLynx</span>
<span class="tl-cmd">cd</span> <span class="tl-str">TurboLynx</span>
<span class="tl-cmd">python3</span> <span class="tl-flag">-m</span> <span class="tl-str">pip</span> <span class="tl-sub">install</span> <span class="tl-str">pybind11</span> <span class="tl-str">wheel</span>
<span class="tl-cmd">cmake</span> <span class="tl-flag">-GNinja</span> <span class="tl-flag">-DCMAKE_BUILD_TYPE</span>=<span class="tl-val">Release</span> <span class="tl-flag">-DBUILD_PYTHON</span>=<span class="tl-val">ON</span> <span class="tl-cont">\</span>
      <span class="tl-flag">-DTURBOLYNX_PORTABLE_DISK_IO</span>=<span class="tl-val">ON</span> <span class="tl-flag">-DENABLE_TCMALLOC</span>=<span class="tl-val">OFF</span> <span class="tl-cont">\</span>
      <span class="tl-flag">-DBUILD_UNITTESTS</span>=<span class="tl-val">OFF</span> <span class="tl-flag">-DTBB_TEST</span>=<span class="tl-val">OFF</span> <span class="tl-flag">-B</span> <span class="tl-str">build-portable</span>
<span class="tl-cmd">cmake</span> <span class="tl-flag">--build</span> <span class="tl-str">build-portable</span>
<span class="tl-cmd">tools/pythonpkg/scripts/build_wheel.sh</span> <span class="tl-str">build-portable</span>
<span class="tl-cmd">python3</span> <span class="tl-flag">-m</span> <span class="tl-str">pip</span> <span class="tl-sub">install</span> <span class="tl-str">tools/pythonpkg/dist/turbolynx-*.whl</span></code></pre>
    </div>

    <div class="tl-install-codebox" data-tab-content="node" hidden>
      <button class="tl-copy-btn" type="button" aria-label="Copy" data-copy="git clone https://github.com/turbolynx-dslab/TurboLynx&#10;cd TurboLynx/tools/nodepkg&#10;npm install && npm pack&#10;npm install ./turbolynx-0.0.1.tgz">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>
      </button>
<pre class="tl-code"><code><span class="tl-cmd">git</span> <span class="tl-sub">clone</span> <span class="tl-str">https://github.com/turbolynx-dslab/TurboLynx</span>
<span class="tl-cmd">cd</span> <span class="tl-str">TurboLynx/tools/nodepkg</span> <span class="tl-cmt"># bundles the WASM runtime, no native build needed</span>
<span class="tl-cmd">npm</span> <span class="tl-sub">install</span> <span class="tl-cont">&amp;&amp;</span> <span class="tl-cmd">npm</span> <span class="tl-sub">pack</span>
<span class="tl-cmd">npm</span> <span class="tl-sub">install</span> <span class="tl-str">./turbolynx-0.0.1.tgz</span>   <span class="tl-cmt"># npm registry release coming soon</span></code></pre>
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
<script src="javascripts/playground.js" defer></script>
