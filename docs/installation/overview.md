---
hide:
  - toc
---

# TurboLynx Installation

<div class="installationselection" data-environment="cli" data-platform="linux">

<div class="selection-foldout open" data-foldout="environment">
  <div class="selection-head">
    <h3>Client<span class="selected"></span></h3>
  </div>
  <div class="selection-content">
    <div class="selection-options" data-role="environment">
      <div class="option" tabindex="0" data-value="cli">
        <span class="symbol"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><polyline points="4 17 10 11 4 5"/><line x1="12" y1="19" x2="20" y2="19"/></svg></span>
        <span class="label">Command line</span>
      </div>
      <div class="option" tabindex="-1" data-value="capi">
        <span class="symbol"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/></svg></span>
        <span class="label">C / C++</span>
      </div>
      <div class="option soon" tabindex="-1" data-value="python">
        <span class="symbol"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M9 3h6c1.7 0 3 1.3 3 3v3H9V8H5C3.3 8 2 9.3 2 11v2c0 1.7 1.3 3 3 3h2"/><path d="M15 21H9c-1.7 0-3-1.3-3-3v-3h9v1h4c1.7 0 3-1.3 3-3v-2c0-1.7-1.3-3-3-3h-2"/><circle cx="8" cy="6" r=".7" fill="currentColor"/><circle cx="16" cy="18" r=".7" fill="currentColor"/></svg></span>
        <span class="label">Python</span>
      </div>
      <div class="option soon" tabindex="-1" data-value="java">
        <span class="symbol"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M8 18s-2 1 0 2 7 1 9 0 1-2 1-2"/><path d="M9 14s-3 1-1 2 9 1 10-1-2-2-2-2"/><path d="M12 3s4 4 0 8c-3 3 0 5 0 5"/></svg></span>
        <span class="label">Java</span>
      </div>
      <div class="option soon" tabindex="-1" data-value="node">
        <span class="symbol"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><polygon points="12 2 21 7 21 17 12 22 3 17 3 7 12 2"/><path d="M12 8v9M9 11l3 2 3-2"/></svg></span>
        <span class="label">Node.js</span>
      </div>
    </div>
  </div>
</div>

<div class="selection-foldout open" data-foldout="platform">
  <div class="selection-head">
    <h3>Platform<span class="selected"></span></h3>
  </div>
  <div class="selection-content">
    <div class="selection-options" data-role="platform">
      <div class="option" tabindex="0" data-value="linux">
        <span class="symbol"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M9 2c-1 1.5-1 4 0 6 .8 1 .8 2.5.8 3.5 0 4-4 5.5-4 8.5 0 2 2 2 6 2s6 0 6-2c0-3-4-4.5-4-8.5 0-1 0-2.5.8-3.5 1-2 1-4.5 0-6-1-1-2-2-2.8-2s-1.8 1-2.8 2Z"/><circle cx="10.5" cy="6" r=".7" fill="currentColor"/><circle cx="13.5" cy="6" r=".7" fill="currentColor"/></svg></span>
        <span class="label">Linux</span>
      </div>
      <div class="option soon" tabindex="-1" data-value="macos">
        <span class="symbol"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M16 3c-1 1-1.6 2.2-1.6 3.6"/><path d="M19 15c-1.5 3.4-3.4 6-6 6-1.6 0-2.1-.9-3.7-.9s-2.2.9-3.7.9C2.5 21 0 16.5 0 12c0-3.7 2.7-7 6-7 1.7 0 2.7 1 4.4 1S13 5 14.7 5c2 0 3.6.9 4.7 2.6-3 1.8-2.6 5.4.6 7.4Z"/></svg></span>
        <span class="label">macOS</span>
      </div>
      <div class="option soon" tabindex="-1" data-value="windows">
        <span class="symbol"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linejoin="round"><path d="M3 5l8-1.2v7.7H3z"/><path d="M3 13.5h8v7.7L3 20z"/><path d="M12 3.6L21 2v9.5h-9z"/><path d="M12 13.5h9V22l-9-1.4z"/></svg></span>
        <span class="label">Windows</span>
      </div>
    </div>
  </div>
</div>

<div class="installation-instructions">

<h2>Installation</h2>

<div id="result"><div class="instruction-wrap"></div></div>

</div><!-- installation-instructions -->

</div><!-- installationselection -->

<!-- ─────────────────────────────────────────────────────────── -->
<!--  HIDDEN INSTRUCTION COLLECTION — JS clones the matching one -->
<!-- ─────────────────────────────────────────────────────────── -->
<div class="instruction-collection" hidden aria-hidden="true" markdown="1">

<div class="instruction" data-environment="cli" data-platform="linux" markdown="1">

```bash
git clone https://github.com/postech-dblab-iitp/turbograph-v3
cd turbograph-v3
cmake -GNinja -DCMAKE_BUILD_TYPE=Release \
      -DENABLE_TCMALLOC=OFF -DBUILD_UNITTESTS=OFF -DTBB_TEST=OFF \
      -B build
cmake --build build
```

#### Verify

```bash
./build/tools/turbolynx --help
```

#### Build dependencies

The first build downloads and compiles bundled dependencies (TBB, hwloc, GP-Xerces) — about 3–5 minutes. Incremental builds finish in seconds.

</div>

<div class="instruction" data-environment="capi" data-platform="linux" markdown="1">

```bash
git clone https://github.com/postech-dblab-iitp/turbograph-v3
cd turbograph-v3
cmake -GNinja -DCMAKE_BUILD_TYPE=Release \
      -DENABLE_TCMALLOC=OFF -DBUILD_UNITTESTS=OFF -DTBB_TEST=OFF \
      -B build
cmake --build build
```

This produces `build/src/libturbolynx.so` and the public C header at `src/include/main/capi/turbolynx.h`.

#### Link from your application

```cmake
target_include_directories(your_app PRIVATE
    ${TURBOLYNX_DIR}/src/include)
target_link_libraries(your_app PRIVATE
    ${TURBOLYNX_DIR}/build/src/libturbolynx.so)
```

```c
#include "main/capi/turbolynx.h"

turbolynx_database db;
turbolynx_open("/path/to/workspace", &db);
```

See the [C API reference](../documentation/client-apis/c-api/overview.md) for the full surface.

</div>

<div class="instruction soon" data-environment="cli" data-platform="macos" markdown="1">
**macOS is not yet supported.** Track the [issue tracker](https://github.com/postech-dblab-iitp/turbograph-v3/issues) for status.
</div>

<div class="instruction soon" data-environment="cli" data-platform="windows" markdown="1">
**Windows is not supported.**
</div>

<div class="instruction soon" data-environment="capi" data-platform="macos" markdown="1">
**macOS is not yet supported.**
</div>

<div class="instruction soon" data-environment="capi" data-platform="windows" markdown="1">
**Windows is not supported.**
</div>

<div class="instruction soon" data-environment="python" data-platform="linux" markdown="1">
**Python bindings are on the roadmap.** `pip install turbolynx` will be the entry point.
</div>
<div class="instruction soon" data-environment="python" data-platform="macos" markdown="1">
**Python bindings are on the roadmap.**
</div>
<div class="instruction soon" data-environment="python" data-platform="windows" markdown="1">
**Python bindings are on the roadmap.**
</div>

<div class="instruction soon" data-environment="java" data-platform="linux" markdown="1">
**JNI bindings are on the roadmap.**
</div>
<div class="instruction soon" data-environment="java" data-platform="macos" markdown="1">
**JNI bindings are on the roadmap.**
</div>
<div class="instruction soon" data-environment="java" data-platform="windows" markdown="1">
**JNI bindings are on the roadmap.**
</div>

<div class="instruction soon" data-environment="node" data-platform="linux" markdown="1">
**Node.js bindings are on the roadmap.**
</div>
<div class="instruction soon" data-environment="node" data-platform="macos" markdown="1">
**Node.js bindings are on the roadmap.**
</div>
<div class="instruction soon" data-environment="node" data-platform="windows" markdown="1">
**Node.js bindings are on the roadmap.**
</div>

</div><!-- instruction-collection -->

<script>
(function() {
  var page = document.querySelector('.installationselection');
  if (!page) return;
  var wrap = page.querySelector('.instruction-wrap');
  var collection = document.querySelector('.instruction-collection');

  function render(env, platform) {
    page.dataset.environment = env;
    page.dataset.platform = platform;

    page.querySelectorAll('.selection-options[data-role="environment"] .option')
      .forEach(function(o) { o.classList.toggle('active', o.dataset.value === env); });
    page.querySelectorAll('.selection-options[data-role="platform"] .option')
      .forEach(function(o) { o.classList.toggle('active', o.dataset.value === platform); });

    var selectedEnvLabel = page.querySelector('.selection-options[data-role="environment"] .option.active .label');
    var selectedPlatformLabel = page.querySelector('.selection-options[data-role="platform"] .option.active .label');
    var envHead = page.querySelector('[data-foldout="environment"] .selected');
    var platHead = page.querySelector('[data-foldout="platform"] .selected');
    if (envHead && selectedEnvLabel) envHead.textContent = selectedEnvLabel.textContent;
    if (platHead && selectedPlatformLabel) platHead.textContent = selectedPlatformLabel.textContent;

    var match = collection.querySelector(
      '.instruction[data-environment="' + env + '"][data-platform="' + platform + '"]');
    wrap.innerHTML = '';
    if (match) wrap.appendChild(match.cloneNode(true));

    var u = new URL(window.location.href);
    u.searchParams.set('environment', env);
    u.searchParams.set('platform', platform);
    history.replaceState(null, '', u.toString());
  }

  // Foldout collapse on head click
  page.querySelectorAll('.selection-foldout').forEach(function(f) {
    var head = f.querySelector('.selection-head');
    head.addEventListener('click', function() { f.classList.toggle('open'); });
  });

  // Option clicks
  page.querySelectorAll('.selection-options[data-role="environment"] .option')
    .forEach(function(o) {
      o.addEventListener('click', function(e) {
        e.stopPropagation();
        render(o.dataset.value, page.dataset.platform);
      });
    });
  page.querySelectorAll('.selection-options[data-role="platform"] .option')
    .forEach(function(o) {
      o.addEventListener('click', function(e) {
        e.stopPropagation();
        render(page.dataset.environment, o.dataset.value);
      });
    });

  // Init from URL
  var qp = new URLSearchParams(window.location.search);
  render(qp.get('environment') || 'cli', qp.get('platform') || 'linux');
})();
</script>
