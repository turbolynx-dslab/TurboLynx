---
hide:
  - navigation
  - toc
---

<div class="tl-demo-gate">
  <section class="tl-demo-gate__intro">
    <span class="tl-demo-gate__eyebrow">TurboLynx Demos</span>
    <h1 class="tl-demo-gate__title">Two demos — pick your path.</h1>
  </section>

  <div class="tl-demo-tabs" role="tablist">
    <button class="tl-demo-tab is-active" data-target="panel-standalone" role="tab" aria-selected="true">Standalone DB Demo</button>
    <button class="tl-demo-tab"           data-target="panel-agentic"     role="tab" aria-selected="false">Agentic DB Demo</button>
  </div>

  <section class="tl-demo-panel is-active" id="panel-standalone" role="tabpanel">
    <div class="tl-demo-gate__player">
      <div class="tl-demo-gate__video-shell">
        <iframe
          src="https://www.youtube-nocookie.com/embed/DwdaBWL3cWs?rel=0"
          title="TurboLynx standalone demo walkthrough"
          loading="lazy"
          referrerpolicy="strict-origin-when-cross-origin"
          allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
          allowfullscreen>
        </iframe>
      </div>
    </div>
    <div class="tl-demo-gate__cta">
      <a href="../demo/" class="tl-btn tl-btn--primary tl-btn--arrow-r tl-demo-gate__cta-btn">Open Standalone Demo</a>
    </div>
  </section>

  <section class="tl-demo-panel" id="panel-agentic" role="tabpanel" hidden>
    <div class="tl-demo-gate__player">
      <div class="tl-demo-gate__video-shell">
        <iframe
          src="https://www.youtube-nocookie.com/embed/SbFNT5j3u0c?rel=0"
          title="TurboLynx agentic demo walkthrough"
          loading="lazy"
          referrerpolicy="strict-origin-when-cross-origin"
          allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
          allowfullscreen>
        </iframe>
      </div>
    </div>
    <div class="tl-demo-gate__cta">
      <a href="../demo-agentic/" class="tl-btn tl-btn--primary tl-btn--arrow-r tl-demo-gate__cta-btn">Open Agentic Demo</a>
    </div>
  </section>
</div>

<script>
(function () {
  const tabs = document.querySelectorAll('.tl-demo-tab');
  const panels = document.querySelectorAll('.tl-demo-panel');
  tabs.forEach((tab) => {
    tab.addEventListener('click', () => {
      const target = tab.dataset.target;
      tabs.forEach((t) => {
        const active = t === tab;
        t.classList.toggle('is-active', active);
        t.setAttribute('aria-selected', active ? 'true' : 'false');
      });
      panels.forEach((p) => {
        const active = p.id === target;
        p.classList.toggle('is-active', active);
        if (active) { p.removeAttribute('hidden'); } else { p.setAttribute('hidden', ''); }
      });
    });
  });
})();
</script>
