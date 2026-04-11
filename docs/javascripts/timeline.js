/* The road to TurboLynx — horizontal-scroll research timeline.
   Renders inline (full-bleed) inside the home page, not in an iframe. */
(function () {
  const track = document.getElementById("tl-road-track");
  const stage = document.getElementById("tl-road-stage");
  if (!track || !stage) return;

  const papers = [
    { hero: true, key: true, year: "2026", title: "TurboLynx",
      subtitle: "Schemaless graph engine for general-purpose analytics",
      venue: "VLDB 2026 · POSTECH DSLAB",
      desc: "TurboLynx is a schemaless graph engine that unifies more than a decade of graph research from the POSTECH Data Systems Lab into a single general-purpose analytics system. Building on the Turbo lineage — from TurboGraph and TurboISO, through TurboFlux, DAF, G-CARE, VEQ, and iTurboGraph — it brings subgraph matching, cardinality estimation, continuous query processing, and scalable graph storage together without the overhead of rigid schemas.",
      tag: "The present — and what's next." },
    { year: "2024", title: "In-depth Analysis of Continuous Subgraph Matching", venue: "SIGMOD 2024",
      desc: "Unifies prior continuous subgraph matching systems under a common delta-query compilation framework, revealing which design choices actually matter for streaming graph workloads. Lee, Kim, Lee, Han.",
      tag: "Streaming · Compilation" },
    { year: "2024", title: "Time-Constrained Continuous Subgraph Matching", venue: "ICDE 2024",
      desc: "Exploits temporal information for filtering and backtracking in continuous subgraph matching over time-windowed streaming graphs. Min, Jang, Park, Giammarresi, Italiano, Han.",
      tag: "Streaming · Temporal" },
    { year: "2022", title: "Graph Isomorphism via Degree Sequences & Color-Label Distributions", venue: "ICDE 2022",
      desc: "Efficient graph isomorphism query processing by combining degree-sequence signatures with color-label distribution filters. Gu, Nam, Park, Galil, Italiano, Han.",
      tag: "Graph isomorphism" },
    { year: "2021", key: true, title: "iTurboGraph: Scaling & Automating Incremental Graph Analytics", venue: "SIGMOD 2021",
      desc: "Extends the Turbo graph engine family to incremental analytics, automatically deriving efficient incremental computation for graph queries on evolving graphs. Ko, Lee, Hong, Lee, Seo, Seo, Han.",
      tag: "Incremental analytics" },
    { year: "2021", key: true, title: "VEQ: Versatile Equivalences for Subgraph Matching", venue: "SIGMOD 2021",
      desc: "Introduces neighbor-equivalence classes and dynamic equivalence to prune the subgraph-matching search space, delivering orders-of-magnitude speedups across diverse workloads. Kim, Choi, Park, Lin, Hong, Han.",
      tag: "Subgraph matching" },
    { year: "2021", title: "Worst-Case Optimal Graph Pattern Cardinality Estimation", venue: "SIGMOD 2021",
      desc: "Combines sampling with synopses to give the first graph pattern cardinality estimator with both worst-case-optimal runtime and quality guarantees. Kim, Kim, Fletcher, Han.",
      tag: "Cardinality estimation" },
    { year: "2021", title: "Scalable Graph Isomorphism via Compressed Candidate Space", venue: "ICDE 2021",
      desc: "Combines pairwise color refinement with backtracking over a compressed candidate space to scale exact graph isomorphism. Gu, Nam, Park, Galil, Italiano, Han.",
      tag: "Graph isomorphism" },
    { year: "2020", key: true, title: "G-CARE: Cardinality Estimation for Subgraph Matching", venue: "SIGMOD 2020",
      desc: "The first systematic benchmark and framework for subgraph-matching cardinality estimation. G-CARE reshaped how the community thinks about graph query optimization and remains the reference point for new estimators. Park, Ko, Bhowmick, Kim, Hong, Han.",
      tag: "Benchmark · Optimizer" },
    { year: "2019", key: true, title: "DAF: Efficient Subgraph Matching via Failing Sets", venue: "SIGMOD 2019",
      desc: "Harmonizes dynamic programming, adaptive matching order, and failing-set–based pruning. For years it defined the state of the art in exact subgraph matching and inspired a whole family of follow-up systems. Han, Kim, Gu, Park, Han.",
      tag: "Subgraph matching" },
    { year: "2018", key: true, title: "TurboFlux: Fast Continuous Subgraph Matching", venue: "SIGMOD 2018",
      desc: "Introduced the data-centric graph (DCG) representation, enabling continuous subgraph matching at streaming speeds. The foundation of every subsequent streaming graph-matching engine in the Turbo lineage. Kim, Seo, Han, Lee, Hong, Chafi, Shin, Jeong.",
      tag: "Streaming" },
    { year: "2018", key: true, title: "TurboGraph++: Scalable & Fast Graph Analytics", venue: "SIGMOD 2018",
      desc: "Extends TurboGraph with a new block-based execution model tailored to modern multi-core, large-memory machines, delivering the scalability needed for industrial-size graph analytics. Ko, Han.",
      tag: "Analytics engine" },
    { year: "2016", title: "DualSim: Parallel Subgraph Enumeration on a Single Machine", venue: "SIGMOD 2016",
      desc: "Shows that, with careful pruning, a single machine can enumerate subgraphs on graphs that previously required full clusters — challenging the 'bigger is better' consensus of the era. Kim, Lee, Bhowmick, Han, Lee, Ko, Jarrah.",
      tag: "Single-machine" },
    { year: "2014", title: "OPT: Overlapped & Parallel Triangulation in Large Graphs", venue: "SIGMOD 2014",
      desc: "A framework for overlapped and parallel triangle enumeration in large-scale graphs, setting a baseline for high-throughput motif counting. Kim, Han, Lee, Park, Yu.",
      tag: "Triangles · Motifs" },
    { year: "2013", key: true, title: "TurboISO: Ultrafast & Robust Subgraph Isomorphism", venue: "SIGMOD 2013",
      desc: "Introduced neighborhood equivalence classes and candidate-region exploration, delivering robust, ultra-fast subgraph isomorphism. The paper that launched the 'Turbo' lineage of subgraph-matching research. Han, Lee, Lee.",
      tag: "Subgraph isomorphism" },
    { year: "2013", key: true, title: "TurboGraph: A Fast Parallel Graph Engine", venue: "KDD 2013",
      desc: "Pioneered pin-and-slide I/O with column views, showing that a single commodity PC with SSDs can outperform large graph-processing clusters on billion-scale graphs. The foundational paper of the Turbo engine family. Han, Lee, Park, Lee, Kim, Kim, Yu.",
      tag: "Disk-based engine" },
    { year: "2012", title: "In-depth Comparison of Subgraph Isomorphism Algorithms", venue: "VLDB 2012",
      desc: "A landmark empirical study that re-examined decades of subgraph isomorphism algorithms under a common framework — and set the stage for the Turbo family of matchers that would follow a year later. Lee, Han, Kasperovics, Lee.",
      tag: "Subgraph isomorphism" },
  ];

  function el(tag, cls, html) {
    const e = document.createElement(tag);
    if (cls) e.className = cls;
    if (html !== undefined) e.innerHTML = html;
    return e;
  }

  papers.forEach((p, i) => {
    if (p.hero) {
      const h = el("div", "tl-road-hero");
      h.innerHTML = `
        <div class="tl-road-hero-inner" data-idx="${i}" role="button" tabindex="0">
          <div class="tl-road-hero-meta">${p.venue}</div>
          <h3 class="tl-road-hero-title">${p.title}</h3>
          <p class="tl-road-hero-sub">${p.subtitle}</p>
        </div>`;
      track.appendChild(h);
    } else {
      const n = el("div", "tl-road-node " + (i % 2 === 0 ? "up" : "down") + (p.key ? " key" : ""));
      n.innerHTML = `
        <div class="tl-road-label-top">
          <span class="tl-road-title">${p.title}</span>
          <span class="tl-road-venue">${p.venue}</span>
        </div>
        <div class="tl-road-tick"></div>
        <button class="tl-road-dot" data-idx="${i}" aria-label="${p.title}"></button>
        <div class="tl-road-label-bot">
          <span class="tl-road-year-label">${p.year}</span><br/>
          <span class="tl-road-title">${p.title}</span>
          <span class="tl-road-venue">${p.venue}</span>
        </div>`;
      track.appendChild(n);
    }
  });

  const end = el("div", "tl-road-end", '<span class="tl-road-end-line"></span> The beginning');
  track.appendChild(end);

  const rail = el("div", "tl-road-rail");
  track.insertBefore(rail, track.firstChild);

  /* Modal */
  const modalBd = document.getElementById("tl-road-modal-bd");
  const modal = document.getElementById("tl-road-modal");
  const mYear = document.getElementById("tl-road-m-year");
  const mTitle = document.getElementById("tl-road-m-title");
  const mVenue = document.getElementById("tl-road-m-venue");
  const mDesc = document.getElementById("tl-road-m-desc");
  const mTag = document.getElementById("tl-road-m-tag");

  function openPaper(i) {
    const p = papers[i];
    if (!p) return;
    modal.classList.toggle("key", !!p.key);
    mYear.textContent = p.year;
    mTitle.textContent = p.hero ? p.title + " — " + p.subtitle : p.title;
    mVenue.textContent = p.venue;
    mDesc.textContent = p.desc;
    mTag.textContent = p.tag || "";
    modalBd.classList.add("open");
  }
  function closePaper() { modalBd.classList.remove("open"); }

  stage.addEventListener("click", (e) => {
    const b = e.target.closest("[data-idx]");
    if (b) openPaper(+b.dataset.idx);
  });
  document.getElementById("tl-road-close").addEventListener("click", closePaper);
  modalBd.addEventListener("click", (e) => { if (e.target === modalBd) closePaper(); });
  document.addEventListener("keydown", (e) => { if (e.key === "Escape") closePaper(); });

  /* Redirect vertical-dominant wheel events to the page so the timeline
     never traps vertical scrolling. Horizontal-dominant deltas (trackpad
     swipe or shift+wheel) still scroll the stage natively. */
  stage.addEventListener("wheel", (e) => {
    if (Math.abs(e.deltaY) > Math.abs(e.deltaX)) {
      window.scrollBy({ top: e.deltaY, left: 0 });
      e.preventDefault();
    }
  }, { passive: false });

  /* Keyboard arrows: only when stage is focused/hovered */
  stage.addEventListener("keydown", (e) => {
    if (e.key === "ArrowRight") stage.scrollBy({ left: window.innerWidth * 0.6, behavior: "smooth" });
    if (e.key === "ArrowLeft") stage.scrollBy({ left: -window.innerWidth * 0.6, behavior: "smooth" });
  });
})();
