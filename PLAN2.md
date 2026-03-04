# TurboLynx — Landing Page Creative Plan (v2)

---

## Color Strategy — POSTECH Red

### Why Red

| | Yellow (DuckDB) | Green (Supabase) | **Red (TurboLynx)** |
|--|--|--|--|
| Association | Friendly, light | Fresh, SaaS | **Speed, power, trust** |
| Academic | None | None | **POSTECH brand** |
| Graph DB | Weak | Unrelated | **Strong (Neo4j is blue → clear differentiation)** |
| Competitor overlap | DuckDB copy | Supabase copy | **Unique** |

Red has intuitive associations with "fast" and "strong" — fitting for a 183.9× faster DB.
Dark background + refined red = premium, not "error/warning".

---

### Palette

```
Primary Red    #C8102E   POSTECH official red
Premium Red    #E8193A   hover/CTA emphasis
Deep Red Glow  rgba(200, 16, 46, 0.15)  section background glow
Muted Red      #8B1C2E   secondary accent
```

```
Dark BG        #0D0608   near-black, slight warm red undertone
Alt BG         #160B0D   card/code background
Border         rgba(200, 16, 46, 0.12)  subtle red-tinted border
```

```
Text           #F5F0F0   warm white (easier on eyes than pure white)
Text Muted     #9B8F90   warm gray
Text Code      #E6EDF3   code text
```

---

### Element Application

| Element | Color | Note |
|---------|-------|------|
| CTA button (primary) | `#C8102E` fill + hover `#E8193A` | "Get Started", "Documentation" |
| CTA button (ghost) | transparent + `#C8102E` border | secondary actions |
| Typewriter cursor | `#C8102E` | hero typing cursor |
| Metric numbers | `#F5F0F0` + red underline | numbers white, accent red |
| Section divider | `#C8102E` 2px | section top border |
| Hero glow | `rgba(200,16,46,0.18)` ellipse | bottom-left radial glow |
| Graphlet colors | `#C8102E` / `#5B8DEF` / `#50C878` / `#F5A623` | 4 graphlets, distinct hues |
| Feature pillar icon | `#C8102E` | SVG stroke |
| GitHub ★ button | `#C8102E` bg | header CTA |

---

### Competitor Color Positioning

```
Neo4j      ████  #0194D3  blue — stable but generic
Kuzu       ████  #5C7CFA  purple-blue
Memgraph   ████  #FB6E00  orange
DuckDB     ████  #F5D06A  yellow
Supabase   ████  #3ECF8E  green

TurboLynx  ████  #C8102E  red — unique, speed, POSTECH
```

Red is used by none of the major graph DBs. **Unique positioning.**

---

## Revision Plan (v2) — What Changes and Why

### Change 0: Hero Visual Discontinuity (the real "legibility" bug)

**Root cause (from check.png):**

The `.tl-welcome::before` pseudo-element applies the red glow as `position: absolute; inset: 0` — meaning the glow is clipped exactly to the `.tl-welcome` container box (1060px wide, centered). The surrounding page background has no glow. This creates a **hard rectangular crop** at the hero section boundary: left/right/bottom edges of the hero box are starkly visible against the plain dark page background.

The `overflow: hidden` on `.tl-welcome` makes it worse by explicitly preventing the glow from bleeding out.

**Fix strategy:**

Move the glow **out of the content container** and apply it at the full-viewport level:

```css
/* Remove from .tl-welcome::before */

/* Apply at the page level instead */
.md-main::before {          /* or body::before */
  content: '';
  position: fixed;          /* or absolute on a full-width wrapper */
  inset: 0;
  pointer-events: none;
  z-index: 0;
  background:
    radial-gradient(ellipse 55% 65% at 0% 80%,  rgba(200,16,46,0.18) 0%, transparent 65%),
    radial-gradient(ellipse 30% 35% at 100% 5%, rgba(200,16,46,0.07) 0%, transparent 55%);
}
```

Also add a bottom fade to `.tl-welcome` so it dissolves into the page:
```css
.tl-welcome::after {
  content: '';
  position: absolute;
  bottom: 0; left: 0; right: 0;
  height: 120px;
  background: linear-gradient(to bottom, transparent, var(--tl-bg));
  pointer-events: none;
}
```

Remove `overflow: hidden` from `.tl-welcome`.

The glow becomes a **full-page ambient light**, not a clipped box.

---

### Change 1: Header — Remove "Installation"

**Current:** Documentation | Installation | GitHub★
**New:** Documentation | GitHub★

Rationale: Fewer links = cleaner header. Installation is secondary content, reachable from Docs.

**File:** `docs/overrides/partials/header.html`
Remove the Installation `<a>` tag entirely.

---

### Change 2: Visibility & Legibility

**Problems:**
- `--tl-text-2` at `rgba(245,240,240,0.55)` is too dim for body/subtitle text
- Section headings may not have enough weight
- Some card text blends into the dark background

**Fixes:**
- Raise `--tl-text-2` opacity to `0.72` (warm readable gray, not pure muted)
- Section subtitles: `font-weight: 500`, `letter-spacing: 0.01em`
- Feature card text: ensure `color: var(--tl-text)` not `--tl-text-2`
- Metric labels: bump to `0.78` opacity
- Dot-grid background: reduce dot opacity slightly so text pops more

---

### Change 3: "In-Process" → "Embedded"

**Current pillar text:** "In-Process — No IPC, No Daemon..."
**New:** "Embedded — Like DuckDB and Kuzu. Import the library, query directly. Zero server setup."

Rationale: "In-Process" is an internal engineering term. "Embedded" is the user-facing concept used by DuckDB, Kuzu, SQLite — immediately understood.

---

### Change 4: Hero — Keep DuckDB Layout, Fix Button Row

**Keep current layout:** Code window + typewriter headline (DuckDB style is fine).

**Fix:** The "Installation" and "Documentation" CTA buttons are wrapping to 2 rows because `.tl-actions` has `flex-wrap: wrap`. On narrower viewports the buttons stack vertically, which looks broken.

**Fix:** Remove `flex-wrap: wrap` from `.tl-actions` (or set `flex-wrap: nowrap`). If needed, reduce button padding slightly so both fit on one line.

```css
.tl-actions {
  display: flex;
  gap: 0.7rem;
  flex-wrap: nowrap;   /* was: wrap */
  align-items: center;
}
```

Note: Per Change 1, "Installation" button will be removed anyway, leaving only "Documentation" — so this wrapping issue will naturally resolve too. But keep buttons on one row regardless.

---

### Change 5: Scroll Animation — Complete Redesign

**Core insight from `turbolynx-demo.jsx`:**
The "aha moment" is: **10 nodes all labeled "Person"** — but each has a completely different attribute set. Row store → 54 NULLs (67.5% waste). TurboLynx graphlets → near-zero NULLs.

**New animation: 3 Acts (simplified from 4)**

---

#### Act 1 (progress 0.00 → 0.35): The Problem — Same Label, Different Schemas

**Query context (appears at top, small):**
```cypher
MATCH (p:Person)-[:knows]->(q:Person)
RETURN p.FN, q.FN
```

**Left text:**
```
Real-world graphs are schemaless.

These are all "Person" nodes —
connected by :knows edges.
But each has different attributes.

A traditional database stores them
in one giant table — full of NULLs.
```

**Right visualization:**

8 nodes, all labeled `Person`, connected with `:knows` edges.
Each node shows a small attribute badge:

```
  ● Person          ● Person          ● Person
  age, FN, LN       FN, LN, gender    gender, major, name

  ● Person    :knows→   ● Person
  name, age             FN, birthday

  ● Person          ● Person
  name, url         FN, gender, url
```

Edges between nodes show `:knows` label.

Below the nodes, a wide table appears (scroll-triggered, slides up):

```
┌──────┬─────┬─────┬────────┬───────┬──────┬──────────┬─────┐
│ name │ age │ FN  │ gender │ major │  LN  │ birthday │ url │
├──────┼─────┼─────┼────────┼───────┼──────┼──────────┼─────┤
│ NULL │  25 │ Ana │  NULL  │  NULL │ Kim  │   NULL   │NULL │
│ Bob  │NULL │NULL │   M    │  NULL │ NULL │   NULL   │NULL │
│ NULL │NULL │ Eve │   F    │  CS   │ NULL │   NULL   │NULL │
│ NULL │  31 │NULL │  NULL  │  NULL │ NULL │   NULL   │NULL │
│  ... │ ... │ ... │  ...   │  ...  │ ...  │   ...    │ ... │
└──────┴─────┴─────┴────────┴───────┴──────┴──────────┴─────┘
```

NULL cells are highlighted in red. A counter below shows:
`54 NULLs — 67.5% wasted storage`

**Transition signal:** Scroll → table fades, nodes begin to gravitate toward each other by similarity.

---

#### Act 2 (progress 0.35 → 0.70): Graphlet Clustering — No NULLs

**Left text:**
```
TurboLynx clusters nodes by
schema similarity into Graphlets.

Each Graphlet is a compact
columnar table — no NULLs,
SIMD-vectorizable.

The CGC algorithm picks the
cost-optimal partition.
```

**Right visualization:**

Nodes drift into 4 colored groups. Each group collapses into a compact table:

```
┌─── gl₁ (blue) ──────┐  ┌─── gl₂ (purple) ────┐
│ age │  FN  │  LN    │  │  FN  │  LN  │ gender │
│─────│──────│────────│  │──────│──────│────────│
│  25 │ Ana  │  Kim   │  │  Eve │ Park │   F    │
│  31 │ Min  │  Lee   │  │  Tom │ Chen │   M    │
└─────────────────────┘  └───────────────────────┘

┌─── gl₃ (amber) ─────┐  ┌─── gl₄ (green) ─────┐
│gender│ major │ name │  │ name │  age │         │
│──────│───────│──────│  │──────│──────│         │
│  F   │  CS   │ Eva  │  │ Bob  │  22  │         │
│  M   │  EE   │ Jin  │  │ Mia  │  29  │         │
└─────────────────────┘  └─────────────────────-─┘
```

No NULL cells. A counter updates:
`~6 NULLs remaining — 93% storage efficiency`

The counter transition from `54` → `6` is animated (count-down style).

---

#### Act 3 (progress 0.70 → 1.00): Query Plan — Cypher → Operator Tree (GEM)

**Left text:**
```
One Cypher query.

TurboLynx compiles it to an
operator tree. Each "Person" scan
becomes a UNION ALL across
matching graphlets.

Then GEM optimization kicks in:
the join is pushed below the
UNION ALL — so each graphlet
gets its own optimal join order.

Results merge via SSRF.
```

**Right visualization — two-phase reveal:**

**Phase A** (Act 3 intro, progress 0.70–0.82): Cypher typed in at top:
```cypher
MATCH (p:Person)-[:knows]->(q:Person)
RETURN p.FN, q.FN
```
Arrow labeled `TurboLynx Optimizer` pointing down.

Naive operator tree fades in (Person ⋈ knows ⋈ Person, each Person scan = UNION ALL):
```
          π (p.FN, q.FN)
                │
          ⋈ (q.id = e.dst)
         ╱               ╲
  ⋈ (p.id = e.src)     UNION ALL   ← Person (q)
  ╱           ╲          gl₁ gl₂ gl₃ gl₄
UNION ALL   Scan(knows)
(Person p)
gl₁ gl₂ gl₃ gl₄
```

**Phase B** (progress 0.82–1.00): GEM optimization — join pushed below UNION ALL, per-group join order:
```
          π (p.FN, q.FN)
                │
            UNION ALL             ← GEM splits Person into subgroups
           ╱          ╲
    ⋈ ⋈ (gl₁,gl₂)   ⋈ ⋈ (gl₃,gl₄)
    p  knows  q       p  knows  q    ← each group: own join order
```

Annotation appears: `"Each graphlet group gets its own cost-optimal join order"`

Each graphlet box pulses when its subtree is activated.
Result rows stream into a result table, then `✓ 2ms` appears.

---

### Act Boundaries and Progress Mapping

| Act | Progress Range | Trigger |
|-----|----------------|---------|
| Act 1 intro | 0.00 → 0.10 | nodes appear + float |
| Act 1 peak  | 0.10 → 0.35 | NULL table slides up, counter |
| Act 2 transition | 0.35 → 0.45 | nodes drift to groups |
| Act 2 peak  | 0.45 → 0.70 | graphlet tables solidify, counter counts down |
| Act 3 intro | 0.70 → 0.80 | Cypher types in |
| Act 3 peak  | 0.80 → 1.00 | operator tree appears, parallel scan, result |

---

## Implementation Details

### File Changes

| File | Change |
|------|--------|
| `docs/overrides/partials/header.html` | Remove Installation link |
| `docs/stylesheets/extra.css` | Raise `--tl-text-2` opacity, fix legibility tokens, hero two-panel layout, new story CSS |
| `docs/index.md` | Redesigned hero (two panels), updated scroll story (3 acts), "Embedded" pillar |

---

### Hero HTML Structure (keep current DuckDB style)

No structural changes to the hero. Keep:
- Left: headline + typewriter + subline + CTA buttons
- Right: code window

Only changes:
1. Fix `.tl-actions { flex-wrap: nowrap }` — buttons stay on one row
2. Remove the "Installation" button from `.tl-actions` (keep Documentation + GitHub★ in header; hero CTA can be "Documentation" primary + "GitHub ★" ghost)
3. Fix the red glow discontinuity (Change 0 above)

---

### Story SVG State Machine

Three SVG states (Act 1, Act 2, Act 3) — all in ONE `<svg>` element.
JS transitions between states by:
1. Moving node `cx`/`cy` attributes (CSS transition handles animation)
2. Toggling `opacity` on groups (`#act1-table`, `#act2-graphlets`, `#act3-plan`)
3. Animating `stroke-dashoffset` for the operator tree edges

```javascript
const ACTS = [
  { progress: 0.00, fn: showAct1 },
  { progress: 0.35, fn: showAct2 },
  { progress: 0.70, fn: showAct3 },
];
```

---

### NULL Counter Animation

```javascript
function animateCounter(from, to, el, suffix = '') {
  const start = performance.now();
  const dur = 800;
  function step(now) {
    const t = Math.min((now - start) / dur, 1);
    const ease = 1 - Math.pow(1 - t, 3); // ease-out cubic
    el.textContent = Math.round(from + (to - from) * ease) + suffix;
    if (t < 1) requestAnimationFrame(step);
  }
  requestAnimationFrame(step);
}
// Act 1 peak: animateCounter(0, 54, counterEl, ' NULLs')
// Act 2 peak: animateCounter(54, 6, counterEl, ' NULLs')
```

---

### Operator Tree Rendering

The query plan tree is rendered as inline SVG (not text-art).
Symbols:
- `π` — projection (circle with π text)
- `⋈` — hash join (bowtie shape or ⋈ unicode in text)
- `σ` — selection/filter (circle with σ text)
- Graphlet boxes at the leaf level (colored rectangles)

Tree edges: `<path>` with `stroke-dasharray` + `stroke-dashoffset` animation
triggered on Act 3 entry (edges "draw" themselves top-to-bottom, 600ms each).

---

### Feature Pillars — Updated Copy

| Pillar | Old | New |
|--------|-----|-----|
| Schemaless | ✓ keep | "Store any graph structure without a schema. TurboLynx adapts." |
| Embedded | **was "In-Process"** | "Like DuckDB and Kuzu — import the library, start querying. No server, no daemon." |
| Analytical | ✓ keep | "Columnar graphlets, SIMD vectorization, GEM optimizer. Built for analytics." |
| Open Source | ✓ keep | "Apache 2.0. POSTECH DB Lab. Published at VLDB 2026." |

---

### CSS Legibility Tokens (updated)

```css
:root {
  --tl-text:   #F5F0F0;          /* primary text — warm white */
  --tl-text-2: rgba(245,240,240,0.72);  /* was 0.55, now more readable */
  --tl-text-3: rgba(245,240,240,0.45);  /* truly muted — use sparingly */
}
```

Section subheadings: `color: var(--tl-text-2); font-weight: 500;`
Card body text: `color: var(--tl-text-2);`
Metric labels: `color: var(--tl-text-2);` (was invisible at 0.55 on dark bg)

---

### Mobile Fallback

- `< 768px`: hero is single-column (node SVG hidden, text only)
- Story section: static 3-card layout, no sticky scroll
- Each card = one Act summary with small static SVG
- `IntersectionObserver` triggers fade-in as cards enter viewport

---

## Page Structure (final)

```
[Header: logo | Documentation | GitHub★]
[Hero: Headline + node viz]
[Metrics: 183.9× | 41.3× | VLDB 2026]
[Story: "How It Works" — 3-Act sticky scroll, ~400vh]
[Feature Pillars: Schemaless | Embedded | Analytical | Open Source]
[Install: code tabs]
[Footer]
```

---

## Implementation Checklist

- [ ] **Header**: Remove Installation link from `header.html`
- [ ] **CSS**: Update `--tl-text-2` opacity + legibility fixes
- [ ] **CSS**: Add two-panel hero layout (`.tl-hero-left`, `.tl-hero-right`)
- [ ] **CSS**: New story CSS for 3 Acts (update existing 4-act CSS)
- [ ] **index.md hero**: Rewrite HTML to two-panel layout
- [ ] **index.md hero**: Update typewriter word list
- [ ] **index.md hero SVG**: 8 floating Person nodes with attribute badges
- [ ] **index.md story**: Rewrite 3-Act scroll story
- [ ] **index.md story SVG**: Act 1 nodes + NULL table
- [ ] **index.md story SVG**: Act 2 graphlet tables + NULL counter
- [ ] **index.md story SVG**: Act 3 Cypher + operator tree (π/⋈/σ)
- [ ] **index.md pillars**: Update "In-Process" → "Embedded" copy
- [ ] **JS**: NULL counter animation
- [ ] **JS**: Operator tree edge draw animation
- [ ] **Deploy**: `mkdocs gh-deploy --force`
