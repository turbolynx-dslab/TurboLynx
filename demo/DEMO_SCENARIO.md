# TurboLynx Interactive Demo — Scenario Guide

> **Venue target:** VLDB 2026 Demo Track
> **System:** TurboLynx — A high-performance graph analytics engine for schemaless property graphs
> **Demo URL:** `http://localhost:3000` (Next.js, single-page interactive)
> **Intro URL:** `http://localhost:3000/intro` (presentation-style intro slides)

---

## 1. Overview

TurboLynx is a graph analytics engine designed for **schemaless property graphs** — graphs in which different nodes carry fundamentally different sets of attributes. Real-world knowledge graphs such as DBpedia exhibit extreme schema heterogeneity: 77 million nodes, 388 million edges, and **282,764 distinct node schemas**. Existing systems handle this heterogeneity poorly.

TurboLynx addresses this with three core technical contributions:

| Contribution | Demo Scene |
|---|---|
| **CGC** — Clustered Graphlet Computation (schema-aware clustering) | Scene 1 |
| **GEM** — Graphlet-aware join Enumeration Method (Virtual Groups + GOO) | Scene 3 |
| **SSRF** — Shared Schema Row Format (compact intermediate results) | Scene 4 |

The demo is a **linear narrative** of six scenes plus a separate intro page. Each scene builds on the previous one. All scenes are interactive and navigable via a top navigation bar with scene tabs, step dots, and Back/Next buttons.

**Navigation:** Scene tabs (Problem, CGC, Query, GEM, SSRF, Performance) in the top bar. Within each scene, step dots and arrows navigate sub-steps. The TurboLynx brand logo resets to Scene 0.

---

## 2. Intro Page (`/intro`)

A separate presentation-style page with animated slides:

- **Slide 0 — Title Banner:** TurboLynx logo, title ("A High-Performance Graph Analytics Engine for Schemaless Property Graphs"), VLDB 2026 | Demo Paper tags.
- **Slide 1 — Use Cases:** Three cards — Fraud Detection, Recommendation, Knowledge Bases — each with an icon, title, and one-line description motivating why schemaless graphs matter.
- Additional slides follow the narration script flow.

Navigates to the main demo via link.

---

## 3. Demo Scene Walkthrough

### Scene 0 — The Problem: Schemaless Property Graph (3 steps)

**Goal:** Establish the core challenge — extreme schema heterogeneity.

**Step 0 — "Schemaless Property Graph with 77 Million Unlabeled Nodes"**

A force-directed graph simulation renders a DBpedia sample (~50 seed nodes + satellites). Nodes are colored by entity type (Person, Film, City, Book, Organisation). Features:

- Custom physics engine (no D3): repulsion, link springs, center gravity, alpha cooling
- Nodes are draggable (reheat on drag)
- Background dots suggest scale beyond visible sample
- Cluster ellipses group same-type nodes

Right panel shows two interactive bar charts:
- **`# Attributes → # Nodes`**: Nodes bucketed by schema size (1–5, 6–10, ..., 26+). Click a bar to highlight matching nodes.
- **`Unique Schema → # Nodes`**: Each bar = one distinct schema. Most bars are height 1–2, visually proving near-unique schemas.

Full-dataset stat cards: 77M nodes, 388M edges, 282,764 unique schemas.

**Step 1 — "Split approach: each node stores its own schema"**

A person table shows nodes with their individual schemas. Each node is scanned one at a time with an animation highlighting per-node schema inspection overhead. A summary extrapolates this cost to the full 77M-node dataset.

**Step 2 — "Merge approach: one flat table. Catastrophic NULLs."**

All nodes merged into a single wide table (40+ columns: abstract, height, weight, birthDate, ..., wikiPageRevisionID). Most cells are NULL (70–80%). A column scan for `WHERE attr IS NOT NULL` shows the waste. NULL count and percentage are displayed.

**Transition:** Both naive approaches fail. TurboLynx takes a middle path — schema-aware clustering into graphlets.

---

### Scene 1 — CGC: Clustered Graphlet Computation (2 steps, internal 5 phases)

**Goal:** Show how TurboLynx clusters heterogeneous nodes into graphlets.

**Step 0 — CGC Clustering (5 internal phases)**

An internal phase navigator (dots + ▶ CLUSTER / Next → / ← Back) guides through:

- **Phase 0 — "Sampled Schemaless Property Graph"**: Raw nodes displayed before clustering.
- **Phase 1 — "Identify Schema Distribution"**: Each unique schema and its node count are enumerated. Multi-node schemas get `SCH-N` IDs, singletons get `sch-n` IDs. Displayed as colored groups with attribute pills.
- **Phase 2 — "Split Schemas into Layers"**: Schemas sorted into three layers by frequency:
  - L1: ≥3 nodes (high frequency)
  - L2: ≥2 nodes (medium)
  - L3: singletons
- **Phase 3 — "Layered Agglomerative Clustering with Cost-Aware Similarity Function"**: The CASIM cost model drives greedy merging:
  - `score = C_SCH − C_NULL × newNulls + C_VEC × (ψ(|Gi|) + ψ(|Gj|) − ψ(|Gi|+|Gj|))`
  - Demo-scale constants: C_SCH=100, C_NULL=4, C_VEC=100, κ=50
  - Each merge step is animated with a playback slider showing: merged pair, new NULLs introduced, cost score
  - Line charts track NULL count and graphlet count over iterations
  - Merge stops when no pair improves cost
- **Phase 4 — "Result — Graphlet Storage"**: Final graphlets with schemas, node assignments, and coverage metrics.

**Step 1 — (Additional detail / summary view)**

---

### Scene 2 — Query: Graphlet-Aware Query Processing (2 steps)

**Goal:** Show how queries map to graphlets, how the Schema Index prunes irrelevant graphlets, and how execution plans work.

**Step 0 — Interactive Graphlet Pruning**

A two-hop Cypher query pattern is shown: `(p)-[:birthPlace]→(c)-[:country]→(co)`. Three columns of graphlet cards:

- **Person** (5 graphlets: GL_p-1 through GL_p-5): Athletes, scientists, goalkeepers, historical figures, etc.
- **City** (3 graphlets: GL_c-1 through GL_c-3): Cities with different attribute profiles
- **Country** (2 graphlets: GL_co-1, GL_co-2): Countries with different schemas

Each graphlet card shows: ID, schema attributes as pills, member nodes.

**Interactive predicate dropdowns** for each variable (p, c, co):
- Person: team, award, occupation, almaMater, deathDate, spouse
- City: area, elevation, utcOffset
- Country: population, gdp

When a predicate is selected:
- Graphlets lacking the required edge (`:birthPlace`, `:country`) or the selected attribute are **pruned** (dimmed to 30% opacity)
- Result rows update live — only nodes from active graphlets whose city→country chain is valid appear
- A pruning summary shows: `N/M graphlets pruned`

Below the graphlet cards, an **SVG execution plan tree** is rendered showing:
```
UnionAll
├─ NLJoin (GL_p-1 × active cities)
│  ├─ NodeScan(GL_p-1)
│  └─ AdjIdxJoin → IdSeek
├─ NLJoin (GL_p-3 × active cities)
│  └─ ...
└─ (pruned graphlets omitted)
```

**Step 1 — (Additional view / expanded plan)**

---

### Scene 3 — GEM: Graphlet-aware Join Enumeration Method (2 steps)

**Goal:** Show why naïve UNION ALL push-down causes combinatorial explosion, and how GEM solves it with Virtual Groups and GOO.

**Step 0 — Join Bloating: Combination Explosion**

Left panel recaps the query plan pipeline from Scene 2:
```
(p)-[:birthPlace]→(c)-[:country]→(co)
UnionAll — Person (N graphlets)
  ↓ :birthPlace
UnionAll — City (M graphlets)
  ↓ :country
UnionAll — Country (K graphlets)
```

Right panel shows a **Combination SVG**: graphlet boxes connected by lines visualizing all possible cross-graphlet join combinations.

Three interactive scale buttons:
| Scale | Counts | Product |
|---|---|---|
| This query | 3 × 2 × 2 | **12** |
| 10× diversity | 10 × 8 × 5 | **400** |
| DBpedia-scale | 50 × 30 × 20 | **30,000** |

An animated counter with spring physics shows the product growing. At DBpedia scale, the number turns red — "30,000 join orderings to evaluate" makes the problem visceral.

**Step 1 — Virtual Groups + GOO Join Ordering**

GEM partitions graphlets into **Virtual Groups (VGs)** and runs the **GOO (Greedy Operator Ordering)** algorithm once per VG combination.

The demo shows:
- Graphlet sizes from catalog statistics (e.g., GL_p-1: 44,200 nodes, GL_c-2: 8,400)
- Edge selectivities: `:birthPlace` ≈ 1/23,400, `:country` ≈ 1/250
- GOO algorithm execution: Union-Find greedy, picking the lowest-cost join at each step
- For each VG combination, a join tree is rendered showing the chosen join order
- Different VG combinations can yield different optimal join orders — demonstrating why one global order is suboptimal
- Cost comparison: per-VG cost vs. a single forced order

---

### Scene 4 — SSRF: Shared Schema Row Format (1 step)

**Goal:** Show that join intermediates suffer from schema explosion, and how SSRF eliminates NULL bloat.

**Interactive query progress bar** at top: `(p) → -[:birthPlace]→(c) → -[:country]→(co)`. Clicking each segment advances through three stages (0: Person only, 1: +City join, 2: +Country join).

Three graphlet schema panels (Person, City, Country) show active schemas with attribute pills. City and Country are labeled "row-packed"; Person stays "columnar".

**Left panel — Naive Columnar (unified wide table)**

A table with all attributes as columns. As joins are added:
- Stage 0: 3 columns (p.name, birth, team) → 3 schemas
- Stage 1: 6 columns (+c.name, pop, area) → 3×2 = **6** schemas
- Stage 2: 9 columns (+co.name, gdp, cont.) → 3×2×2 = **12** schemas

NULL cells highlighted in red. NULL percentage displayed (e.g., 46% at stage 2). Schema count grows **multiplicatively**.

**Right panel — SSRF (left columnar + right row-packed)**

A table with:
- Person columns (columnar, NULLs allowed for sparse person attributes)
- Per-hop: base column (c.name / co.name) + **SchPtr** (schema pointer to graphlet ID)
- No NULLs in right-side columns

Below the table:
- **SchemaInfos**: Per right-operand group (City, Country), each graphlet schema shows offset arrays (e.g., `[0,8]` for present attributes, `[-1]` for absent)
- **TupleStores**: One per join hop (City, Country). Row-packed, zero NULLs. Values shown as colored pills.

Bottom metrics:
- Schemas: **N** (additive: 3+2+2 = **7** vs. multiplicative 12)
- Right-side NULLs: **0**

**Key insight:** Schemas grow additively (3 + 2 instead of 3 × 2), and the right side carries zero NULLs.

---

### Scene 5 — Performance: Live Query Execution (2 steps)

**Goal:** Let the audience run queries and see the full execution pipeline.

**Step 0 — Live Query Runner**

Left panel: query editor with preset tabs and a Custom mode.

**Preset queries:**
1. **Person → Film**: `MATCH (p)-[:directed]->(f) WHERE p.birthDate IS NOT NULL RETURN p.name, f.title LIMIT 20`
2. **Film → Location**: `MATCH (f)-[:filmed_in]->(l) WHERE f.year >= 2000 RETURN f.title, f.year, l.name ORDER BY f.year DESC LIMIT 15`
3. **2-hop Person → Film → Location**: `MATCH (p)-[:starring]->(f)-[:filmed_in]->(l) WHERE p.nationality = "American" RETURN p.name, f.title, l.name LIMIT 20`

Custom mode: free-form Cypher textarea.

Pressing `▶ Run` triggers a **progressive pipeline animation** — each stage appears sequentially:

| Stage | Description |
|---|---|
| Parse Cypher | Tokenize + AST |
| Schema Resolve | e.g., "Person(5 GLs) × Film(4 GLs)" |
| GEM Optimize | e.g., "Split → 5×4 = 20 VG combos, GOO join ordering" |
| Physical Plan | SVG plan tree (Projection → UnionAll → NLJoin → Scan) |
| Execute | Result rows + total execution time |

Each pipeline step shows its own latency (e.g., Parse: 0.2ms, Schema: 0.1ms, GEM: 1.2ms, Plan: 0.3ms, Exec: 481ms).

Right panel: plan tree visualization and result table with execution time.

**Step 1 — Benchmark Comparison**

Performance comparison against other systems with bar charts and speedup multipliers.

---

## 4. Intended Audience Interaction

The demo is self-guided and click-friendly for conference kiosk use.

| Audience Type | Likely Focus |
|---|---|
| Graph DB researcher | Scene 0 (heterogeneity stats) → Scene 2 (graphlet pruning) → Scene 3 (GEM + GOO) |
| Storage / systems researcher | Scene 1 (CASIM cost model) → Scene 4 (SSRF layout) → Scene 5 (benchmark) |
| Industry attendee | Scene 5 (live query + pipeline visualization) |
| Student | Full linear walkthrough starting from `/intro` |

Each scene is reachable via the top navigation bar. Within each scene, steps are navigated with ← Back / Next → buttons and step dots.

---

## 5. Technical Stack

| Component | Technology |
|---|---|
| Frontend | Next.js 15 (App Router), React 19, Framer Motion |
| Graph simulation | Custom force-directed physics (no D3 dependency) |
| Backend (query execution) | TurboLynx native engine (C++17), exposed via HTTP API |
| Dataset | DBpedia (sampled graph JSON for Scene 0; graphlet data hardcoded for Scenes 1–4) |
| State management | React useState + Redux Toolkit (available) |

---

## 6. Key Messages per Scene

| Scene | One-line message |
|---|---|
| **Problem** | Real knowledge graphs have hundreds of thousands of distinct schemas — existing storage designs break down. |
| **CGC** | TurboLynx clusters nodes into dense graphlets via a cost-aware similarity function, eliminating NULLs. |
| **Query** | Every node scan becomes `UnionAll(Scan per graphlet)`; predicates prune irrelevant graphlets before any scan. |
| **GEM** | Naïve UNION ALL push-down explodes the join space (30,000 orderings at DBpedia scale); GEM + GOO reduce it to a handful. |
| **SSRF** | Join intermediates inherit schema heterogeneity; SSRF keeps schemas additive and right-side NULLs at zero. |
| **Performance** | TurboLynx's full pipeline — parse, schema resolve, GEM, plan, execute — runs in milliseconds on real queries. |
