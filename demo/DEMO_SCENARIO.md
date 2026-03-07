# TurboLynx Interactive Demo — Scenario Guide

> **Venue target:** VLDB / SIGMOD Demo Track
> **System:** TurboLynx — A high-performance graph DBMS for schemaless property graphs
> **Demo URL:** `http://localhost:3000` (Next.js, single-page interactive)

---

## 1. Overview

TurboLynx is a graph database system designed for **schemaless property graphs** — graphs in which different nodes carry fundamentally different sets of attributes (properties). Real-world knowledge graphs such as DBpedia exhibit extreme schema heterogeneity: the full DBpedia dataset contains 77 million nodes, 388 million edges, and **282,764 distinct node schemas**. Existing systems handle this heterogeneity poorly, either by embedding a per-node schema (high per-query interpretation overhead) or by merging everything into a single flat table (catastrophic NULL density).

TurboLynx addresses this with three core technical contributions, each visualized in the demo:

| Contribution | Demo Scene |
|---|---|
| **CGC** — Clustered Graphlet Computation (schema-aware clustering) | Scene 1 |
| **GEM** — Graphlet-aware join Enumeration Method (join ordering) | Scene 3 |
| **SSRF** — Shared Schema Row Format (compact intermediate results) | Scene 4 |

The demo is designed as a **linear narrative** of six scenes. Each scene builds on the previous one, guiding the audience from the fundamental problem through to live query execution and benchmark comparison. All scenes are interactive.

---

## 2. Demo Scene Walkthrough

### Scene 0 — The Problem: Schemaless Property Graph

**Goal:** Establish the core challenge — extreme schema heterogeneity in real-world property graphs.

**Step 0 — Graph Visualization**

A physics-simulated force-directed graph renders a DBpedia sample (~50 seed nodes, ~150 satellite nodes). Node colors encode entity types (Person, Film, City, Book, Organisation). On the right panel, two interactive bar charts provide statistics:

- **`# Attributes → # Nodes`**: Shows how many nodes have schemas of a given attribute-count range (1–5, 6–10, ..., 26+). Users can click a bar to highlight matching nodes in the graph.
- **`Unique Schema → # Nodes`**: **Each bar represents one distinct schema** (i.e., an exact set of attributes). Bar height = number of nodes sharing that schema. Because schemas are so diverse, most bars are height 1 or 2 — visually demonstrating that nearly every schema is unique. Clicking a bar highlights only the nodes with that exact schema.

The full-dataset statistics (77M nodes, 388M edges, 282,764 unique schemas) are shown as stat cards for reference.

**Key takeaway for audience:** Schemaless graphs are not a theoretical corner case. Real data contains hundreds of thousands of distinct schemas, making uniform table design impractical.

**Step 1 — Split Approach (per-node schema)**

A `SELECT * WHERE attr IS NOT NULL` query is animated against a list of person nodes. Each node must be opened and its embedded schema inspected at query time to determine whether the target attribute is present. The animation scans one node at a time, showing the schema-lookup overhead. A summary card extrapolates the per-node schema-inspection cost to the full 77M-node dataset (~billions of attribute checks per query).

**Step 2 — Merge Approach (flat table)**

All nodes are merged into a single wide table with 40+ columns. A column scan reveals that most cells are NULL — typically 70–80% of the table. Running `WHERE attr IS NOT NULL` across this table requires scanning every row and checking the target column, wasting I/O proportional to total NULL density. The NULL count and percentage are surfaced as stat cards.

**Transition message to audience:** Both naive approaches are expensive. TurboLynx takes a middle path: **schema-aware clustering into graphlets** — reducing NULL density while avoiding per-node schema interpretation.

---

### Scene 1 — CGC: Clustered Graphlet Computation

**Goal:** Show how TurboLynx clusters heterogeneous nodes into graphlets and why this reduces cost.

**Step 0 — Preview**

A sample of ~1,000 DBpedia nodes is shown as colored dots (by entity type) before clustering. A brief callout explains that TurboLynx will cluster these nodes based on schema similarity using the **CGC algorithm**.

**Step 1 — CGC Animation**

Three sub-phases animate sequentially:

1. **Schema inventory:** Each unique schema and its node count is enumerated into a histogram — mirroring the `Unique Schema → # Nodes` bar chart from Scene 0.
2. **Layered ordering:** Schemas are sorted by frequency (node count) into layers — high-frequency schemas at the top, rare schemas at the bottom. This layering guides the merge priority in the clustering step.
3. **Greedy merge:** The algorithm iteratively merges schema pairs whose combination reduces the estimated global query cost (modeled as NULL-rate × node count). At each step, the current graphlet count, NULL percentage, and cost reduction are displayed. The merge stops when further merges no longer reduce cost — visually, the cost curve flattens and the graphlet count stabilizes.

**Step 2 — Final Graphlet Summary**

After clustering converges, a panel shows:
- The resulting graphlets (GL-1 through GL-5), each with its schema attributes listed as pills
- Per-graphlet node count and node coverage
- Summary metrics: total graphlets, NULL reduction vs. flat table, and schema coverage

**Key takeaway:** CGC partitions nodes into a small number of compact, semantically coherent graphlets. Each graphlet is stored as a dense columnar table — no NULLs for within-graphlet queries.

---

### Scene 2 — Query: UNION ALL Plan + Schema Index + Live Query Demo

**Goal:** Show how queries are translated into graphlet-aware plans and how the Schema Index enables fast predicate-driven graphlet pruning.

**Step 0 — Concept: UNION ALL Plan + Schema Index**

Two panels are shown side by side:

**① UNION ALL Plan**

In TurboLynx, `MATCH (n)` compiles into a `UnionAll` of one `Scan` per graphlet. The tree is shown as:

```
MATCH (n)
└ UnionAll (5 graphlets)
  ├ Scan(GL-1)
  ├ Scan(GL-2)
  ├ Scan(GL-3)
  ├ Scan(GL-4)
  └ Scan(GL-5)
```

A graphlet schema matrix (graphlets × attributes) is displayed to make the schema coverage of each graphlet visible.

**② Schema Index**

TurboLynx maintains an inverted index mapping each attribute to the set of graphlets whose schema includes it. When a query carries a predicate `WHERE team IS NOT NULL`, the Schema Index is consulted in microseconds to identify only the graphlets that contain `team`. Graphlets lacking `team` are **pruned** before any scan begins.

The demo shows:
- Index lookup result per graphlet (✓ / —)
- The resulting pruned plan:
  ```
  MATCH (n) WHERE team IS NOT NULL
  └ UnionAll (2 graphlets — GL-1, GL-3)
    ├ Scan(GL-1)
    └ Scan(GL-3)
       (GL-2, GL-4, GL-5 pruned)
  ```
- A `⚡ 3/5 graphlets pruned` indicator

**Step 1 — Live Query Demo**

An interactive query runner is presented with three DBpedia preset queries (Goalkeeper join, Scholar filter, Athlete filter) and a `✏ Custom` mode for free-form Cypher input.

Upon pressing `▶ Run`, the system executes the query against a live TurboLynx instance. After execution:
- **Query Plan tab**: A recursive plan tree is rendered showing the physical operators (Projection, Filter, AdjIdxJoin, UnionAll, Scan). Operators are color-coded by type. For multi-node patterns such as `MATCH (p)-[:birthPlace]->(c)`, both sides of the join are expanded as `UnionAll(Scan)` — person-type graphlets on the left, city-type graphlets (`GL-C`) on the right.
- **Results tab**: The result tuples are shown in a table with execution time in milliseconds.

**Key takeaway:** TurboLynx's query plan is transparent and schema-aware. Every node scan is expressed as a `UnionAll` of graphlet scans, and graphlets are pruned at plan time — not at runtime.

---

### Scene 3 — GEM: Graphlet-aware Join Enumeration Method

**Goal:** Demonstrate why naïve UNION ALL push-down leads to exponential join-order search space explosion, and how GEM controls it via graphlet grouping.

**Step 0 — Search Space Explosion**

For a join query `MATCH (a)-[e1]->(b)-[e2]->(c)`, pushing the UNION ALL operator down through the join tree produces one plan per combination of graphlet assignments across join nodes. The demo interactively shows:

- For `|GL| = 2` graphlets: **N** possible plans
- For `|GL| = 4`: dramatically more
- As the number of hops increases, the plan count grows super-exponentially

An animated counter shows the combinatorial explosion as the user adjusts the number of graphlets and join hops.

**Step 1 — Graphlet Grouping**

GEM reduces the search space by **grouping graphlets** into a smaller number of equivalence classes and selecting one join order per group rather than per graphlet. The user can interactively assign graphlets to groups (Group A, Group B, ...). The demo responds in real time:
- Group A graphlets adopt one join order (e.g., left-deep, `(a⋈b)⋈c`)
- Group B graphlets adopt another (e.g., right-deep, `a⋈(b⋈c)`)
- The resulting per-group plan trees are visualized

**Key takeaway:** GEM's grouping strategy keeps the optimizer tractable while still allowing different graphlets to exploit their locally optimal join order.

---

### Scene 4 — SSRF: Shared Schema Row Format

**Goal:** Show that join intermediate results also suffer from NULL bloat, and how SSRF eliminates it via schema sharing.

**Step 0 — Intermediate Result Schema Explosion**

For a join `MATCH (p)-[:directed]->(f)`, the result schema is the cross-product of person graphlet schemas and film graphlet schemas. The demo presents a **Cartesian schema grid**: 4 person graphlets × 5 film graphlets = up to 20 distinct intermediate schemas. Under a naïve flat-tuple representation, each intermediate row must accommodate all possible attributes of both sides — generating enormous NULL density. A NULL-count heatmap illustrates this.

**Step 1 — SSRF Layout**

SSRF stores intermediate results in three compact structures:

| Structure | Contents |
|---|---|
| **SchemaInfos** | One entry per distinct intermediate schema. Each entry holds `offset_infos[]` — byte offsets into the TupleStore for each sparse (nullable) attribute. Offset = -1 means the attribute is absent. Multiple tuples with the same schema **share** one SchemaInfo entry. |
| **TupleStore** | A packed byte array containing only the values of sparse attributes that are actually present — no NULLs, no base-column values. |
| **OffsetArr** | Per-tuple index: `[byte_offset_into_TupleStore → SchemaInfo pointer]`. Base-column values (attributes present in all tuples of a graphlet pair) are stored outside SSRF. |

The demo renders all three structures as interactive tables. Tuples are color-coded by their SchemaInfo. Tuples that share a SchemaInfo entry are visually linked — demonstrating that schema information is not repeated per-tuple.

Metrics panel (full width, 3 columns):
- **NULL cells eliminated** vs. flat-tuple baseline
- **Distinct schema variants** (SchemaInfo entries)
- **Schema sharing factor** (avg. tuples per SchemaInfo)

**Key takeaway:** SSRF decouples schema metadata from tuple data. Common schemas are stored once; rare schemas incur no overhead for other tuples. This dramatically reduces intermediate result size in heterogeneous joins.

---

### Scene 5 — Performance: Live Query Execution + Benchmark Comparison

**Goal:** Let the audience run real queries against TurboLynx and compare results to published benchmark numbers.

**Left Panel — Live Query Runner**

Three DBpedia preset queries (no node labels — DBpedia is label-free) are available:
1. Director + birthDate filter
2. Film + filming location (year ≥ 2000)
3. Person → Film → Location 3-hop join

A `✏ Custom` mode allows free-form Cypher input. Pressing `▶ Run on TurboLynx` executes the query against a live backend. After execution:
- Execution time is displayed in milliseconds (green)
- **Query Plan** tab shows the physical plan tree
- **Results** tab shows the result tuples (first N rows)

**Right Panel — Benchmark Comparison**

Performance numbers from **Table 4 of the TurboLynx paper** are presented interactively. Dataset groups:
- **LDBC SNB** (SF1, SF10, SF100)
- **TPC-H** (SF1, SF10, SF100)
- **DBpedia**

For each benchmark configuration, a bar chart shows the **speedup of TurboLynx vs. each competitor** (Neo4j, AgensGraph, Memgraph, Kuzu, DuckDB, Umbra, NebulaGraph). Bar widths use a √-scale to handle large speedup ranges without obscuring smaller ones. Hovering over a bar reveals the exact query time in milliseconds for both TurboLynx and the competitor, along with the speedup multiplier.

---

## 3. Intended Audience Interaction

The demo is self-guided and touch/click-friendly for conference kiosk use. Expected interaction patterns:

| Audience Type | Likely Focus |
|---|---|
| Graph DB researcher | Scene 0 (heterogeneity stats) → Scene 2 (plan inspection) → Scene 3 (join ordering) |
| Storage / systems researcher | Scene 4 (SSRF layout detail) → Scene 5 (benchmark) |
| Industry attendee | Scene 5 (live query + speedup numbers) |
| Student | Full linear walkthrough |

Each scene can be reached independently via the top navigation bar. Within each scene, steps are navigated with Previous / Next buttons.

---

## 4. Technical Stack

| Component | Technology |
|---|---|
| Frontend | Next.js 15 (App Router), React 19, Framer Motion |
| Graph simulation | Custom force-directed physics (no D3 dependency) |
| Backend (query execution) | TurboLynx native engine (C++17), exposed via HTTP API |
| Dataset | DBpedia (sampled graph JSON for visualization; full dataset for live queries) |
| Benchmark data | Table 4 from TurboLynx paper (hardcoded, SF1/10/100) |

---

## 5. Key Messages per Scene (elevator-pitch form)

| Scene | One-line message |
|---|---|
| **Problem** | Real knowledge graphs have hundreds of thousands of distinct schemas — existing storage designs break down. |
| **CGC** | TurboLynx clusters nodes into dense graphlets, eliminating NULLs while keeping schemas manageable. |
| **Query** | Every node scan becomes `UnionAll(Scan per graphlet)`; a Schema Index prunes irrelevant graphlets in microseconds. |
| **GEM** | Naïve UNION ALL push-down explodes the join search space; GEM groups graphlets to keep optimization tractable. |
| **SSRF** | Join intermediates inherit schema heterogeneity; SSRF stores them compactly by sharing schema metadata across tuples. |
| **Performance** | TurboLynx outperforms Neo4j, Kuzu, DuckDB, and others across LDBC, TPC-H, and DBpedia at all scale factors. |
