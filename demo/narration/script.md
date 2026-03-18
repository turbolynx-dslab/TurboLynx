# TurboLynx Demo Script (5 min)

> Timing guide: ~700 words total. [Scene.Step] markers match the demo navigation.
> *[italics]* = presenter action on the demo site.

---

## [S0.0] The Problem — Schemaless Property Graphs (40s)

Property graphs like DBpedia have **no fixed schema**. This graph shows a sample of DBpedia's 77 million nodes — and on the left panel you can see the actual distribution. *[click a distribution bar]* — There are over **282,000 unique schemas**. Some nodes have 30 properties, others just 3 — and each combination is different.

The challenge: how do you store this efficiently?

## [S0.1] Naive Approach 1: Split (15s)

One approach is to **split**: each node stores only its own properties. *[press Run]* — This eliminates NULLs, but every query must scan all nodes one by one. At 77 million nodes, that means billions of attribute checks per query.

## [S0.2] Naive Approach 2: Merge (15s)

The other extreme is to **merge** into one wide table. *[press Run, watch the scan animate]* — The result is catastrophic: most cells are NULL. Storage bloated to hundreds of gigabytes of wasted space.

**Neither approach works.** We need something in between.

---

## [S1.0] CGC — Graphlets in Action (40s)

This is our answer: **Columnar Graphlet Clustering**. We group nodes by schema similarity into a moderate number of **graphlets** — not one table, not thousands.

Here you can see the result: six graphlets, each with a compact schema. *[select a query attribute, press Run]* — Now watch: graphlets that lack the required property are **pruned instantly** — we never scan them. This is schema-driven pruning, and it comes for free from the graphlet structure.

## [S1.1] The CGC Algorithm (25s)

How do we build these graphlets? *[navigate through the phases]* — We start with raw nodes, identify distinct schemas, then run **layered agglomerative clustering**. *[press Cluster, watch the animation]* — At each step, the algorithm merges the two groups with the lowest cost — minimizing newly introduced NULLs while maximizing schema consolidation. The result is a compact graphlet catalog where NULLs are minimized within each group.

---

## [S2.0] Graph Queries on Graphlets (40s)

Now, how do we run **graph queries** on this storage? Here's a standard Cypher pattern: find persons, their birth city, and that city's country — a two-hop traversal.

*[adjust the predicate selectors]* — When we add `p.team IS NOT NULL`, GL-4 and GL-5 are **pruned** — they don't have the `team` property. We never touch them. *[click a result row]* — You can trace the exact path highlighted in the graph: person to city to country, crossing graphlet boundaries.

## [S2.1] Physical Execution Plan (25s)

Here's the actual plan. A **UnionAll** fans out across Person graphlets. Each branch runs: **NodeScan** on the partition, **AdjIdxJoin** using the CSR adjacency index, then **IdSeek** for O(1) target lookup. This repeats for each hop. GL-4, GL-5, and GL-8 are absent — pruned by schema.

---

## [S3.0] GEM — The Join Bloating Problem (30s)

But there's a combinatorial problem. 3 Person graphlets × 2 City × 2 Country = **12** join plans. *[click the scale buttons]* — At DBpedia scale, that explodes to over **one million** optimizer plans.

## [S3.1] Graphlet Early Merge (30s)

GEM solves this. We **partition graphlets into Virtual Groups** and run the GOO join optimizer once per group. *[toggle the split target, assign A/B]* — Different partitions yield different optimal join trees because cardinalities differ. A million plans reduced to a handful, with negligible cost error.

---

## [S4.0] SSRF — Schema Bloating in Joins (20s)

Joins also bloat schemas: each graphlet combination produces a different intermediate schema. *[click DBpedia-scale]* — Over a million schema variants, each needing its own expression tree.

## [S4.1] Shared Schema Row Format (30s)

SSRF eliminates this. *[point to the left table]* — The naive approach wastes 50% of cells on NULLs. On the right, SSRF separates **base columns** (always present) from **sparse columns** (vary per tuple). Each tuple stores only its non-NULL sparse values in a contiguous **TupleStore**, plus a pointer to a shared **SchemaInfo** with byte offsets. Zero NULLs in the sparse region, and schemas are created lazily and shared.

---

## [S5.0] Live Demo (15s)

You can try queries yourself here. *[click a preset, press Run]* — The right panel shows the full execution pipeline: parsing, schema resolution, GEM optimization, plan generation, and execution — all in under a second.

## [S5.1] Benchmark Results (25s)

Finally, the benchmarks from our paper. *[click through LDBC, TPC-H, DBpedia]* — TurboLynx achieves **4× to 30×** geometric mean speed-up over seven competitors — including Neo4j, Kuzu, DuckDB, and Umbra. On DBpedia, up to **86× faster** than Neo4j. *[hover rows for exact timings]*

The system and all queries are available for hands-on exploration.

**Thank you.**
