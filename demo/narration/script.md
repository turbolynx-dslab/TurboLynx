Hello. We'll be demonstrating TurboLynx.

In the real world, data rarely fits into rigid, predefined structures. As business needs evolve, data models must adapt instantly without costly downtime or complex migrations. This demand for ultimate agility is exactly why modern enterprises rely on schemaless property graphs for dynamic applications like fraud detection, recommendation engines, and massive knowledge bases like DBpedia.

While engineering teams value the inherent freedom of this data model, the industry has long accepted a difficult trade-off: flexibility comes at the expense of analytical speed. Traditional systems simply struggle to process unpredictable schemas efficiently.

With TurboLynx, we eliminate that compromise. We have engineered a graph analytics engine with schemaless performance built into its core, outperforming state-of-the-art graph databases by up to 183 times.

In our interactive demo, we will walk you through exactly how we achieve this paradigm shift.

Here is TurboLynx's architecture — three layers, each designed around the schemaless property. Storage clusters data into columnar graphlets. The query processor uses SSRF to eliminate NULLs in heterogeneous intermediates. And the optimizer applies GEM to tame the plan search space.

We will demonstrate these techniques through two live scenarios on a full DBpedia instance — 77 million nodes across 1,304 graphlets.

Let's dive in.

In the Data tab, you can see what schemaless property graphs look like — the schema-level distribution and how nodes vary wildly in their attributes.

In the Storage tab, we show TurboLynx's solution: graphlets. Nodes are clustered by schema similarity into 1,304 columnar graphlets. You can browse the full catalog — click any graphlet to see its schema, row count, and column count. The heatmap shows how data is distributed: a few large graphlets hold the majority of rows, while many small ones capture niche schemas.

Now let's move to actual queries. In the Query tab, you can select from preset scenarios or build your own query using the query builder. Our demo walks through two representative scenarios.

**Scenario A** demonstrates early graphlet pruning on a selective property-filter query.

**Scenario B** demonstrates how GEM and SSRF optimize a multi-hop query with aggregation.

We start with a simple one-hop query: MATCH (p)-[:birthPlace]->(c) WHERE p.birthDate IS NOT NULL.

The key observation: birthDate is a sparse property — only 28% of graphlets contain it. In the Plan tab, the system highlights which graphlets contain birthDate and which are pruned before scanning. 359 out of 1,304 graphlets survive — 72% are eliminated immediately, before any data is read.

This pruning reshapes the physical plan. The pruned plan fans out over only 359 graphlets instead of 1,304. The before/after comparison shows how dramatically the plan shrinks.

We then run this query. The first panel executes the optimized plan directly from the Plan tab. We add a second run with pruning disabled. The result: roughly 15 milliseconds with pruning versus 420 milliseconds without — a 28x improvement, simply by skipping irrelevant graphlets early.

Now a two-hop query: person to city to country, returning five properties on co with aggregation. This query has three node variables — p, c, and co — each matching all 1,304 graphlets. If we enumerate join orderings via DP and then push joins below UnionAll, each ordering expands into 1,304 cubed sub-trees — over 2.2 billion per ordering, roughly 6.6 billion in total.

GEM collapses this to just 6 by partitioning graphlets into virtual groups. Each group yields a different join order — forward versus reverse — so GEM finds qualitatively different plans, not just fewer.

We run the query: 480 milliseconds with all optimizations, versus 1,535 without GEM and SSRF — a 3.2x difference from optimizer and intermediate overhead alone.

In the Inspect tab, we select IdSeek(co) to see why SSRF matters. On the right — SSRF OFF — sparse properties like elevation and areaTotal produce a wall of NULLs. On the left — SSRF ON — a schema pointer per row, target data packed contiguously in the TupleStore with zero NULLs. The cumulative counter shows roughly 59,000 NULLs eliminated across all batches.

That concludes our demonstration. Thank you.
