Hello. We'll be demonstrating TurboLynx.

In the real world, data rarely fits into rigid, predefined structures. As business needs evolve, data models must adapt instantly without costly downtime or complex migrations. This demand for ultimate agility is exactly why modern enterprises rely on schemaless property graphs for dynamic applications like fraud detection, recommendation engines, and massive knowledge bases like DBpedia.

While engineering teams value the inherent freedom of this data model, the industry has long accepted a difficult trade-off: flexibility comes at the expense of analytical speed. Traditional systems simply struggle to process unpredictable schemas efficiently.

With TurboLynx, we eliminate that compromise. We have engineered a graph analytics engine with schemaless performance built into its core, outperforming state-of-the-art graph databases by up to 183 times.

In our interactive demo, we will walk you through exactly how we achieve this paradigm shift. 

We will start by examining the inherent overhead of schemaless data, and then dive into our architecture:

First, our Graph-Native Storage: You will see how our CGC algorithm organizes data into Graphlets.

Second, our Query Processing & Optimization: We will explore how our Vectorized Graph Query Processor and Orca-based Optimizer leverage SSRF and GEM to find the fastest query plan and fast process data.

Finally, the Payoff: We will run live queries, allowing you to experience the raw performance difference firsthand.

Let’s dive in.

In the Problem section, users explore what schemaless property graphs look like. 

Here, on a sampled DBpedia dataset, you can see the schema-level node distribution and how nodes are actually laid out in the graph. 

You can also understand two naive approaches to storing this data. 

Split: store each node's properties separately — no NULLs, but every query scans all nodes individually. 

Merge: one wide table — catastrophic NULLs, storage bloats to hundreds of gigabytes. Neither works. 

We take something in between.

This is where Graphlets come in. Users can intuitively understand the idea: we group nodes by schema similarity into a moderate number of graphlets. 

Graphlets that lack a required property are pruned instantly — we never scan them. 

And here, users can watch the clustering algorithm in action. The animation shows how layered agglomerative clustering works step by step — merging the pair with the lowest cost at each iteration, minimizing NULLs while consolidating schemas into a compact graphlet catalog.

Now, how do graph queries work on this storage? Here's a two-hop Cypher pattern: person → birth city → country. By default, GL_p-4 and GL_p-5 are pruned — they lack the birthPlace edge. GL_c-3 is also pruned — no country.

Now GL_p-2 is also gone — no team attribute. Only GL_p-1 and GL_p-3 survive. You can trace the exact path across graphlet boundaries.

Users can also see how this translates into an actual execution plan: UnionAll fans out across active partitions, each branch running NodeScan → AdjIdxJoin → IdSeek per hop.

One key challenge: every graphlet combination needs its own join ordering — 3 × 2 × 2 = 12 for this query. At DBpedia scale, 50 × 30 × 20 = 30,000.

GEM partitions graphlets into Virtual Groups and runs the optimizer once per group. Users can explore how different partitions yield different optimal join orders — thousands of orderings reduced to a handful, with negligible cost error.

As joins are added, columnar storage must store every schema combination — the count grows multiplicatively, and NULLs pile up fast.

SSRF solves this. The left operand stays columnar; each right operand is row-packed with a schema pointer. Schemas grow additively — 3 + 2 instead of 3 × 2 — and the right side carries zero NULLs.

In the Performance section, users can run queries directly on our database. The right panel shows the full execution pipeline — parsing, schema resolution, GEM optimization, plan generation, and execution. And here, users can compare results against other systems. Thank you.