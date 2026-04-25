# Intro Slides — Agentic DB Demo

Short deck to play before the 6-step walkthrough. Target: under 90 seconds, six slides.

---

## Slide 1 — Title

> **Agentic Database**
>
> An LLM agent, a schemaless graph, and code generated on the fly.
>
> *TurboLynx demo · 2026*

*Speaker note* — We're not demonstrating a feature of TurboLynx. We're demonstrating a division of labour between the database and an autonomous agent.

---

## Slide 2 — What is an agentic database?

- **Not a passive store.** It is the structure over which an agent plans, retrieves, and reasons.
- Plans, reasoning paths, tool routing — **inherently graph-shaped.**
- A schemaless graph engine is a natural fit: heterogeneous entities, free-form relations, no rigid schema to fight with.

*Speaker note* — The concept is data-model agnostic, but graphs have the right topology. Calling it "agentic" is a role statement, not a technical label.

---

## Slide 3 — Separation of roles

```
┌─────────────┐      ┌───────────┐      ┌────────────┐
│    User     │ ─→  │   Agent   │ ─→  │ TurboLynx  │
│  (natural   │      │  (plans,  │      │  (stores,  │
│  language)  │ ←── │  writes   │ ←── │  queries,  │
│             │      │  code)    │      │  reports)  │
└─────────────┘      └───────────┘      └────────────┘
                         │
                         ▼
                  ┌────────────────┐
                  │ Python sandbox │
                  │  (networkx,    │
                  │   scikit,      │
                  │   plotly, …)   │
                  └────────────────┘
```

- **DB:** schema, reads, writes.
- **Agent:** retrieval strategy, analysis, visualization, reporting.
- **Python sandbox:** anything the agent writes on the fly.

*Speaker note* — Every time we add a capability to the demo it lands in one of these three boxes. Today, nothing new goes into the DB.

---

## Slide 4 — The dataset

- DBpedia movie subgraph: **2,841 nodes · 9,712 edges.**
- Loaded under a **single `NODE` label**. DBpedia is schemaless; we preserve that.
- Type membership is an edge — `(:NODE)-[:type]->(:NODE)` — not a label at the catalog level.
- The agent discovers types at runtime by walking those edges.

*Speaker note* — This is the hard case for analytics. A label-based system would force one schema per partition and hide the heterogeneity we actually want to surface.

---

## Slide 5 — The UI

- **Left · Conversation** — user turns + agent reasoning, tool calls inline.
- **Center · Agent Canvas** — everything the agent renders. Not provided by the DB.
- **Right · DB State** — node / edge counters and the stream of queries TurboLynx actually executed.

*Speaker note* — The three panels match the three roles. If something appears in the center, the agent made it.

---

## Slide 6 — What to watch for

Six steps follow. Keep count of two things:

| What the database does | What the agent does |
|---|---|
| Report schema | Discover types |
| Read the graph | Build a network |
| Return structured results | Detect communities (Louvain) |
|  | Rank centrality (PageRank) |
|  | Generate a Sankey + temporal heatmap |
|  | Compile an audit-trail report |

**Three DB capabilities. Six analyses.** The difference is code the agent wrote this session.

*Speaker note* — That ratio — three vs six — is the whole argument. Analytical reach is bounded not by the DB's feature list but by what the model can express in code, which is effectively unbounded. Let's walk through it.

---

*Go to Step 1 of the live demo.*
