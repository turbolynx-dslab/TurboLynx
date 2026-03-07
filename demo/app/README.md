# TurboLynx Demo — Next.js Frontend

Interactive demo site for the TurboLynx graph DBMS paper.
Targets **VLDB / SIGMOD Demo Track**.

For full scenario documentation see [`../DEMO_SCENARIO.md`](../DEMO_SCENARIO.md).

---

## Quick Start

```bash
cd demo/app
pnpm install      # first time only
pnpm dev          # starts on http://localhost:3000
```

> **Node ≥ 18** and **pnpm ≥ 8** required.

For a production build:

```bash
pnpm build
pnpm start        # serves the production build
```

---

## Project Structure

```
demo/app/
├── app/
│   ├── page.tsx              # Root page — scene router, step state
│   └── layout.tsx            # HTML shell, global styles
├── components/
│   └── scenes/
│       ├── SceneNav.tsx      # Top navigation bar (scene + step controls)
│       ├── S0_Problem.tsx    # Scene 0: Schemaless property graph & the problem
│       ├── S1_CGC.tsx        # Scene 1: CGC clustering algorithm animation
│       ├── S2_Query.tsx      # Scene 2: UNION ALL plan, Schema Index, live query demo
│       ├── S3_GEM.tsx        # Scene 3: GEM join-order search space & grouping
│       ├── S4_SSRF.tsx       # Scene 4: SSRF intermediate result format
│       └── S5_Performance.tsx# Scene 5: Live query runner + benchmark comparison
├── lib/
│   └── demo-data.ts          # Graphlet definitions, demo persons, query presets
└── public/
    ├── dbpedia_graph.json    # Sampled DBpedia subgraph for Scene 0 visualization
    └── cgc_sample.json       # ~1000-node DBpedia sample for Scene 1 CGC animation
```

---

## Scene Overview

| # | Scene | Steps | Key interactions |
|---|---|---|---|
| 0 | **Problem** | 3 | Click bars to highlight nodes; animated split/merge scan |
| 1 | **CGC** | 2 | Step through clustering phases; watch cost curve drop |
| 2 | **Query** | 2 | Schema Index concept; live query with plan + results |
| 3 | **GEM** | 2 | Slider to see search space explosion; interactive grouping |
| 4 | **SSRF** | 2 | Preset selector; SchemaInfos / TupleStore / OffsetArr visualization |
| 5 | **Performance** | 1 | Run live queries; browse Table 4 benchmark results |

Navigation: **top bar** switches scenes; **Prev / Next** buttons step within a scene.

---

## Data Files (`public/`)

These JSON files are pre-generated from DBpedia and checked in.
Regenerate them only if the source data changes (see `../scripts/`).

| File | Size | Description |
|---|---|---|
| `dbpedia_graph.json` | ~54 KB | Force-directed graph for Scene 0. ~200 nodes, ~300 edges, with `schema[]` per node. |
| `cgc_sample.json` | ~270 KB | Person-type nodes for Scene 1 CGC animation. ~1000 nodes with schema arrays. |

---

## Live Query Backend

Scene 2 and Scene 5 send queries to a running TurboLynx instance.
The backend URL is configured in `lib/demo-data.ts` (or via `NEXT_PUBLIC_TURBO_URL` env var).

If no backend is running the UI falls back to **simulated results** (mock data + realistic timing).

---

## Tech Stack

| Layer | Library |
|---|---|
| Framework | Next.js 15 (App Router) |
| Animation | Framer Motion |
| Graph physics | Custom force simulation (no D3) |
| Styling | Inline CSS-in-JS (no Tailwind) |
| Package manager | pnpm |
