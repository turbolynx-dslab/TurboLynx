# Agentic DB Demo (TurboLynx × LLM × Python Sandbox)

Separate from the VLDB scene-demo under `/demo/app`, this folder scaffolds the
interactive demo described in `TurboLynx_Agentic_DB_Demo_Scenario.docx`.

## Status

- **extract/** — cinema subgraph extractor from the local DBpedia dump
  (`/source-data/dbpedia`) → per-label node CSVs + per-(predicate, src_lbl,
  dst_lbl) edge CSVs ready for `turbolynx import`.
- **app/** — standalone Next.js 16 + React 19 + Tailwind 4 app that renders
  the 3-zone layout (chat timeline · viz canvas · DB state panel).
  MVP is scenario-driven: each of the 7 steps is hard-coded in
  `app/lib/scenario.ts` and navigable via the prompt bar / thumbnail strip.
  No live LLM / DB call yet.

## Runtime

### 1. Build the cinema subgraph (one-off, ~30 min)

```bash
# Stage 1 — distinct type node IDs (~1 min, scans type.backward)
python3 /turbograph-v3/demo/agentic-demo/extract/extract_cinema.py stage1

# Stage 2 — resolve type URIs (~10 min, one pass of 16 GB nodes.json)
python3 /turbograph-v3/demo/agentic-demo/extract/extract_cinema.py stage2

# Stage 3 — classify entities (~2 min)
python3 /turbograph-v3/demo/agentic-demo/extract/extract_cinema.py stage3

# Stage 4 — filter cinema-relevant edges (small CSVs only, ~5 min)
python3 /turbograph-v3/demo/agentic-demo/extract/extract_cinema.py stage4

# Stage 5 — extract node properties (~10 min, second pass of nodes.json)
python3 /turbograph-v3/demo/agentic-demo/extract/extract_cinema.py stage5

# Stage 6 — split edges by (src_label, dst_label) pairs (~1 min)
python3 /turbograph-v3/demo/agentic-demo/extract/extract_cinema.py stage6

# Or all at once:
python3 /turbograph-v3/demo/agentic-demo/extract/extract_cinema.py all
```

Outputs:

- Working files (intermediate cache): `/data/dbpedia-cinema-work/`
- Final CSVs: `/data/dbpedia-cinema-src/{nodes,edges}/`

### 2. Load into TurboLynx

```bash
# Programmatic import — see scripts/load-cinema.sh (to be added)
/turbograph-v3/build-release/tools/turbolynx import \
    --workspace /data/dbpedia-cinema \
    --skip-histogram \
    --nodes Film /data/dbpedia-cinema-src/nodes/Film.csv \
    --nodes Actor /data/dbpedia-cinema-src/nodes/Actor.csv \
    ...
    --relationships DIRECTED_BY /data/dbpedia-cinema-src/edges/DIRECTED_BY__Film_FilmDirector.csv \
    ...
```

### 3. Run the MCP server against the cinema workspace

```bash
TURBOLYNX_WORKSPACE=/data/dbpedia-cinema \
TURBOLYNX_ALLOW_WRITES=1 \
  node /turbograph-v3/tools/mcp/src/server.js
```

Already exposes every tool the scenario needs: `list_labels`, `describe_label`,
`sample_label`, `query_cypher`, `explain_cypher`, `mutate_cypher`.

### 4. UI (MVP — scenario-driven)

```bash
cd /turbograph-v3/demo/agentic-demo/app
pnpm install
pnpm dev    # → http://localhost:3100
```

## Next steps

- **Live agent loop**: Claude Messages API with the same 6 tools mapped to
  direct `query_cypher` etc. bindings against the TurboLynx WASM build. A
  thin FastAPI server can sit between the Next.js UI and the agent.
- **Python sandbox**: subprocess with stripped env + wall-clock + memory
  limits. Ship a requirements file with the libraries the scenario calls out
  (networkx, python-louvain, sentence-transformers, pyvis, pandas,
  matplotlib, python-docx).
- **Real visualizations**: replace canvas placeholders in
  `components/VizCanvas.tsx` with actual network / Sankey / heatmap renders
  from the Python side streamed back as PNGs or JSON that a React component
  paints.
- **Seed duplicates** for the dedup scene: either introduce them synthetically
  during load or mine DBpedia's wiki-redirect pairs to produce real-looking
  cases.
