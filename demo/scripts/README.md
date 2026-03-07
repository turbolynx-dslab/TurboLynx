# Demo Data Generation Scripts

Python scripts that extract and pre-process DBpedia data for the demo frontend.
Run these only when the source DBpedia data changes or you need to regenerate
the public JSON files.

**Source data location:** `/source-data/dbpedia/` (not in repo — must be present locally)

---

## Scripts

### `extract_graph_sample.py`

Generates `demo/app/public/dbpedia_graph.json` — the force-directed graph used in **Scene 0**.

**Strategy:**
1. Loads seed node IDs from `dbpedia_demo_data.json` (pre-computed representative nodes).
2. Expands via edge files to form a connected ~200-node subgraph.
3. Scans `nodes.json` once to collect schema and type info for collected nodes.
4. Outputs nodes with `id`, `name`, `group`, `color`, `schema[]`, `schemaSize`, `isSeed`.

```bash
python3 extract_graph_sample.py
# Output: ../app/public/dbpedia_graph.json
```

---

### `extract_cgc_sample.py`

Generates `demo/app/public/cgc_sample.json` — the ~1000-node dataset used in **Scene 1** (CGC animation).

**Strategy:**
- Focuses on Person-type entities to ensure meaningful schema overlap.
- Targets schemas of size 4–12 attributes with multiple nodes per schema (ideal for agglomerative clustering visualization).
- Outputs `{ nodes: [ { id, name, schema, schemaSize } ] }`.

```bash
python3 extract_cgc_sample.py
# Output: ../app/public/cgc_sample.json
```

---

### `analyze_dbpedia.py`

Analytical script for exploring the full DBpedia dataset.
Not required for the demo but useful for understanding schema diversity statistics (type distribution, schema size histograms, unique schema counts).

```bash
python3 analyze_dbpedia.py [--output demo_data.json]
# Output: dbpedia_demo_data.json (used as seed input by extract_graph_sample.py)
```

---

### `dbpedia_demo_data.json`

Intermediate artifact produced by `analyze_dbpedia.py`.
Contains pre-selected seed node IDs (representative nodes per entity type) used by `extract_graph_sample.py`.
Checked into the repo so `extract_graph_sample.py` can run without re-running the full analysis.

---

## Requirements

```bash
pip install -r requirements.txt   # if present, else: pip install tqdm
```

Python 3.9+. No external graph libraries needed.
