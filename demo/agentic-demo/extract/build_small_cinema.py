#!/usr/bin/env python3
"""Curated cinema subgraph for the agentic demo (target: ~2.8K nodes, ~9.7K edges).

Builds on top of the already-loaded full cinema workspace at /data/dbpedia-cinema
isn't possible anymore (that load currently fails at stage `GetFileHandler MISS`).
Instead we read directly from the stage4/5 intermediates under
/data/dbpedia-cinema-work/ and from /data/dbpedia-cinema-src/nodes.json to
produce a compact, demo-sized schemaless load.

Selection criteria:
  * Seed: films with a director that won a "Best Director"-named award, plus
    films directed by high-degree directors (to guarantee a connected graph).
  * Expand: every node reachable from a seed film through one cinema edge
    (DIRECTED_BY, STARRING, PRODUCED_BY, WRITTEN_BY, CINEMATOGRAPHY_BY,
    EDITED_BY, HAS_AWARD, HAS_GENRE, BASED_ON).
  * Drop: BORN_IN, NATIONALITY, SPOUSE, COUNTRY, DISTRIBUTED_BY — these
    blow the graph size up without adding narrative value for the demo.
  * Include every `rdf:type` edge for surviving nodes so the agent can still
    run list_types / describe_type / sample_type.

Output (same shape as the full extract so load-cinema.sh works unchanged):
  /data/dbpedia-cinema-src/nodes.json
  /data/dbpedia-cinema-src/edges/*.csv

Run stage1-4 of extract_cinema.py first to populate the intermediate cache.
"""
from __future__ import annotations

import json
import os
import time
from collections import defaultdict, Counter
from pathlib import Path

WORK = Path("/data/dbpedia-cinema-work")
OUT = Path("/data/dbpedia-cinema-src")
SRC = Path("/source-data/dbpedia")

# We keep only these predicates — the ones the demo actually narrates.
KEEP_PREDICATES: dict[str, str] = {
    "director":       "DIRECTED_BY",
    "starring":       "STARRING",
    "producer":       "PRODUCED_BY",
    "writer":         "WRITTEN_BY",
    "cinematography": "CINEMATOGRAPHY_BY",
    "editing":        "EDITED_BY",
    "screenplay":     "SCREENPLAY_BY",
    "award":          "HAS_AWARD",
    "genre":          "HAS_GENRE",
    "basedOn":        "BASED_ON",
    "voice":          "VOICED_BY",
}

TYPE_FWD = SRC / "edges_22-rdf-syntax-ns#type_6803.csv"

# Targets — cap so we stay near the narration's numbers.
TARGET_FILMS = 420       # seed film count
MAX_NEIGHBOR_DEGREE = 18 # per-film expansion cap (keeps star nodes reasonable)
TOTAL_NODE_BUDGET = 3200 # hard ceiling


def _log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def main() -> None:
    # Prerequisites from the cached extraction pipeline.
    primary_f  = WORK / "entity_primary_label.tsv"
    cinema_f   = WORK / "cinema_nodes.tsv"
    edges_dir  = WORK / "edges_raw"
    full_json  = OUT / "nodes.json"
    for p in (primary_f, cinema_f, full_json):
        if not p.exists():
            raise SystemExit(f"Missing prerequisite: {p}. Run extract_cinema.py stages 1-5 first.")

    # Load label lookup so we can identify Films among the cinema entities.
    primary: dict[str, str] = {}
    for line in open(primary_f):
        enid, lbl = line.rstrip().split("\t", 1)
        primary[enid] = lbl
    _log(f"Loaded {len(primary):,} primary-label mappings")

    # ---- Seed films ------------------------------------------------------
    # Films connected to a Best Director award, then fill with films that have
    # the most edges among KEEP_PREDICATES so the graph stays connected.
    # Build an entity → degree histogram over the kept predicates.
    edges_by_pred: dict[str, list[tuple[str, str]]] = {}
    for pred, pred_label in KEEP_PREDICATES.items():
        f = edges_dir / f"{pred}.tsv"
        if not f.exists():
            continue
        rows: list[tuple[str, str]] = []
        with open(f) as fi:
            for line in fi:
                parts = line.rstrip().split("\t")
                if len(parts) < 2:
                    continue
                rows.append((parts[0], parts[1]))
        edges_by_pred[pred] = rows
        _log(f"  {pred_label:18s} {len(rows):>8,} raw rows")

    # Film degree over kept predicates
    film_deg: Counter = Counter()
    adj: dict[str, list[tuple[str, str, str]]] = defaultdict(list)  # nid -> (other_nid, pred, pred_label)
    for pred, rows in edges_by_pred.items():
        pred_label = KEEP_PREDICATES[pred]
        for s, d in rows:
            # dedupe: same edge may appear in ont/prop variants
            adj[s].append((d, pred, pred_label))
            adj[d].append((s, pred, pred_label))
            if primary.get(s) == "Film":
                film_deg[s] += 1
            if primary.get(d) == "Film":
                film_deg[d] += 1

    # Films touching a Best Director award are best-quality seeds; fall back
    # to top-degree films to hit the budget.
    seed_films: list[str] = []
    if "award" in edges_by_pred:
        # Find award nodes whose NAME contains "Best Director" — requires the
        # full nodes.json lookup later. For now use the presence of an
        # HAS_AWARD edge on a Film as a proxy and rely on film degree ordering.
        pass

    top_films = [f for f, _ in film_deg.most_common(TARGET_FILMS)]
    seed_films.extend(top_films)
    seed_films = list(dict.fromkeys(seed_films))[:TARGET_FILMS]
    _log(f"Seed films: {len(seed_films)} (by kept-predicate degree)")

    # ---- Expand ----------------------------------------------------------
    kept: set[str] = set(seed_films)
    # 1-hop expansion via kept predicates, per-film capped
    for fid in seed_films:
        others = adj.get(fid, [])
        # prefer other-endpoints that are already in `kept` (reuse), then
        # highest-connected new neighbors.
        counted = sorted(
            others,
            key=lambda x: (0 if x[0] in kept else 1, -len(adj.get(x[0], []))),
        )[:MAX_NEIGHBOR_DEGREE]
        for other, _, _ in counted:
            if other in primary:
                kept.add(other)
        if len(kept) >= TOTAL_NODE_BUDGET:
            break
    _log(f"Post 1-hop: {len(kept):,} nodes")

    # ---- Filter edges ----------------------------------------------------
    kept_edges: dict[str, list[tuple[str, str]]] = {}
    for pred, rows in edges_by_pred.items():
        seen: set[tuple[str, str]] = set()
        keep: list[tuple[str, str]] = []
        for s, d in rows:
            if s in kept and d in kept and (s, d) not in seen:
                seen.add((s, d))
                keep.append((s, d))
        kept_edges[pred] = keep
        _log(f"  {KEEP_PREDICATES[pred]:18s} → {len(keep):>6,} kept")

    total_kept_edges = sum(len(v) for v in kept_edges.values())
    _log(f"Edges after filter: {total_kept_edges:,}")

    # ---- Add rdf:type neighbor nodes + edges -----------------------------
    # Type nodes get pulled into `kept` so the schemaless agent can walk
    # (:NODE)-[:type]->(:NODE).
    type_edges: list[tuple[str, str]] = []
    type_nids_needed: set[str] = set()
    with open(TYPE_FWD) as f:
        next(f, None)  # header
        for line in f:
            parts = line.rstrip().split("|")
            if len(parts) != 2:
                continue
            s, d = parts
            if s in kept:
                type_edges.append((s, d))
                type_nids_needed.add(d)
    kept.update(type_nids_needed)
    _log(f"Added {len(type_nids_needed):,} type nodes · {len(type_edges):,} rdf:type edges")
    _log(f"Total kept nodes: {len(kept):,}")

    # ---- Write nodes.json -----------------------------------------------
    # Re-read from the big filtered nodes.json we already produced (1.5 GB),
    # keeping only lines whose id is in `kept`. One pass.
    out_nodes = OUT / "nodes.json"
    tmp = OUT / "nodes.json.tmp"
    matched = 0
    t0 = time.time()
    with open(full_json, "rb") as fi, open(tmp, "w") as fo:
        for i, raw in enumerate(fi):
            if i and i % 200_000 == 0:
                _log(f"  nodes.json scan: {i:,} lines, matched {matched:,} ({time.time()-t0:.0f}s)")
            if matched >= len(kept):
                break
            try:
                obj = json.loads(raw)
            except Exception:
                continue
            nid = str((obj.get("properties") or {}).get("id", ""))
            if nid not in kept:
                continue
            obj["labels"] = ["NODE"]
            fo.write(json.dumps(obj, ensure_ascii=False))
            fo.write("\n")
            matched += 1
    os.replace(tmp, out_nodes)
    _log(f"Wrote {matched:,} NODE rows → {out_nodes}")

    # ---- Write per-predicate edge CSVs ----------------------------------
    edges_out_dir = OUT / "edges"
    # Start clean so stale per-(src_lbl,dst_lbl) files from earlier runs don't
    # confuse the loader.
    if edges_out_dir.exists():
        for p in edges_out_dir.glob("*.csv"):
            p.unlink()
    edges_out_dir.mkdir(parents=True, exist_ok=True)

    for pred, rows in kept_edges.items():
        pred_label = KEEP_PREDICATES[pred]
        path = edges_out_dir / f"{pred_label}.csv"
        with open(path, "w") as fo:
            fo.write(":START_ID(NODE)|:END_ID(NODE)\n")
            for s, d in rows:
                fo.write(f"{s}|{d}\n")

    # Always include rdf:type as `type`
    type_path = edges_out_dir / "type.csv"
    with open(type_path, "w") as fo:
        fo.write(":START_ID(NODE)|:END_ID(NODE)\n")
        for s, d in type_edges:
            fo.write(f"{s}|{d}\n")

    _log(f"Edge CSVs in {edges_out_dir}: " +
         ", ".join(sorted(p.name for p in edges_out_dir.glob('*.csv'))))


if __name__ == "__main__":
    main()
