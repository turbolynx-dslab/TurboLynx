#!/usr/bin/env python3
"""Pull the Best Director collaborator network out of the loaded cinema
workspace and write it as a JSON file the UI can serve statically.

Run the turbolynx shell via subprocess with JSON output, then trim and
emit to `/demo/agentic-demo/app/public/network.json`.
"""
from __future__ import annotations

import json
import subprocess
import random
import re
from pathlib import Path

TL = "/turbograph-v3/build-release/tools/turbolynx"
WS = "/data/dbpedia-cinema"
OUT = Path("/turbograph-v3/demo/agentic-demo/app/public/network.json")
OUT.parent.mkdir(parents=True, exist_ok=True)


def run(cypher: str) -> list[dict]:
    out = subprocess.check_output(
        [TL, "shell", "-w", WS, "-q", cypher, "-m", "json"],
        text=True, stderr=subprocess.DEVNULL,
    )
    # Output has framing lines like "Database Connected" — grab the JSON array.
    m = re.search(r"\[\s*{.*?}\s*\]", out, re.DOTALL)
    if not m:
        return []
    # Try strict first — fall back to lenient if the shell's JSON writer
    # emits anything quirky.
    try:
        return json.loads(m.group(0))
    except json.JSONDecodeError:
        # Last-resort: the shell's json sink sometimes uses single quotes for
        # strings. Replace them cautiously only when the string is a bareword.
        return json.loads(m.group(0).replace("'", '"'))


def main() -> None:
    # 1. Top directors by film count. We originally keyed on "Best Director"
    # award edges, but DBpedia's award-name coverage is sparse for our loaded
    # subset (< 20 matching directors). Using direction count produces a
    # recognisable roster (Scorsese, Spielberg, Hitchcock, Kurosawa, …) in
    # the same order of magnitude.
    dirs = run(
        "MATCH (f:Film)-[:DIRECTED_BY]->(p:Person) "
        "WITH p, count(DISTINCT f) AS nfilms "
        "WHERE nfilms >= 8 "
        "RETURN p.id AS id, p.name AS name, nfilms AS nfilms "
        "ORDER BY nfilms DESC LIMIT 50"
    )
    # Tack on the award-matching directors we originally wanted so the
    # agent's "Best Director" narrative still has grounding.
    award_dirs = run(
        "MATCH (p:Person)-[:HAS_AWARD]->(a:Award) "
        "WHERE a.name CONTAINS 'Best Director' "
        "RETURN DISTINCT p.id AS id, p.name AS name LIMIT 40"
    )
    seen: set[int] = set()
    combined: list[dict] = []
    for d in dirs + award_dirs:
        if d.get("name") and d["id"] not in seen:
            combined.append(d); seen.add(d["id"])
    dirs = combined
    director_ids = [d["id"] for d in dirs]
    if not director_ids:
        print("No directors found — is the cinema workspace loaded?")
        return

    print(f"Directors: {len(director_ids)}")

    # 2. Films directed by those directors.
    films_rows = run(
        f"MATCH (f:Film)-[:DIRECTED_BY]->(p:Person) "
        f"WHERE p.id IN [{','.join(str(i) for i in director_ids)}] "
        f"RETURN DISTINCT f.id AS fid, f.name AS fname, f.releaseYear AS year, p.id AS pid"
    )
    print(f"Films: {len(films_rows)}")
    film_ids = sorted({r["fid"] for r in films_rows})

    # 3. Starring links: film -> actors/persons
    if film_ids:
        stars_rows = run(
            f"MATCH (f:Film)-[:STARRING]->(x:Person) "
            f"WHERE f.id IN [{','.join(str(i) for i in film_ids[:500])}] "
            f"RETURN DISTINCT f.id AS fid, x.id AS xid, x.name AS xname"
        )
    else:
        stars_rows = []
    print(f"Starring edges: {len(stars_rows)}")

    # 4. Build node+edge lists (people-only network w/ weighted co-work edges).
    # Collapse Film into edges: if director D and collaborator C both worked on
    # film F, bump w(D,C). Keeps the graph dense enough for louvain to chew on.
    people: dict[int, dict] = {}
    for d in dirs:
        people.setdefault(d["id"], {"id": d["id"], "name": d["name"], "role": "director"})

    film_directors: dict[int, list[int]] = {}
    for r in films_rows:
        film_directors.setdefault(r["fid"], []).append(r["pid"])

    edges_w: dict[tuple[int, int], int] = {}
    for r in stars_rows:
        if not r.get("xname"):
            continue
        people.setdefault(r["xid"], {"id": r["xid"], "name": r["xname"], "role": "collaborator"})
        dirs_on = film_directors.get(r["fid"], [])
        for did in dirs_on:
            if did == r["xid"]:
                continue
            key = (min(did, r["xid"]), max(did, r["xid"]))
            edges_w[key] = edges_w.get(key, 0) + 1

    # Prune isolates
    incident = {n for e in edges_w for n in e}
    nodes = [p for p in people.values() if p["id"] in incident]
    # Stable ordering so force-directed layout is reproducible
    nodes.sort(key=lambda n: (-sum(1 for e in edges_w if n["id"] in e), n["name"]))

    # Reindex: short 0..N indices for the viz
    idx = {n["id"]: i for i, n in enumerate(nodes)}
    edges_out = [
        {"s": idx[a], "t": idx[b], "w": w}
        for (a, b), w in edges_w.items()
        if a in idx and b in idx
    ]

    # --- Louvain communities + PageRank ----------------------------------
    # Run them here at extract time so the browser doesn't have to ship
    # python-louvain / scipy — a few hundred nodes fit comfortably on
    # server-side and the JSON is small.
    import networkx as nx
    import community as community_louvain

    G = nx.Graph()
    for i, n in enumerate(nodes):
        G.add_node(i)
    for e in edges_out:
        G.add_edge(e["s"], e["t"], weight=e["w"])
    partition = community_louvain.best_partition(G, weight="weight", random_state=42)
    pagerank = nx.pagerank(G, weight="weight")

    # Normalize PageRank into [0,1] so the UI can scale node radius without
    # needing to know the raw value range.
    pr_values = list(pagerank.values())
    pr_min = min(pr_values) if pr_values else 0.0
    pr_max = max(pr_values) if pr_values else 1.0
    pr_span = (pr_max - pr_min) or 1.0

    out = {
        "nodes": [
            {
                "i": i,
                "name": n["name"],
                "role": n["role"],
                "c": int(partition.get(i, 0)),
                "pr": round((pagerank.get(i, 0.0) - pr_min) / pr_span, 4),
            }
            for i, n in enumerate(nodes)
        ],
        "edges": edges_out,
        "stats": {
            "n_directors": sum(1 for n in nodes if n["role"] == "director"),
            "n_collaborators": sum(1 for n in nodes if n["role"] == "collaborator"),
            "n_edges": len(edges_out),
            "n_communities": len(set(partition.values())),
        },
    }
    OUT.write_text(json.dumps(out, separators=(",", ":")))
    print(f"Wrote {OUT}: {out['stats']}")


if __name__ == "__main__":
    main()
