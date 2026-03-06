#!/usr/bin/env python3
"""
DBpedia Graph Sample Extractor (Fast version)
==============================================
Uses pre-known node IDs from dbpedia_demo_data.json as seeds,
then expands via edge files to get a connected ~100-node subgraph.

Fast strategy:
  1. Load seeds from dbpedia_demo_data.json (already computed)
  2. Scan small edge files to find neighbor IDs (fast, files are 5-50MB)
  3. Scan nodes.json ONCE, collecting only needed node details
  4. Scan backward rdf:type file for type info of collected nodes

Output: /turbograph-v3/demo/app/public/dbpedia_graph.json
"""

import json
import random
import os
from collections import defaultdict

random.seed(42)

NODES_FILE      = "/source-data/dbpedia/nodes.json"
TYPE_BACKWARD   = "/source-data/dbpedia/edges_22-rdf-syntax-ns#type_6803.csv.backward"
DEMO_DATA_FILE  = "/turbograph-v3/demo/scripts/dbpedia_demo_data.json"

EDGE_FILES = {
    "birthPlace":    "/source-data/dbpedia/edges_birthPlace_2141.csv",
    "director":      "/source-data/dbpedia/edges_director_5557.csv",
    "author":        "/source-data/dbpedia/edges_author_2937.csv",
    "spouse":        "/source-data/dbpedia/edges_spouse_7025.csv",
    "starring":      "/source-data/dbpedia/edges_starring_5082.csv",
    "country":       "/source-data/dbpedia/edges_country_8183.csv",
    "team":          "/source-data/dbpedia/edges_team_8095.csv",
    "genre":         "/source-data/dbpedia/edges_genre_3794.csv",
    "isPartOf":      "/source-data/dbpedia/edges_isPartOf_3904.csv",
    "deathPlace":    "/source-data/dbpedia/edges_deathPlace_1201.csv",
    "placeOfBirth":  "/source-data/dbpedia/edges_placeOfBirth_2675.csv",
    "clubs":         "/source-data/dbpedia/edges_clubs_7515.csv",
    "location":      "/source-data/dbpedia/edges_location_4045.csv",
    "nationality":   "/source-data/dbpedia/edges_nationality_797.csv",
    "almaMater":     "/source-data/dbpedia/edges_almaMater_7268.csv",
    "careerStation": "/source-data/dbpedia/edges_careerStation_4347.csv",
    "producer":      "/source-data/dbpedia/edges_producer_1062.csv",
    "recordLabel":   "/source-data/dbpedia/edges_recordLabel_2294.csv",
    "occupation":    "/source-data/dbpedia/edges_occupation_2234.csv",
    "position":      "/source-data/dbpedia/edges_position_656.csv",
}

TARGET_GRAPH_SIZE = 200  # nodes in output sample graph
MAX_CITY_NODES    = 55   # cap City nodes to keep balance
MAX_EDGE_SCAN     = 500_000

TYPE_GROUP = {
    "Person": "Person", "Athlete": "Person",
    "MusicalArtist": "Person", "Politician": "Person", "Actor": "Person",
    "SportsManager": "Person", "SoccerPlayer": "Person", "BasketballPlayer": "Person",
    "Film": "Film", "TelevisionShow": "Film",
    "City": "City", "Place": "City", "Country": "City", "Settlement": "City",
    "Book": "Book", "WrittenWork": "Book",
    "Organisation": "Organisation", "Company": "Organisation", "SportsTeam": "Organisation",
}
GROUP_COLORS = {
    "Person":       "#3B82F6",
    "Film":         "#8B5CF6",
    "City":         "#F59E0B",
    "Book":         "#10B981",
    "Organisation": "#e84545",
}

def shorten_uri(uri):
    return uri.split("/")[-1].replace("_", " ")

def shorten_key(k):
    return k.split("/")[-1].split("#")[-1]

# ── Step 1: Load seeds from demo data ─────────────────────────────────────────
print("Loading seeds from demo data...")
with open(DEMO_DATA_FILE) as f:
    demo = json.load(f)

seeds = set()
for ttype, tdata in demo["types"].items():
    for node in tdata["sample_nodes"]:
        seeds.add(str(node["id"]))
print(f"  Seeds: {len(seeds)} nodes from demo data")

# ── Step 2: Scan edge files for neighbors ─────────────────────────────────────
print("Scanning edge files...")
raw_edges = {}        # label -> [(src, dst)]
adj = defaultdict(list)  # nid -> [(neighbor, label)]

for label, path in EDGE_FILES.items():
    if not os.path.exists(path):
        print(f"  MISSING: {path}")
        continue
    edges = []
    with open(path) as f:
        next(f)
        for i, line in enumerate(f):
            if i >= MAX_EDGE_SCAN:
                break
            parts = line.strip().split("|")
            if len(parts) == 2:
                src, dst = parts[0], parts[1]
                edges.append((src, dst))
                adj[src].append((dst, label))
                adj[dst].append((src, label))
    raw_edges[label] = edges
    print(f"  {label}: {len(edges):,} edges scanned")

# ── Step 3: BFS expand from seeds (type-balanced, min-degree) ─────────────────
print("Expanding graph via BFS...")
included = set(seeds)
queue = list(seeds)
random.shuffle(queue)
city_count = 0  # track City nodes added during expansion

i = 0
while i < len(queue) and len(included) < TARGET_GRAPH_SIZE:
    nid = queue[i]; i += 1
    neighbors = adj.get(nid, [])
    # Sort by degree descending (prefer well-connected neighbors)
    neighbors = sorted(neighbors, key=lambda x: -len(adj.get(x[0], [])))
    for neighbor, _ in neighbors:
        if neighbor not in included and len(included) < TARGET_GRAPH_SIZE:
            # Require min 2 connections for non-seed expansion candidates
            if len(adj.get(neighbor, [])) < 2:
                continue
            included.add(neighbor)
            queue.append(neighbor)

print(f"  Graph: {len(included)} nodes")

# ── Step 3b: Remove isolated seeds (no edges to other included nodes) ──────────
# We'll handle this after edge collection in step 7

# ── Step 4: Get type info from backward rdf:type file ─────────────────────────
print("Loading type info from backward rdf:type file...")
node_types = defaultdict(list)  # nid -> [type_uri]
type_nodes = {}  # type_nid -> type_uri

# The backward file format: END_ID(NODE)|START_ID(NODE)
# meaning: type_node_id | entity_node_id
# So we need: for entities in `included`, find their type
with open(TYPE_BACKWARD) as f:
    next(f)  # skip header
    for i, line in enumerate(f):
        if i % 5_000_000 == 0:
            print(f"  scanned {i//1_000_000}M type edges...")
        parts = line.strip().split("|")
        if len(parts) != 2:
            continue
        type_nid, entity_nid = parts[0], parts[1]
        if entity_nid in included:
            node_types[entity_nid].append(type_nid)
            type_nodes[type_nid] = None  # placeholder

print(f"  Found types for {len(node_types)} of {len(included)} nodes")
print(f"  Unique type nodes: {len(type_nodes)}")

# ── Step 5: Scan nodes.json for names + type URIs ─────────────────────────────
print("Loading node names from nodes.json...")
needed = included | set(type_nodes.keys())
node_info = {}  # nid -> {uri, name, schema_size}

with open(NODES_FILE) as f:
    for i, line in enumerate(f):
        if i % 1_000_000 == 0 and i > 0:
            print(f"  {i//1_000_000}M rows scanned, found {len(node_info)}/{len(needed)} needed")
        if len(node_info) >= len(needed):
            break
        try:
            obj = json.loads(line.strip())
            props = obj.get("properties", {})
            nid = str(props.get("id", ""))
            if nid in needed:
                uri = props.get("uri", "")
                keys = [shorten_key(k) for k in props if k not in ("id", "uri")]
                node_info[nid] = {
                    "uri": uri,
                    "name": shorten_uri(uri)[:30],
                    "schema_size": len(keys),
                    "schema": keys,
                }
        except Exception:
            pass

print(f"  Loaded {len(node_info)} node details")

# Resolve type URIs
for tnid in list(type_nodes.keys()):
    info = node_info.get(tnid, {})
    uri = info.get("uri", "")
    if uri:
        type_nodes[tnid] = uri

# ── Step 6: Assign groups to nodes ────────────────────────────────────────────
def get_group(nid):
    types = node_types.get(nid, [])
    for tnid in types:
        uri = type_nodes.get(tnid) or ""
        tname = uri.split("/")[-1] if uri else ""
        if tname in TYPE_GROUP:
            return TYPE_GROUP[tname]
        # broad fallback
        if "Person" in tname or "Player" in tname or "Manager" in tname:
            return "Person"
        if "Film" in tname or "Television" in tname or "Show" in tname:
            return "Film"
        if "City" in tname or "Place" in tname or "Country" in tname:
            return "City"
        if "Book" in tname or "Work" in tname:
            return "Book"
        if "Organisation" in tname or "Company" in tname or "Team" in tname:
            return "Organisation"
    return None

# ── Step 7: Build output ──────────────────────────────────────────────────────
print("Building output...")
out_nodes = []
nid_to_idx = {}

# Prioritize seeds first
ordered = list(seeds) + [n for n in included if n not in seeds]

for nid in ordered:
    group = get_group(nid)
    if group is None:
        continue  # skip untyped
    info = node_info.get(nid, {})
    name = info.get("name", f"Node{nid}")
    idx = len(out_nodes)
    nid_to_idx[nid] = idx
    out_nodes.append({
        "id":         nid,
        "name":       name,
        "group":      group,
        "color":      GROUP_COLORS.get(group, "#71717a"),
        "schemaSize": info.get("schema_size", 0),
        "schema":     info.get("schema", []),
        "isSeed":     nid in seeds,
    })

# Collect edges between output nodes
out_edges = []
seen_edges = set()
for label, edges in raw_edges.items():
    for src, dst in edges:
        if src in nid_to_idx and dst in nid_to_idx:
            key = (min(src,dst), max(src,dst))
            if key not in seen_edges:
                seen_edges.add(key)
                out_edges.append({
                    "s": nid_to_idx[src],
                    "d": nid_to_idx[dst],
                    "l": label,
                })

# ── Post-process: remove isolates, cap City nodes ─────────────────────────────
from collections import Counter

# Compute degrees
deg = [0] * len(out_nodes)
for e in out_edges:
    deg[e["s"]] += 1
    deg[e["d"]] += 1

# Keep only connected nodes; cap City to MAX_CITY_NODES (keep highest-degree)
city_indices = sorted([i for i, n in enumerate(out_nodes) if n["group"] == "City"],
                      key=lambda i: -deg[i])
city_keep = set(city_indices[:MAX_CITY_NODES])

keep_idx = []
for i, n in enumerate(out_nodes):
    if deg[i] == 0:
        continue  # drop isolates
    if n["group"] == "City" and i not in city_keep:
        continue  # cap excess cities
    keep_idx.append(i)

old_to_new = {old: new for new, old in enumerate(keep_idx)}
out_nodes = [out_nodes[i] for i in keep_idx]
out_edges_filtered = []
seen2 = set()
for e in out_edges:
    s, d = e["s"], e["d"]
    if s in old_to_new and d in old_to_new:
        key = (min(s, d), max(s, d))
        if key not in seen2:
            seen2.add(key)
            out_edges_filtered.append({"s": old_to_new[s], "d": old_to_new[d], "l": e["l"]})
out_edges = out_edges_filtered

group_counts = Counter(n["group"] for n in out_nodes)
print(f"\n=== Output (after filtering) ===")
print(f"  Nodes: {len(out_nodes)} ({sum(1 for n in out_nodes if n['isSeed'])} seeds)")
print(f"  Edges: {len(out_edges)}")
for g, c in sorted(group_counts.items()):
    print(f"    {g}: {c}")

result = {
    "nodes": out_nodes,
    "edges": out_edges,
    "stats": dict(group_counts),
}

out_path = "/turbograph-v3/demo/app/public/dbpedia_graph.json"
with open(out_path, "w") as f:
    json.dump(result, f, separators=(",", ":"))

size_kb = os.path.getsize(out_path) / 1024
print(f"\nSaved → {out_path} ({size_kb:.1f} KB)")
