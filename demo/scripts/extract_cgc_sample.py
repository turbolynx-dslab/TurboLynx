#!/usr/bin/env python3
"""
CGC Demo Sample Extractor
=========================
Extracts Person-type entities from DBpedia, optimized for the CGC demo.
Goal: ~1000 nodes where schemas overlap significantly (e.g. athletes with
different combinations of birthDate/team/award/height/weight/occupation).
This produces many schema groups of size 2-50, ideal for agglomerative
clustering visualization.

Output: /turbograph-v3/demo/app/public/cgc_sample.json
  { "nodes": [ { id, name, schema, schemaSize }, ... ] }
"""

import json
import os
from collections import defaultdict

NODES_FILE = "/source-data/dbpedia/nodes.json"
OUT_PATH   = "/turbograph-v3/demo/app/public/cgc_sample.json"

# Attributes that are hallmarks of Person entities in DBpedia
PERSON_INDICATORS = frozenset([
    "birthDate","birthPlace","deathDate","deathPlace",
    "nationality","occupation","award","almaMater",
    "spouse","team","height","weight","genre",
    "orderInOffice","religion","party","employer",
    "activeYearsStartYear","activeYearsEndYear",
    "successor","predecessor","education",
])

# We want nodes that have ≥3 Person indicators (so we're confident it's a person)
# AND at most 25 total attributes (otherwise it's noise-heavy)
MIN_INDICATORS = 3
MAX_SCHEMA_SIZE = 25
TARGET_COLLECT  = 3000   # collect this many candidates first


def shorten_key(k):
    return k.split("/")[-1].split("#")[-1]

def shorten_uri(uri):
    return uri.split("/")[-1].replace("_", " ")


# ── Phase 1: Collect candidate person nodes ────────────────────────────────────
print("Phase 1: Scanning nodes.json for Person candidates...")
candidates = []

with open(NODES_FILE) as f:
    for i, line in enumerate(f):
        if i % 2_000_000 == 0 and i > 0:
            print(f"  {i//1_000_000}M rows scanned, {len(candidates)} candidates found")
        if len(candidates) >= TARGET_COLLECT:
            break
        try:
            obj = json.loads(line.strip())
            props = obj.get("properties", {})
            nid = str(props.get("id", ""))
            if not nid:
                continue
            uri = props.get("uri", "")
            keys = list(set(shorten_key(k) for k in props if k not in ("id", "uri")))

            # Quality filter
            indicator_count = sum(1 for k in keys if k in PERSON_INDICATORS)
            if indicator_count < MIN_INDICATORS:
                continue
            if len(keys) > MAX_SCHEMA_SIZE:
                continue

            name = shorten_uri(uri)[:32]
            schema = sorted(keys)
            candidates.append({
                "id":         nid,
                "name":       name,
                "schema":     schema,
                "schemaSize": len(schema),
            })
        except Exception:
            pass

print(f"Collected {len(candidates)} candidates")

# ── Phase 2: Analyse schema distribution ──────────────────────────────────────
print("\nPhase 2: Analysing schema groups...")
schema_groups = defaultdict(list)
for c in candidates:
    key = "\0".join(c["schema"])
    schema_groups[key].append(c)

counts = sorted(schema_groups.items(), key=lambda x: -len(x[1]))
print(f"  Distinct schemas: {len(counts)}")
print(f"  Top 20 schema group sizes:")
for key, nodes in counts[:20]:
    attrs = key.split("\0")
    print(f"    {len(nodes):4d}× [{', '.join(attrs[:6])}{'...' if len(attrs)>6 else ''}]")

singletons = sum(1 for _, ns in counts if len(ns) == 1)
multi      = sum(1 for _, ns in counts if len(ns) >= 2)
print(f"\n  Singletons: {singletons},  Multi-node groups: {multi}")

# ── Phase 3: Select final 1000 nodes with good distribution ───────────────────
# Strategy:
#   - Keep all nodes from groups with ≥2 members (capped at 50 per group)
#   - Fill remainder from singletons up to 1000 total
print("\nPhase 3: Selecting final node set...")
TARGET_MULTI  = 800   # nodes from multi-node groups
TARGET_SINGLE = 200   # singleton nodes (for L3 layer demo)
selected = []

# Multi-node groups: top 60 by size, cap 15 nodes each
top_multi = [(k, ns) for k, ns in counts if len(ns) >= 2][:60]
for key, nodes in top_multi:
    selected.extend(nodes[:15])
# Trim to TARGET_MULTI
selected = selected[:TARGET_MULTI]

print(f"  From multi-node groups: {len(selected)} nodes ({len(top_multi)} distinct schemas)")

# Singletons for L3 layer
singleton_list = [nodes[0] for key, nodes in counts if len(nodes) == 1]
selected.extend(singleton_list[:TARGET_SINGLE])

print(f"  Total: {len(selected)} nodes")

# ── Phase 4: Recheck distribution ─────────────────────────────────────────────
final_schema_groups = defaultdict(list)
for c in selected:
    key = "\0".join(c["schema"])
    final_schema_groups[key].append(c)

fcounts = sorted(final_schema_groups.items(), key=lambda x: -len(x[1]))
fsingletons = sum(1 for _, ns in fcounts if len(ns) == 1)
fmulti      = sum(1 for _, ns in fcounts if len(ns) >= 2)
print(f"\n  Final distribution:")
print(f"    Total nodes: {len(selected)}")
print(f"    Distinct schemas: {len(fcounts)}")
print(f"    Singleton schemas: {fsingletons}")
print(f"    Multi-node schemas: {fmulti}")
print(f"  Top 15 final schema groups:")
for key, nodes in fcounts[:15]:
    attrs = key.split("\0")
    print(f"    {len(nodes):3d}× [{', '.join(attrs[:7])}{'...' if len(attrs)>7 else ''}]")

# ── Phase 5: Save output ───────────────────────────────────────────────────────
out_nodes = [{"id": c["id"], "name": c["name"],
              "schema": c["schema"], "schemaSize": c["schemaSize"]}
             for c in selected]

with open(OUT_PATH, "w") as f:
    json.dump({"nodes": out_nodes}, f, separators=(",", ":"))

size_kb = os.path.getsize(OUT_PATH) / 1024
print(f"\nSaved → {OUT_PATH} ({size_kb:.1f} KB, {len(out_nodes)} nodes)")
