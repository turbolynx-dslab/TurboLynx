#!/usr/bin/env python3
"""
DBpedia Data Analyzer for TurboLynx Demo
=========================================
Analyzes the DBpedia graph to extract:
1. Type distribution (rdf:type edges)
2. Schema diversity within each type
3. Sample nodes for demo visualization

Usage:
    python3 analyze_dbpedia.py [--output demo_data.json]

Data sources:
    /source-data/dbpedia/nodes.json
    /source-data/dbpedia/edges_22-rdf-syntax-ns#type_6803.csv
"""

import json
import random
import argparse
from collections import defaultdict, Counter

NODES_FILE = "/source-data/dbpedia/nodes.json"
TYPE_EDGES_FILE = "/source-data/dbpedia/edges_22-rdf-syntax-ns#type_6803.csv"

# Types we want to highlight in the demo
TARGET_TYPES = [
    "Person", "Athlete", "Film", "City", "Book",
    "Place", "Organisation", "Animal", "MusicalArtist", "Politician",
]

def shorten_key(k):
    """Strip URI prefix from property key."""
    return k.split('/')[-1].split('#')[-1]

def shorten_uri(uri):
    """Get human-readable name from URI."""
    return uri.split('/')[-1].replace('_', ' ')


def load_nodes(max_rows=10_000_000):
    """Load node id -> {uri, schema_keys} mapping."""
    print(f"Loading nodes (up to {max_rows:,})...")
    nodes = {}
    with open(NODES_FILE) as f:
        for i, line in enumerate(f):
            if i >= max_rows:
                break
            if i % 2_000_000 == 0:
                print(f"  {i // 1_000_000}M rows loaded...")
            try:
                node = json.loads(line.strip())
                props = node.get("properties", {})
                nid = str(props.get("id", ""))
                uri = props.get("uri", "")
                keys = [shorten_key(k) for k in props.keys()
                        if k not in ("id", "uri")]
                nodes[nid] = {"uri": uri, "keys": keys, "props": {
                    shorten_key(k): v for k, v in props.items()
                    if k not in ("id", "uri") and isinstance(v, (str, int, float))
                    and len(str(v)) < 100  # skip very long values
                }}
            except Exception:
                pass
    print(f"  Loaded {len(nodes):,} nodes")
    return nodes


def load_type_edges(nodes, max_rows=10_000_000):
    """Build node_id -> [type_name] mapping from rdf:type edges."""
    print(f"Loading type edges (up to {max_rows:,})...")
    node_types = defaultdict(list)
    type_freq = Counter()

    with open(TYPE_EDGES_FILE) as f:
        next(f)  # skip header
        for i, line in enumerate(f):
            if i >= max_rows:
                break
            parts = line.strip().split("|")
            if len(parts) != 2:
                continue
            src, dst = parts
            turi = nodes.get(dst, {}).get("uri", "")
            # Accept both dbpedia.org/class/ and dbpedia.org/ontology/
            if "dbpedia.org" in turi:
                tname = turi.split("/")[-1]
                node_types[src].append(tname)
                type_freq[tname] += 1

    print(f"  Nodes with type: {len(node_types):,}")
    print(f"  Unique types: {len(type_freq):,}")
    return node_types, type_freq


def sample_diverse_nodes(type_nodes_list, nodes, n=6):
    """
    From a list of (node_id, node_info) pick n nodes with maximally
    different schemas. Returns list of dicts ready for JSON output.
    """
    random.shuffle(type_nodes_list)
    picked = []
    seen_schemas = set()
    for nid, info in type_nodes_list:
        sk = tuple(sorted(info["keys"]))
        if sk not in seen_schemas and len(info["keys"]) >= 2:
            picked.append({
                "id": nid,
                "name": shorten_uri(info["uri"]),
                "uri": info["uri"],
                "schema": info["keys"][:12],  # limit to 12 keys for display
                "num_props": len(info["keys"]),
                "sample_props": dict(list(info["props"].items())[:5]),
            })
            seen_schemas.add(sk)
            if len(picked) >= n:
                break
    return picked


def analyze(output_file="demo_data.json", max_node_rows=10_000_000, max_type_rows=5_000_000):
    nodes = load_nodes(max_node_rows)
    node_types, type_freq = load_type_edges(nodes, max_type_rows)

    print(f"\nTop 20 types by frequency:")
    for t, c in type_freq.most_common(20):
        print(f"  {c:6d}  {t}")

    # For each target type, gather nodes and analyze schema diversity
    results = {}
    for ttype in TARGET_TYPES:
        type_nodes = [
            (nid, nodes[nid])
            for nid, types in node_types.items()
            if ttype in types and nid in nodes
        ]
        if not type_nodes:
            print(f"  {ttype}: no nodes found")
            continue

        # Schema diversity stats
        all_schemas = [tuple(sorted(nodes[nid]["keys"])) for nid, _ in type_nodes]
        unique_schemas = len(set(all_schemas))
        all_keys = [k for s in all_schemas for k in s]
        key_freq = Counter(all_keys)

        # Sample diverse nodes
        sample = sample_diverse_nodes(type_nodes, nodes, n=6)

        results[ttype] = {
            "total_nodes": len(type_nodes),
            "unique_schemas": unique_schemas,
            "top_keys": [k for k, _ in key_freq.most_common(10)],
            "sample_nodes": sample,
        }
        print(f"\n=== {ttype} ({len(type_nodes):,} nodes, {unique_schemas:,} unique schemas) ===")
        for s in sample:
            print(f"  {s['name'][:40]:40s}  {s['schema'][:5]} ({s['num_props']} props)")

    # Save
    with open(output_file, "w") as f:
        json.dump({
            "type_freq": dict(type_freq.most_common(50)),
            "types": results,
        }, f, indent=2)
    print(f"\nSaved to {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="demo_data.json")
    parser.add_argument("--max-nodes", type=int, default=10_000_000)
    parser.add_argument("--max-types", type=int, default=5_000_000)
    args = parser.parse_args()
    analyze(args.output, args.max_nodes, args.max_types)
