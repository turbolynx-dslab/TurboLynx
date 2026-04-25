#!/usr/bin/env bash
# Load the cinema subgraph into a fresh TurboLynx workspace.
# Prerequisite: stages 1-6 of extract_cinema.py have produced
#   /data/dbpedia-cinema-src/nodes.json
#   /data/dbpedia-cinema-src/edges/*.csv
#
# This loads every node under the single `NODE` label — DBpedia is a
# schemaless property graph and we preserve that here. Type membership is
# expressed by `rdf-syntax-ns#type`-style edges (edge label = `type`).

set -euo pipefail

SRC=/data/dbpedia-cinema-src
WS=${WS:-/data/dbpedia-cinema}
TL=/turbograph-v3/build-release/tools/turbolynx

if [[ ! -f "$SRC/nodes.json" ]]; then
  echo "Missing $SRC/nodes.json — run extract_cinema.py (stage5) first." >&2
  exit 1
fi
if [[ ! -d "$SRC/edges" ]]; then
  echo "Missing $SRC/edges — run extract_cinema.py (stage6) first." >&2
  exit 1
fi

if [[ -d "$WS" ]]; then
  echo "Workspace $WS already exists. Delete it (rm -rf $WS) before re-loading." >&2
  exit 1
fi
mkdir -p "$WS"

args=(--workspace "$WS" --skip-histogram)

# Single node partition — schemaless.
args+=(--nodes NODE --nodes "$SRC/nodes.json")

# One edge label per file. The file basename is the edge type used in Cypher
# (e.g. DIRECTED_BY, STARRING, type). Tiny files with very few rows tripped
# loader corner cases in earlier runs, so still skip those.
MIN_ROWS=${MIN_ROWS:-20}
for f in "$SRC"/edges/*.csv; do
  pred=$(basename "$f" .csv)
  n=$(wc -l < "$f")
  if (( n <= MIN_ROWS )); then
    echo "skip tiny ($((n-1)) rows): $pred" >&2
    continue
  fi
  args+=(--relationships "$pred" --relationships "$f")
done

echo "Running:"
printf '  %q \\\n' "$TL" import "${args[@]}"
echo
exec "$TL" import "${args[@]}"
