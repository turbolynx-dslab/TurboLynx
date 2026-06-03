#!/bin/bash
# Load the committed DBpedia mini fixture (test/data/dbpedia-mini) into
# a TurboLynx workspace.  Used by CI and any developer who wants to run
# the [dbpedia-mini] bulkload test locally without staging the full
# 16 GB snapshot.
#
# Usage:  bash scripts/load-dbpedia-mini.sh <build-dir> <workspace-dir>

set -euo pipefail

BUILD_DIR=${1:-}
WS=${2:-}
if [ -z "$BUILD_DIR" ] || [ -z "$WS" ]; then
    echo "usage: $0 <build-dir> <workspace-dir>"
    exit 2
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA="$REPO_ROOT/test/data/dbpedia-mini"
TURBOLYNX="$BUILD_DIR/tools/turbolynx"

if [ ! -x "$TURBOLYNX" ]; then
    echo "ERROR: turbolynx binary not found at $TURBOLYNX"
    exit 1
fi
if [ ! -d "$DATA" ]; then
    echo "ERROR: fixture not found at $DATA"
    exit 1
fi

rm -rf "$WS"
mkdir -p "$WS"

# Only the three edge types that survived the V × V intersection on
# this host's source data are loaded — see
# scripts/preprocessors/dbpedia-preprocessors/README.md.  The full
# DBpedia bulkload uses 32 edge types and is loaded by load-dbpedia.sh
# against /source-data/dbpedia/.
#
# Clustering cost overrides (see graph_simdjson_parser.hpp):
#   Production defaults (CostSchemaVal=100, CostNullVal=0.3,
#   CostVectorizationVal=10000) reproduce the SOSP paper's clustering
#   numbers. With this mini fixture's small per-schema cardinality
#   (avg ~2 tuples), those defaults collapse all 282 raw property
#   schemas into a single merged graphlet because the vectorization
#   saving from merging tiny clusters dwarfs every other term — and
#   that defeats the schemaless purpose of the fixture. Dropping the
#   vectorization weight to 10 here keeps the schema/null balance at
#   the production ratio but stops the vec term from steamrolling
#   tiny clusters; the resulting fixture lands at ~7 graphlets and
#   exercises the multi-graphlet read path.
export TURBOLYNX_COST_VEC_VAL=${TURBOLYNX_COST_VEC_VAL:-10}

"$TURBOLYNX" import \
    --workspace "$WS" \
    --skip-histogram \
    --nodes NODE "$DATA/nodes.json" \
    --relationships "http://dbpedia.org/ontology/nationality" \
        "$DATA/edges_nationality_797.csv" \
    --relationships "http://dbpedia.org/ontology/country" \
        "$DATA/edges_country_8183.csv" \
    --relationships "http://dbpedia.org/ontology/birthPlace" \
        "$DATA/edges_birthPlace_2950.csv" \
    --log-level warn

echo "DBpedia mini fixture loaded to $WS"
