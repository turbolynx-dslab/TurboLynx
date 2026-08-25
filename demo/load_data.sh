#!/usr/bin/env bash
# =============================================================================
# Import the booth-ladder source (gen_data.py output) into a TurboLynx
# workspace.  This is the exact v7 load procedure the certified numbers were
# measured on (evidence/FINAL_GROUPING_LADDER.txt, section DATA).
#
#   ./load_data.sh                       # /data/ladder-v7-src -> /data/ladder-v7-ws
#   WS=/data/other-ws ./load_data.sh     # override either side
#
# env: BIN  turbolynx binary   (default <repo>/build-release/tools/turbolynx)
#      SRC  generated source   (default /data/ladder-v7-src)
#      WS   workspace to build (default /data/ladder-v7-ws, WIPED and rebuilt)
#
# Takes ~6 min and produces a ~2.1 GB workspace.
# =============================================================================
set -euo pipefail

DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$DEMO_DIR/.." && pwd)"
BIN=${BIN:-$REPO_ROOT/build-release/tools/turbolynx}
SRC=${SRC:-/data/ladder-v7-src}
WS=${WS:-/data/ladder-v7-ws}

if [ ! -x "$BIN" ]; then
    echo "ERROR: turbolynx binary not found at $BIN" >&2
    echo "       build it with: cd $REPO_ROOT/build-release && ninja" >&2
    exit 1
fi
if [ ! -f "$SRC/nodes.json" ]; then
    echo "ERROR: no generated source at $SRC (run gen_data.py $SRC first)" >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# LOADER ENV — deliberately NOT set, and that is load-bearing.
#
# TURBOLYNX_COST_{SCHEMA,NULL,VEC}_VAL retune the property-schema clustering
# that decides how many GRAPHLETS a label collapses into.  The certified v7
# workspace was loaded with the engine defaults and clusters to
#     NODE -> 14 graphlets      PROFILE -> 40 category graphlets
# and those two numbers are quoted all over the demo: the SI rung's whole
# story is "14 graphlets scanned -> 4 after pruning", and the 40 PROFILE
# graphlets are what make the 200-column union.  Overriding these (as
# demo/legacy/gem-divergent-order/load_synth_y.sh and scripts/load-dbpedia-mini.sh do
# for their own fixtures) changes the graphlet count and silently invalidates
# the certified figures AND the qa/ assertions.  Leave them alone unless you
# are deliberately re-certifying.
# ---------------------------------------------------------------------------
if [ -n "${TURBOLYNX_COST_VEC_VAL:-}${TURBOLYNX_COST_SCHEMA_VAL:-}${TURBOLYNX_COST_NULL_VAL:-}" ]; then
    echo "WARNING: TURBOLYNX_COST_*_VAL is set in the environment." >&2
    echo "         The certified 14 NODE / 40 PROFILE graphlet split assumes the" >&2
    echo "         engine defaults; the demo's SI numbers will not reproduce." >&2
fi

# --skip-histogram: the ladder never runs a range predicate, and histogram
# generation adds minutes to the load for no plan difference here.
#
# LABEL PINNING: the two labels MUST be imported as exactly NODE and PROFILE.
# Every query in server.py pins them (`(a:\`NODE\`)`, `(p:\`PROFILE\`)`) because
# an unlabelled variable on a multi-label workspace silently DISABLES converter
# graphlet pruning (caveat C1 / S-H4c) — the SI rung would flatline.  Renaming
# a label here means renaming it in server.py and re-certifying.

rm -rf "$WS"
mkdir -p "$WS"

echo "loading $SRC -> $WS  (~6 min)"
"$BIN" import \
    --workspace "$WS" \
    --skip-histogram \
    --nodes NODE "$SRC/nodes.json" \
    --nodes PROFILE "$SRC/profiles.json" \
    --relationships FOLLOWS "$SRC/follows.csv" \
    --relationships RECOMMENDS "$SRC/recommends.csv" \
    --relationships VISITS "$SRC/visits.csv" \
    --relationships PROFILE_OF "$SRC/profile_of.csv"

echo "booth-ladder loaded to $WS ($(du -sh "$WS" | cut -f1))"
