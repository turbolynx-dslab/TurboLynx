#!/bin/bash
# Generate (if needed) and load the synth-y dataset into a TurboLynx workspace.
# Usage: bash demo/legacy/gem-divergent-order/load_synth_y.sh <build-dir> <workspace-dir> [data-dir]

set -euo pipefail

BUILD_DIR=${1:-}
WS=${2:-}
DATA=${3:-/data/synth-y-src}
if [ -z "$BUILD_DIR" ] || [ -z "$WS" ]; then
    echo "usage: $0 <build-dir> <workspace-dir> [data-dir]"
    exit 2
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
TURBOLYNX="$BUILD_DIR/tools/turbolynx"

if [ ! -x "$TURBOLYNX" ]; then
    echo "ERROR: turbolynx binary not found at $TURBOLYNX"
    exit 1
fi

if [ ! -f "$DATA/nodes.json" ]; then
    python3 "$REPO_ROOT/demo/legacy/gem-divergent-order/gen_synth_y.py" "$DATA"
fi

rm -rf "$WS"
mkdir -p "$WS"

export TURBOLYNX_COST_VEC_VAL=${TURBOLYNX_COST_VEC_VAL:-10000}

"$TURBOLYNX" import \
    --workspace "$WS" \
    --skip-histogram \
    --nodes NODE "$DATA/nodes.json" \
    --relationships "http://ex.org/e1" "$DATA/e1.csv" \
    --relationships "http://ex.org/e2" "$DATA/e2.csv" \
    --relationships "http://ex.org/e3" "$DATA/e3.csv" \
    --log-level warn

echo "synth-y loaded to $WS"
