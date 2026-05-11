#!/bin/bash
# Load the types-mini fixture into a TurboLynx workspace. The fixture is
# a single :TypeRow label whose properties span every loader-supported
# scalar type (INT/LONG/ULONG/FLOAT/DOUBLE/DECIMAL with three widths/
# scales/STRING/DATE/DATE_EPOCHMS) plus a low-cardinality `category`
# column for GROUP BY testing.
#
# Usage:  bash scripts/load-types-mini.sh <build-dir> <workspace-dir>

set -euo pipefail

BUILD_DIR=${1:-}
WS=${2:-}
if [ -z "$BUILD_DIR" ] || [ -z "$WS" ]; then
    echo "usage: $0 <build-dir> <workspace-dir>"
    exit 2
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA="$REPO_ROOT/test/data/types-mini"
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

"$TURBOLYNX" import \
    --workspace "$WS" \
    --nodes TypeRow "$DATA/typerow.tbl" \
    --log-level warn

echo "types-mini fixture loaded to $WS"
