#!/bin/bash
# Load the OSS supply-chain test fixture into TurboLynx.
# Usage: bash scripts/load-oss.sh [BUILD_DIR]
#   BUILD_DIR defaults to build-release; falls back to build-lwtest.

set -e

DATA=/turbograph-v3/applications/oss-supply-chain/tests/fixtures
WS=/data/oss/small

BUILD_DIR="${1:-}"
if [ -z "$BUILD_DIR" ]; then
    if [ -x /turbograph-v3/build-release/tools/turbolynx ]; then
        BUILD_DIR=/turbograph-v3/build-release
    elif [ -x /turbograph-v3/build-lwtest/tools/turbolynx ]; then
        BUILD_DIR=/turbograph-v3/build-lwtest
    else
        echo "ERROR: No turbolynx binary found. Build first."
        exit 1
    fi
fi

TURBOLYNX="$BUILD_DIR/tools/turbolynx"
echo "Using: $TURBOLYNX"
echo "Source: $DATA"
echo "Target: $WS"

[ -n "$WS" ] || { echo "ERROR: WS path is empty"; exit 1; }
rm -rf "$WS"
mkdir -p "$WS"

$TURBOLYNX import \
    --workspace "$WS" \
    --nodes Package        --nodes "$DATA/nodes_package.csv" \
    --nodes Version        --nodes "$DATA/nodes_version.csv" \
    --nodes Maintainer     --nodes "$DATA/nodes_maintainer.csv" \
    --nodes Repository     --nodes "$DATA/nodes_repository.csv" \
    --nodes License        --nodes "$DATA/nodes_license.csv" \
    --nodes CVE            --nodes "$DATA/nodes_cve.csv" \
    --relationships HAS_VERSION    --relationships "$DATA/edges_has_version.csv" \
    --relationships DEPENDS_ON     --relationships "$DATA/edges_depends_on.csv" \
    --relationships MAINTAINED_BY  --relationships "$DATA/edges_maintained_by.csv" \
    --relationships HOSTED_AT      --relationships "$DATA/edges_hosted_at.csv" \
    --relationships DECLARES       --relationships "$DATA/edges_declares.csv" \
    --relationships AFFECTED_BY    --relationships "$DATA/edges_affected_by.csv"

echo "OSS supply-chain fixture loaded to $WS"
