#!/bin/bash
# Load the committed TPC-H SF0.01 fixture (test/data/tpch-mini) into a
# TurboLynx workspace. Used by CI and any developer who wants to run
# the [tpch] query tests locally without generating SF1 data.
#
# Usage:  bash scripts/load-tpch-mini.sh <build-dir> <workspace-dir>

set -euo pipefail

BUILD_DIR=${1:-}
WS=${2:-}
if [ -z "$BUILD_DIR" ] || [ -z "$WS" ]; then
    echo "usage: $0 <build-dir> <workspace-dir>"
    exit 2
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA="$REPO_ROOT/test/data/tpch-mini"
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
    --nodes CUSTOMER "$DATA/customer.tbl" \
    --nodes LINEITEM "$DATA/lineitem.tbl" \
    --nodes NATION   "$DATA/nation.tbl" \
    --nodes ORDERS   "$DATA/orders.tbl" \
    --nodes PART     "$DATA/part.tbl" \
    --nodes REGION   "$DATA/region.tbl" \
    --nodes SUPPLIER "$DATA/supplier.tbl" \
    --relationships CUST_BELONG_TO  "$DATA/customer_belongTo_nation.tbl" \
    --relationships COMPOSED_BY     "$DATA/lineitem_composedBy_part.tbl" \
    --relationships IS_PART_OF      "$DATA/lineitem_isPartOf_orders.tbl" \
    --relationships SUPPLIED_BY     "$DATA/lineitem_suppliedBy_supplier.tbl" \
    --relationships IS_LOCATED_IN   "$DATA/nation_isLocatedIn_region.tbl" \
    --relationships MADE_BY         "$DATA/orders_madeBy_customer.tbl" \
    --relationships SUPP_BELONG_TO  "$DATA/supplier_belongTo_nation.tbl" \
    --relationships PARTSUPP        "$DATA/partsupp.tbl" \
    --log-level warn

echo "TPC-H SF0.01 mini fixture loaded to $WS"
