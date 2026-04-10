#!/bin/bash
# Load TPC-H SF1 data into TurboLynx.
# Usage: bash scripts/load-tpch.sh [BUILD_DIR]
#   BUILD_DIR defaults to build-release; falls back to build-lwtest.

set -e

DATA=/source-data/tpch/sf1
WS=/data/tpch/sf1

# Resolve build directory
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

mkdir -p "$WS"

$TURBOLYNX import \
    --workspace "$WS" \
    --nodes CUSTOMER    --nodes "$DATA/customer.tbl" \
    --nodes LINEITEM    --nodes "$DATA/lineitem.tbl" \
    --nodes NATION      --nodes "$DATA/nation.tbl" \
    --nodes ORDERS      --nodes "$DATA/orders.tbl" \
    --nodes PART        --nodes "$DATA/part.tbl" \
    --nodes REGION      --nodes "$DATA/region.tbl" \
    --nodes SUPPLIER    --nodes "$DATA/supplier.tbl" \
    --relationships CUST_BELONG_TO   --relationships "$DATA/customer_belongTo_nation.tbl" \
    --relationships COMPOSED_BY      --relationships "$DATA/lineitem_composedBy_part.tbl" \
    --relationships IS_PART_OF       --relationships "$DATA/lineitem_isPartOf_orders.tbl" \
    --relationships SUPPLIED_BY      --relationships "$DATA/lineitem_suppliedBy_supplier.tbl" \
    --relationships IS_LOCATED_IN    --relationships "$DATA/nation_isLocatedIn_region.tbl" \
    --relationships MADE_BY          --relationships "$DATA/orders_madeBy_customer.tbl" \
    --relationships SUPP_BELONG_TO   --relationships "$DATA/supplier_belongTo_nation.tbl" \
    --relationships PARTSUPP         --relationships "$DATA/partsupp.tbl"

echo "TPC-H SF1 loaded to $WS"
