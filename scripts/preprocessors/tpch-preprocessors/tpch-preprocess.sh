#!/bin/bash
# Convert standard TPC-H .tbl files (.tbl.original) into the TurboLynx
# graph fixture format: vertex .tbl with typed headers + forward/backward
# edge .tbl files.
#
# Adapted from scripts/preprocessors/tpch-preprocessors/tpch-preprocess.sh
# (commit fc8ced385f) for portability:
#   - Skips the leading sed 's/.$//' step because DuckDB's COPY ... TO does
#     not append the trailing '|' that GNU dbgen does.
#   - Replaces GNU `sed -i` (which is incompatible with BSD sed on macOS)
#     with portable temp-file rewrites.
#
# Usage:  bash tpch-preprocess.sh <basedir>
#   <basedir> must already contain {table}.tbl.original for all 8 TPC-H tables.

set -euo pipefail

basedir=${1:-}
if [ -z "$basedir" ] || [ ! -d "$basedir" ]; then
    echo "usage: $0 <basedir>"
    exit 2
fi

# 1) Strip trailing '|' if it exists (TPC-H GNU dbgen output has it; DuckDB
#    output does not). Cheap heuristic: peek at first byte of last char.
strip_or_copy() {
    local in="$1"
    local out="$2"
    if [ "$(tail -c 2 "$in" | head -c 1)" = "|" ]; then
        sed 's/.$//' "$in" > "$out"
    else
        cp "$in" "$out"
    fi
}
for t in customer lineitem nation orders part region supplier partsupp; do
    strip_or_copy "${basedir}/${t}.tbl.original" "${basedir}/${t}.tbl"
done

# 2) Insert vertex headers (write header + body to a temp, then mv).
prepend_header() {
    local file="$1"
    local header="$2"
    {
        printf '%s\n' "$header"
        cat "$file"
    } > "${file}.tmp"
    mv "${file}.tmp" "$file"
}

prepend_header "${basedir}/customer.tbl" "C_CUSTKEY:ID(CUSTOMER)|C_NAME:STRING|C_ADDRESS:STRING|C_NATIONKEY:ULONG|C_PHONE:STRING|C_ACCTBAL:DECIMAL(12,2)|C_MKTSEGMENT:STRING|C_COMMENT:STRING"
prepend_header "${basedir}/lineitem.tbl" "L_ORDERKEY:ID_1(LINEITEM)|L_PARTKEY:ULONG|L_SUPPKEY:ULONG|L_LINENUMBER:ID_2(LINEITEM)|L_QUANTITY:INT|L_EXTENDEDPRICE:DECIMAL(12,2)|L_DISCOUNT:DECIMAL(12,2)|L_TAX:DECIMAL(12,2)|L_RETURNFLAG:STRING|L_LINESTATUS:STRING|L_SHIPDATE:DATE|L_COMMITDATE:DATE|L_RECEIPTDATE:DATE|L_SHIPINSTRUCT:STRING|L_SHIPMODE:STRING|L_COMMENT:STRING"
prepend_header "${basedir}/nation.tbl" "N_NATIONKEY:ID(NATION)|N_NAME:STRING|N_REGIONKEY:ULONG|N_COMMENT:STRING"
prepend_header "${basedir}/orders.tbl" "O_ORDERKEY:ID(ORDERS)|O_CUSTKEY:ULONG|O_ORDERSTATUS:STRING|O_TOTALPRICE:DECIMAL(12,2)|O_ORDERDATE:DATE|O_ORDERPRIORITY:STRING|O_CLERK:STRING|O_SHIPPRIORITY:INT|O_COMMENT:STRING"
prepend_header "${basedir}/part.tbl" "P_PARTKEY:ID(PART)|P_NAME:STRING|P_MFGR:STRING|P_BRAND:STRING|P_TYPE:STRING|P_SIZE:INT|P_CONTAINER:STRING|P_RETAILPRICE:DECIMAL(12,2)|P_COMMENT:STRING"
prepend_header "${basedir}/region.tbl" "R_REGIONKEY:ID(REGION)|R_NAME:STRING|R_COMMENT:STRING"
prepend_header "${basedir}/supplier.tbl" "S_SUPPKEY:ID(SUPPLIER)|S_NAME:STRING|S_ADDRESS:STRING|S_NATIONKEY:ULONG|S_PHONE:STRING|S_ACCTBAL:DECIMAL(12,2)|S_COMMENT:STRING"
prepend_header "${basedir}/partsupp.tbl" ":START_ID(PART)|:END_ID(SUPPLIER)|PS_AVAILQTY:INT|PS_SUPPLYCOST:DECIMAL(12,2)|PS_COMMENT:STRING"

# 3) Generate forward edge data from vertex tables (skip header row).
tail -n+2 "${basedir}/customer.tbl"  | awk -F'|' '{print $1"|"$4}'           > "${basedir}/customer_belongTo_nation.tbl"
tail -n+2 "${basedir}/lineitem.tbl"  | awk -F'|' '{print $1"|"$4"|"$1}'      > "${basedir}/lineitem_isPartOf_orders.tbl"
tail -n+2 "${basedir}/lineitem.tbl"  | awk -F'|' '{print $1"|"$4"|"$2}'      > "${basedir}/lineitem_composedBy_part.tbl"
tail -n+2 "${basedir}/lineitem.tbl"  | awk -F'|' '{print $1"|"$4"|"$3}'      > "${basedir}/lineitem_suppliedBy_supplier.tbl"
tail -n+2 "${basedir}/nation.tbl"    | awk -F'|' '{print $1"|"$3}'           > "${basedir}/nation_isLocatedIn_region.tbl"
tail -n+2 "${basedir}/orders.tbl"    | awk -F'|' '{print $1"|"$2}'           > "${basedir}/orders_madeBy_customer.tbl"
tail -n+2 "${basedir}/supplier.tbl"  | awk -F'|' '{print $1"|"$4}'           > "${basedir}/supplier_belongTo_nation.tbl"

# 4) Generate backward edge data (sorted by destination side).
sort -t'|' -n -k2 -k1     "${basedir}/customer_belongTo_nation.tbl"     | awk -F'|' '{print $2"|"$1}'         > "${basedir}/customer_belongTo_nation.tbl.backward"
sort -t'|' -n -k3 -k1 -k2 "${basedir}/lineitem_isPartOf_orders.tbl"     | awk -F'|' '{print $3"|"$1"|"$2}'    > "${basedir}/lineitem_isPartOf_orders.tbl.backward"
sort -t'|' -n -k3 -k1 -k2 "${basedir}/lineitem_composedBy_part.tbl"     | awk -F'|' '{print $3"|"$1"|"$2}'    > "${basedir}/lineitem_composedBy_part.tbl.backward"
sort -t'|' -n -k3 -k1 -k2 "${basedir}/lineitem_suppliedBy_supplier.tbl" | awk -F'|' '{print $3"|"$1"|"$2}'    > "${basedir}/lineitem_suppliedBy_supplier.tbl.backward"
sort -t'|' -n -k2 -k1     "${basedir}/nation_isLocatedIn_region.tbl"    | awk -F'|' '{print $2"|"$1}'         > "${basedir}/nation_isLocatedIn_region.tbl.backward"
sort -t'|' -n -k2 -k1     "${basedir}/orders_madeBy_customer.tbl"       | awk -F'|' '{print $2"|"$1}'         > "${basedir}/orders_madeBy_customer.tbl.backward"
sort -t'|' -n -k2 -k1     "${basedir}/supplier_belongTo_nation.tbl"     | awk -F'|' '{print $2"|"$1}'         > "${basedir}/supplier_belongTo_nation.tbl.backward"
tail -n+2 "${basedir}/partsupp.tbl"  | sort -t'|' -n -k2 -k1            | awk -F'|' '{print $2"|"$1}'         > "${basedir}/partsupp.tbl.backward"

# 5) Insert forward edge headers.
prepend_header "${basedir}/customer_belongTo_nation.tbl"     ":START_ID(CUSTOMER)|:END_ID(NATION)"
prepend_header "${basedir}/lineitem_isPartOf_orders.tbl"     ":START_ID_1(LINEITEM)|:START_ID_2(LINEITEM)|:END_ID(ORDERS)"
prepend_header "${basedir}/lineitem_composedBy_part.tbl"     ":START_ID_1(LINEITEM)|:START_ID_2(LINEITEM)|:END_ID(PART)"
prepend_header "${basedir}/lineitem_suppliedBy_supplier.tbl" ":START_ID_1(LINEITEM)|:START_ID_2(LINEITEM)|:END_ID(SUPPLIER)"
prepend_header "${basedir}/nation_isLocatedIn_region.tbl"    ":START_ID(NATION)|:END_ID(REGION)"
prepend_header "${basedir}/orders_madeBy_customer.tbl"       ":START_ID(ORDERS)|:END_ID(CUSTOMER)"
prepend_header "${basedir}/supplier_belongTo_nation.tbl"     ":START_ID(SUPPLIER)|:END_ID(NATION)"

# 6) Insert backward edge headers.
prepend_header "${basedir}/customer_belongTo_nation.tbl.backward"     ":END_ID(NATION)|:START_ID(CUSTOMER)"
prepend_header "${basedir}/lineitem_isPartOf_orders.tbl.backward"     ":END_ID(ORDERS)|:START_ID_1(LINEITEM)|:START_ID_2(LINEITEM)"
prepend_header "${basedir}/lineitem_composedBy_part.tbl.backward"     ":END_ID(PART)|:START_ID_1(LINEITEM)|:START_ID_2(LINEITEM)"
prepend_header "${basedir}/lineitem_suppliedBy_supplier.tbl.backward" ":END_ID(SUPPLIER)|:START_ID_1(LINEITEM)|:START_ID_2(LINEITEM)"
prepend_header "${basedir}/nation_isLocatedIn_region.tbl.backward"    ":END_ID(REGION)|:START_ID(NATION)"
prepend_header "${basedir}/orders_madeBy_customer.tbl.backward"       ":END_ID(CUSTOMER)|:START_ID(ORDERS)"
prepend_header "${basedir}/supplier_belongTo_nation.tbl.backward"     ":END_ID(NATION)|:START_ID(SUPPLIER)"
prepend_header "${basedir}/partsupp.tbl.backward"                     ":END_ID(SUPPLIER)|:START_ID(PART)"

echo "tpch-preprocess: done"
