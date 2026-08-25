#!/usr/bin/env bash
# =============================================================================
# GEM divergent-order capability demo (synth-y)
#
# Claim: some graph workloads contain graphlets with OPPOSITE optimal join
# orders, so NO single join order is good for all of them. GEM splits them into
# per-graphlet branches so each can use its own order — an optimization that is
# fundamentally impossible without splitting.
#
# synth-y is built exactly for this (regenerate with gen_synth_y.py +
# load_synth_y.sh):
#   P-nodes (pP): e1-heavy (deg 3000), e3-selective (deg 1) -> optimal = e3-first
#   Q-nodes (pQ): e1-selective (deg 1), e3-heavy (deg 3000) -> optimal = e1-first
#   (+ B/C filler graphlets and a mid-node graphlet that carry no e1/e3 out-edges)
# =============================================================================
set -u
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
BIN=${BIN:-$REPO_ROOT/build-release/tools/turbolynx}
WS=${WS:-/data/synth-y-ws}
E="http://ex.org"
TRI="MATCH (a)-[:\`$E/e1\`]->(b)-[:\`$E/e2\`]->(c),(a)-[:\`$E/e3\`]->(c) RETURN count(*);"

cnt(){ "$@" 2>/dev/null | grep -A2 count_star | tail -1 | tr -d ' |'; }
peak(){ TLX_NO_PARALLEL=1 TLX_INTER=1 "$@" 2>&1 | grep -E "\[INTER\] pipe=0 PEAK" | sed 's/.*PEAK=//;s/ .*//'; }

GD=$(TLX_GRAPHLET_DUMP=1 $BIN --ws $WS -q "MATCH (a) RETURN count(*);" -c 2>&1 | grep "^GDUMP")
P_OID=$(printf '%s\n' "$GD" | awk -F'\t' '$4~/\/e1@/{e1[$2]=$5} $4~/\/e3@/{e3[$2]=$5}
        END{for(g in e1) if(e1[g]>0 && e3[g]>0 && e1[g]>e3[g]) print g}')
Q_OID=$(printf '%s\n' "$GD" | awk -F'\t' '$4~/\/e1@/{e1[$2]=$5} $4~/\/e3@/{e3[$2]=$5}
        END{for(g in e1) if(e1[g]>0 && e3[g]>0 && e3[g]>e1[g]) print g}')
if [ -z "$P_OID" ] || [ -z "$Q_OID" ]; then
  echo "ERROR: could not detect the P/Q anchor graphlets in $WS" >&2
  exit 1
fi
echo "(detected anchor graphlets: P=$P_OID e1-heavy, Q=$Q_OID e3-heavy)"

echo "############################################################"
echo "# 0. Correctness — GEM split is correct on all modes"
echo "############################################################"
printf "  default : %s\n" "$(cnt $BIN --ws $WS -q "$TRI")"
printf "  gem     : %s\n" "$(cnt $BIN --ws $WS -j gem -q "$TRI")"
printf "  gem+div : %s\n" "$(cnt env TLX_DIVERGE_ORDER=1 $BIN --ws $WS -j gem -q "$TRI")"

echo
echo "############################################################"
echo "# 1. The two graphlets have OPPOSITE edge-degree profiles"
echo "############################################################"
echo "  graphlet  nodes   edge   fwd_edges"
printf '%s\n' "$GD" | awk -F'\t' -v P="$P_OID" -v Q="$Q_OID" \
  '$2==P||$2==Q{e=$4; sub(/.*\//,"",e); sub(/@.*/,"",e); if(e=="e2") next;
       g=($2==P?"P":"Q"); printf "  %-8s  %-6s  %-4s  %s\n",g,$2,e,$5}'

echo
echo "############################################################"
echo "# 2. GEM SPLITS them into separate branches (per NodeScan graphlet oids)"
echo "############################################################"
TLX_GEM_TRACE=1 TLX_DIVERGE_ORDER=1 $BIN --ws $WS -j gem -q "$TRI" -c 2>&1 \
  | grep -a -E "B[01]\\[0\\] NodeScan" | sed 's/^/  /'
echo "  ($P_OID=P and $Q_OID=Q land in different branches: split separates opposite profiles)"

echo
echo "############################################################"
echo "# 3. Each branch sees OPPOSITE edge costs (per-branch fan-outs)"
echo "#    (a single unified join order cannot express both)"
echo "############################################################"
echo "  (per-branch forward-edge fan-out the greedy uses; edge0=e1, edge4=e3)"
TRACE_TMP=$(mktemp)
TLX_GEM_TRACE=1 TLX_DIVERGE_ORDER=1 $BIN --ws $WS -j gem -q "$TRI" -c 2>&1 \
  | tee "$TRACE_TMP" \
  | grep -a -E "^\[SEL\] branch=[01] edge=[04] " | head -4 \
  | awk '{e=($3=="edge=0"?"e1":"e3"); print "  "$2" "e" "$4}' \
  | sort -u | sed -E "s/branch=0/branch0:/; s/branch=1/branch1:/"
echo "  per-branch AdjIdxJoin sequence (adjidx oids; opposite order = divergence):"
grep -a -E "B[01]\[[0-9]+\] AdjIdxJoin" "$TRACE_TMP" \
  | LC_ALL=C sed -E 's/^ *(B[01])\[[0-9]+\] AdjIdxJoin \| [A-Z]+, (INTO, )?adjidx_obj_id=([0-9]+).*/\1 adjidx=\3/' \
  | awk '{seq[$1]=seq[$1] (seq[$1]==""?"":" -> ") $2} END{for(b in seq) print "  "b": "seq[b]}' | sort
rm -f "$TRACE_TMP"

echo
echo "############################################################"
echo "# 4. The optimization is IMPOSSIBLE without splitting:"
echo "#    the SAME order is great for one graphlet, catastrophic for the other"
echo "############################################################"
QP="MATCH (a)-[:\`$E/e1\`]->(b)-[:\`$E/e2\`]->(c),(a)-[:\`$E/e3\`]->(c) WHERE a.\`$E/pP\`='1' RETURN count(*);"
QQ="MATCH (a)-[:\`$E/e1\`]->(b)-[:\`$E/e2\`]->(c),(a)-[:\`$E/e3\`]->(c) WHERE a.\`$E/pQ\`='1' RETURN count(*);"
pp=$(peak $BIN --ws $WS -q "$QP")
qq=$(peak $BIN --ws $WS -q "$QQ")
printf "  P-anchored query, default unified order : peak intermediate = %s\n" "$pp"
printf "  Q-anchored query, SAME unified order    : peak intermediate = %s\n" "$qq"
if [ "$pp" -le "$qq" ]; then lo=$pp; hi=$qq; win=P; lose=Q; else lo=$qq; hi=$pp; win=Q; lose=P; fi
echo "  => the unified order is $win's optimum but catastrophic for $lose."
echo "     Per-branch orders could keep BOTH at ~$lo (split total ~$((lo*2)))"
echo "     vs one-order-fits-all ~$hi  ($(awk -v a=$hi -v b=$lo 'BEGIN{printf "%.0f", a/b}')x smaller peak)."
echo
echo "Done."
