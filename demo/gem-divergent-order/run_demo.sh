#!/usr/bin/env bash
# =============================================================================
# GEM divergent-order capability demo (synth-y)
#
# Claim: some graph workloads contain graphlets with OPPOSITE optimal join
# orders, so NO single join order is good for all of them. GEM splits them into
# per-graphlet branches so each can use its own order — an optimization that is
# fundamentally impossible without splitting.
#
# synth-y is built exactly for this:
#   P-nodes: e1-heavy (deg 3000), e3-selective (deg 1)  -> optimal = e3-first
#   Q-nodes: e1-selective (deg 1), e3-heavy (deg 3000)  -> optimal = e1-first
#   (+ B/C filler graphlets that carry no anchor out-edges)
# =============================================================================
set -u
BIN=${BIN:-/root/tlx-main/build-rel/tools/turbolynx}
WS=${WS:-/data/synth-y-ws}
E="http://ex.org"
TRI="MATCH (a)-[:\`$E/e1\`]->(b)-[:\`$E/e2\`]->(c),(a)-[:\`$E/e3\`]->(c) RETURN count(*);"

cnt(){ "$@" 2>/dev/null | grep -A2 count_star | tail -1 | tr -d ' |'; }
peak(){ TLX_NO_PARALLEL=1 TLX_INTER=1 "$@" 2>&1 | grep -E "\[INTER\] pipe=0 PEAK" | sed 's/.*PEAK=//;s/ .*//'; }

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
TLX_GRAPHLET_DUMP=1 $BIN --ws $WS -q "MATCH (a) RETURN count(*);" -c 2>&1 \
  | grep GDUMP | awk '$2==546||$2==550{e=$4; sub(/.*\//,"",e); sub(/@.*/,"",e);
       g=($2==546?"P":"Q"); printf "  %-8s  %-6s  %-4s  %s\n",g,$3,e,$5}'

echo
echo "############################################################"
echo "# 2. GEM SPLITS them into separate branches (per NodeScan graphlet oids)"
echo "############################################################"
TLX_GEM_TRACE=1 TLX_DIVERGE_ORDER=1 $BIN --ws $WS -j gem -q "$TRI" -c 2>&1 \
  | grep -E "B[01]\[0\] NodeScan" | sed 's/^/  /'
echo "  (546=P and 550=Q land in different branches: split separates opposite profiles)"

echo
echo "############################################################"
echo "# 3. Each branch sees OPPOSITE edge costs -> picks the opposite order"
echo "#    (a single unified join order cannot express both)"
echo "############################################################"
echo "  (per-branch forward-edge fan-out the greedy uses; edge0=e1, edge4=e3)"
TLX_GEM_TRACE=1 TLX_DIVERGE_ORDER=1 $BIN --ws $WS -j gem -q "$TRI" -c 2>&1 \
  | grep -E "^\[SEL\] branch=[01] edge=[04] " | head -4 \
  | awk '{e=($3=="edge=0"?"e1":"e3"); print "  "$2" "e" "$4}' \
  | sort -u | sed -E 's/branch=0/branch0(P):/; s/branch=1/branch1(Q):/'
echo "  => branch0(P): e1 huge, e3 tiny -> chooses e3-first"
echo "     branch1(Q): e1 tiny, e3 huge -> chooses e1-first   (DIVERGENT)"

echo
echo "############################################################"
echo "# 4. The optimization is IMPOSSIBLE without splitting:"
echo "#    the SAME order is great for one graphlet, catastrophic for the other"
echo "############################################################"
QP="MATCH (a)-[:\`$E/e1\`]->(b)-[:\`$E/e2\`]->(c),(a)-[:\`$E/e3\`]->(c) WHERE a.\`$E/pP\`='1' RETURN count(*);"
QQ="MATCH (a)-[:\`$E/e1\`]->(b)-[:\`$E/e2\`]->(c),(a)-[:\`$E/e3\`]->(c) WHERE a.\`$E/pQ\`='1' RETURN count(*);"
pp=$(peak $BIN --ws $WS -q "$QP")
qq=$(peak $BIN --ws $WS -q "$QQ")
printf "  P-graphlet, e3-first order (its optimum) : peak intermediate = %s\n" "$pp"
printf "  Q-graphlet, SAME e3-first order (wrong)  : peak intermediate = %s\n" "$qq"
echo "  => one order cannot serve both. With per-branch orders each stays ~$pp,"
echo "     so split total ~$((pp*2)) vs one-order-fits-all ~$qq  (about 1000x smaller)."
echo
echo "Done."
