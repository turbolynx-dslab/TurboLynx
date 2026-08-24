# GEM per-branch join ordering demo

`./run_demo.sh` (env: `BIN`, `WS` override the binary / workspace).

## Regenerating the data
```bash
python3 demo/gem-divergent-order/gen_synth_y.py /data/synth-y-src        # ~10 s
bash demo/gem-divergent-order/load_synth_y.sh <build-dir> /data/synth-y-ws  # ~40 s
```
The generator is deterministic per seed, but the exact numbers below are
data-dependent: the total count(*) (~4200–4500), the peak-intermediate values,
which side of the P/Q mirror the default unified order happens to favor, and
the graphlet oids (`run_demo.sh` auto-detects them from `TLX_GRAPHLET_DUMP`).
The opposite-profile structure and the ~1000x peak ratio are what reproduce.

## The idea
Some graph workloads contain graphlet groups with **opposite optimal join
orders**, so no single join order serves all of them. GEM can split such groups
into separate UnionAll branches and give each branch its own join order — a plan
shape a single unified order cannot express.

## Data (`/data/synth-y-ws`)
| graphlet | nodes | e1 fanout | e3 fanout | optimal order |
|----------|-------|-----------|-----------|---------------|
| **P** | 2000 | 6,000,000 | 2,000 | **e3-first** |
| **Q** | 2000 | 2,000 | 6,000,000 | **e1-first** |
| M (mids) | 10000 | — | — | (e2 interior; BP/CP/BQ/CQ) |
| B | 100000 | 0 | 0 | (filler, no anchor edges) |
| C | 100000 | 0 | 0 | (filler) |

Query: `MATCH (a)-[:e1]->(b)-[:e2]->(c),(a)-[:e3]->(c) RETURN count(*)`

## What the demo shows (all reproduced by the script)
1. **Correctness** — default = gem = gem+diverge = **|e2|** (4466 on the current
   demo workspace; seed-dependent).
2. **Opposite profiles** — P is e1-heavy/e3-light; Q is the mirror.
3. **GEM splits them** — branch0 = {P,B}, branch1 = {Q,C} (from the physical
   NodeScan graphlet oids). The split separates the opposite profiles.
4. **Divergent per-branch orders** — the greedy sees per-branch fan-outs
   (branch0: e1=6M,e3=2K → e3-first; branch1: e1=2K,e3=6M → e1-first). One unified
   order cannot express both.
5. **The payoff** — on the current workspace the unified order lands e1-first,
   so running the SAME order on both groups gives:
   - Q: peak intermediate **2,266** (its optimum)
   - P: peak intermediate **6,000,000**
   With per-branch orders each group stays ~2.2K → ~4.5K total vs ~6.0M ≈ **1300×**
   smaller peak intermediate. (Which side floods is data-dependent per the note
   above; an earlier workspace favored P and flooded Q instead.)

## Feature flag
Everything is opt-in behind `TLX_DIVERGE_ORDER` while the feature is being
evaluated across workloads (plan quality and compile-time cost). Default
behavior is unchanged: unittest 223/223, query_test 785/785.

Main pieces:
- Per-branch greedy join ordering (`CJoinOrderGEM`) driven by per-graphlet
  forward-edge fan-outs (`GemBranchEdgeFanout`).
- Per-branch virtual edge tables (`AddVirtualEdgeTable`) so the optimizer costs
  each branch with branch-specific edge cardinalities.
- Measurement probes used by this demo: `TLX_INTER` (per-operator intermediate
  sizes), `TLX_NO_PARALLEL`, `TLX_GEM_TRACE`, `TLX_GRAPHLET_DUMP`.
