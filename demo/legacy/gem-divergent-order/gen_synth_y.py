"""synth-y: two 2000-node anchor graphlets with opposite edge-degree profiles.

P nodes {pP,name}: e1 out-degree 3000 (to all 3000 BP mids), e3 out-degree 1
  (to a private CP mid)  -> 6,000,000 e1 edges, 2,000 e3 edges.
Q nodes {pQ,name}: the mirror -- e1 out-degree 1 (private BQ mid), e3
  out-degree 3000 (all 3000 CQ mids) -> 2,000 e1 edges, 6,000,000 e3 edges.
Mid nodes {pM} (BP 3000, CP 2000, BQ 2000, CQ 3000) form one graphlet; e2
  edges BP->CP and BQ->CQ each close exactly one (a)-[e1]->(b)-[e2]->(c),
  (a)-[e3]->(c) triangle, so count(*) = |e2| (~4200, seed-dependent).
B/C fillers {pB}/{pC}: 100,000 nodes each, no e1/e2/e3 edges.
"""
import argparse
import json
import os
import random

PFX = "http://ex.org/"
HEADER = ":START_ID(NODE)|:END_ID(NODE)\n"

N_P = 2000
N_Q = 2000
N_BP = 3000
N_CP = 2000
N_BQ = 2000
N_CQ = 3000
N_FILLER = 100000

P0 = 0
Q0 = P0 + N_P
BP0 = Q0 + N_Q
CP0 = BP0 + N_BP
BQ0 = CP0 + N_CP
CQ0 = BQ0 + N_BQ
B0 = 100000
C0 = 200000


def write_nodes(path):
    with open(path, "w") as f:
        def emit(nid, props):
            props["id"] = nid
            f.write(json.dumps({"labels": ["NODE"], "properties": props},
                               separators=(",", ":")) + "\n")
        for i in range(N_P):
            emit(P0 + i, {PFX + "pP": "1", PFX + "name": "p%d" % i})
        for i in range(N_Q):
            emit(Q0 + i, {PFX + "pQ": "1", PFX + "name": "q%d" % i})
        for i in range(N_BP + N_CP + N_BQ + N_CQ):
            emit(BP0 + i, {PFX + "pM": "1"})
        for i in range(N_FILLER):
            emit(B0 + i, {PFX + "pB": "1"})
        for i in range(N_FILLER):
            emit(C0 + i, {PFX + "pC": "1"})


def write_edges(out_dir, rng):
    with open(os.path.join(out_dir, "e1.csv"), "w") as f:
        f.write(HEADER)
        for i in range(N_P):
            a = P0 + i
            f.write("".join("%d|%d\n" % (a, BP0 + j) for j in range(N_BP)))
        for i in range(N_Q):
            f.write("%d|%d\n" % (Q0 + i, BQ0 + i))

    with open(os.path.join(out_dir, "e3.csv"), "w") as f:
        f.write(HEADER)
        for i in range(N_P):
            f.write("%d|%d\n" % (P0 + i, CP0 + i))
        for i in range(N_Q):
            a = Q0 + i
            f.write("".join("%d|%d\n" % (a, CQ0 + j) for j in range(N_CQ)))

    e2 = set()
    for i in range(N_CP):
        e2.add((BP0 + rng.randrange(N_BP), CP0 + i))
    for i in range(N_BQ):
        e2.add((BQ0 + i, CQ0 + rng.randrange(N_CQ)))
    for _ in range(rng.randrange(150, 300)):
        e2.add((BP0 + rng.randrange(N_BP), CP0 + rng.randrange(N_CP)))
    for i in rng.sample(range(N_BQ), rng.randrange(150, 300)):
        e2.add((BQ0 + i, CQ0 + rng.randrange(N_CQ)))
    with open(os.path.join(out_dir, "e2.csv"), "w") as f:
        f.write(HEADER)
        for s, t in sorted(e2):
            f.write("%d|%d\n" % (s, t))
    return len(e2)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("out_dir")
    ap.add_argument("--seed", type=int, default=20260823)
    args = ap.parse_args()
    rng = random.Random(args.seed)
    os.makedirs(args.out_dir, exist_ok=True)
    write_nodes(os.path.join(args.out_dir, "nodes.json"))
    n_e2 = write_edges(args.out_dir, rng)
    print("synth-y generated in %s (expected count(*) = %d)" % (args.out_dir, n_e2))


if __name__ == "__main__":
    main()
