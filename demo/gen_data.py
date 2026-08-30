"""Generator for the VLDB booth "feature ladder" dataset.

Run it bare — every flag below already defaults to the certified v7 recipe, so

    python3 gen_data.py                     # -> /data/ladder-v7-src

reproduces the certified v7 graph shape with semantic venue-profile property
names. The explicit form kept in the evidence docs is the same recipe spelled
out:

    python3 gen_data.py /data/ladder-v7-src \\
            --r-mult 4 --f-fill-v 280 --n-sink 300000 --prof-junk 0 --prof-n 5

Everything stays overridable; the generator is deterministic per --seed.
Output: nodes.json, profiles.json, follows.csv, recommends.csv, visits.csv,
profile_of.csv, profile_cols.txt (~670 MB; time varies by filesystem).

WHAT THE DATA IS
  A social/venue graph with TWO MIRRORED DISTRICTS.  Both halves close the same
  triangle FOLLOWS -> RECOMMENDS + VISITS, but from opposite ends:
    music side   fan -[FOLLOWS]-> page  -[RECOMMENDS]-> fav_place <-[VISITS]- fan
    oldtown side local -[FOLLOWS]-> home_page -[RECOMMENDS]-> place <-[VISITS]- local
  The two districts have OPPOSITE edge-degree profiles, so no single join order
  is good for both — that is the divergent-order property the GEM rung exploits.
  21,500 anchors per district x --r-mult 4 = 172,000 traversal rows exactly.

  Every venue also owns a VENUE_PROFILE SIDECAR: --prof-n 5 records, each in a
  different semantic section graphlet (Basics, Hours, Amenities, Events, ...).
  The union of the 40 sections is 200 distinct properties and any one record
  fills 5 of them, so a materialized profile row is 97.5% NULL — the null mass
  the SSRF rung compresses.

  Around the mirror sit JUNK and BOT FILLERS: nodes that follow pages and visit
  places but close NO triangle.  They are pure work for a plan that cannot
  prune them, which is what makes the schema-pruning (SI) rung visible.

WHY EACH CERTIFIED KNOB MATTERS (full derivation in evidence/)
  --prof-n 5        THE lever that makes this a real grouping query.  Five
                    heterogeneous records per node means the post-GROUP-BY
                    attach materializes 33,452 x 5 = 167,260 wide rows and
                    32.62M NULL cells.  At --prof-n 1 the null mass is 5x
                    smaller and the SSRF rung collapses to -19.2%.
  --f-fill-v 280    Junk VISITS mass (4.48M filler edges).  The WITH shape moved
                    the PROFILE_OF join out of the aggregating part, which shrank
                    the baseline's backward-plan filler tax; 280 restores the SI
                    rung to -45.4% AND pulls rung 0 down to ~4.1 s (better booth
                    pacing).  At the v6 value 120 the SI rung reads only -27.4%.
  --n-sink 300000   Sink targets for the bot fillers, so bot VISITS land outside
                    the mirror and stay triangle-neutral.
  --prof-junk 0     No edge-less junk VENUE_PROFILE extents. They were an
                    ssrf8-era
                    lever to steer GEM's cross-target race; on this shape they
                    are unnecessary and only bloat the load.
  --r-mult 4        Triangles per anchor => count(*) = 2 x 21,500 x 4 = 172,000.
  --seed 20260823   The certified seed.  Change it and the ladder still works,
                    but the exact result identity in the docs will not match.

Totals: 450,000 NODE + 2,250,000 VENUE_PROFILE (40 section graphlets),
FOLLOWS 9,165,500, RECOMMENDS 172,000, VISITS 8,325,500, PROFILE_OF 2,250,000.

--- original header (mechanics) -------------------------------------------
Booth ladder generator = gen_ssrf8.py (hero6 core + VENUE_PROFILE sidecar) + 2 levers:

  --r-mult R    : R distinct recommenders per private place (music side) and
                  R distinct recommended pool places per LPAGE (oldtown side),
                  both sampled from the anchor's own follow/visit target set tg
                  => EXACTLY R triangles per anchor, count(*) = 2*A*R.
                  Scales the triangle output rows (SSRF attach mass) and the
                  RECOMMENDS-bwd intermediate (join waste) together. R <= f_reg.
  --prof-junk J : J junk VENUE_PROFILE nodes (ids past the NODE id space, cat = id%K,
                  same 5-prop blocks => SAME 40 graphlets), NO edges =>
                  result-neutral archive mass.

Why junk must be EDGE-LESS: GEM's cross-target race is decided by DCost
(sum of child row counts over the split plan). Junk rows enter the p-anchored
plan twice (anchor scan rows + PROFILE_OF fanout if they had edges) but a
junk PROFILE_OF edge also lands in the NODE-split plan at FULL relation size
(fanout-0 edges are not virtual-swapped there), so junk-with-edges shifts the
race at only (2-1)x. Edge-less junk inflates ONLY the p-anchored plan (2x/0x)
=> the NODE-side split wins robustly once J > ~a-plan DCost floor (~ music-F
+ oldtown-V fanout sums, ~8M at hero6 scale). p then arrives via IdSeek = SSRF.

Edge direction: PROFILE_OF goes VENUE_PROFILE -> NODE ("profile OF its venue") —
keeps GemBranchEdgeFanout honest for the p-anchored trial pricing too
(NODE->VENUE_PROFILE gives the vp-branch fanout 0 = the S-H4a "free backward
expansion" misanchor at the greedy level).

Triangle-closure invariant unchanged: music rec edges only target FPLACE
(VISITS-indeg 1, only its own fan), oldtown rec sources are private LPAGEs
(FOLLOWS-indeg 1, only its own local); junk/bot fillers follow FPAGE / visit
LPLACE or sinks and close nothing. count(*) = 2*A*R exactly.
"""
import argparse
import json
import os
import random

from venue_profile_schema import SECTIONS as PROFILE_SECTIONS, example_value


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("out_dir", nargs="?", default="/data/ladder-v7-src")
    ap.add_argument("--a-reg", type=int, default=20000)
    ap.add_argument("--a-pow", type=int, default=1500)
    ap.add_argument("--f-reg", type=int, default=40)
    ap.add_argument("--f-pow", type=int, default=2000)
    ap.add_argument("--pool", type=int, default=12000)
    ap.add_argument("--window", type=int, default=200)
    ap.add_argument("--n-junk", type=int, default=2)
    ap.add_argument("--filler", type=int, default=8000)
    ap.add_argument("--f-fill", type=int, default=4)
    ap.add_argument("--f-fill-v", type=int, default=280)   # certified v7
    ap.add_argument("--n-junk-h", type=int, default=6)
    ap.add_argument("--filler-h", type=int, default=4000)
    ap.add_argument("--f-fill-h", type=int, default=220)
    ap.add_argument("--n-sink", type=int, default=300000)  # certified v7
    ap.add_argument("--seed", type=int, default=20260823)
    ap.add_argument("--prof-k", type=int, default=40)
    ap.add_argument("--prof-p", type=int, default=5)
    ap.add_argument("--r-mult", type=int, default=4)       # certified v7
    ap.add_argument("--prof-junk", type=int, default=0)     # certified v7
    ap.add_argument("--prof-n", type=int, default=5)        # certified v7
    args = ap.parse_args()
    rng = random.Random(args.seed)
    os.makedirs(args.out_dir, exist_ok=True)

    A = args.a_reg + args.a_pow
    M = args.pool
    K = args.prof_k
    P = args.prof_p
    R = args.r_mult
    assert R <= args.f_reg, "r-mult must be <= f-reg (sampled from tg)"
    assert K <= len(PROFILE_SECTIONS), "prof-k exceeds the venue profile catalog"
    assert P <= 5, "each venue profile section defines five properties"

    F0 = 0
    L0 = F0 + A
    FPAGE0 = L0 + A
    LPLACE0 = FPAGE0 + M
    FPLACE0 = LPLACE0 + M
    LPAGE0 = FPLACE0 + A
    SINK0 = LPAGE0 + A
    JUNK0 = SINK0 + args.n_sink
    JUNKH0 = JUNK0 + args.n_junk * args.filler
    n_nodes = JUNKH0 + args.n_junk_h * args.filler_h
    JPROF0 = n_nodes  # junk VENUE_PROFILE ids start past the NODE id space

    # --- nodes.json: EXACTLY gen_hero6 (no profile props here) ---
    with open(os.path.join(args.out_dir, "nodes.json"), "w") as f:
        def emit(nid, props):
            props["id"] = nid
            f.write(json.dumps({"labels": ["NODE"], "properties": props},
                               separators=(",", ":")) + "\n")
        for i in range(A):
            p = {"name": "fan_%d" % i, "kind": "person",
                 "genre": "music", "follows": "music_pages"}
            if i >= args.a_reg:
                p["verified"] = "1"
            emit(F0 + i, p)
        for i in range(A):
            p = {"name": "local_%d" % i, "kind": "person",
                 "neighborhood": "old_town", "since": "resident"}
            if i >= args.a_reg:
                p["verified"] = "1"
            emit(L0 + i, p)
        for i in range(M):
            emit(FPAGE0 + i, {"title": "page_%d" % i})
        for i in range(M):
            emit(LPLACE0 + i, {"title": "place_%d" % i})
        for i in range(A):
            emit(FPLACE0 + i, {"title": "fav_place_%d" % i})
        for i in range(A):
            emit(LPAGE0 + i, {"title": "home_page_%d" % i})
        for i in range(args.n_sink):
            emit(SINK0 + i, {"noise": "s%d" % i})
        for k in range(args.n_junk):
            base = JUNK0 + k * args.filler
            key = "junk%d" % k
            for i in range(args.filler):
                emit(base + i, {key: "j%d" % i})
        for k in range(args.n_junk_h):
            base = JUNKH0 + k * args.filler_h
            key = "bot%d" % k
            for i in range(args.filler_h):
                emit(base + i, {key: "b%d" % i})

    # --- profiles.json (label VENUE_PROFILE) + profile_of.csv, deterministic ---
    # EVERY node gets exactly one profile (1:1 by id). Coverage matters for the
    # DEFAULT planner: with only title nodes profiled, |PROFILE_OF| / |NODE|
    # ~= 0.15 makes ORCA price the attach join as REDUCING and pull it below
    # RECOMMENDS (200-col attach on the 3.8M-row VISITS intermediate, rung1
    # inversion). Full coverage => selectivity ~1 => the wide attach is
    # estimate-neutral and lands last; it also keeps RECOMMENDS the smallest
    # edge relation so rung0 anchors on it (the backward filler-tax plan).
    profile_ids = list(range(0, JUNKH0 + args.n_junk_h * args.filler_h))

    # --prof-n N: N profile RECORDS per node, each in a DIFFERENT category
    # graphlet (record k of node nid -> cat (nid + k*13) % K).  The grouping
    # query collapses the traversal to one row per venue and then materializes
    # that venue's N heterogeneous records, so the wide seek runs on
    # N x (#groups) rows -- the null mass SSRF compresses -- while the query
    # keeps real GROUP BY semantics.  N=1 reproduces gen_ladder.py exactly.
    N = args.prof_n

    def prof_props(pid, nid, k):
        cat = (nid + k * 13) % K
        props = {key: example_value(key, nid + k + j, cat, j)
                 for j, key in enumerate(PROFILE_SECTIONS[cat][1][:P])}
        props["id"] = pid
        return props

    with open(os.path.join(args.out_dir, "profiles.json"), "w") as f, \
         open(os.path.join(args.out_dir, "profile_of.csv"), "w") as g:
        g.write(":START_ID(VENUE_PROFILE)|:END_ID(NODE)\n")
        for nid in profile_ids:
            for k in range(N):
                pid = nid * N + k
                f.write(json.dumps({"labels": ["VENUE_PROFILE"],
                                    "properties": prof_props(pid, nid, k)},
                                   separators=(",", ":")) + "\n")
                g.write("%d|%d\n" % (pid, nid))
        for i in range(args.prof_junk):  # junk: edge-less (see docstring)
            pid = JPROF0 * N + i
            f.write(json.dumps({"labels": ["VENUE_PROFILE"],
                                "properties": prof_props(pid, JPROF0 + i, 0)},
                               separators=(",", ":")) + "\n")

    def heavy_targets(idx, base):
        if idx < args.a_reg:
            w = min(args.window, M)
            lo = rng.randrange(0, M - w + 1) if M > w else 0
            picks = rng.sample(range(lo, lo + w), min(args.f_reg, w))
        else:
            picks = rng.sample(range(M), min(args.f_pow, M))
        return [base + p for p in picks]

    follows, visits, recommends = [], [], []
    for i in range(A):
        a = F0 + i
        tg = heavy_targets(i, FPAGE0)
        follows.extend((a, b) for b in tg)
        visits.append((a, FPLACE0 + i))
        recommends.extend((b, FPLACE0 + i) for b in rng.sample(tg, R))
    for i in range(A):
        a = L0 + i
        tg = heavy_targets(i, LPLACE0)
        visits.extend((a, c) for c in tg)
        follows.append((a, LPAGE0 + i))
        recommends.extend((LPAGE0 + i, c) for c in rng.sample(tg, R))
    for k in range(args.n_junk):
        base = JUNK0 + k * args.filler
        for i in range(args.filler):
            a = base + i
            for b in rng.sample(range(M), args.f_fill):
                follows.append((a, FPAGE0 + b))
            for c in rng.sample(range(M), args.f_fill_v):
                visits.append((a, LPLACE0 + c))
    for k in range(args.n_junk_h):
        base = JUNKH0 + k * args.filler_h
        for i in range(args.filler_h):
            a = base + i
            for b in rng.sample(range(M), args.f_fill_h):
                follows.append((a, FPAGE0 + b))
            visits.append((a, SINK0 + rng.randrange(args.n_sink)))

    hdr = ":START_ID(NODE)|:END_ID(NODE)\n"
    for name, edges in (("follows", follows), ("recommends", recommends),
                        ("visits", visits)):
        with open(os.path.join(args.out_dir, name + ".csv"), "w") as f:
            f.write(hdr)
            for s, t in edges:
                f.write("%d|%d\n" % (s, t))

    with open(os.path.join(args.out_dir, "profile_cols.txt"), "w") as f:
        cols = ["vp.%s AS %s" % (key, key)
                for _, fields in PROFILE_SECTIONS[:K] for key in fields[:P]]
        f.write(", ".join(cols) + "\n")

    print("gen_ladder: %d NODE + %d VENUE_PROFILE (%d real + %d junk; K=%d x P=%d), "
          "F=%d R=%d V=%d HP=%d, r_mult=%d, expected count(*)=%d"
          % (n_nodes, len(profile_ids) * N + args.prof_junk, len(profile_ids) * N,
             args.prof_junk, K, P, len(follows), len(recommends), len(visits),
             len(profile_ids) * N, R, 2 * A * R))


if __name__ == "__main__":
    main()
