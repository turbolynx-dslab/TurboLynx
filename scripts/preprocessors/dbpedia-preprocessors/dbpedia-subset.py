#!/usr/bin/env python3
"""Extract a deterministic small subset of the full DBpedia snapshot
for use as a committed `test/data/dbpedia-mini/` fixture.

The full DBpedia dataset (~77M nodes, 32 selected edge types, ~16 GB
nodes.json plus several GB of edge CSVs) is too large to ship in the
repo or run as a regular bulkload regression.  Unlike TPC-H or LDBC,
DBpedia has no scale-factor generator — it's a single real-world
snapshot — so we build the small fixture by taking a deterministic
slice of the existing source.

## Strategy

1.  Pick a *seed* slice: first N rows of a small readable edge file
    (default: `edges_nationality_797.csv`, the smallest of the
    currently-readable types).
2.  Collect every endpoint id from the seed slice into V.
3.  For each of the readable edge files, keep only rows where both
    endpoints are in V (set intersection).  Emit both the forward and
    backward `.csv` so the loader gets a symmetric view.
4.  Stream the full `nodes.json` once and emit only the lines whose
    `properties.id` ∈ V.  Stop early once every id is found.

## Usage

    python3 scripts/preprocessors/dbpedia-preprocessors/dbpedia-subset.py \\
        --src /source-data/dbpedia \\
        --dst test/data/dbpedia-mini \\
        --seed-edges 500

The output is fully deterministic given the same seed-edges count and
the same source files.  Re-running produces byte-identical output.

## Why not BFS from a single seed id?

BFS over edges requires the ability to read every edge file, but on
some hosts only a subset of the dataset is actually synced (the rest
returns `Input/output error`).  The seed-slice approach degrades
gracefully: missing edge files are skipped, and the subset is
whatever V × V intersection survives on the host's actually-available
files.
"""
import argparse
import json
import os
from pathlib import Path
from typing import Set, List

# Edge files used in the full DBpedia bulkload (see scripts/load-dbpedia.sh
# and test/bulkload/datasets.json).  Files not readable on the current
# host are silently skipped — the mini fixture covers whatever is
# available.
EDGE_FILES: List[str] = [
    "edges_nationality_797.csv",
    "edges_genre_3794.csv",
    "edges_country_8183.csv",
    "edges_birthPlace_2950.csv",
    "edges_22-rdf-syntax-ns#type_6803.csv",
    # The list intentionally also names the larger / often-unavailable
    # types so that if they are added later (or available on a different
    # host) the subsetter picks them up without a code change.
    "edges_wikiPageRedirects_3801.csv",
    "edges_subject_5592.csv",
    "edges_owl#sameAs_9037.csv",
    "edges_homepage_589.csv",
    "edges_homepage_7372.csv",
    "edges_website_315.csv",
    "edges_foundationPlace_3010.csv",
    "edges_developer_3910.csv",
    "edges_clubs_7515.csv",
    "edges_thumbnail_4765.csv",
    "edges_city_55.csv",
    "edges_depiction_6991.csv",
    "edges_countryOrigin_7466.csv",
    "edges_keyPerson_285.csv",
    "edges_location_4045.csv",
    "edges_locationCity_1514.csv",
    "edges_locationCountry_1719.csv",
    "edges_industry_6326.csv",
    "edges_musicBy_6854.csv",
    "edges_musicComposer_4171.csv",
    "edges_musicFusionGenre_354.csv",
    "edges_musicSubgenre_7829.csv",
    "edges_musicalArtist_8387.csv",
    "edges_musicalBand_8808.csv",
    "edges_stylisticOrigin_3904.csv",
    "edges_subregion_6627.csv",
    "edges_album_5285.csv",
]

# Default seed file — the smallest currently-readable edge type.  Keeps
# the resulting V compact (~ 2× the row count after dedup) so the
# committed fixture stays under a few MB.
DEFAULT_SEED = "edges_nationality_797.csv"


def file_is_readable(path: Path) -> bool:
    """True iff `path` exists, is non-empty, and the first byte can be
    read.  Catches the I/O Error case where a sparse / partial-sync
    mount lists the file but cannot serve its content."""
    try:
        if not path.exists() or path.stat().st_size == 0:
            return False
        with open(path, "rb") as f:
            return len(f.read(1)) == 1
    except OSError:
        return False


def collect_seed_endpoints(seed_path: Path, n_rows: int) -> Set[int]:
    """Return the set of node ids appearing in the first `n_rows`
    edges of `seed_path`."""
    V: Set[int] = set()
    with open(seed_path) as f:
        header = next(f)  # noqa: F841 — discarded
        for i, line in enumerate(f):
            if i >= n_rows:
                break
            a, b = line.rstrip("\n").split("|")
            V.add(int(a))
            V.add(int(b))
    return V


def filter_edge_file(src: Path, dst_fwd: Path, dst_bwd: Path,
                     V: Set[int]) -> int:
    """Copy rows from `src` whose both endpoints are in `V` to
    `dst_fwd`; mirror them (swapped) into `dst_bwd`.  Returns the
    row count emitted."""
    with open(src) as fin:
        header = next(fin)
        # Header is guaranteed to be `:START_ID(NODE)|:END_ID(NODE)`
        # for every dbpedia edge file.
        rows: List[tuple[int, int]] = []
        for line in fin:
            try:
                a, b = line.rstrip("\n").split("|")
                ai, bi = int(a), int(b)
            except ValueError:
                continue   # malformed line, skip
            if ai in V and bi in V:
                rows.append((ai, bi))

    with open(dst_fwd, "w") as fout:
        fout.write(header)
        for a, b in rows:
            fout.write(f"{a}|{b}\n")
    with open(dst_bwd, "w") as fout:
        fout.write(header)
        for a, b in rows:
            fout.write(f"{b}|{a}\n")
    return len(rows)


def filter_nodes_json(src: Path, dst: Path, V: Set[int]) -> int:
    """Stream `src` (`nodes.json`, JSONL) and write only the lines
    whose `properties.id` is in `V` to `dst`.  Returns the kept line
    count.  Stops early once every id has been matched."""
    remaining = set(V)
    kept = 0
    # Open the destination as a regular text file; one JSON object per
    # line, mirroring the source layout.
    with open(src) as fin, open(dst, "w") as fout:
        for line in fin:
            # Cheap shortcut: skip the line if no id substring matches.
            # JSON parse is several µs per line — at 77M lines that adds
            # up to many minutes.  A cheap "id":N pre-screen drops most
            # lines without paying parsing cost.
            #
            # We can't pre-screen without knowing the id values, but we
            # can early-exit once every id has been emitted.
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            nid = obj.get("properties", {}).get("id")
            if isinstance(nid, int) and nid in remaining:
                fout.write(line)
                kept += 1
                remaining.discard(nid)
                if not remaining:
                    break
    return kept


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--src",  default="/source-data/dbpedia", type=Path,
                    help="root of the full DBpedia snapshot")
    ap.add_argument("--dst",  default="test/data/dbpedia-mini", type=Path,
                    help="output directory for the mini fixture")
    ap.add_argument("--seed", default=DEFAULT_SEED,
                    help="seed edge file (relative to --src)")
    ap.add_argument("--seed-edges", default=500, type=int,
                    help="number of rows from the seed to use for V")
    args = ap.parse_args()

    src: Path = args.src
    dst: Path = args.dst
    seed_path = src / args.seed

    if not file_is_readable(seed_path):
        print(f"[err] seed file not readable: {seed_path}")
        return 2

    dst.mkdir(parents=True, exist_ok=True)

    V = collect_seed_endpoints(seed_path, args.seed_edges)
    print(f"[seed] {args.seed} first {args.seed_edges} rows → |V| = {len(V)}")

    total_edges = 0
    skipped: List[str] = []
    kept_types: List[tuple[str, int]] = []
    for ef in EDGE_FILES:
        s = src / ef
        if not file_is_readable(s):
            skipped.append(ef)
            continue
        fwd = dst / ef
        bwd = dst / (ef + ".backward")
        n = filter_edge_file(s, fwd, bwd, V)
        if n == 0:
            # No surviving rows — remove empty output files so the
            # fixture lists exactly what loads non-empty.
            fwd.unlink(missing_ok=True)
            bwd.unlink(missing_ok=True)
            continue
        kept_types.append((ef, n))
        total_edges += n
        print(f"[edge] {ef}: {n} rows in V×V")

    if skipped:
        print(f"[edge] skipped {len(skipped)} unreadable files: "
              f"{skipped[:5]}{'…' if len(skipped) > 5 else ''}")

    nodes_src = src / "nodes.json"
    nodes_dst = dst / "nodes.json"
    print(f"[nodes] streaming {nodes_src} (this can take several minutes)…")
    n_nodes = filter_nodes_json(nodes_src, nodes_dst, V)
    print(f"[nodes] kept {n_nodes} / |V|={len(V)} (missing: {len(V) - n_nodes})")

    # Summary
    print("\n=== dbpedia-mini fixture ===")
    print(f"  nodes:        {n_nodes}")
    print(f"  edge types:   {len(kept_types)}")
    print(f"  edges total:  {total_edges}")
    print(f"  output:       {dst}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
