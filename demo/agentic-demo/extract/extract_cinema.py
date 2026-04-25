#!/usr/bin/env python3
"""Cinema subgraph extractor for the Agentic DB demo.

Input:  /source-data/dbpedia  (77M-node DBpedia dump, pipe-separated edge CSVs + JSON-Lines nodes)
Output: /data/dbpedia-cinema-src  (single-label NODE JSONL + per-predicate NODE→NODE edge CSVs)

Pipeline:
  stage1  Distinct type node IDs               (scan type.backward once)
  stage2  Resolve type-node URIs               (scan nodes.json, filter ~500 IDs)
  stage3  Classify entities                    (scan type.backward again)
  stage4  Filter cinema edges                  (scan ~20 small edge CSVs)
  stage5  Emit nodes.json                     (scan nodes.json, filter kept IDs + type nodes)
  stage6  Emit per-predicate NODE→NODE edges   (includes rdf:type edges)
  all     run stage1..stage6

DBpedia is a schemaless property graph, and the whole point of this demo is
to keep it that way. Every node lives under the single `NODE` label; types
are expressed as edges (`rdf-syntax-ns#type`) pointing to the type node.
Agents discover structure by walking these edges at runtime.

Intermediate cache at /data/dbpedia-cinema-work/ so any stage can resume.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

SRC = Path("/source-data/dbpedia")
WORK = Path("/data/dbpedia-cinema-work")
OUT = Path("/data/dbpedia-cinema-src")

NODES_JSON = SRC / "nodes.json"
TYPE_FWD = SRC / "edges_22-rdf-syntax-ns#type_6803.csv"            # entity -> type_node
TYPE_BWD = SRC / "edges_22-rdf-syntax-ns#type_6803.csv.backward"   # type_node -> entity

# -- Label mapping -----------------------------------------------------------
# One primary label per node (most specific wins). `:LABEL` column records all
# relevant DBpedia types that matched so the agent can still see e.g. Actor vs
# FilmDirector subtype after everything gets rolled up to "Person".
# Ordered: earlier == more specific (wins when multiple match).
LABEL_TYPES: dict[str, list[str]] = {
    "Film":         ["http://dbpedia.org/ontology/Film"],
    "Actor":        ["http://dbpedia.org/ontology/Actor",
                     "http://dbpedia.org/ontology/AdultActor",
                     "http://dbpedia.org/ontology/VoiceActor"],
    "Writer":       ["http://dbpedia.org/ontology/Writer",
                     "http://dbpedia.org/ontology/ScreenWriter"],
    "MusicalArtist":["http://dbpedia.org/ontology/MusicalArtist",
                     "http://dbpedia.org/ontology/Band"],
    "Person":       ["http://dbpedia.org/ontology/Person"],
    "Award":        ["http://dbpedia.org/ontology/Award"],
    "Company":      ["http://dbpedia.org/ontology/Company",
                     "http://dbpedia.org/ontology/BusCompany",
                     "http://dbpedia.org/ontology/Non-ProfitOrganisation"],
    "Organisation": ["http://dbpedia.org/ontology/Organisation"],
    "Genre":        ["http://dbpedia.org/ontology/Genre",
                     "http://dbpedia.org/ontology/MusicGenre"],
    "Country":      ["http://dbpedia.org/ontology/Country"],
    "City":         ["http://dbpedia.org/ontology/City"],
    "Place":        ["http://dbpedia.org/ontology/Place",
                     "http://dbpedia.org/ontology/PopulatedPlace",
                     "http://dbpedia.org/ontology/HistoricPlace"],
}
# Primary label precedence: Film wins outright. Person-subtypes (Actor / Writer
# / MusicalArtist) beat plain Person. Company beats Organisation. Country/City
# beat Place.
LABEL_PRIORITY = [
    "Film", "Actor", "Writer", "MusicalArtist", "Person",
    "Award", "Company", "Organisation", "Genre",
    "Country", "City", "Place",
]

# Reverse lookup — URI -> label name
URI_TO_LABEL: dict[str, str] = {}
for lbl, uris in LABEL_TYPES.items():
    for u in uris:
        URI_TO_LABEL[u] = lbl

# -- Cinema-relevant edge predicates ----------------------------------------
# Local filename -> normalized predicate name emitted in the workspace.
# Both forward (primary) and backward variants live in /source-data/dbpedia;
# we only use forward files since they carry the canonical direction.
CINEMA_EDGE_FILES: dict[str, str] = {
    "director":          "DIRECTED_BY",
    "starring":          "STARRING",
    "producer":          "PRODUCED_BY",
    "executiveProducer": "EXEC_PRODUCED_BY",
    "writer":            "WRITTEN_BY",
    "screenplay":        "SCREENPLAY_BY",
    "cinematography":    "CINEMATOGRAPHY_BY",
    "editing":           "EDITED_BY",
    "distributor":       "DISTRIBUTED_BY",
    "award":             "HAS_AWARD",
    "genre":             "HAS_GENRE",
    "basedOn":           "BASED_ON",
    "voice":             "VOICED_BY",
    "spouse":            "SPOUSE",
    "country":           "COUNTRY",
    "birthPlace":        "BORN_IN",
    "nationality":       "NATIONALITY",
}


# -- Per-label node properties to export ------------------------------------
# Everything below is stored as string in the CSV for simplicity. `name` is
# derived from the Wikipedia-style URI tail when no explicit `rdfs:label` or
# foaf:name exists — good enough for the demo.
PROP_URIS = {
    "http://www.w3.org/2000/01/rdf-schema#label",
    "http://xmlns.com/foaf/0.1/name",
    "http://dbpedia.org/property/name",
    "http://dbpedia.org/ontology/abstract",
    "http://www.w3.org/2000/01/rdf-schema#comment",
    "http://dbpedia.org/ontology/releaseDate",
    "http://dbpedia.org/property/released",
    "http://dbpedia.org/ontology/runtime",
    "http://dbpedia.org/ontology/budget",
    "http://dbpedia.org/ontology/gross",
    "http://dbpedia.org/ontology/birthDate",
    "http://dbpedia.org/ontology/deathDate",
    "http://dbpedia.org/property/title",
    "http://dbpedia.org/property/genre",
    "http://dbpedia.org/property/country",
}

# Per-label column order (id first, name second, then optional attrs).
LABEL_COLUMNS: dict[str, list[str]] = {
    "Film":         ["id", "uri", "name", "releaseYear", "runtime", "abstract"],
    "Actor":        ["id", "uri", "name", "birthDate", "deathDate", "abstract"],
    "Writer":       ["id", "uri", "name", "birthDate", "deathDate", "abstract"],
    "MusicalArtist":["id", "uri", "name", "birthDate", "deathDate", "abstract"],
    "Person":       ["id", "uri", "name", "birthDate", "deathDate", "abstract"],
    "Award":        ["id", "uri", "name", "abstract"],
    "Company":      ["id", "uri", "name", "abstract"],
    "Organisation": ["id", "uri", "name", "abstract"],
    "Genre":        ["id", "uri", "name", "abstract"],
    "Country":      ["id", "uri", "name"],
    "City":         ["id", "uri", "name"],
    "Place":        ["id", "uri", "name"],
}


# -- Helpers -----------------------------------------------------------------

def _log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def _uri_tail(uri: str) -> str:
    t = uri.rsplit("/", 1)[-1].rsplit("#", 1)[-1]
    return t.replace("_", " ")


def _sanitize(s) -> str:
    """CSV-cell scrub: pipe is our delimiter, newlines must die."""
    if s is None:
        return ""
    s = str(s).replace("|", " ").replace("\n", " ").replace("\r", " ").replace("\t", " ")
    return s.strip()


def _as_iso_date(v) -> str:
    """DBpedia dates as ms-since-epoch → YYYY-MM-DD. Keep strings as-is."""
    if v is None or v == "":
        return ""
    s = str(v)
    try:
        n = int(float(s))
        if abs(n) >= 10**10:
            import datetime
            return datetime.datetime.utcfromtimestamp(n / 1000).strftime("%Y-%m-%d")
    except ValueError:
        pass
    return s


def _as_year(v) -> str:
    """DBpedia stores dates as milliseconds-since-epoch (int or string). Some
    older dumps also have 'YYYY-MM-DD' strings or bare years. Handle all."""
    if v is None or v == "":
        return ""
    s = str(v)
    # ms-since-epoch (10+ digits, all numeric, optionally negative)
    try:
        n = int(float(s))
        if abs(n) >= 10**10:
            import datetime
            return str(datetime.datetime.utcfromtimestamp(n / 1000).year)
        # bare year
        if 1800 <= n <= 2100:
            return str(n)
    except ValueError:
        pass
    # ISO "YYYY-MM-DD" or just "YYYY"
    if len(s) >= 4 and s[:4].isdigit():
        y = int(s[:4])
        if 1800 <= y <= 2100:
            return str(y)
    return ""


def _first(props, keys):
    for k in keys:
        if k in props and props[k] not in (None, ""):
            v = props[k]
            if isinstance(v, list):
                v = v[0] if v else ""
            return v
    return ""


# ---------------------------------------------------------------------------
# stage1 — distinct type node IDs
# ---------------------------------------------------------------------------
def stage1() -> None:
    WORK.mkdir(parents=True, exist_ok=True)
    out = WORK / "type_nids.txt"
    if out.exists():
        _log(f"stage1: {out} already exists, skipping")
        return
    _log(f"stage1: scanning {TYPE_BWD} for distinct type-node LHS")
    seen: set[str] = set()
    with open(TYPE_BWD) as f:
        next(f, None)  # header
        for i, line in enumerate(f):
            if i and i % 10_000_000 == 0:
                _log(f"  {i//1_000_000}M rows, {len(seen)} distinct")
            lhs = line.split("|", 1)[0]
            if lhs:
                seen.add(lhs)
    out.write_text("\n".join(sorted(seen, key=int)))
    _log(f"stage1: wrote {len(seen)} type-node IDs → {out}")


# ---------------------------------------------------------------------------
# stage2 — resolve type-node URIs (one pass of nodes.json, but filter by ID)
# ---------------------------------------------------------------------------
def stage2() -> None:
    out = WORK / "type_nid_to_uri.json"
    if out.exists():
        _log(f"stage2: {out} already exists, skipping")
        return
    _log("stage2: resolving type-node URIs")
    type_nids = set((WORK / "type_nids.txt").read_text().split())
    _log(f"  looking up {len(type_nids)} URIs in nodes.json")
    resolved: dict[str, str] = {}
    t0 = time.time()
    with open(NODES_JSON, "rb") as f:
        for i, raw in enumerate(f):
            if i and i % 5_000_000 == 0:
                _log(f"  {i//1_000_000}M rows, resolved {len(resolved)}/{len(type_nids)} ({time.time()-t0:.0f}s)")
            if len(resolved) >= len(type_nids):
                break
            try:
                obj = json.loads(raw)
            except Exception:
                continue
            props = obj.get("properties") or {}
            nid = str(props.get("id", ""))
            if nid in type_nids:
                resolved[nid] = props.get("uri", "")
    out.write_text(json.dumps(resolved, ensure_ascii=False))
    _log(f"stage2: wrote {len(resolved)} URIs → {out}")


# ---------------------------------------------------------------------------
# stage3 — classify entities
# ---------------------------------------------------------------------------
def stage3() -> None:
    out_primary = WORK / "entity_primary_label.tsv"
    out_sublabels = WORK / "entity_sublabels.tsv"
    if out_primary.exists() and out_sublabels.exists():
        _log(f"stage3: {out_primary} already exists, skipping")
        return

    type_nid_to_uri: dict[str, str] = json.loads((WORK / "type_nid_to_uri.json").read_text())

    # For each label, the set of type-node IDs that should map to it.
    label_to_type_nids: dict[str, set[str]] = defaultdict(set)
    for nid, uri in type_nid_to_uri.items():
        lbl = URI_TO_LABEL.get(uri)
        if lbl:
            label_to_type_nids[lbl].add(nid)

    _log("stage3: label -> #type-nodes (tracked)")
    for lbl in LABEL_PRIORITY:
        _log(f"  {lbl:14s} {len(label_to_type_nids.get(lbl, set()))}")

    # type_nid -> label
    tnid_to_label: dict[str, str] = {}
    for lbl, tnids in label_to_type_nids.items():
        for t in tnids:
            tnid_to_label[t] = lbl

    # Pass over type.backward: for each (type_nid, entity_nid) if type_nid maps
    # to one of our labels, add label to entity's set.
    _log("stage3: classifying entities (pass 2 of type.backward)")
    entity_labels: dict[str, set[str]] = defaultdict(set)
    t0 = time.time()
    with open(TYPE_BWD) as f:
        next(f, None)
        for i, line in enumerate(f):
            if i and i % 10_000_000 == 0:
                _log(f"  {i//1_000_000}M rows, {len(entity_labels)} classified ({time.time()-t0:.0f}s)")
            parts = line.rstrip().split("|")
            if len(parts) != 2:
                continue
            tnid, enid = parts
            lbl = tnid_to_label.get(tnid)
            if lbl:
                entity_labels[enid].add(lbl)

    _log(f"stage3: {len(entity_labels)} entities got at least one tracked label")

    # Pick primary label: first hit in LABEL_PRIORITY. Sub-labels: all others.
    prio_index = {lbl: i for i, lbl in enumerate(LABEL_PRIORITY)}
    counts = Counter()
    with open(out_primary, "w") as fp_prim, open(out_sublabels, "w") as fp_sub:
        for enid, labels in entity_labels.items():
            ordered = sorted(labels, key=lambda L: prio_index.get(L, 999))
            primary = ordered[0]
            subs = ordered[1:]
            # For Person bucket consolidation: if an entity is tagged both
            # Person AND a Person-subtype (Actor/FilmDirector/Writer), promote
            # the subtype. The loop above already handles that via PRIORITY.
            # Place-likes: similar story.
            counts[primary] += 1
            fp_prim.write(f"{enid}\t{primary}\n")
            if subs:
                fp_sub.write(f"{enid}\t{';'.join(subs)}\n")

    _log("stage3: primary label distribution")
    for lbl, c in counts.most_common():
        _log(f"  {lbl:14s} {c}")


# ---------------------------------------------------------------------------
# stage4 — filter cinema edges and constrain node set
# ---------------------------------------------------------------------------
def _load_primary_label() -> dict[str, str]:
    m: dict[str, str] = {}
    for line in open(WORK / "entity_primary_label.tsv"):
        enid, lbl = line.rstrip().split("\t", 1)
        m[enid] = lbl
    return m


def _cinema_predicate_files() -> dict[str, list[Path]]:
    """Return predicate -> [forward-CSV path, ...] (may have duplicates for ont/prop)."""
    out: dict[str, list[Path]] = defaultdict(list)
    for p in SRC.iterdir():
        name = p.name
        if not (name.startswith("edges_") and name.endswith(".csv")):
            continue
        if name.endswith(".backward.csv") or ".csv.backward" in name:
            continue
        # "edges_<pred>_<suffix>.csv"
        core = name[len("edges_"):-len(".csv")]
        # Split pred from suffix (suffix is always trailing digits)
        i = core.rfind("_")
        if i < 0:
            continue
        pred = core[:i]
        if pred in CINEMA_EDGE_FILES:
            out[pred].append(p)
    return out


def stage4() -> None:
    """First pass: decide cinema node set by taking Film ∪ (everything that is
    the other endpoint of a cinema-relevant edge against a Film/Person).

    For edges, we keep a row only when both endpoints are in the final set.
    """
    out_nodes = WORK / "cinema_nodes.tsv"
    out_edges_dir = WORK / "edges_raw"
    done_marker = WORK / "stage4.done"
    if done_marker.exists():
        _log(f"stage4: already done (marker {done_marker})")
        return
    out_edges_dir.mkdir(parents=True, exist_ok=True)

    primary = _load_primary_label()  # enid -> label
    _log(f"stage4: {len(primary)} entities with a tracked label")

    # Film/Person-ish anchors: the "cinema set" grows from these.
    ANCHOR = {"Film", "Actor", "Writer", "MusicalArtist", "Person"}
    # Acceptable endpoint labels (we drop edges where either end is non-cinema)
    CINEMA_LABELS = {
        "Film", "Actor", "Writer", "MusicalArtist", "Person",
        "Award", "Company", "Organisation", "Genre",
        "Country", "City", "Place",
    }

    pred_files = _cinema_predicate_files()
    _log(f"stage4: cinema predicates found: {sorted(pred_files)}")

    # Collect edges and accumulate endpoint set (starting from *any* anchor that
    # appears on at least one cinema edge). We only keep endpoints whose
    # primary label is in CINEMA_LABELS — that implicitly drops unlabeled nodes.
    #
    # One pass strategy: for each edge (s,d), if
    #   - primary(s) and primary(d) both in CINEMA_LABELS, AND
    #   - at least one of (s,d) is an ANCHOR or Film,
    # keep it.
    kept_nodes: set[str] = set()
    kept_edge_count = Counter()
    dropped_no_label = 0
    dropped_other = 0

    for pred, files in pred_files.items():
        pred_out = out_edges_dir / f"{pred}.tsv"
        n_in = n_out = 0
        with open(pred_out, "w") as fpo:
            for src_file in files:
                with open(src_file) as fi:
                    next(fi, None)  # header
                    for line in fi:
                        n_in += 1
                        parts = line.rstrip().split("|")
                        if len(parts) != 2:
                            continue
                        s, d = parts
                        ls = primary.get(s)
                        ld = primary.get(d)
                        if ls is None or ld is None:
                            dropped_no_label += 1
                            continue
                        if ls not in CINEMA_LABELS or ld not in CINEMA_LABELS:
                            dropped_other += 1
                            continue
                        # At least one side must be a cinema anchor
                        if ls not in ANCHOR and ld not in ANCHOR:
                            dropped_other += 1
                            continue
                        kept_nodes.add(s)
                        kept_nodes.add(d)
                        fpo.write(f"{s}\t{d}\t{ls}\t{ld}\n")
                        n_out += 1
        kept_edge_count[pred] = n_out
        _log(f"  {pred:18s} {n_in:>10,} in  →  {n_out:>10,} out  (→ {pred_out.name})")

    _log(f"stage4: kept {len(kept_nodes):,} nodes, {sum(kept_edge_count.values()):,} edges")
    _log(f"  dropped (no-label): {dropped_no_label:,}  (non-cinema label or no-anchor): {dropped_other:,}")

    with open(out_nodes, "w") as fp:
        for enid in sorted(kept_nodes, key=int):
            fp.write(f"{enid}\t{primary[enid]}\n")
    _log(f"stage4: wrote {out_nodes}")
    done_marker.touch()


# ---------------------------------------------------------------------------
# stage5 — emit nodes.json (single NODE label, schemaless)
# ---------------------------------------------------------------------------
def stage5() -> None:
    """Produce /data/dbpedia-cinema-src/nodes.json with one line per kept
    node, in DBpedia's native `{"labels":["NODE"],"properties":{...}}` format.

    Kept nodes = cinema entities (from stage4) ∪ every type-node referenced
    by those entities' rdf:type edges. Including the type nodes means an
    agent can query `MATCH (n)-[:type]->(t {uri:"…/Film"})` — the types are
    first-class NODE rows like everything else.

    Every property that DBpedia had for the node is preserved. Schema
    heterogeneity is the whole point of this load — different Films carry
    different property sets, and the agent discovers that at runtime via
    `describe_type`.
    """
    out_path = OUT / "nodes.json"
    done_marker = WORK / "stage5.done"
    if done_marker.exists():
        _log(f"stage5: already done (marker {done_marker})")
        return

    # kept = cinema entities
    kept: set[str] = set()
    for line in open(WORK / "cinema_nodes.tsv"):
        enid, _ = line.rstrip().split("\t", 1)
        kept.add(enid)
    n_entities = len(kept)

    # Plus every type-node touched by a cinema entity's rdf:type edge. We'll
    # harvest those type IDs in a single pass over type.backward, filtering
    # by entity ∈ kept.
    kept_type_nids: set[str] = set()
    _log(f"stage5a: scanning type.backward to find type nodes touched by {n_entities:,} entities")
    with open(TYPE_BWD) as f:
        next(f, None)
        for line in f:
            parts = line.rstrip().split("|")
            if len(parts) != 2:
                continue
            tnid, enid = parts
            if enid in kept:
                kept_type_nids.add(tnid)
    _log(f"stage5a: type nodes to include as NODE rows: {len(kept_type_nids):,}")
    kept.update(kept_type_nids)

    _log(f"stage5b: scanning {NODES_JSON} for {len(kept):,} node rows (entities + types)")
    t0 = time.time()
    matched = 0
    with open(NODES_JSON, "rb") as fi, open(out_path, "w") as fo:
        for i, raw in enumerate(fi):
            if i and i % 5_000_000 == 0:
                _log(f"  {i//1_000_000}M rows scanned, matched {matched:,} ({time.time()-t0:.0f}s)")
            if matched >= len(kept):
                break
            try:
                obj = json.loads(raw)
            except Exception:
                continue
            props = obj.get("properties") or {}
            nid = str(props.get("id", ""))
            if nid not in kept:
                continue
            # Pass through the whole node record; keep the same shape
            # /scripts/load-dbpedia.sh expects.
            obj["labels"] = ["NODE"]
            fo.write(json.dumps(obj, ensure_ascii=False))
            fo.write("\n")
            matched += 1

    _log(f"stage5: wrote {matched:,} NODE rows → {out_path}  ({time.time()-t0:.0f}s)")
    done_marker.touch()


# ---------------------------------------------------------------------------
# stage6 — emit per-predicate NODE→NODE edge CSVs (incl. rdf:type)
# ---------------------------------------------------------------------------
def stage6() -> None:
    """Produce one CSV per cinema predicate plus one `type.csv` for
    rdf:type edges, all headered `:START_ID(NODE)|:END_ID(NODE)`.

    Label information is no longer encoded in per-file naming. The agent
    discovers type membership by walking `(:NODE)-[:type]->(:NODE)`.
    """
    out_dir = OUT / "edges"
    done_marker = WORK / "stage6.done"
    if done_marker.exists():
        _log(f"stage6: already done (marker {done_marker})")
        return
    out_dir.mkdir(parents=True, exist_ok=True)

    raw_dir = WORK / "edges_raw"

    kept_nodes: set[str] = set()
    for line in open(WORK / "cinema_nodes.tsv"):
        enid, _ = line.rstrip().split("\t", 1)
        kept_nodes.add(enid)
    # Also include type nodes we harvested in stage5 (type.backward) — so
    # that rdf:type edges land with valid :END_ID(NODE). We re-derive the
    # set here instead of persisting another intermediate.
    type_nids: set[str] = set()
    with open(TYPE_BWD) as f:
        next(f, None)
        for line in f:
            parts = line.rstrip().split("|")
            if len(parts) != 2:
                continue
            tnid, enid = parts
            if enid in kept_nodes:
                type_nids.add(tnid)
    kept_nodes.update(type_nids)

    # ---- Cinema predicate edges --------------------------------------
    for pred, edge_label in CINEMA_EDGE_FILES.items():
        src_tsv = raw_dir / f"{pred}.tsv"
        if not src_tsv.exists():
            continue
        path = out_dir / f"{edge_label}.csv"
        seen: set[tuple[str, str]] = set()
        n_in = n_out = 0
        with open(src_tsv) as fi, open(path, "w") as fo:
            fo.write(":START_ID(NODE)|:END_ID(NODE)\n")
            for line in fi:
                parts = line.rstrip().split("\t")
                if len(parts) < 2:
                    continue
                s, d = parts[0], parts[1]
                n_in += 1
                if s not in kept_nodes or d not in kept_nodes:
                    continue
                key = (s, d)
                if key in seen:
                    continue
                seen.add(key)
                fo.write(f"{s}|{d}\n")
                n_out += 1
        _log(f"  {edge_label:18s} {n_in:>10,} in → {n_out:>10,} out  (→ {path.name})")

    # ---- rdf:type edges ----------------------------------------------
    # Source is the forward type file (entity → type_node). We filter to
    # edges whose source is in our kept entity set.
    type_out = out_dir / "type.csv"
    n_in = n_out = 0
    with open(TYPE_FWD) as fi, open(type_out, "w") as fo:
        next(fi, None)
        fo.write(":START_ID(NODE)|:END_ID(NODE)\n")
        for line in fi:
            parts = line.rstrip().split("|")
            if len(parts) != 2:
                continue
            s, d = parts
            n_in += 1
            if s in kept_nodes and d in kept_nodes:
                fo.write(f"{s}|{d}\n")
                n_out += 1
    _log(f"  {'type':18s} {n_in:>10,} in → {n_out:>10,} out  (→ {type_out.name})")
    _log(f"stage6: wrote per-predicate NODE→NODE edge CSVs to {out_dir}")
    done_marker.touch()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("stage", choices=["stage1", "stage2", "stage3", "stage4",
                                       "stage5", "stage6", "all"])
    args = ap.parse_args()

    WORK.mkdir(parents=True, exist_ok=True)
    OUT.mkdir(parents=True, exist_ok=True)

    if args.stage in ("stage1", "all"):
        stage1()
    if args.stage in ("stage2", "all"):
        stage2()
    if args.stage in ("stage3", "all"):
        stage3()
    if args.stage in ("stage4", "all"):
        stage4()
    if args.stage in ("stage5", "all"):
        stage5()
    if args.stage in ("stage6", "all"):
        stage6()

    _log("done.")


if __name__ == "__main__":
    main()
