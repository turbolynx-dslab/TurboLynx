#!/usr/bin/env python3
"""LDBC SNB Datagen (Hadoop variant, v0.3.6+) → TurboLynx fixture.

Hadoop datagen output differences vs the Spark variant the existing
preprocessor targets:

* No `deletionDate` / `explicitlyDeleted` columns on entities or edges.
* Multi-valued fields (`speaks`, `email`) live in their own
  `person_speaks_language_*.csv` / `person_email_emailaddress_*.csv`
  files instead of being inlined.
* Datetimes are emitted as `2010-02-14T15:32:10.447+0000` (no colon in
  the offset).
* Files are single-shot lowercase `entity_0_0.csv`, not
  `EntityName/part-XXXXX-{uuid}.csv`.
* Some edges have duplicate header names (e.g. `Person.id|Person.id`),
  so positional access is required.

This script synthesises the missing columns (deletionDate = a
far-future sentinel, explicitlyDeleted = 0) so the output is drop-in
compatible with `scripts/load-ldbc.sh`. Per-row deletion semantics
aren't exercised by the [ldbc] regression suite.

Usage:  python3 ldbc-hadoop-preprocess.py <input_root> <output_dir>
   <input_root>  e.g. ~/ldbc-hadoop-sf01/out/social_network
   <output_dir>  e.g. /tmp/ldbc-hadoop-sf01-data
"""

from __future__ import annotations

import csv
import datetime as dt
import glob
import sys
from pathlib import Path

# Sentinel matching what the Spark preprocessor emits when deletionDate
# is absent in raw data (mid-2030 ms-since-epoch).
DELETION_SENTINEL_MS = "1893456000000"


# ---------------------------------------------------------------------------
# Conversions
# ---------------------------------------------------------------------------

def iso_to_epoch_ms(s: str) -> str:
    """Hadoop emits offsets without a colon (`+0000`); fromisoformat in
    Python 3.10 only accepts `+00:00`. Normalise both forms."""
    if not s:
        return ""
    s = s.strip()
    if len(s) >= 5 and (s[-5] in "+-") and s[-3] != ":":
        s = s[:-2] + ":" + s[-2:]
    d = dt.datetime.fromisoformat(s)
    return str(int(d.timestamp() * 1000))


# ---------------------------------------------------------------------------
# Schema definitions — vertices use named columns since Hadoop emits
# clean unique headers for entity files.
# ---------------------------------------------------------------------------

VERTICES = [
    # section, glob, out_name, label, columns
    # columns = (raw_col_name | "__synth_del__" | "__synth_zero__",
    #            typed_output, conv_for_real_col)
    dict(section="dynamic", raw_glob="person_*_0.csv", out_name="Person.csv",
         columns=[
             ("creationDate", "creationDate:DATE_EPOCHMS",  iso_to_epoch_ms),
             ("__synth_del__","deletionDate:DATE_EPOCHMS",  None),
             ("__synth_zero__","explicitlyDeleted:INT",     None),
             ("id",           "id:ID(Person)",              None),
             ("firstName",    "firstName:STRING",           None),
             ("lastName",     "lastName:STRING",            None),
             ("gender",       "gender:STRING",              None),
             ("birthday",     "birthday:DATE",              None),
             ("locationIP",   "locationIP:STRING",          None),
             ("browserUsed",  "browserUsed:STRING",         None),
         ]),
    dict(section="dynamic", raw_glob="forum_*_0.csv", out_name="Forum.csv",
         columns=[
             ("creationDate", "creationDate:DATE_EPOCHMS",  iso_to_epoch_ms),
             ("__synth_del__","deletionDate:DATE_EPOCHMS",  None),
             ("__synth_zero__","explicitlyDeleted:INT",     None),
             ("id",           "id:ID(Forum)",               None),
             ("title",        "title:STRING",               None),
         ]),
    dict(section="dynamic", raw_glob="post_*_0.csv", out_name="Post.csv",
         columns=[
             ("creationDate", "creationDate:DATE_EPOCHMS",  iso_to_epoch_ms),
             ("__synth_del__","deletionDate:DATE_EPOCHMS",  None),
             ("__synth_zero__","explicitlyDeleted:INT",     None),
             ("id",           "id:ID(Post)",                None),
             ("imageFile",    "imageFile:STRING",           None),
             ("locationIP",   "locationIP:STRING",          None),
             ("browserUsed",  "browserUsed:STRING",         None),
             ("language",     "language:STRING",            None),
             ("content",      "content:STRING",             None),
             ("length",       "length:INT",                 None),
         ]),
    dict(section="dynamic", raw_glob="comment_*_0.csv", out_name="Comment.csv",
         columns=[
             ("creationDate", "creationDate:DATE_EPOCHMS",  iso_to_epoch_ms),
             ("__synth_del__","deletionDate:DATE_EPOCHMS",  None),
             ("__synth_zero__","explicitlyDeleted:INT",     None),
             ("id",           "id:ID(Comment)",             None),
             ("locationIP",   "locationIP:STRING",          None),
             ("browserUsed",  "browserUsed:STRING",         None),
             ("content",      "content:STRING",             None),
             ("length",       "length:INT",                 None),
         ]),
    dict(section="static", raw_glob="place_*_0.csv", out_name="Place.csv",
         columns=[
             ("id",   "id:ID(Place)", None),
             ("name", "name:STRING",  None),
             ("url",  "url:STRING",   None),
             ("type", "type:STRING",  None),
         ]),
    dict(section="static", raw_glob="organisation_*_0.csv",
         out_name="Organisation.csv",
         columns=[
             ("id",   "id:ID(Organisation)", None),
             ("type", "type:STRING",         None),
             ("name", "name:STRING",         None),
             ("url",  "url:STRING",          None),
         ]),
    dict(section="static", raw_glob="tag_*_0.csv", out_name="Tag.csv",
         columns=[
             ("id",   "id:ID(Tag)",  None),
             ("name", "name:STRING", None),
             ("url",  "url:STRING",  None),
         ]),
    dict(section="static", raw_glob="tagclass_*_0.csv", out_name="TagClass.csv",
         columns=[
             ("id",   "id:ID(TagClass)", None),
             ("name", "name:STRING",     None),
             ("url",  "url:STRING",      None),
         ]),
]


# Edges: defined positionally because some Hadoop files repeat the same
# header name (`Person.id|Person.id`). Each entry describes how the
# *output* row is built from the input row.
#
# `date_pos`: index in the Hadoop row of the per-edge date column (None
#   if the edge has no date — we synthesise then).
# `extra_pos`: index of an extra typed property (classYear / workFrom).
# `extra_typed`: typed header for that extra property.
# `has_explicit_delete`: emit explicitlyDeleted column? (4-col edges
#   like `*_hasTag_*` skip it to match the Spark mini layout.)
EDGES = [
    # Static (just src, dst).
    dict(section="static", raw_glob="place_isPartOf_place_*_0.csv",
         out_name="Place_isPartOf_Place.csv",
         start_label="Place", end_label="Place",
         date_pos=None, extra_pos=None, has_explicit_delete=False,
         emit_creation=False, emit_deletion=False),
    dict(section="static", raw_glob="organisation_isLocatedIn_place_*_0.csv",
         out_name="Organisation_isLocatedIn_Place.csv",
         start_label="Organisation", end_label="Place",
         date_pos=None, extra_pos=None, has_explicit_delete=False,
         emit_creation=False, emit_deletion=False),
    dict(section="static", raw_glob="tagclass_isSubclassOf_tagclass_*_0.csv",
         out_name="TagClass_isSubclassOf_TagClass.csv",
         start_label="TagClass", end_label="TagClass",
         date_pos=None, extra_pos=None, has_explicit_delete=False,
         emit_creation=False, emit_deletion=False),
    dict(section="static", raw_glob="tag_hasType_tagclass_*_0.csv",
         out_name="Tag_hasType_TagClass.csv",
         start_label="Tag", end_label="TagClass",
         date_pos=None, extra_pos=None, has_explicit_delete=False,
         emit_creation=False, emit_deletion=False),

    # Edges with creationDate + explicitlyDeleted (most dynamic edges in
    # the Spark mini schema).
    dict(section="dynamic", raw_glob="person_knows_person_*_0.csv",
         out_name="Person_knows_Person.csv",
         start_label="Person", end_label="Person",
         date_pos=2, extra_pos=None, has_explicit_delete=True),
    dict(section="dynamic", raw_glob="person_likes_comment_*_0.csv",
         out_name="Person_likes_Comment.csv",
         start_label="Person", end_label="Comment",
         date_pos=2, extra_pos=None, has_explicit_delete=True),
    dict(section="dynamic", raw_glob="person_likes_post_*_0.csv",
         out_name="Person_likes_Post.csv",
         start_label="Person", end_label="Post",
         date_pos=2, extra_pos=None, has_explicit_delete=True),
    dict(section="dynamic", raw_glob="forum_hasMember_person_*_0.csv",
         out_name="Forum_hasMember_Person.csv",
         start_label="Forum", end_label="Person",
         date_pos=2, extra_pos=None, has_explicit_delete=True),

    # Edges Hadoop emits without a per-edge date but the TurboLynx
    # schema still has a creationDate column — synthesise.
    dict(section="dynamic", raw_glob="comment_hasCreator_person_*_0.csv",
         out_name="Comment_hasCreator_Person.csv",
         start_label="Comment", end_label="Person",
         date_pos=None, extra_pos=None, has_explicit_delete=True),
    dict(section="dynamic", raw_glob="comment_isLocatedIn_place_*_0.csv",
         out_name="Comment_isLocatedIn_Place.csv",
         start_label="Comment", end_label="Place",
         date_pos=None, extra_pos=None, has_explicit_delete=True),
    dict(section="dynamic", raw_glob="comment_replyOf_comment_*_0.csv",
         out_name="Comment_replyOf_Comment.csv",
         start_label="Comment", end_label="Comment",
         date_pos=None, extra_pos=None, has_explicit_delete=True),
    dict(section="dynamic", raw_glob="comment_replyOf_post_*_0.csv",
         out_name="Comment_replyOf_Post.csv",
         start_label="Comment", end_label="Post",
         date_pos=None, extra_pos=None, has_explicit_delete=True),
    dict(section="dynamic", raw_glob="post_hasCreator_person_*_0.csv",
         out_name="Post_hasCreator_Person.csv",
         start_label="Post", end_label="Person",
         date_pos=None, extra_pos=None, has_explicit_delete=True),
    dict(section="dynamic", raw_glob="post_isLocatedIn_place_*_0.csv",
         out_name="Post_isLocatedIn_Place.csv",
         start_label="Post", end_label="Place",
         date_pos=None, extra_pos=None, has_explicit_delete=True),
    dict(section="dynamic", raw_glob="forum_containerOf_post_*_0.csv",
         out_name="Forum_containerOf_Post.csv",
         start_label="Forum", end_label="Post",
         date_pos=None, extra_pos=None, has_explicit_delete=True),
    dict(section="dynamic", raw_glob="forum_hasModerator_person_*_0.csv",
         out_name="Forum_hasModerator_Person.csv",
         start_label="Forum", end_label="Person",
         date_pos=None, extra_pos=None, has_explicit_delete=True),
    dict(section="dynamic", raw_glob="person_isLocatedIn_place_*_0.csv",
         out_name="Person_isLocatedIn_Place.csv",
         start_label="Person", end_label="Place",
         date_pos=None, extra_pos=None, has_explicit_delete=True),

    # Edges without explicitlyDeleted (4-col Spark mini layout).
    dict(section="dynamic", raw_glob="comment_hasTag_tag_*_0.csv",
         out_name="Comment_hasTag_Tag.csv",
         start_label="Comment", end_label="Tag",
         date_pos=None, extra_pos=None, has_explicit_delete=False),
    dict(section="dynamic", raw_glob="forum_hasTag_tag_*_0.csv",
         out_name="Forum_hasTag_Tag.csv",
         start_label="Forum", end_label="Tag",
         date_pos=None, extra_pos=None, has_explicit_delete=False),
    dict(section="dynamic", raw_glob="person_hasInterest_tag_*_0.csv",
         out_name="Person_hasInterest_Tag.csv",
         start_label="Person", end_label="Tag",
         date_pos=None, extra_pos=None, has_explicit_delete=False),
    dict(section="dynamic", raw_glob="post_hasTag_tag_*_0.csv",
         out_name="Post_hasTag_Tag.csv",
         start_label="Post", end_label="Tag",
         date_pos=None, extra_pos=None, has_explicit_delete=False),

    # Edges with extra typed property — Hadoop emits the value as plain
    # text in the third column.
    dict(section="dynamic", raw_glob="person_studyAt_organisation_*_0.csv",
         out_name="Person_studyAt_Organisation.csv",
         start_label="Person", end_label="Organisation",
         date_pos=None, extra_pos=2, extra_typed="classYear:INT",
         has_explicit_delete=False),
    dict(section="dynamic", raw_glob="person_workAt_organisation_*_0.csv",
         out_name="Person_workAt_Organisation.csv",
         start_label="Person", end_label="Organisation",
         date_pos=None, extra_pos=2, extra_typed="workFrom:INT",
         has_explicit_delete=False),
]


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def find_input(input_root: Path, section: str, raw_glob: str) -> Path | None:
    candidates = sorted(glob.glob(str(input_root / section / raw_glob)))
    return Path(candidates[0]) if candidates else None


def write_typed_vertex(in_path: Path, out_path: Path, columns):
    typed_header = "|".join(typed for _, typed, _ in columns)
    with in_path.open("r", encoding="utf-8") as fin:
        reader = csv.reader(fin, delimiter="|")
        raw_header = next(reader)
        col_idx = {name: i for i, name in enumerate(raw_header)}
        for raw_name, _, _ in columns:
            if raw_name.startswith("__synth"):
                continue
            if raw_name not in col_idx:
                raise SystemExit(f"missing column {raw_name} in {in_path}; "
                                 f"have {raw_header}")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", encoding="utf-8", newline="") as fout:
            fout.write(typed_header + "\n")
            writer = csv.writer(fout, delimiter="|", lineterminator="\n")
            for row in reader:
                out_row = []
                for raw_name, _, conv in columns:
                    if raw_name == "__synth_del__":
                        val = DELETION_SENTINEL_MS
                    elif raw_name == "__synth_zero__":
                        val = "0"
                    else:
                        val = row[col_idx[raw_name]]
                        if conv is not None:
                            val = conv(val)
                    out_row.append(val)
                writer.writerow(out_row)


def write_typed_edge(in_path: Path, out_path: Path, edge):
    src_label = edge["start_label"]
    dst_label = edge["end_label"]
    date_pos = edge["date_pos"]
    extra_pos = edge.get("extra_pos")
    has_explicit_delete = edge["has_explicit_delete"]
    extra_typed = edge.get("extra_typed")
    emit_creation = edge.get("emit_creation", True)
    emit_deletion = edge.get("emit_deletion", True)

    # Build typed header in TurboLynx (Spark mini) order.
    fwd_header = [f":START_ID({src_label})", f":END_ID({dst_label})"]
    bwd_header = [f":END_ID({dst_label})", f":START_ID({src_label})"]
    extras = []
    if emit_creation:
        extras.append("creationDate:DATE_EPOCHMS")
    if emit_deletion:
        extras.append("deletionDate:DATE_EPOCHMS")
    if has_explicit_delete:
        extras.append("explicitlyDeleted:INT")
    if extra_typed:
        extras.append(extra_typed)
    fwd_header += extras
    bwd_header += extras

    with in_path.open("r", encoding="utf-8") as fin:
        reader = csv.reader(fin, delimiter="|")
        next(reader)  # skip header — positional access below
        rows_fwd = []
        for row in reader:
            src = row[0]
            dst = row[1]
            tail = []
            if emit_creation:
                tail.append(iso_to_epoch_ms(row[date_pos])
                            if date_pos is not None else DELETION_SENTINEL_MS)
            if emit_deletion:
                tail.append(DELETION_SENTINEL_MS)
            if has_explicit_delete:
                tail.append("0")
            if extra_typed and extra_pos is not None:
                tail.append(row[extra_pos])
            rows_fwd.append([src, dst] + tail)

    rows_fwd.sort(key=lambda r: (int(r[0]), int(r[1])))

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as fout:
        fout.write("|".join(fwd_header) + "\n")
        w = csv.writer(fout, delimiter="|", lineterminator="\n")
        w.writerows(rows_fwd)

    bwd_path = out_path.with_suffix(out_path.suffix + ".backward")
    with bwd_path.open("w", encoding="utf-8", newline="") as fout:
        fout.write("|".join(bwd_header) + "\n")
        w = csv.writer(fout, delimiter="|", lineterminator="\n")
        rows_bwd = sorted(([r[1], r[0]] + r[2:] for r in rows_fwd),
                          key=lambda r: (int(r[0]), int(r[1])))
        w.writerows(rows_bwd)


def main():
    if len(sys.argv) != 3:
        print("usage: ldbc-hadoop-preprocess.py <input_root> <output_dir>",
              file=sys.stderr)
        sys.exit(2)
    input_root = Path(sys.argv[1]).expanduser().resolve()
    output_dir = Path(sys.argv[2]).expanduser().resolve()

    for spec in VERTICES:
        in_path = find_input(input_root, spec["section"], spec["raw_glob"])
        if not in_path:
            print(f"  [skip] vertex {spec['out_name']}: no match for "
                  f"{spec['section']}/{spec['raw_glob']}", file=sys.stderr)
            continue
        out_path = output_dir / spec["section"] / spec["out_name"]
        write_typed_vertex(in_path, out_path, spec["columns"])
        print(f"  vertex {spec['out_name']:25s} -> "
              f"{out_path.relative_to(output_dir)}")

    for edge in EDGES:
        in_path = find_input(input_root, edge["section"], edge["raw_glob"])
        if not in_path:
            print(f"  [skip] edge {edge['out_name']}: no match for "
                  f"{edge['section']}/{edge['raw_glob']}", file=sys.stderr)
            continue
        out_path = output_dir / edge["section"] / edge["out_name"]
        write_typed_edge(in_path, out_path, edge)
        print(f"  edge   {edge['out_name']:45s} -> "
              f"{out_path.relative_to(output_dir)} (+ .backward)")


if __name__ == "__main__":
    main()
