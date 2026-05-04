#!/usr/bin/env python3
"""LDBC SNB Datagen (Spark, raw mode, singular-projected-fk) → TurboLynx fixture.

Reads the output of:
    docker run ldbc/datagen-standalone:0.5.0-2.12_spark3.2 \\
      --parallelism 1 -- --format csv --scale-factor 0.003 \\
      --mode raw --explode-edges --explode-attrs

and writes a fixture compatible with `turbolynx import`:

* one CSV per vertex/edge (rather than `EntityName/part-XXXXX-{uuid}.csv`)
* TurboLynx typed headers (`:ID(LABEL)`, `:START_ID(SRC)`, `STRING`,
  `DATE_EPOCHMS`, `DATE`, `INT`)
* ISO 8601 datetimes (`2010-01-03T15:10:31.499+00:00`) converted to
  Unix epoch milliseconds (matches Cypher `timestamp()` semantics)
* "true"/"false" booleans rewritten to `1`/`0` (no native bool type
  in the bulkloader)
* edge column order rearranged so START/END id come first
* `.backward` companion sorted by (END_ID, START_ID)

Usage:  python3 ldbc-preprocess.py <input_root> <output_dir>
   <input_root>  e.g. ~/ldbc-raw/graphs/csv/raw/singular-projected-fk
   <output_dir>  e.g. test/data/ldbc-mini
"""

from __future__ import annotations

import csv
import datetime as dt
import glob
import os
import sys
from pathlib import Path


# ---------------------------------------------------------------------------
# Schema definitions
# ---------------------------------------------------------------------------

# Vertices: section, raw_dir under {static|dynamic}/, output filename,
# columns = list of (raw_col_name, turbolynx_typed_name, conversion).
# conversion ∈ {None, 'datetime', 'bool'} drives per-column value rewriting.
VERTICES = {
    "Person": dict(section="dynamic", raw_dir="Person", out_name="Person.csv", columns=[
        ("creationDate",      "creationDate:DATE_EPOCHMS", "datetime"),
        ("deletionDate",      "deletionDate:DATE_EPOCHMS", "datetime"),
        ("explicitlyDeleted", "explicitlyDeleted:INT",     "bool"),
        ("id",                "id:ID(Person)",             None),
        ("firstName",         "firstName:STRING",          None),
        ("lastName",          "lastName:STRING",           None),
        ("gender",            "gender:STRING",             None),
        ("birthday",          "birthday:DATE",             None),
        ("locationIP",        "locationIP:STRING",         None),
        ("browserUsed",       "browserUsed:STRING",        None),
    ]),
    "Forum": dict(section="dynamic", raw_dir="Forum", out_name="Forum.csv", columns=[
        ("creationDate",      "creationDate:DATE_EPOCHMS", "datetime"),
        ("deletionDate",      "deletionDate:DATE_EPOCHMS", "datetime"),
        ("explicitlyDeleted", "explicitlyDeleted:INT",     "bool"),
        ("id",                "id:ID(Forum)",              None),
        ("title",             "title:STRING",              None),
    ]),
    "Post": dict(section="dynamic", raw_dir="Post", out_name="Post.csv", columns=[
        ("creationDate",      "creationDate:DATE_EPOCHMS", "datetime"),
        ("deletionDate",      "deletionDate:DATE_EPOCHMS", "datetime"),
        ("explicitlyDeleted", "explicitlyDeleted:INT",     "bool"),
        ("id",                "id:ID(Post)",               None),
        ("imageFile",         "imageFile:STRING",          None),
        ("locationIP",        "locationIP:STRING",         None),
        ("browserUsed",       "browserUsed:STRING",        None),
        ("language",          "language:STRING",           None),
        ("content",           "content:STRING",            None),
        ("length",            "length:INT",                None),
    ]),
    "Comment": dict(section="dynamic", raw_dir="Comment", out_name="Comment.csv", columns=[
        ("creationDate",      "creationDate:DATE_EPOCHMS", "datetime"),
        ("deletionDate",      "deletionDate:DATE_EPOCHMS", "datetime"),
        ("explicitlyDeleted", "explicitlyDeleted:INT",     "bool"),
        ("id",                "id:ID(Comment)",            None),
        ("locationIP",        "locationIP:STRING",         None),
        ("browserUsed",       "browserUsed:STRING",        None),
        ("content",           "content:STRING",            None),
        ("length",            "length:INT",                None),
    ]),
    "Place": dict(section="static", raw_dir="Place", out_name="Place.csv", columns=[
        ("id",   "id:ID(Place)", None),
        ("name", "name:STRING",  None),
        ("url",  "url:STRING",   None),
        ("type", "type:STRING",  None),
    ]),
    "Organisation": dict(section="static", raw_dir="Organisation", out_name="Organisation.csv", columns=[
        ("id",   "id:ID(Organisation)", None),
        ("type", "type:STRING",         None),
        ("name", "name:STRING",         None),
        ("url",  "url:STRING",          None),
    ]),
    "Tag": dict(section="static", raw_dir="Tag", out_name="Tag.csv", columns=[
        ("id",   "id:ID(Tag)",  None),
        ("name", "name:STRING", None),
        ("url",  "url:STRING",  None),
    ]),
    "TagClass": dict(section="static", raw_dir="TagClass", out_name="TagClass.csv", columns=[
        ("id",   "id:ID(TagClass)", None),
        ("name", "name:STRING",     None),
        ("url",  "url:STRING",      None),
    ]),
}


# Edge shape helpers — the Spark raw output has a few recurring shapes.
def _e_full(section, raw_dir, out_name, src_label, dst_label, src_col, dst_col):
    """Edge with creationDate + deletionDate + explicitlyDeleted + START/END."""
    return dict(
        section=section, raw_dir=raw_dir, out_name=out_name,
        start_label=src_label, end_label=dst_label,
        raw_columns=[
            ("creationDate",      "creationDate:DATE_EPOCHMS", "datetime"),
            ("deletionDate",      "deletionDate:DATE_EPOCHMS", "datetime"),
            ("explicitlyDeleted", "explicitlyDeleted:INT",     "bool"),
            (src_col,             None,                        None),
            (dst_col,             None,                        None),
        ],
        src_col_idx=3, dst_col_idx=4,
    )


def _e_dates_only(section, raw_dir, out_name, src_label, dst_label, src_col, dst_col):
    """Edge with creationDate + deletionDate + START/END (no explicitlyDeleted)."""
    return dict(
        section=section, raw_dir=raw_dir, out_name=out_name,
        start_label=src_label, end_label=dst_label,
        raw_columns=[
            ("creationDate", "creationDate:DATE_EPOCHMS", "datetime"),
            ("deletionDate", "deletionDate:DATE_EPOCHMS", "datetime"),
            (src_col,        None,                        None),
            (dst_col,        None,                        None),
        ],
        src_col_idx=2, dst_col_idx=3,
    )


def _e_static(raw_dir, out_name, src_label, dst_label, src_col, dst_col):
    """Static edge with just START/END columns."""
    return dict(
        section="static", raw_dir=raw_dir, out_name=out_name,
        start_label=src_label, end_label=dst_label,
        raw_columns=[
            (src_col, None, None),
            (dst_col, None, None),
        ],
        src_col_idx=0, dst_col_idx=1,
    )


EDGES = [
    # Static edges (4).
    _e_static("Place_isPartOf_Place",            "Place_isPartOf_Place.csv",
              "Place", "Place",        "Place1Id",       "Place2Id"),
    _e_static("Organisation_isLocatedIn_Place",  "Organisation_isLocatedIn_Place.csv",
              "Organisation", "Place", "OrganisationId", "PlaceId"),
    _e_static("TagClass_isSubclassOf_TagClass",  "TagClass_isSubclassOf_TagClass.csv",
              "TagClass", "TagClass",  "TagClass1Id",    "TagClass2Id"),
    _e_static("Tag_hasType_TagClass",            "Tag_hasType_TagClass.csv",
              "Tag", "TagClass",       "TagId",          "TagClassId"),

    # Dynamic edges with creationDate + deletionDate + explicitlyDeleted (12).
    _e_full("dynamic", "Person_knows_Person",      "Person_knows_Person.csv",
            "Person", "Person",   "Person1Id",  "Person2Id"),
    _e_full("dynamic", "Person_likes_Comment",     "Person_likes_Comment.csv",
            "Person", "Comment",  "PersonId",   "CommentId"),
    _e_full("dynamic", "Person_likes_Post",        "Person_likes_Post.csv",
            "Person", "Post",     "PersonId",   "PostId"),
    _e_full("dynamic", "Person_isLocatedIn_City",  "Person_isLocatedIn_Place.csv",
            "Person", "Place",    "PersonId",   "CityId"),
    _e_full("dynamic", "Comment_hasCreator_Person",   "Comment_hasCreator_Person.csv",
            "Comment", "Person",  "CommentId",  "PersonId"),
    _e_full("dynamic", "Comment_isLocatedIn_Country", "Comment_isLocatedIn_Place.csv",
            "Comment", "Place",   "CommentId",  "CountryId"),
    _e_full("dynamic", "Comment_replyOf_Comment",  "Comment_replyOf_Comment.csv",
            "Comment", "Comment", "Comment1Id", "Comment2Id"),
    _e_full("dynamic", "Comment_replyOf_Post",     "Comment_replyOf_Post.csv",
            "Comment", "Post",    "CommentId",  "PostId"),
    _e_full("dynamic", "Post_hasCreator_Person",   "Post_hasCreator_Person.csv",
            "Post", "Person",     "PostId",     "PersonId"),
    _e_full("dynamic", "Post_isLocatedIn_Country", "Post_isLocatedIn_Place.csv",
            "Post", "Place",      "PostId",     "CountryId"),
    _e_full("dynamic", "Forum_containerOf_Post",   "Forum_containerOf_Post.csv",
            "Forum", "Post",      "ForumId",    "PostId"),
    _e_full("dynamic", "Forum_hasMember_Person",   "Forum_hasMember_Person.csv",
            "Forum", "Person",    "ForumId",    "PersonId"),
    _e_full("dynamic", "Forum_hasModerator_Person", "Forum_hasModerator_Person.csv",
            "Forum", "Person",    "ForumId",    "PersonId"),

    # Dynamic edges with only creationDate + deletionDate (4).
    _e_dates_only("dynamic", "Comment_hasTag_Tag",     "Comment_hasTag_Tag.csv",
                  "Comment", "Tag",  "CommentId", "TagId"),
    _e_dates_only("dynamic", "Forum_hasTag_Tag",       "Forum_hasTag_Tag.csv",
                  "Forum",   "Tag",  "ForumId",   "TagId"),
    _e_dates_only("dynamic", "Person_hasInterest_Tag", "Person_hasInterest_Tag.csv",
                  "Person",  "Tag",  "PersonId",  "TagId"),
    _e_dates_only("dynamic", "Post_hasTag_Tag",        "Post_hasTag_Tag.csv",
                  "Post",    "Tag",  "PostId",    "TagId"),

    # Dynamic edges with extra property after START/END (2).
    dict(section="dynamic", raw_dir="Person_studyAt_University",
         out_name="Person_studyAt_Organisation.csv",
         start_label="Person", end_label="Organisation",
         raw_columns=[
             ("creationDate", "creationDate:DATE_EPOCHMS", "datetime"),
             ("deletionDate", "deletionDate:DATE_EPOCHMS", "datetime"),
             ("PersonId",     None,                        None),
             ("UniversityId", None,                        None),
             ("classYear",    "classYear:INT",             None),
         ],
         src_col_idx=2, dst_col_idx=3),
    dict(section="dynamic", raw_dir="Person_workAt_Company",
         out_name="Person_workAt_Organisation.csv",
         start_label="Person", end_label="Organisation",
         raw_columns=[
             ("creationDate", "creationDate:DATE_EPOCHMS", "datetime"),
             ("deletionDate", "deletionDate:DATE_EPOCHMS", "datetime"),
             ("PersonId",     None,                        None),
             ("CompanyId",    None,                        None),
             ("workFrom",     "workFrom:INT",              None),
         ],
         src_col_idx=2, dst_col_idx=3),

    # Skipped — datagen emits but TurboLynx LDBC schema has no Email/Language vertex.
    dict(section="dynamic", raw_dir="Person_email_EmailAddress",
         out_name=None, start_label=None, end_label=None,
         raw_columns=[], src_col_idx=None, dst_col_idx=None),
    dict(section="dynamic", raw_dir="Person_speaks_Language",
         out_name=None, start_label=None, end_label=None,
         raw_columns=[], src_col_idx=None, dst_col_idx=None),
]


# ---------------------------------------------------------------------------
# Conversions
# ---------------------------------------------------------------------------

def iso_to_epoch_ms(s: str) -> str:
    if not s:
        return ""
    d = dt.datetime.fromisoformat(s)
    return str(int(d.timestamp() * 1000))


def bool_to_int(s: str) -> str:
    return "1" if (s or "").strip().lower() == "true" else "0"


CONVERTERS = {"datetime": iso_to_epoch_ms, "bool": bool_to_int}


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def find_part_csv(input_root: Path, section: str, raw_dir: str):
    candidates = sorted(glob.glob(str(input_root / section / raw_dir / "part-*.csv")))
    return Path(candidates[0]) if candidates else None


def write_typed_vertex(in_path: Path, out_path: Path, columns):
    typed_header = "|".join(typed for _, typed, _ in columns)
    with in_path.open("r", encoding="utf-8") as fin:
        reader = csv.reader(fin, delimiter="|")
        raw_header = next(reader)
        col_idx = {name: i for i, name in enumerate(raw_header)}
        for raw_name, _, _ in columns:
            if raw_name not in col_idx:
                raise SystemExit(f"missing column {raw_name} in {in_path}")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", encoding="utf-8", newline="") as fout:
            fout.write(typed_header + "\n")
            writer = csv.writer(fout, delimiter="|", lineterminator="\n")
            for row in reader:
                out_row = []
                for raw_name, _, conv in columns:
                    val = row[col_idx[raw_name]]
                    if conv:
                        val = CONVERTERS[conv](val)
                    out_row.append(val)
                writer.writerow(out_row)


def write_typed_edge(in_path: Path, out_path: Path, edge):
    raw_columns = edge["raw_columns"]
    src_idx_in_raw = edge["src_col_idx"]
    dst_idx_in_raw = edge["dst_col_idx"]
    src_label = edge["start_label"]
    dst_label = edge["end_label"]

    prop_indices = [i for i, (_, typed, _) in enumerate(raw_columns) if typed is not None]
    prop_typed_headers = [raw_columns[i][1] for i in prop_indices]
    fwd_header = [f":START_ID({src_label})", f":END_ID({dst_label})"] + prop_typed_headers
    bwd_header = [f":END_ID({dst_label})", f":START_ID({src_label})"] + prop_typed_headers

    with in_path.open("r", encoding="utf-8") as fin:
        reader = csv.reader(fin, delimiter="|")
        raw_header = next(reader)
        col_idx = {name: i for i, name in enumerate(raw_header)}
        for raw_name, _, _ in raw_columns:
            if raw_name not in col_idx:
                raise SystemExit(f"missing column {raw_name} in {in_path}")
        rows_fwd = []
        for row in reader:
            src = row[col_idx[raw_columns[src_idx_in_raw][0]]]
            dst = row[col_idx[raw_columns[dst_idx_in_raw][0]]]
            props = []
            for i in prop_indices:
                raw_name, _, conv = raw_columns[i]
                val = row[col_idx[raw_name]]
                if conv:
                    val = CONVERTERS[conv](val)
                props.append(val)
            rows_fwd.append([src, dst] + props)

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
        print("usage: ldbc-preprocess.py <input_root> <output_dir>", file=sys.stderr)
        sys.exit(2)
    input_root = Path(sys.argv[1]).expanduser().resolve()
    output_dir = Path(sys.argv[2]).expanduser().resolve()

    for label, spec in VERTICES.items():
        in_path = find_part_csv(input_root, spec["section"], spec["raw_dir"])
        if not in_path:
            print(f"  [skip] vertex {label}: no part-*.csv under "
                  f"{spec['section']}/{spec['raw_dir']}", file=sys.stderr)
            continue
        out_path = output_dir / spec["section"] / spec["out_name"]
        write_typed_vertex(in_path, out_path, spec["columns"])
        print(f"  vertex {label:14s} -> {out_path.relative_to(output_dir)}")

    for edge in EDGES:
        if edge["out_name"] is None:
            continue
        in_path = find_part_csv(input_root, edge["section"], edge["raw_dir"])
        if not in_path:
            print(f"  [skip] edge {edge['out_name']}: no part-*.csv under "
                  f"{edge['section']}/{edge['raw_dir']}", file=sys.stderr)
            continue
        out_path = output_dir / edge["section"] / edge["out_name"]
        write_typed_edge(in_path, out_path, edge)
        print(f"  edge   {edge['out_name']:50s} -> {out_path.relative_to(output_dir)} (+ .backward)")


if __name__ == "__main__":
    main()
