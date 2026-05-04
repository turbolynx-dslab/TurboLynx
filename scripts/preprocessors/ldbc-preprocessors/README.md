# LDBC SNB preprocessor

Converts the output of LDBC SNB Datagen (Spark variant, `raw` mode,
`singular-projected-fk` layout) into the TurboLynx graph fixture
format: vertex `.csv` files with TurboLynx typed headers, plus forward
and backward edge `.csv` files matching the schema in
`scripts/load-ldbc.sh`.

## Usage

```bash
python3 scripts/preprocessors/ldbc-preprocessors/ldbc-preprocess.py \
    <input_root> <output_dir>
```

* `<input_root>`: directory containing `static/` and `dynamic/`
  subdirectories with the per-entity `EntityName/part-*.csv` layout
  produced by the Spark datagen.
* `<output_dir>`: where to emit the converted fixture (e.g.
  `test/data/ldbc-mini/`). Will contain `static/EntityName.csv` and
  `dynamic/EntityName.csv` plus `*.tbl.backward` companions for each
  edge.

## Generating raw input from the Spark Datagen

```bash
mkdir -p $HOME/ldbc-raw
docker run --rm \
    --mount type=bind,source=$HOME/ldbc-raw,target=/out \
    ldbc/datagen-standalone:0.5.0-2.12_spark3.2 \
    --parallelism 1 \
    -- \
    --format csv --scale-factor 0.003 --mode raw \
    --explode-edges --explode-attrs

python3 scripts/preprocessors/ldbc-preprocessors/ldbc-preprocess.py \
    $HOME/ldbc-raw/graphs/csv/raw/singular-projected-fk \
    test/data/ldbc-mini/
```

Spark Datagen output is deterministic at a given scale factor — the
fixture committed at `test/data/ldbc-mini/` was produced by the
exact commands above.

## What the preprocessor does

* **Flattens** `EntityName/part-XXXXX-{uuid}.csv` to `EntityName.csv`.
* Adds **TurboLynx typed headers** (`:ID(Label)`, `:START_ID(Src)`,
  `:STRING`, `:DATE_EPOCHMS`, `:DATE`, `:INT`).
* Converts ISO 8601 datetimes (`2010-01-03T15:10:31.499+00:00`) to
  Unix epoch milliseconds. The bulkloader's `DATE_EPOCHMS` keyword is
  internally mapped to `LogicalType::TIMESTAMP_MS`, so this preserves
  full datetime semantics for Cypher queries.
* Rewrites `"true"` / `"false"` to `1` / `0` (the bulkloader has no
  native boolean type).
* For each edge, **reorders columns** so `:START_ID` and `:END_ID` come
  first (the Spark output has them last), then sorts by
  `(START_ID, END_ID)`.
* Emits a `.backward` companion sorted by `(END_ID, START_ID)` for each
  forward edge — TurboLynx's bulkloader expects both directions to be
  present for traversal.

## Schema

The schema map (vertex labels, edge labels, column types) is encoded
in the `VERTICES` and `EDGES` constants at the top of
`ldbc-preprocess.py`. Edits to the LDBC schema (e.g. new edge types,
renamed columns) need to be reflected there.

Supported labels:

* **Vertex**: `Person`, `Comment`, `Post`, `Forum` (dynamic);
  `Place`, `Organisation`, `Tag`, `TagClass` (static).
* **Edge**: 21 forward edges (4 static + 17 dynamic), each with a
  `.backward` companion.

The Spark Datagen also emits `Person_email_EmailAddress` and
`Person_speaks_Language` files; these are skipped because the
TurboLynx LDBC schema (per `scripts/load-ldbc.sh`) has no
`EmailAddress` or `Language` vertex labels.
