# DBpedia preprocessor

Extracts a deterministic small subset of the full DBpedia snapshot
for use as the committed `test/data/dbpedia-mini/` regression fixture.
Unlike TPC-H or LDBC, DBpedia has no scale-factor generator — it is a
single real-world snapshot — so we build the small fixture by taking
a fixed slice of the existing source.

## Usage

```bash
python3 scripts/preprocessors/dbpedia-preprocessors/dbpedia-subset.py \
    --src /source-data/dbpedia \
    --dst test/data/dbpedia-mini \
    --seed-edges 500
```

The output is fully deterministic given the same `--seed-edges` count
and the same source files.

## Strategy

1. Pick a *seed slice*: the first N rows of a small readable edge file
   (default `edges_nationality_797.csv`, the smallest currently-readable
   type at 112 K rows).
2. Collect every endpoint id from the seed into V.
3. For each of the 32 DBpedia edge files, keep only rows where both
   endpoints lie in V (set intersection).  Edge files that cannot be
   read on the current host are silently skipped — the fixture covers
   whatever is locally available.
4. Stream the full 16 GB `nodes.json` once and emit only the lines
   whose `properties.id` ∈ V.  The scan exits early once every id has
   been found.

## Fixture committed at `test/data/dbpedia-mini/`

Produced with `--seed-edges 500` on a host where the following
five edge files are locally readable (the other 27 return I/O Error):

| Source edge file                          | Rows in V × V |
|-------------------------------------------|---------------|
| `edges_nationality_797.csv`               | 500           |
| `edges_country_8183.csv`                  | 3             |
| `edges_birthPlace_2950.csv`               | 84            |
| `edges_genre_3794.csv`                    | 0 (dropped)   |
| `edges_22-rdf-syntax-ns#type_6803.csv`    | 0 (dropped)   |

Result: 576 nodes, 587 edges across 3 edge types, ~1.3 MB on disk.
The node subset preserves the schemaless property mix of real
DBpedia entries (abstract, isbn, wikiPageID, foaf:name, …) so the
bulkload loader's property-schema handling is exercised even on the
tiny graph.

## Re-running

The script is idempotent against a fixed snapshot.  If `/source-data/`
gets re-synced (more edge files become readable, or fewer), re-running
will pick up additional types automatically.  Commit the result with
the row counts in `test/bulkload/datasets.json` updated to match.
