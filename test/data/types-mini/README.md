# types-mini

A small single-label graph fixture whose `:TypeRow` node carries one
column per loader-supported scalar type, plus a low-cardinality
`category` column for GROUP BY tests.

20 rows, byte-for-byte committed under `typerow.tbl`.

## Schema

| column          | type            | notes                                        |
|-----------------|-----------------|----------------------------------------------|
| `id`            | `ID(TypeRow)`   | primary key (UBIGINT internally)             |
| `t_int`         | `INT`           | INTEGER                                      |
| `t_long`        | `LONG`          | BIGINT                                       |
| `t_ulong`       | `ULONG`         | UBIGINT                                      |
| `t_float`       | `FLOAT`         | 32-bit float                                 |
| `t_double`      | `DOUBLE`        | 64-bit float                                 |
| `t_dec_small`   | `DECIMAL(5,2)`  | small decimal                                |
| `t_dec_default` | `DECIMAL(12,2)` | TPC-H standard width/scale                   |
| `t_dec_wide`    | `DECIMAL(18,4)` | wider scale, INT64 internal                  |
| `t_str`         | `STRING`        | VARCHAR                                      |
| `t_date`        | `DATE`          | day-precision date                           |
| `t_ts`          | `DATE_EPOCHMS`  | TIMESTAMP_MS (ms-since-epoch on the wire)    |
| `category`      | `STRING`        | 5 values (A–E) for GROUP BY                  |

## Loading

```bash
bash scripts/load-types-mini.sh build-portable /tmp/types-mini-ws
```

## Running tests

```bash
./build-portable/test/query/query_test "[types]" --types-path /tmp/types-mini-ws
```

## What the fixture is for

- Type-promotion edges in scalar arithmetic (`INT + DOUBLE`,
  `DECIMAL(5,2) * DECIMAL(12,2)`, …).
- CASE LCT across mixed types.
- Aggregates over each type with at least one grouping key.

Global aggregates (0 grouping keys) are deliberately not exercised —
tracked under issue #111.

## Loader-unsupported types

`BOOLEAN`, `TINYINT`, `SMALLINT`, `INTERVAL`, `LIST`, `STRUCT`, and
`MAP` are not currently accepted by the TBL importer (`STRING_TO_TYPE`
in `graph_simdcsv_parser.hpp`). When the importer learns them, extend
`typerow.tbl` and the test file together.
