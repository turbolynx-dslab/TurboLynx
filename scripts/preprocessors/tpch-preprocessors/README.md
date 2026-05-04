# TPC-H preprocessor

Converts standard TPC-H `.tbl` files (output of `dbgen` or DuckDB's
`tpch.dbgen()`) into the TurboLynx graph fixture format: vertex `.tbl`
files with typed headers, plus forward and backward edge `.tbl` files.

## Usage

The script expects 8 raw input files named `{table}.tbl.original` for
each TPC-H table (`customer`, `lineitem`, `nation`, `orders`, `part`,
`partsupp`, `region`, `supplier`) in the input directory. After
running, the same directory will hold the converted `.tbl` files plus
`.tbl.backward` companions, ready for `turbolynx import`.

```bash
bash scripts/preprocessors/tpch-preprocessors/tpch-preprocess.sh /path/to/tbl/dir
```

## Generating raw input from DuckDB (small SF, no dbgen install)

```bash
python3 -m pip install duckdb
mkdir -p /tmp/tpch-raw
python3 -c "
import duckdb
con = duckdb.connect()
con.execute('INSTALL tpch; LOAD tpch; CALL dbgen(sf=0.01);')
for tbl in ['customer','lineitem','nation','orders','part','partsupp','region','supplier']:
    con.execute(f\"COPY (SELECT * FROM {tbl}) TO '/tmp/tpch-raw/{tbl}.tbl.original' (FORMAT CSV, DELIMITER '|', HEADER FALSE, QUOTE '');\")
"
bash scripts/preprocessors/tpch-preprocessors/tpch-preprocess.sh /tmp/tpch-raw
```

The generated SF0.01 fixture committed to `test/data/tpch-mini/` was
produced with exactly the commands above. Re-running them yields
identical files (`dbgen` is deterministic at a given scale factor).

## Fixture committed at `test/data/tpch-mini/`

| Table    | Rows at SF0.01 |
|----------|---------------:|
| CUSTOMER | 1,500          |
| LINEITEM | 60,175         |
| NATION   | 25             |
| ORDERS   | 15,000         |
| PART     | 2,000          |
| REGION   | 5              |
| SUPPLIER | 100            |

## Schema

Vertex labels: `CUSTOMER`, `LINEITEM` (composite key `ID_1+ID_2`),
`NATION`, `ORDERS`, `PART`, `REGION`, `SUPPLIER`.

Edge labels: `customer_belongTo_nation`, `lineitem_composedBy_part`,
`lineitem_isPartOf_orders`, `lineitem_suppliedBy_supplier`,
`nation_isLocatedIn_region`, `orders_madeBy_customer`,
`supplier_belongTo_nation`, `partsupp` — each with a `.tbl.backward`
companion.

## Differences from the original `tpch-preprocess.sh`

- Skips the leading `sed 's/.$//'` step when the input does not have
  a trailing `|` (DuckDB's `COPY ... TO` does not append it; GNU dbgen
  does).
- Replaces GNU `sed -i` (incompatible with BSD sed on macOS) with
  portable temp-file rewrites, so the script runs on Linux and macOS.
