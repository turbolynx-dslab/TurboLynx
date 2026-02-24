# Quickstart

This guide walks you through loading a dataset and running your first Cypher query.

## Prerequisites

Build TurboLynx first: see [Installation](../../installation/overview.md).

## Step 1 — Load a Dataset

```bash
cd build
./tools/bulkload --workspace /path/to/db --data /path/to/dataset
```

- `--workspace` — directory where the database is stored (`catalog.bin` + data files)
- `--data` — directory containing source CSV or JSON files

For LDBC datasets, use the helper script:

```bash
bash scripts/bulkload/run-ldbc-bulkload.sh /path/to/db /path/to/dataset
```

A sample LDBC SF1 dataset is available [here](https://drive.google.com/file/d/1PqXw_Fdp9CDVwbUqTQy0ET--mgakGmOA/view?usp=drive_link).

## Step 2 — Build Statistics (first time only)

Statistics are required for the ORCA cost-based optimizer to generate optimal query plans.

```bash
./tools/client --workspace /path/to/db
TurboLynx >> analyze
TurboLynx >> :exit
```

## Step 3 — Run a Query

```bash
./tools/client --workspace /path/to/db
TurboLynx >> MATCH (n:Person) RETURN n.firstName, n.lastName LIMIT 10;
TurboLynx >> :exit
```

## Run a Single Query Non-Interactively

```bash
./tools/client --workspace /path/to/db \
               --query "MATCH (n:Person) RETURN n.firstName LIMIT 5;"
```

## Example: Hop Query

```cypher
MATCH (a:Person)-[:KNOWS]->(b:Person)
WHERE a.firstName = 'Alice'
RETURN b.firstName, b.lastName
LIMIT 20;
```

---

## Next Steps

- [Dataset format specification](../data-import/formats.md)
- [Supported Cypher syntax](../cypher/overview.md)
