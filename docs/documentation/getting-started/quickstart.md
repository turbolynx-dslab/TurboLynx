# Quickstart

This guide walks you through loading a dataset and running your first Cypher query.

## Prerequisites

Build TurboLynx first: see [Building TurboLynx](../development/building-turbolynx/overview.md).

## Step 1 — Import a Dataset

```bash
turbolynx import \
    --workspace /path/to/db \
    --nodes Person   data/Person.csv \
    --nodes Comment  data/Comment.csv \
    --relationships KNOWS data/Person_knows_Person.csv
```

- `--workspace` — directory where the database is stored (`catalog.bin`, `store.db`, `.store_meta`)
- `--nodes <Label> <file>` — vertex CSV file (repeatable)
- `--relationships <TYPE> <file>` — edge CSV file (repeatable)

For CSV format details, see [Data Import](../data-import/formats.md).

A sample LDBC SF1 dataset is available [here](https://drive.google.com/file/d/1PqXw_Fdp9CDVwbUqTQy0ET--mgakGmOA/view?usp=drive_link).

## Step 2 — Open the Shell

```bash
turbolynx shell --workspace /path/to/db
```

```
TurboLynx shell — type '.help' for commands, ':exit' to quit
TurboLynx >>
```

## Step 3 — Run a Query

```
TurboLynx >> MATCH (n:Person) RETURN n.firstName, n.lastName LIMIT 10;
```

## Step 4 — Build Statistics (first time only)

Statistics are required for the ORCA cost-based optimizer to generate optimal plans. Run `.analyze` after loading data:

```
TurboLynx >> .analyze
```

## Run a Single Query Non-Interactively

```bash
turbolynx shell --workspace /path/to/db \
                --query "MATCH (n:Person) RETURN n.firstName LIMIT 5;"
```

Combine with `--mode` to control output format:

```bash
turbolynx shell --workspace /path/to/db \
                --mode csv \
                --query "MATCH (n:Person) RETURN n.firstName, n.lastName;"
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

- [CLI reference](../client-apis/cli/overview.md) — all shell options and dot commands
- [Data Import formats](../data-import/formats.md)
- [Supported Cypher syntax](../cypher/overview.md)
