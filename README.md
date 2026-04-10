<div align="center">
  <img src="docs/assets/logo.png" alt="TurboLynx" height="120">
</div>

# TurboLynx

Fast, scalable OLAP graph database in C++17.
Single-process embedded architecture — no daemon required.

## TurboLynx

TurboLynx is an analytical graph database designed for read-heavy workloads.
It provides a Cypher query interface backed by a cost-based optimizer (ORCA),
extent-based columnar storage, and embeds directly into the calling process
like DuckDB or SQLite — no separate server to manage.

For more information, see the [TurboLynx documentation](docs/).

## Installation

If you want to build TurboLynx, please see our [Installation](docs/installation/overview.md) page for instructions.

## Data Import

```bash
./tools/bulkload --workspace /path/to/db --data /path/to/dataset
```

Refer to our [Data Import](docs/documentation/data-import/formats.md) section for CSV and JSON format details.

## Cypher

```cypher
MATCH (a:Person)-[:KNOWS]->(b:Person)
WHERE a.firstName = 'Alice'
RETURN b.firstName, b.lastName LIMIT 10;
```

The documentation contains a [Cypher reference](docs/documentation/cypher/overview.md).

## Development

For development, TurboLynx requires CMake, Ninja, and GCC 11+.
All dependencies are bundled — no external libraries need to be installed.
Run `cmake -GNinja .. && ninja` in the build directory to compile.

Please refer to our [Building TurboLynx](docs/documentation/development/building-turbolynx/overview.md) guide.

## Testing

See [Testing](docs/documentation/development/testing.md).
