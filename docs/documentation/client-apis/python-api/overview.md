# Python API

TurboLynx provides a native Python API built with pybind11. It follows the [DB-API 2.0](https://peps.python.org/pep-0249/) specification and adds DuckDB-style Relation chaining and graph-specific query builders.

## Installation

```bash
# From a pre-built wheel
pip install turbolynx-0.0.1-*.whl

# Or build from source (requires CMake build first)
cd turbograph-v3/build-lwtest && ninja
cd ../tools/pythonpkg
TURBOLYNX_BUILD_DIR=../../build-lwtest pip wheel . -w dist/
pip install dist/turbolynx-*.whl
```

## Quick Start

```python
import turbolynx

# Connect to a database
conn = turbolynx.connect("/path/to/database")

# Execute a Cypher query
result = conn.execute("MATCH (n:Person) RETURN n.firstName, n.id LIMIT 5")
for row in result.fetchall():
    print(row)

# Relation API — lazy chaining
df = (conn.sql("MATCH (n:Person) RETURN n.firstName, n.age")
      .filter("n.age > 30")
      .order("n.age DESC")
      .limit(10)
      .fetchdf())

# Graph API — node traversal
neighbors = conn.node("Person", firstName="Marc").neighbors("KNOWS").limit(5).fetchdf()

# Schema inspection
print(conn.labels())
print(conn.schema("Person"))

conn.close()
```

## API Sections

| Section | Description |
|---------|-------------|
| [Connection](connection.md) | Connecting, executing queries, transactions, prepared statements |
| [Relation API](relation.md) | DuckDB-style lazy query chaining (filter, project, order, limit, aggregate) |
| [Graph API](graph.md) | Node traversal, path queries, degree analysis |
| [Schema](schema.md) | Inspecting labels, relationship types, and property schemas |
