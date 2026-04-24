# Python API

TurboLynx provides a native Python API built with pybind11. It follows the [DB-API 2.0](https://peps.python.org/pep-0249/) specification and adds DuckDB-style Relation chaining and graph-specific query builders.

## Installation

Native wheels are platform-specific. Build the wheel on the same host and
architecture that will import it. A Linux wheel cannot be reused on macOS.

From a TurboLynx checkout, the portable build works on both Linux and macOS:

```bash
python3 -m pip install pybind11 wheel
cmake -GNinja -DCMAKE_BUILD_TYPE=Release -DBUILD_PYTHON=ON \
      -DTURBOLYNX_PORTABLE_DISK_IO=ON -DENABLE_TCMALLOC=OFF \
      -DBUILD_UNITTESTS=OFF -DTBB_TEST=OFF \
      -B build-portable
cmake --build build-portable
tools/pythonpkg/scripts/build_wheel.sh build-portable
python3 -m pip install tools/pythonpkg/dist/turbolynx-*.whl
```

On Linux you can replace `build-portable` with `build` if you specifically
want the native AIO fast path. See the
[installation guide](../../../installation/overview.md?environment=python) for
the Linux-specific build flow.

## Quick Start

```python
import turbolynx

# Connect to a database
conn = turbolynx.connect("/path/to/workspace")

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
