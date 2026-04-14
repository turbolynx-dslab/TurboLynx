# Client APIs

TurboLynx exposes its functionality through four client interfaces:

| Client | Description |
|--------|-------------|
| [Python API](python-api/overview.md) | DB-API 2.0 compliant Python interface with Relation chaining and graph traversal |
| [CLI](cli/overview.md) | Interactive Cypher shell and bulk-load tool (`turbolynx` binary) |
| [C API](c-api/overview.md) | Embeddable C interface (`libturbolynx.so`) for native application integration |
| [Node.js API](node-api/overview.md) | WASM-based Node.js bindings — read-only Cypher queries, no native build needed |

---

## Python API

The Python API provides a native pybind11 interface with:

- DB-API 2.0 compliance (connect, execute, fetch, transactions, exceptions)
- DuckDB-style Relation chaining (filter, project, order, limit, aggregate)
- Graph-specific query builders (node traversal, path queries, degree analysis)
- Schema inspection (labels, relationship types, property schemas)

See [Python API Overview](python-api/overview.md) to get started.

---

## CLI

The `turbolynx` binary provides:

- An interactive REPL for running Cypher queries
- A bulk-load subcommand for importing CSV datasets
- Dot commands for configuring output, inspecting schema, and scripting

See [CLI Overview](cli/overview.md) to get started.

---

## C API

`libturbolynx.so` exposes a C-compatible interface for embedding TurboLynx into native applications without a network round-trip.

See [C API Overview](c-api/overview.md) for the full function reference.

---

## Node.js API

The Node.js bindings wrap the TurboLynx WebAssembly runtime (no native build
step) and expose a small async API (`open`, `query`, `labels`, `schema`,
`close`) for read-only Cypher execution.

See [Node.js API Overview](node-api/overview.md) to get started.
