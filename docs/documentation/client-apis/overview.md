# Client APIs

TurboLynx exposes its functionality through five client interfaces. Native clients
(CLI, C API, Python) produce platform-specific artifacts, so build them on the
machine that will run them.

| Client | Description |
|--------|-------------|
| [Python API](python-api/overview.md) | DB-API 2.0 compliant Python interface with Relation chaining and graph traversal |
| [CLI](cli/overview.md) | Interactive Cypher shell and bulk-load tool (`turbolynx` binary) |
| [C API](c-api/overview.md) | Embeddable C interface (`libturbolynx.so` on Linux, `libturbolynx.dylib` on macOS) for native application integration |
| [Node.js API](node-api/overview.md) | WASM-based Node.js bindings — read-only Cypher queries, no native build needed |
| [MCP server](mcp/overview.md) | Model Context Protocol server — plug TurboLynx into Claude Desktop, Cursor, Codex, and other MCP agents |

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

The C API exposes a C-compatible interface for embedding TurboLynx into native applications without a network round-trip.

See [C API Overview](c-api/overview.md) for the full function reference.

---

## Node.js API

The Node.js bindings wrap the TurboLynx WebAssembly runtime (no native build
step) and expose a small async API (`open`, `query`, `labels`, `schema`,
`close`) for read-only Cypher execution.

See [Node.js API Overview](node-api/overview.md) to get started.

---

## MCP server

The Model Context Protocol server exposes a TurboLynx workspace to MCP-compatible
agents such as Claude Desktop, Cursor, and Codex. Read-only in v0
(`query_cypher`, `list_labels`, `describe_label`, `sample_label`), with a
`turbolynx://schema` resource for schema discovery.

See [MCP Server Overview](mcp/overview.md) for setup and Claude Desktop config.
