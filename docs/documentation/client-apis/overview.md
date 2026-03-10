# Client APIs

TurboLynx exposes its functionality through two client interfaces:

| Client | Description |
|--------|-------------|
| [CLI](cli/overview.md) | Interactive Cypher shell and bulk-load tool (`turbolynx` binary) |
| [C API](c-api/overview.md) | Embeddable C interface (`libturbolynx.so`) for native application integration |

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
