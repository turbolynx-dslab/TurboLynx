# TurboLynx CLI — Overview

The `turbolynx` binary is the all-in-one command-line interface for TurboLynx.
It exposes two subcommands:

| Subcommand | Description |
|---|---|
| `turbolynx shell` | Interactive Cypher query shell (default) |
| `turbolynx import` | Bulk-load graph data from CSV files |

Build instructions live in the [installation guide](../../../installation/overview.md?environment=cli). Native binaries are platform-specific: Linux fast-path builds produce `./build/tools/turbolynx`, while the portable Linux/macOS build produces `./build-portable/tools/turbolynx`.

The native shell can execute both read and write Cypher queries. `CREATE`, `SET`, `DELETE`, `DETACH DELETE`, and basic node `MERGE` go through the same mutation path used by the C API and Python API.

---

## Starting the Shell

```bash
turbolynx shell --workspace /path/to/db
```

If no subcommand is given, `shell` is assumed:

```bash
turbolynx --workspace /path/to/db
```

The shell opens an interactive REPL:

```
TurboLynx shell — type '.help' for commands, ':exit' to quit
TurboLynx >>
```

---

## Shell Options

| Flag | Short | Description |
|---|---|---|
| `--workspace <path>` | `-w` | Path to the database directory (required) |
| `--query <string>` | `-q` | Execute a single query and exit (non-interactive) |
| `--mode <name>` | `-m` | Set output format at startup (see [Output Formats](output-formats.md)) |
| `--iterations <n>` | `-i` | Repeat each query N times (benchmarking) |
| `--warmup` | | Discard the first iteration from timing statistics |
| `--profile` | | Enable query profiling |
| `--explain` | | Print the selected physical plan (no execution) |
| `--compile-only` | `-c` | Parse and plan without executing |
| `--standalone` | `-S` | Accepted for compatibility; currently has no effect in the native CLI |
| `--log-level <level>` | `-L` | Logging verbosity (`trace`, `debug`, `info`, `warn`, `error`) |

> **Running a file of queries.** There is no `--query-file` flag yet. To execute a script of Cypher statements, start the shell and use the [`.read <file>`](dot-commands.md) dot command, which runs each statement in the file in order.
>
> **Toggling profile / explain at runtime.** `--profile` can be turned on and off inside an interactive session with [`.profile on`](dot-commands.md) / `.profile off`. Explain mode currently has no dot-command equivalent — to switch in and out of explain, restart the shell with or without `--explain`.

### Join Order Options

| Flag | Description |
|---|---|
| `--join-order-optimizer <type>` | `query` · `greedy` · `exhaustive` · `exhaustive2` · `gem` |
| `--index-join-only` | Force index nested-loop join |
| `--hash-join-only` | Force hash join |
| `--merge-join-only` | Force merge join |
| `--disable-merge-join` | Disable merge join |
| `--disable-index-join` | Disable index join |
| `--debug-orca` | Print ORCA optimizer debug output |

---

## Non-Interactive Mode

Run a single query and write results to stdout:

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

---

## Entering Queries

- Cypher queries must end with a **semicolon** (`;`).
- Multi-line input is supported — the shell continues reading until it finds `;`.
- Dot commands (`.mode`, `.tables`, etc.) do **not** require a semicolon.

```
TurboLynx >> MATCH (n:Person)
TurboLynx -> RETURN n.firstName
TurboLynx -> LIMIT 10;
```

---

## Keyboard Shortcuts

| Key | Action |
|---|---|
| `Tab` | Autocomplete dot commands, Cypher keywords, labels, and edge types |
| `Ctrl+D` | Exit the shell (EOF) |
| `Ctrl+L` | Clear the screen |
| `↑` / `↓` | Navigate command history |

---

## Exiting

```
TurboLynx >> :exit
TurboLynx >> .exit
TurboLynx >> .quit
```

Or press `Ctrl+D`.

---

## Import Subcommand

See [Data Import](../../data-import/formats.md) for full import documentation.

```bash
turbolynx import \
    --workspace /path/to/db \
    --nodes Person dynamic/Person.csv \
    --relationships KNOWS dynamic/Person_knows_Person.csv
```

| Flag | Description |
|---|---|
| `--workspace <path>` | Output database directory |
| `--nodes <label> <file>` | Vertex CSV file (repeatable) |
| `--relationships <type> <file>` | Edge CSV file (repeatable) |
| `--incremental <true\|false>` | Append edges to existing database |
| `--skip-histogram` | Skip histogram generation after load |
| `--log-level <level>` | Logging verbosity |
