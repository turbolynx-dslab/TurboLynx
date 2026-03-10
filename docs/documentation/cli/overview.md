# TurboLynx CLI — Overview

The `turbolynx` binary is the all-in-one command-line interface for TurboLynx.
It exposes two subcommands:

| Subcommand | Description |
|---|---|
| `turbolynx shell` | Interactive Cypher query shell (default) |
| `turbolynx import` | Bulk-load graph data from CSV files |

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

| Flag | Description |
|---|---|
| `--workspace <path>` | Path to the database directory (required) |
| `--query <string>` | Execute a single query and exit (non-interactive) |
| `--mode <name>` | Set output format at startup (see [Output Formats](output-formats.md)) |
| `--iterations <n>` | Repeat each query N times (benchmarking) |
| `--warmup` | Discard the first iteration from timing statistics |
| `--profile` | Enable query profiling |
| `--compile-only` | Parse and plan without executing |
| `--standalone` | Open without exclusive writer lock (read-only safe) |
| `--log-level <level>` | Logging verbosity (`trace`, `debug`, `info`, `warn`, `error`) |

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

See [Data Import](../data-import/formats.md) for full import documentation.

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
| `--standalone` | Do not acquire exclusive writer lock |
| `--log-level <level>` | Logging verbosity |
