# Dot Commands

Dot commands control the shell's behavior. They start with a `.` and do **not** require a trailing semicolon.

```
TurboLynx >> .mode box
TurboLynx >> .tables
```

---

## Output Format

### `.mode <format>`

Set the result output format. See [Output Formats](output-formats.md) for details.

```
TurboLynx >> .mode box
Output mode: box

TurboLynx >> .mode csv
Output mode: csv
```

With no argument, prints the current mode.

### `.headers <on|off>`

Show or hide the header row in results. Default: `on`.

```
TurboLynx >> .headers off
```

### `.nullvalue <string>`

Set the display string for `NULL` values. Default: empty string.

```
TurboLynx >> .nullvalue NULL
TurboLynx >> .nullvalue N/A
```

### `.separator <col>`

Set the column separator used in `list` and `csv` modes. Default: `,`.

```
TurboLynx >> .separator |
TurboLynx >> .separator \t
```

### `.maxrows <n>`

Limit the number of rows displayed. `0` means unlimited (default).

```
TurboLynx >> .maxrows 100
```

### `.width <n>`

Set the minimum column display width in `table`, `box`, and `column` modes. `0` means automatic (default).

```
TurboLynx >> .width 20
```

---

## File Redirection

### `.output [file]`

Redirect all subsequent query results to a file. Call with no argument to revert to stdout.

```
TurboLynx >> .output results.csv
TurboLynx >> .mode csv
TurboLynx >> MATCH (n:Person) RETURN n.firstName;

TurboLynx >> .output
Output: stdout
```

### `.once <file>`

Redirect the **next** query result only to a file, then automatically revert to stdout.

```
TurboLynx >> .once snapshot.json
TurboLynx >> .mode json
TurboLynx >> MATCH (n:Person) RETURN n LIMIT 10;
```

### `.log [file]`

Append all executed queries and their timing to a log file. Call with no argument to stop logging.

```
TurboLynx >> .log queries.log
```

Log format:
```
-- MATCH (n:Person) RETURN count(*)
-- compile: 12.3 ms, execute: 45.6 ms
```

---

## Schema Inspection

### `.tables`

List all vertex labels and edge types in the current graph.

```
TurboLynx >> .tables
Vertex labels (4):
  Person
  Comment
  Post
  Forum

Edge types (3):
  KNOWS
  LIKES
  HAS_CREATOR
```

### `.schema <label|type>`

Show the property schema (column names and types) for a vertex label or edge type.

```
TurboLynx >> .schema Person
Vertex label: Person
  graphlets: 1
  columns:
    id            BIGINT
    firstName     VARCHAR
    lastName      VARCHAR
    birthday      DATE
    creationDate  TIMESTAMP
```

### `.indexes`

Show index information. *(Not yet implemented — stub for future use.)*

---

## Execution

### `.read <file>`

Execute Cypher queries and dot commands from a file. Queries must end with `;`; dot commands do not.

```
TurboLynx >> .read /path/to/script.cypher
```

Example script (`script.cypher`):
```cypher
.mode box
.timer on

MATCH (n:Person)
RETURN n.firstName, n.lastName
LIMIT 10;
```

### `.analyze`

Rebuild column statistics (histograms) for the cost-based optimizer. Run this after loading data or making significant schema changes.

```
TurboLynx >> .analyze
```

### `.timer <on|off>`

Toggle query timing display. Default: `on`.

```
TurboLynx >> .timer off
TurboLynx >> .timer on
Time: compile 12.1 ms, execute 34.5 ms, total 46.6 ms
```

### `.echo <on|off>`

Print each query to stdout before executing it. Useful when running scripts with `.read`. Default: `off`.

```
TurboLynx >> .echo on
```

### `.bail <on|off>`

Stop execution on the first error when running a script via `.read`. Default: `off`.

```
TurboLynx >> .bail on
```

---

## Shell

### `.shell <command>` / `.system <command>`

Execute an OS shell command.

```
TurboLynx >> .shell clear
TurboLynx >> .shell ls -lh /path/to/db
TurboLynx >> .system date
```

### `.print <text>`

Print literal text. Useful in scripts.

```
TurboLynx >> .print === Loading complete ===
=== Loading complete ===
```

### `.prompt <string>`

Change the shell prompt.

```
TurboLynx >> .prompt mydb
mydb >>
```

### `.show`

Display all current shell settings.

```
TurboLynx >> .show
Current settings:
  mode:      box
  headers:   on
  nullvalue: ""
  separator: ","
  maxrows:   unlimited
  width:     auto
  output:    stdout
  log:       off
  timer:     on
  echo:      off
  bail:      off
  prompt:    "TurboLynx"
  workspace: /path/to/db
```

### `.help`

Print the built-in command reference.

### `.exit` / `.quit` / `:exit`

Exit the shell.

---

## Quick Reference

| Command | Description |
|---|---|
| `.mode <fmt>` | Set output format |
| `.headers <on\|off>` | Toggle header row |
| `.nullvalue <str>` | NULL display string |
| `.separator <col>` | Column separator |
| `.maxrows <n>` | Limit output rows |
| `.width <n>` | Minimum column width |
| `.output [file]` | Redirect all output |
| `.once <file>` | Redirect next result only |
| `.log [file]` | Log queries to file |
| `.tables` | List labels and edge types |
| `.schema <name>` | Show property schema |
| `.read <file>` | Execute script file |
| `.analyze` | Rebuild statistics |
| `.timer <on\|off>` | Toggle timing |
| `.echo <on\|off>` | Echo queries |
| `.bail <on\|off>` | Stop on error |
| `.shell <cmd>` | Run OS command |
| `.print <text>` | Print text |
| `.prompt <str>` | Change prompt |
| `.show` | Show all settings |
| `.help` | Show help |
| `.exit` / `.quit` | Exit |
