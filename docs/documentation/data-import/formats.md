# Data Import — File Formats

TurboLynx loads datasets via the `turbolynx import` command. Two file formats are supported: **CSV** and **JSON**.

---

## CSV

### Delimiter

Fields are separated by a **pipe character (`|`)**, not a comma.

```
id:ID(Person)|firstName:STRING|lastName:STRING|age:INT
1|Alice|Smith|30
2|Bob|Jones|25
```

### Header Row

The first row is always a header. Each column has the form:

```
columnName:TYPE
```

The header annotation drives schema inference — there is no separate schema file.

---

### ID Column Annotations

Special column annotations identify vertex IDs and edge endpoints.

#### Vertex ID — `:ID(Label)`

```
:ID(Person)|name:STRING|age:INT
1|Alice|30
2|Bob|25
```

- The label inside `(...)` is the vertex type name used for cross-referencing edge files.
- The ID value must be a non-negative integer (`UBIGINT` internally).
- Each vertex type has its own ID namespace; IDs only need to be unique within a type.

#### Compound (multi-column) vertex ID — `:ID_1(Label)` / `:ID_2(Label)`

When a vertex is identified by two columns, use `_1` and `_2` suffixes:

```
:ID_1(Order)|:ID_2(Order)|amount:DECIMAL(10,2)
100|2024|99.50
```

#### Edge source / destination — `:START_ID(Label)` / `:END_ID(Label)`

```
:START_ID(Person)|:END_ID(Person)|since:INT
1|2|2020
```

- Values must match IDs declared in the corresponding vertex file.

---

### Property Column Types

| CSV type annotation | Internal type | Notes |
|---|---|---|
| `STRING` | VARCHAR | UTF-8 text |
| `STRING[]` | VARCHAR | Multi-value stored as plain text |
| `INT` | INTEGER | 32-bit signed integer |
| `INTEGER` | INTEGER | Alias for `INT` |
| `LONG` | BIGINT | 64-bit signed integer |
| `BIGINT` | BIGINT | Alias for `LONG` |
| `ULONG` | UBIGINT | 64-bit unsigned integer |
| `UBIGINT` | UBIGINT | Alias for `ULONG` |
| `FLOAT` | FLOAT | 32-bit IEEE 754 floating-point |
| `DOUBLE` | DOUBLE | 64-bit IEEE 754 floating-point |
| `BOOLEAN` | BOOLEAN | `true` / `false` (JSON only; not yet supported in CSV) |
| `DATE` | DATE | Calendar date — see [Date format](#date-format) |
| `DATE_EPOCHMS` | DATE | Milliseconds since Unix epoch — see [Epoch milliseconds](#epoch-milliseconds) |
| `DECIMAL(p,s)` | DECIMAL | Fixed-point — see [Decimal format](#decimal-format) |

---

### Date Format

Type annotation: `DATE`

Accepted input format: **ISO 8601 date**

```
YYYY-MM-DD
```

Examples:

```
createdAt:DATE
2024-03-15
1999-01-01
```

---

### Epoch Milliseconds

Type annotation: `DATE_EPOCHMS`

The value is an integer representing milliseconds since the Unix epoch (1970-01-01 00:00:00 UTC).
The parser divides the value by 1000 to obtain a Unix timestamp in seconds, then converts to a calendar date.

```
createdAt:DATE_EPOCHMS
1710460800000
```

> **Note:** Sub-second precision is truncated when converting to a date.

---

### Timestamp Format

TurboLynx uses the `TIMESTAMP` type internally (microsecond resolution, stored as `int64_t`).
When a column is declared `DATE_EPOCHMS`, the raw integer milliseconds value is accepted.

For string-formatted timestamps (used in queries and future CSV extensions), the parser accepts **ISO 8601** with the following rules:

```
YYYY-MM-DD[T| ]HH:MM:SS[.mmm][Z | ±HH[:MM]]
```

| Component | Description |
|---|---|
| `YYYY-MM-DD` | Date part (required) |
| `T` or ` ` | Separator between date and time (either is accepted) |
| `HH:MM:SS` | Time part in 24-hour clock |
| `.mmm` | Optional milliseconds (1–3 digits) |
| `Z` | Optional UTC suffix |
| `+HH:MM` / `-HH:MM` | Optional UTC offset; offsets are subtracted to normalize to UTC |

Examples of valid timestamp strings:

```
2024-03-15 10:30:00
2024-03-15T10:30:00
2024-03-15T10:30:00.123
2024-03-15T10:30:00Z
2024-03-15T10:30:00+09:00
2024-03-15T10:30:00-05:30
```

A bare date (`2024-03-15`) is also valid and is interpreted as midnight UTC.

---

### Decimal Format

Type annotation: `DECIMAL(precision, scale)`

- `precision` — total number of significant digits
- `scale` — number of digits to the right of the decimal point

```
price:DECIMAL(10,2)
12345.67
99.00
-0.50
```

Both `.`-separated and integer-only inputs are accepted.
The value is stored as a scaled integer (e.g., `12345.67` with scale 2 is stored as `1234567`).

---

### Null Values

An empty field is treated as NULL:

```
name:STRING|age:INT|score:DOUBLE
Alice|30|
Bob||7.5
```

- `Alice` row: `score` is NULL
- `Bob` row: `age` is NULL

---

### Edge Files — Forward and Backward

TurboLynx stores two adjacency lists per edge type: one for forward traversal (start → end) and one for backward traversal (end → start).
Both files must have the same property columns.

**Forward file** (`:START_ID` first):

```
:START_ID(Person)|:END_ID(Person)|since:INT
1|2|2020
1|3|2021
```

**Backward file** (`:END_ID` first, rows sorted by the first column):

```
:END_ID(Person)|:START_ID(Person)|since:INT
2|1|2020
3|1|2021
```

The backward file is the same data with the ID columns swapped and the rows re-sorted by the new first column (END_ID).

> **Convention:** Name backward files with a `.backward` suffix, e.g., `knows.csv` → `knows.csv.backward`.

---

## JSON

TurboLynx parses graph JSON files with [simdjson](https://github.com/simdjson/simdjson).

### Top-level structure

A JSON file must be a single object with either a `"vertices"` key (for vertex files) or an `"edges"` key (for edge files), whose value is an array of objects.

**Vertex file:**

```json
{
  "vertices": [
    { "id": 1, "firstName": "Alice", "lastName": "Smith", "age": 30 },
    { "id": 2, "firstName": "Bob",   "lastName": "Jones", "age": 25 }
  ]
}
```

**Edge file:**

```json
{
  "edges": [
    { "src": 1, "dst": 2, "since": 2020 }
  ]
}
```

### Supported JSON value types

| JSON type | Mapped to |
|---|---|
| `boolean` | BOOLEAN |
| `integer` | INTEGER / BIGINT / UBIGINT |
| `number` (float) | FLOAT / DOUBLE |
| `string` | VARCHAR |

> **Note:** `DECIMAL` is not supported in the JSON path.

### Parser flags

The JSON reader is permissive: it allows `Inf`/`NaN` values and trailing commas.

---

## Directory Layout

Place all vertex and edge files in a flat directory.
The `turbolynx import` command scans the directory and infers file roles from the header annotations.

```
dataset/
├── person.csv               ← vertex file  (:ID annotation present)
├── comment.csv              ← vertex file
├── person_knows_person.csv  ← edge file    (:START_ID / :END_ID present)
├── person_knows_person.csv.backward  ← backward edge file
└── ...
```

### File-type detection

| Header contains | Interpreted as |
|---|---|
| `:ID(...)` | Vertex file |
| `:START_ID(...)` and `:END_ID(...)` | Edge file (forward) |
| `:END_ID(...)` appears first | Edge file (backward) |

---

## Running Import

For full CLI usage, options, and examples, see the [Import Tool](import-tool.md) page.

---

## Complete Example

### Vertex file — `person.csv`

```
:ID(Person)|firstName:STRING|lastName:STRING|age:INT|score:DOUBLE|joined:DATE
1|Alice|Smith|30|9.5|2022-01-15
2|Bob|Jones|25||2023-06-01
```

### Edge file — `knows.csv`

```
:START_ID(Person)|:END_ID(Person)|since:INT|weight:DECIMAL(5,2)
1|2|2020|1.50
```

### Edge file — `knows.csv.backward`

```
:END_ID(Person)|:START_ID(Person)|since:INT|weight:DECIMAL(5,2)
2|1|2020|1.50
```
