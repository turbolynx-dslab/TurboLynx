# Output Formats

Use `.mode <format>` (or `--mode <format>` on startup) to control how query results are displayed.

```
TurboLynx >> .mode box
Output mode: box
```

With no argument, `.mode` prints the current format.

---

## table *(default)*

ASCII table with dashes and pipes. Column widths auto-fit content.

```
TurboLynx >> .mode table
TurboLynx >> MATCH (n:Person) RETURN n.firstName, n.age LIMIT 3;
+-----------+-----+
| firstName | age |
+-----------+-----+
| Alice     | 30  |
| Bob       | 25  |
| Carol     | 42  |
+-----------+-----+
```

---

## box

Unicode box-drawing border (`┌─┬─┐` style). Visually cleaner than `table`.

```
TurboLynx >> .mode box
TurboLynx >> MATCH (n:Person) RETURN n.firstName, n.age LIMIT 3;
┌───────────┬─────┐
│ firstName │ age │
├───────────┼─────┤
│ Alice     │ 30  │
│ Bob       │ 25  │
│ Carol     │ 42  │
└───────────┴─────┘
```

---

## column

Whitespace-aligned columns, no borders. Suitable for terminal pipelines.

```
TurboLynx >> .mode column
firstName  age
---------  ---
Alice      30
Bob        25
Carol      42
```

---

## csv

RFC 4180 comma-separated values with a header row. Use `.separator` to change the delimiter.

```
TurboLynx >> .mode csv
firstName,age
Alice,30
Bob,25
Carol,42
```

Redirect to a file:

```
TurboLynx >> .output result.csv
TurboLynx >> MATCH (n:Person) RETURN n.firstName, n.age;
TurboLynx >> .output
```

---

## tabs

Tab-separated values (TSV). Equivalent to `.mode csv` + `.separator \t`.

```
TurboLynx >> .mode tabs
firstName	age
Alice	30
Bob	25
```

---

## list

Delimiter-separated values using `.separator` (default `|`). No quoting.

```
TurboLynx >> .mode list
TurboLynx >> .separator |
firstName|age
Alice|30
Bob|25
```

---

## json

Results as a JSON array of objects. Each row is one object; keys are column names.

```
TurboLynx >> .mode json
[{"firstName":"Alice","age":30},{"firstName":"Bob","age":25},{"firstName":"Carol","age":42}]
```

---

## jsonlines

Newline-delimited JSON (NDJSON). One JSON object per line — ideal for streaming pipelines.

```
TurboLynx >> .mode jsonlines
{"firstName":"Alice","age":30}
{"firstName":"Bob","age":25}
{"firstName":"Carol","age":42}
```

---

## line

Each column printed on its own line as `column = value`. Rows separated by a blank line. Useful for wide result sets.

```
TurboLynx >> .mode line
firstName = Alice
      age = 30

firstName = Bob
      age = 25
```

---

## markdown

GitHub-flavored Markdown table.

```
TurboLynx >> .mode markdown
| firstName | age |
|-----------|-----|
| Alice     | 30  |
| Bob       | 25  |
| Carol     | 42  |
```

---

## html

HTML `<table>` element with `<th>` headers and `<td>` cells.

```
TurboLynx >> .mode html
<table>
<tr><th>firstName</th><th>age</th></tr>
<tr><td>Alice</td><td>30</td></tr>
<tr><td>Bob</td><td>25</td></tr>
</table>
```

---

## latex

LaTeX `tabular` environment, ready to paste into a `.tex` document.

```
TurboLynx >> .mode latex
\begin{tabular}{ll}
\hline
firstName & age \\
\hline
Alice & 30 \\
Bob & 25 \\
\hline
\end{tabular}
```

---

## insert

Cypher-style `INSERT INTO label VALUES (...)` statements. Useful for generating load scripts.
Set the label name with `.mode insert <label>` or via the `insert_label` setting.

```
TurboLynx >> .mode insert Person
INSERT INTO Person VALUES('Alice',30);
INSERT INTO Person VALUES('Bob',25);
```

---

## trash

Discards all output. Useful for benchmarking query execution without I/O overhead.

```
TurboLynx >> .mode trash
TurboLynx >> MATCH (n:Person) RETURN n;
-- (no output)
Time: compile 2.1 ms, execute 134.5 ms, total 136.6 ms
```

---

## Quick Reference

| Mode | Description |
|------|-------------|
| `table` | ASCII table (default) |
| `box` | Unicode box-drawing table |
| `column` | Space-aligned columns, no borders |
| `csv` | Comma-separated values |
| `tabs` | Tab-separated values (TSV) |
| `list` | Delimiter-separated (configurable via `.separator`) |
| `json` | JSON array of objects |
| `jsonlines` | Newline-delimited JSON (NDJSON) |
| `line` | One column per line |
| `markdown` | Markdown table |
| `html` | HTML `<table>` |
| `latex` | LaTeX `tabular` |
| `insert` | INSERT INTO statements |
| `trash` | Discard output (benchmarking) |
