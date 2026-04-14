# Relation API

The Relation API provides DuckDB-style lazy query chaining. Operations are recorded but not executed until a terminal method is called.

## Creating a Relation

### `conn.sql(query)`

```python
rel = conn.sql("MATCH (n:Person) RETURN n.firstName, n.age")
# No query executed yet — rel is lazy
```

---

## Chaining Methods

All chaining methods return a **new** Relation (immutable). The original is not modified.

### `rel.filter(expr)`

Add a WHERE condition.

```python
rel.filter("n.age > 30")
rel.filter("n.firstName = 'Marc'")

# Multiple filters (stacked with AND)
rel.filter("n.age > 20").filter("n.age < 50")
```

### `rel.project(*cols)`

Replace the RETURN columns.

```python
rel.project("n.firstName, n.age")
rel.project("n.firstName", "n.age")  # also accepts multiple args
```

### `rel.order(expr)` / `rel.sort(expr)`

Set or replace ORDER BY.

```python
rel.order("n.age DESC")
rel.order("n.firstName ASC, n.age DESC")
```

### `rel.limit(n)`

Set or replace LIMIT.

```python
rel.limit(10)
```

### `rel.skip(n)`

Set or replace SKIP (offset).

```python
rel.skip(5)          # skip first 5 rows
rel.skip(10).limit(5)  # rows 11-15
```

### `rel.distinct()`

Add DISTINCT to the RETURN clause.

```python
rel.distinct()
```

### `rel.union(other)` / `rel.union_distinct(other)`

Combine two Relations with UNION ALL or UNION.

```python
r1 = conn.sql("MATCH (n:Person) RETURN n.firstName")
r2 = conn.sql("MATCH (n:Company) RETURN n.name AS firstName")
combined = r1.union(r2)
```

### `rel.aggregate(aggr_exprs, group_by=None)`

Apply aggregation.

```python
rel.aggregate("count(n.firstName) AS cnt", "n.firstName")
rel.aggregate("avg(n.age) AS avg_age")
```

### `rel.count()`

Replace RETURN with a count expression.

```python
total = rel.count().fetchall()  # [(9892,)]
```

---

## Terminal Methods

These trigger query execution and return results.

### `rel.execute()`

Execute and return a `TurboLynxPyResult` object.

```python
result = rel.execute()
```

### `rel.fetchall()` / `rel.fetchone()` / `rel.fetchmany(n)`

Execute and fetch rows directly.

```python
rows = rel.limit(10).fetchall()
row = rel.limit(1).fetchone()
batch = rel.fetchmany(5)
```

### `rel.fetchdf()` / `rel.df()`

Execute and return a pandas DataFrame.

```python
df = (conn.sql("MATCH (n:Person) RETURN n.firstName, n.age")
      .filter("n.age > 30")
      .order("n.age DESC")
      .limit(10)
      .fetchdf())
```

### `rel.fetchnumpy()`

Execute and return a dict of numpy arrays.

### `rel.show(n=20)`

Print a formatted table to stdout, return `self` for further chaining.

```python
conn.sql("MATCH (n:Person) RETURN n.firstName, n.id").limit(5).show()
```

Output:

```
n.firstName | n.id
------------+-----
Marc        | 65
K.          | 94
Anson       | 96
Philibert   | 102
Maria       | 143
(5 rows)
```

---

## Properties

### `rel.query`

Return the built Cypher query string without executing.

```python
q = conn.sql("MATCH (n:Person) RETURN n.firstName").filter("n.age > 30").limit(5).query
print(q)
# MATCH (n:Person) WITH * WHERE n.age > 30 RETURN n.firstName LIMIT 5
```

### `rel.columns`

Return column names (executes a LIMIT 1 probe).

```python
conn.sql("MATCH (n:Person) RETURN n.firstName, n.id").columns
# ['n.firstName', 'n.id']
```

### `rel.types`

Return column types.

```python
conn.sql("MATCH (n:Person) RETURN n.firstName, n.id").types
# ['VARCHAR', 'UBIGINT']
```

### `rel.shape`

Return `(row_count, col_count)`.

---

## Python Protocols

```python
rel = conn.sql("MATCH (n:Person) RETURN n.firstName").limit(5)

# Iteration
for row in rel:
    print(row)

# Length
len(rel)  # 5

# repr (shows first 10 rows as table)
rel
```

---

## Full Example

```python
import turbolynx

conn = turbolynx.connect("/data/ldbc/sf1")

# Find the most common first names among people over 30
df = (conn.sql("MATCH (n:Person) RETURN n.firstName, n.birthday")
      .filter("n.birthday < date('1994-01-01')")
      .aggregate("count(n.firstName) AS cnt", "n.firstName")
      .order("cnt DESC")
      .limit(10)
      .fetchdf())

print(df)
conn.close()
```
