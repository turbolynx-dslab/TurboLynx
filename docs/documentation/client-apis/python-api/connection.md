# Connection & Query Execution

## Connecting

### `turbolynx.connect(database, read_only=False)`

Open a connection to a TurboLynx database.

```python
import turbolynx

conn = turbolynx.connect("/path/to/database")
conn_ro = turbolynx.connect("/path/to/database", read_only=True)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `database` | `str` | required | Path to the database directory |
| `read_only` | `bool` | `False` | Open in read-only mode (allows concurrent readers) |

### Context Manager

```python
with turbolynx.connect("/path/to/database") as conn:
    result = conn.execute("MATCH (n:Person) RETURN n.firstName LIMIT 3")
    print(result.fetchall())
# Connection closed automatically
```

### `conn.close()`

Close the connection and release resources.

### `conn.is_connected()`

Returns `True` if the connection is open.

---

## Query Execution

### `conn.execute(query, params=None)`

Execute a Cypher query and return a `TurboLynxPyResult`.

```python
# Simple query
result = conn.execute("MATCH (n:Person) RETURN n.firstName LIMIT 5")

# With named parameters
result = conn.execute(
    "MATCH (n:Person) WHERE n.firstName = $name RETURN n.id",
    {"name": "Marc"}
)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `query` | `str` | Cypher query string |
| `params` | `dict` or `None` | Named parameters (`$name` style) |

### `conn.executemany(query, params_list)`

Execute a query repeatedly with different parameter sets.

```python
conn.executemany(
    "CREATE (n:Person {name: $name, age: $age})",
    [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
)
```

---

## Result Object

`TurboLynxPyResult` provides multiple ways to consume query results.

### Fetching Rows

```python
result = conn.execute("MATCH (n:Person) RETURN n.firstName, n.id LIMIT 10")

# Single row
row = result.fetchone()        # ('Marc', 65) or None

# Multiple rows
rows = result.fetchmany(5)     # [('Marc', 65), ('K.', 94), ...]

# All rows
all_rows = result.fetchall()   # [('Marc', 65), ('K.', 94), ...]
```

### DataFrames

```python
# pandas DataFrame
df = result.fetchdf()

# Chunked DataFrames
chunks = result.fetch_df_chunk(vectors_per_chunk=10)

# numpy arrays
arrays = result.fetchnumpy()
```

### Metadata

```python
result.column_names()   # ['n.firstName', 'n.id']
result.column_types()   # ['VARCHAR', 'UBIGINT']
result.description      # DB-API 2.0 description
result.rowcount         # Total row count
```

### Iterator

```python
for row in result:
    print(row)
```

---

## Prepared Statements

### `conn.prepare(query)`

Create a reusable prepared statement with parameter placeholders.

```python
stmt = conn.prepare("MATCH (n:Person) WHERE n.firstName = $name RETURN n.id")

# Execute with dict
result1 = stmt.execute({"name": "Marc"})
print(result1.fetchall())

# Execute with list (positional)
result2 = stmt.execute(["John"])
print(result2.fetchall())

# Metadata
print(stmt.num_params)  # 1
print(stmt.query)       # original query string

stmt.close()
```

---

## Transaction Control

TurboLynx uses implicit transactions. These methods are provided for DB-API 2.0 compatibility.

```python
conn.begin()       # No-op (implicit transactions)
conn.commit()      # No-op
conn.rollback()    # Clears in-memory mutations (clear_delta)
```

---

## Database Operations

### `conn.checkpoint()`

Flush the DeltaStore to disk, persisting all in-memory mutations.

### `conn.clear_delta()`

Discard all in-memory mutations without persisting.

### `conn.set_max_threads(n)`

Set the maximum number of threads for parallel query execution.

```python
conn.set_max_threads(8)
```

---

## Query Control

### `conn.interrupt()`

Cancel a currently executing query from another thread.

```python
import threading

def cancel_after(conn, seconds):
    import time
    time.sleep(seconds)
    conn.interrupt()

# Start cancellation timer
t = threading.Thread(target=cancel_after, args=(conn, 5))
t.start()

try:
    result = conn.execute("MATCH (n:Person)-[*1..10]-(m) RETURN count(m)")
except RuntimeError:
    print("Query interrupted")
```

### `conn.query_progress()`

Returns the number of rows processed by the currently executing query, or `-1` if idle.

---

## Module Attributes

```python
turbolynx.__version__    # '0.0.1'
turbolynx.apilevel       # '2.0'
turbolynx.threadsafety   # 1
turbolynx.paramstyle     # 'named'
```

---

## Exceptions

TurboLynx follows the [DB-API 2.0 exception hierarchy](https://peps.python.org/pep-0249/#exceptions):

```
Exception
  └── Error
       └── DatabaseError
            ├── OperationalError
            ├── ProgrammingError
            ├── DataError
            ├── IntegrityError
            ├── InternalError
            └── NotSupportedError
```

```python
try:
    conn.execute("INVALID CYPHER")
except turbolynx.ProgrammingError as e:
    print(f"Syntax error: {e}")
```
