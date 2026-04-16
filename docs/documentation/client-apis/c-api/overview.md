# C API

TurboLynx exposes a C-compatible interface for embedding directly into native applications. Include `turbolynx.h` and link against `libturbolynx.so` on Linux or `libturbolynx.dylib` on macOS. See the [installation guide](../../../installation/overview.md?environment=capi) for platform-specific build commands.

```c
#include "main/capi/turbolynx.h"
```

---

## Connection

### `turbolynx_connect`

```c
int64_t turbolynx_connect(const char *dbname);
```

Open a database in read-write mode and return a connection ID (`>= 0`). Returns `-1` on failure. Acquires an exclusive writer lock — only one writer may be open at a time.

### `turbolynx_connect_readonly`

```c
int64_t turbolynx_connect_readonly(const char *dbname);
```

Open an existing database in read-only mode. Multiple readers may coexist. Fails if a writer holds the exclusive lock.

### `turbolynx_connect_with_client_context`

```c
int64_t turbolynx_connect_with_client_context(void *client_context);
```

Open a connection that reuses an existing `ClientContext` owned by the caller (advanced embedding). The caller is responsible for keeping the context alive for the lifetime of the returned connection. Returns a connection ID or `-1` on failure.

### `turbolynx_disconnect`

```c
void turbolynx_disconnect(int64_t conn_id);
```

Close the connection and release all associated resources and locks.

### `turbolynx_reopen`

```c
int turbolynx_reopen(int64_t conn_id);
```

Check whether the catalog has changed since this connection was opened. Returns `1` if the catalog version changed (reconnect recommended), `0` if up-to-date, `-1` on error.

### `turbolynx_is_connected`

```c
turbolynx_conn_state turbolynx_is_connected(int64_t conn_id);
```

Returns `TURBOLYNX_CONNECTED` or `TURBOLYNX_NOT_CONNECTED`.

### `turbolynx_get_last_error`

```c
turbolynx_error_code turbolynx_get_last_error(char **errmsg);
```

Retrieve the error code and message from the most recent failed call.

### `turbolynx_get_version`

```c
turbolynx_version turbolynx_get_version();
```

Return a version string, e.g. `"1.0.0"`.

---

## Metadata

### `turbolynx_get_metadata_from_catalog`

```c
turbolynx_num_metadata turbolynx_get_metadata_from_catalog(
    int64_t conn_id,
    turbolynx_label_name label,
    bool like_flag,
    bool filter_flag,
    turbolynx_metadata **metadata
);
```

List vertex labels and edge types registered in the catalog. Pass `NULL` for `label` to retrieve all entries. Returns the count; populate `*metadata` as a linked list.

### `turbolynx_close_metadata`

```c
turbolynx_state turbolynx_close_metadata(turbolynx_metadata *metadata);
```

Free the metadata linked list returned by `turbolynx_get_metadata_from_catalog`.

### `turbolynx_get_property_from_catalog`

```c
turbolynx_num_properties turbolynx_get_property_from_catalog(
    int64_t conn_id,
    turbolynx_label_name label,
    turbolynx_metadata_type type,
    turbolynx_property **property
);
```

Retrieve the property schema (names and types) for a given label. `type` is `TURBOLYNX_NODE` or `TURBOLYNX_EDGE`.

### `turbolynx_close_property`

```c
turbolynx_state turbolynx_close_property(turbolynx_property *property);
```

Free the property linked list.

---

## Query Execution

### `turbolynx_prepare`

```c
turbolynx_prepared_statement* turbolynx_prepare(int64_t conn_id, turbolynx_query query);
```

Parse and plan a Cypher query string. Returns a prepared statement, or `NULL` on parse error.

### `turbolynx_close_prepared_statement`

```c
turbolynx_state turbolynx_close_prepared_statement(turbolynx_prepared_statement *stmt);
```

Free a prepared statement.

### `turbolynx_execute`

```c
turbolynx_num_rows turbolynx_execute(
    int64_t conn_id,
    turbolynx_prepared_statement *stmt,
    turbolynx_resultset_wrapper **result
);
```

Execute the prepared statement. Returns the total number of result rows; populates `*result`.

### `turbolynx_close_resultset`

```c
turbolynx_state turbolynx_close_resultset(turbolynx_resultset_wrapper *result);
```

Free the result set.

---

## Result Iteration

### `turbolynx_fetch_next`

```c
turbolynx_fetch_state turbolynx_fetch_next(turbolynx_resultset_wrapper *result);
```

Advance the cursor to the next row. Returns:

| Value | Meaning |
|-------|---------|
| `TURBOLYNX_MORE_RESULT` (`1`) | Row available — call getters |
| `TURBOLYNX_END_OF_RESULT` (`0`) | No more rows |
| `TURBOLYNX_ERROR_RESULT` (`-1`) | Error |

### Getters

After a successful `turbolynx_fetch_next`, read column values by zero-based `col_idx`:

| Function | Return type |
|----------|-------------|
| `turbolynx_get_bool(result, col_idx)` | `bool` |
| `turbolynx_get_int8(result, col_idx)` | `int8_t` |
| `turbolynx_get_int16(result, col_idx)` | `int16_t` |
| `turbolynx_get_int32(result, col_idx)` | `int32_t` |
| `turbolynx_get_int64(result, col_idx)` | `int64_t` |
| `turbolynx_get_uint8(result, col_idx)` | `uint8_t` |
| `turbolynx_get_uint16(result, col_idx)` | `uint16_t` |
| `turbolynx_get_uint32(result, col_idx)` | `uint32_t` |
| `turbolynx_get_uint64(result, col_idx)` | `uint64_t` |
| `turbolynx_get_float(result, col_idx)` | `float` |
| `turbolynx_get_double(result, col_idx)` | `double` |
| `turbolynx_get_date(result, col_idx)` | `turbolynx_date` |
| `turbolynx_get_time(result, col_idx)` | `turbolynx_time` |
| `turbolynx_get_timestamp(result, col_idx)` | `turbolynx_timestamp` |
| `turbolynx_get_varchar(result, col_idx)` | `turbolynx_string` |
| `turbolynx_get_decimal(result, col_idx)` | `turbolynx_decimal` |
| `turbolynx_get_id(result, col_idx)` | `uint64_t` |

---

## Parameter Binding

Bind typed values to `?` placeholders in a prepared statement before calling `turbolynx_execute`. Parameters are **1-indexed**.

All bind functions take the same shape — a prepared statement pointer, a 1-based parameter index, and a typed value:

```c
turbolynx_state turbolynx_bind_boolean  (turbolynx_prepared_statement *stmt, idx_t param_idx, bool val);
turbolynx_state turbolynx_bind_int8     (turbolynx_prepared_statement *stmt, idx_t param_idx, int8_t val);
turbolynx_state turbolynx_bind_int16    (turbolynx_prepared_statement *stmt, idx_t param_idx, int16_t val);
turbolynx_state turbolynx_bind_int32    (turbolynx_prepared_statement *stmt, idx_t param_idx, int32_t val);
turbolynx_state turbolynx_bind_int64    (turbolynx_prepared_statement *stmt, idx_t param_idx, int64_t val);
turbolynx_state turbolynx_bind_uint8    (turbolynx_prepared_statement *stmt, idx_t param_idx, uint8_t val);
turbolynx_state turbolynx_bind_uint16   (turbolynx_prepared_statement *stmt, idx_t param_idx, uint16_t val);
turbolynx_state turbolynx_bind_uint32   (turbolynx_prepared_statement *stmt, idx_t param_idx, uint32_t val);
turbolynx_state turbolynx_bind_uint64   (turbolynx_prepared_statement *stmt, idx_t param_idx, uint64_t val);
turbolynx_state turbolynx_bind_hugeint  (turbolynx_prepared_statement *stmt, idx_t param_idx, turbolynx_hugeint val);
turbolynx_state turbolynx_bind_float    (turbolynx_prepared_statement *stmt, idx_t param_idx, float val);
turbolynx_state turbolynx_bind_double   (turbolynx_prepared_statement *stmt, idx_t param_idx, double val);
turbolynx_state turbolynx_bind_varchar  (turbolynx_prepared_statement *stmt, idx_t param_idx, const char *val);
turbolynx_state turbolynx_bind_varchar_length(turbolynx_prepared_statement *stmt, idx_t param_idx,
                                               const char *val, idx_t length);
turbolynx_state turbolynx_bind_date     (turbolynx_prepared_statement *stmt, idx_t param_idx, turbolynx_date val);
turbolynx_state turbolynx_bind_date_string(turbolynx_prepared_statement *stmt, idx_t param_idx, const char *val);
turbolynx_state turbolynx_bind_time     (turbolynx_prepared_statement *stmt, idx_t param_idx, turbolynx_time val);
turbolynx_state turbolynx_bind_timestamp(turbolynx_prepared_statement *stmt, idx_t param_idx, turbolynx_timestamp val);
turbolynx_state turbolynx_bind_decimal  (turbolynx_prepared_statement *stmt, idx_t param_idx, turbolynx_decimal val);
turbolynx_state turbolynx_bind_value    (turbolynx_prepared_statement *stmt, idx_t param_idx, turbolynx_value val);
turbolynx_state turbolynx_bind_null     (turbolynx_prepared_statement *stmt, idx_t param_idx);
```

**Date binding example** — days since 1970-01-01:
```c
turbolynx_date d = {14};  // 1970-01-15
turbolynx_bind_date(stmt, 1, d);
// or use the string form:
turbolynx_bind_date_string(stmt, 1, "1970-01-15");
```

**Decimal binding example:**
```c
turbolynx_hugeint hi = {100010, 0};
turbolynx_decimal dec = {6, 2, hi};  // represents 1000.10
turbolynx_bind_decimal(stmt, 1, dec);
```

---

## Types

### Date / Time / Timestamp

```c
typedef struct { int32_t days;   } turbolynx_date;       // days since 1970-01-01
typedef struct { int64_t micros; } turbolynx_time;       // microseconds since 00:00:00
typedef struct { int64_t micros; } turbolynx_timestamp;  // microseconds since 1970-01-01
```

### String

```c
typedef struct {
    char   *data;
    idx_t   size;
} turbolynx_string;
```

The `data` pointer is valid until `turbolynx_close_resultset` is called.

### Decimal

```c
typedef struct {
    uint8_t          width;
    uint8_t          scale;
    turbolynx_hugeint value;
} turbolynx_decimal;
```

Use `turbolynx_decimal_to_string(val)` to convert to a human-readable string.

---

## Error Codes

| Constant | Value | Meaning |
|----------|-------|---------|
| `TURBOLYNX_NO_ERROR` | 0 | Success |
| `TURBOLYNX_ERROR_CONNECTION_FAILED` | -1 | Could not open database |
| `TURBOLYNX_ERROR_INVALID_STATEMENT` | -2 | Parse or plan error |
| `TURBOLYNX_ERROR_INVALID_PARAMETER_INDEX` | -3 | Bind index out of range |
| `TURBOLYNX_ERROR_UNSUPPORTED_OPERATION` | -4 | Operation not supported |
| `TURBOLYNX_ERROR_INVALID_METADATA` | -5 | Bad metadata handle |
| `TURBOLYNX_ERROR_INVALID_LABEL` | -6 | Label not found in catalog |
| `TURBOLYNX_ERROR_INVALID_PROPERTY` | -7 | Property not found |
| `TURBOLYNX_ERROR_INVALID_RESULT_SET` | -12 | Bad result set handle |
| `TURBOLYNX_ERROR_INVALID_COLUMN_INDEX` | -13 | Column index out of range |
| `TURBOLYNX_ERROR_INVALID_COLUMN_TYPE` | -14 | Type mismatch on getter |
| `TURBOLYNX_ERROR_INVALID_CURSOR` | -15 | fetch_next not called |

---

## Example

```c
#include "main/capi/turbolynx.h"
#include <stdio.h>

int main(void) {
    // Open database
    int64_t conn = turbolynx_connect("/path/to/db");
    if (conn < 0) { fprintf(stderr, "connect failed\n"); return 1; }

    // Prepare query
    turbolynx_prepared_statement *stmt =
        turbolynx_prepare(conn, "MATCH (n:Person) RETURN n.firstName, n.age LIMIT 5;");

    // Execute
    turbolynx_resultset_wrapper *result = NULL;
    turbolynx_num_rows nrows = turbolynx_execute(conn, stmt, &result);
    printf("%zu rows\n", nrows);

    // Iterate
    while (turbolynx_fetch_next(result) == TURBOLYNX_MORE_RESULT) {
        turbolynx_string name = turbolynx_get_varchar(result, 0);
        int32_t age           = turbolynx_get_int32(result, 1);
        printf("%.*s — %d\n", (int)name.size, name.data, age);
    }

    // Clean up
    turbolynx_close_resultset(result);
    turbolynx_close_prepared_statement(stmt);
    turbolynx_disconnect(conn);
    return 0;
}
```

Compile:

```bash
gcc example.c -I/path/to/include -L/path/to/lib -lturbolynx -o example
```
