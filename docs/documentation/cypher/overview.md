# Cypher Query Language

TurboLynx uses a subset of the openCypher specification for graph queries.

## Supported Clauses

| Clause | Status | Notes |
|---|---|---|
| `MATCH` | ✅ | Pattern matching on vertices and edges |
| `WHERE` | ✅ | Filter predicates |
| `RETURN` | ✅ | Projection |
| `WITH` | ✅ | Pipeline results between clauses |
| `ORDER BY` | ✅ | Ascending / descending sort |
| `LIMIT` | ✅ | Limit result count |
| `SKIP` | ✅ | Skip N results |
| `CREATE` | ✅ | Create vertices and edges |
| `SET` | ✅ | Update properties |
| `DELETE` | ✅ | Delete vertices or edges |
| `OPTIONAL MATCH` | 🚧 | Partial support |
| `UNION` | 🚧 | Planned |

## Supported Functions

| Function | Description |
|---|---|
| `COUNT(*)` | Count all results |
| `COUNT(column)` | Count non-null values — `COUNT()` not supported |

> **Note:** `COUNT()` (no argument) is not supported. Use `COUNT(*)` or `COUNT(n)`.

## Join Strategies

TurboLynx's ORCA optimizer selects join strategy automatically based on statistics:

| Join Type | Description |
|---|---|
| Index join | Uses adjacency list index — fast for selective patterns |
| Hash join | General-purpose for larger intermediate results |
| Merge join | For ordered streams — can be disabled with `--disable-merge-join` |

## Example Queries

### 1-hop traversal

```cypher
MATCH (a:Person)-[:KNOWS]->(b:Person)
WHERE a.firstName = 'Alice'
RETURN b.firstName, b.lastName;
```

### Aggregation

```cypher
MATCH (n:Person)
RETURN n.city, COUNT(*) AS cnt
ORDER BY cnt DESC
LIMIT 10;
```

### Multi-hop

```cypher
MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)
WHERE a.id = 1
RETURN DISTINCT c.firstName;
```

### CREATE

```cypher
CREATE (n:Person {firstName: 'Charlie', lastName: 'Brown', age: 35});
```

### SET

```cypher
MATCH (n:Person)
WHERE n.firstName = 'Alice'
SET n.age = 31;
```
