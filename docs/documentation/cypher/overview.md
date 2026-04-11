# Cypher Query Language

TurboLynx's query language is Cypher, as defined by the [openCypher](https://opencypher.org/) specification. A subset of the specification's clauses is currently implemented — see the compatibility matrix below.

## Supported Clauses

| Clause | Status | Notes |
|---|---|---|
| `MATCH` | ✅ | Pattern matching on vertices and edges |
| `OPTIONAL MATCH` | ✅ | Left-outer join semantics |
| `WHERE` | ✅ | Filter predicates on node/rel properties |
| `RETURN` | ✅ | Projection; supports aliases |
| `RETURN DISTINCT` / `WITH DISTINCT` | ✅ | Deduplicate projected rows |
| `WITH` | ✅ | Pipeline results between clauses |
| `UNWIND` | ✅ | Iterate over a list |
| `ORDER BY` | ✅ | Ascending / descending sort |
| `LIMIT` | ✅ | Limit result count |
| `SKIP` | ✅ | Skip N results |
| `CREATE` | ❌ | Not yet supported |
| `SET` | ❌ | Not yet supported |
| `DELETE` | ❌ | Not yet supported |
| `UNION` / `UNION ALL` | 🚧 | Single-query only; multi-query UNION pending |

## Inline Property Filters

Node and relationship patterns support inline property filters using `{key: value}` syntax. These are semantically equivalent to WHERE predicates:

```cypher
-- Inline filter (equivalent to WHERE p.id = 933)
MATCH (p:Person {id: 933})-[:KNOWS]->(friend:Person)
RETURN count(friend)

-- WHERE form (equivalent)
MATCH (p:Person)-[:KNOWS]->(friend:Person)
WHERE p.id = 933
RETURN count(friend)
```

## Supported Functions

| Function | Description |
|---|---|
| `count(*)` | Count all matching rows |
| `count(expr)` | Count non-null values of an expression |
| `collect(expr)` | Aggregate into a list |
| `min(expr)`, `max(expr)` | Min/max aggregation |
| `sum(expr)` | Sum aggregation |
| `avg(expr)` | Arithmetic mean |

## Supported Expressions

| Expression | Example |
|---|---|
| Property access | `n.firstName` |
| Arithmetic | `a.age + 1`, `b.score * 2.0` |
| Comparison | `=`, `<>`, `<`, `>`, `<=`, `>=` |
| Logical | `AND`, `OR`, `NOT` |
| String | `n.name STARTS WITH 'Al'` (via function) |
| IS NULL / IS NOT NULL | `WHERE n.email IS NOT NULL` |
| CASE expression | `CASE WHEN ... THEN ... ELSE ... END` |

## Variable-Length Paths

```cypher
-- 1 to 3 hops
MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
WHERE a.id = 1
RETURN b.firstName, b.lastName
```

The lower bound defaults to 1; the upper bound can be omitted for unbounded traversal (`*`).

## Example Queries

### Filtered hop traversal

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

### Inline property filter

```cypher
MATCH (p:Person {id: 933})-[:KNOWS]->(friend:Person)
RETURN friend.firstName, friend.lastName;
```

### WITH pipeline

```cypher
MATCH (p:Person)-[:KNOWS]->(friend:Person)
WHERE p.id = 1
WITH friend
ORDER BY friend.lastName
LIMIT 20
RETURN friend.firstName, friend.lastName;
```
