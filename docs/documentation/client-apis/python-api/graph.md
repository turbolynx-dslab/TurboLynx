# Graph API

The Graph API provides graph-specific query builders that generate Cypher queries under the hood and return [Relation](relation.md) objects for further chaining.

## Node Queries

### `conn.node(label, **props)`

Start a graph traversal from nodes matching a label and optional properties.

```python
# All persons
conn.node("Person")

# Persons with specific properties
conn.node("Person", firstName="Marc")
```

### `node.where(expr)`

Add a WHERE condition on the source node (referenced as `n`).

```python
conn.node("Person").where("n.age > 30")
conn.node("Person").where("n.firstName STARTS WITH 'Ma'")
```

### `node.properties(*cols)`

Return specified properties of matching nodes.

```python
# Specific properties
conn.node("Person", firstName="Marc").properties("firstName", "lastName", "id").fetchall()
# [('Marc', 'Ravalomanana', 65), ('Marc', 'Simon', 8796093030591), ...]

# All properties (uses schema lookup)
conn.node("Person", firstName="Marc").properties().limit(3).fetchdf()
```

### `node.count()`

Count matching nodes.

```python
conn.node("Person").count().fetchall()
# [(9892,)]

conn.node("Person", firstName="Marc").count().fetchall()
# [(6,)]
```

---

## Traversal

### `node.neighbors(rel_type=None, direction="both", target_label=None)`

Traverse to neighboring nodes via relationships.

```python
# All KNOWS neighbors
conn.node("Person", firstName="Marc").neighbors("KNOWS").limit(5).fetchdf()

# Outgoing only
conn.node("Person", firstName="Marc").neighbors("KNOWS", direction="out").limit(5).fetchdf()

# Incoming only
conn.node("Person", firstName="Marc").neighbors("KNOWS", direction="in").limit(5).fetchdf()

# Any relationship type
conn.node("Person", firstName="Marc").neighbors().limit(5).fetchdf()

# Filter target label
conn.node("Person", firstName="Marc").neighbors("STUDY_AT", target_label="University").fetchall()
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `rel_type` | `str` or `None` | `None` | Relationship type filter |
| `direction` | `str` | `"both"` | `"out"`, `"in"`, or `"both"` |
| `target_label` | `str` or `None` | `None` | Target node label filter |

### `node.edges(rel_type=None, direction="both", target_label=None)`

Return relationship (edge) properties instead of neighbor nodes.

```python
conn.node("Person", firstName="Marc").edges("KNOWS").limit(5).fetchall()
# [(1292579812039,), (1299407888225,), ...]
```

---

## Graph Analysis

### `node.degree(rel_type=None, direction="both")`

Calculate the degree (number of relationships) for matching nodes.

```python
# Total degree
conn.node("Person").degree("KNOWS").order("degree DESC").limit(10).fetchdf()
#    node_id          degree
# 0  2199023262543    814
# 1  2783             736
# ...

# Out-degree
conn.node("Person").degree("KNOWS", direction="out").order("degree DESC").limit(5).fetchdf()

# In-degree
conn.node("Person").degree("KNOWS", direction="in").order("degree DESC").limit(5).fetchdf()

# Degree for specific node
conn.node("Person", firstName="Marc").degree("KNOWS").fetchall()
# [(65, 42)]  — node_id=65, degree=42
```

---

## Path Queries

### `conn.shortest_path(source_label, target_label, rel=None, ...)`

Find the shortest path between two node types using variable-length edge traversal.

```python
paths = conn.shortest_path(
    "Person", "Person",
    rel="KNOWS",
    min_hops=1,
    max_hops=4,
    source_filter="a.firstName = 'Marc'",
    target_filter="b.firstName = 'John'"
).fetchall()

for path in paths:
    print(f"Hops: {path[-1]}")
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `source_label` | `str` | required | Source node label |
| `target_label` | `str` | required | Target node label |
| `rel` | `str` or `None` | `None` | Relationship type |
| `min_hops` | `int` | `1` | Minimum path length |
| `max_hops` | `int` or `None` | `None` | Maximum path length (`None` = unbounded) |
| `source_filter` | `str` or `None` | `None` | WHERE condition for source node (variable `a`) |
| `target_filter` | `str` or `None` | `None` | WHERE condition for target node (variable `b`) |
| `direction` | `str` | `"both"` | `"out"`, `"in"`, or `"both"` |

### `conn.all_shortest_paths(...)`

Find all shortest paths (returns multiple results if equal-length alternatives exist).

```python
all_paths = conn.all_shortest_paths(
    "Person", "Person",
    rel="KNOWS",
    source_filter="a.id = 933",
    target_filter="b.id = 4139"
).fetchall()
```

### Chaining with Path Queries

Path queries return Relations, so you can chain further operations:

```python
conn.shortest_path("Person", "Person", rel="KNOWS",
                    source_filter="a.firstName = 'Marc'",
                    target_filter="b.firstName = 'John'",
                    max_hops=4).limit(3).show()
```

### Query Inspection

```python
pq = conn.shortest_path("Person", "Person", rel="KNOWS", max_hops=3,
                          source_filter="a.firstName = 'Marc'")
print(pq.query)
# MATCH path = shortestPath((a:Person)-[:KNOWS*1..3]-(b:Person))
# WHERE a.firstName = 'Marc' RETURN path, length(path) AS hops
```

---

## Full Example

```python
import turbolynx

conn = turbolynx.connect("/data/ldbc/sf1")

# Find Marc's most connected friends (2-hop network)
marc = conn.node("Person", firstName="Marc")

# Direct neighbors
friends = marc.neighbors("KNOWS", direction="out").limit(10).fetchdf()
print("Direct friends:", len(friends))

# Marc's degree
print("Degree:", marc.degree("KNOWS").fetchall())

# Shortest path to John
paths = conn.shortest_path("Person", "Person", rel="KNOWS",
                            source_filter="a.firstName = 'Marc'",
                            target_filter="b.firstName = 'John'",
                            max_hops=4).fetchall()
print(f"Found {len(paths)} paths to John")

# Top connected people in the network
conn.node("Person").degree("KNOWS").order("degree DESC").limit(10).show()

conn.close()
```
