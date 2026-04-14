# Schema Inspection

TurboLynx exposes catalog metadata through the connection object, allowing you to discover the graph structure programmatically.

## Labels

### `conn.labels()`

Return all node labels in the database.

```python
conn.labels()
# ['Person', 'Comment:Message', 'Post:Message', 'Forum',
#  'University:Organisation', 'Company:Organisation',
#  'Continent:Place', 'City:Place', 'Country:Place',
#  'Tag', 'TagClass', 'Message', 'Organisation', 'Place']
```

!!! note
    Labels with `:` indicate inheritance. `Comment:Message` means `Comment` is a subtype of `Message`.

---

## Relationship Types

### `conn.relationship_types()`

Return all relationship types in the database (deduplicated).

```python
conn.relationship_types()
# ['HAS_CREATOR', 'IS_LOCATED_IN', 'KNOWS', 'LIKES',
#  'HAS_INTEREST', 'STUDY_AT', 'REPLY_OF', 'HAS_TAG',
#  'CONTAINER_OF', 'HAS_MODERATOR', 'HAS_MEMBER',
#  'WORK_AT', 'IS_PART_OF', 'IS_SUBCLASS_OF', 'HAS_TYPE']
```

---

## Property Schema

### `conn.schema(label, edge=False)`

Return a dict mapping property names to their SQL types.

```python
# Node properties
conn.schema("Person")
# {'id': 'UBIGINT', 'firstName': 'VARCHAR', 'lastName': 'VARCHAR',
#  'gender': 'VARCHAR', 'birthday': 'DATE', 'creationDate': 'BIGINT',
#  'locationIP': 'VARCHAR', 'browserUsed': 'VARCHAR',
#  'speaks': 'VARCHAR', 'email': 'VARCHAR'}

# Edge properties
conn.schema("KNOWS", edge=True)
# {'_sid': 'UBIGINT', '_tid': 'UBIGINT', 'creationDate': 'BIGINT'}
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `label` | `str` | required | Node label or relationship type name |
| `edge` | `bool` | `False` | Set `True` for relationship types |

!!! note
    Edge schemas include internal `_sid` (source ID) and `_tid` (target ID) columns.

---

## Version

### `turbolynx.__version__`

```python
turbolynx.__version__
# '0.0.1'
```

### `turbolynx.TurboLynxPyConnection.GetVersion()`

Returns the engine version string from the C library.

---

## Full Example

```python
import turbolynx

conn = turbolynx.connect("/data/ldbc/sf1")

# Discover the graph structure
print("=== Node Labels ===")
for label in conn.labels():
    schema = conn.schema(label)
    print(f"  :{label} ({len(schema)} properties)")
    for prop, dtype in schema.items():
        print(f"    .{prop}: {dtype}")

print("\n=== Relationship Types ===")
for rel in conn.relationship_types():
    schema = conn.schema(rel, edge=True)
    user_props = {k: v for k, v in schema.items() if not k.startswith('_')}
    print(f"  [:{rel}] ({len(user_props)} user properties)")
    for prop, dtype in user_props.items():
        print(f"    .{prop}: {dtype}")

conn.close()
```
