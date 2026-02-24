# Data Import — File Formats

TurboLynx loads datasets via the `bulkload` tool. Two file formats are supported.

## CSV

Headers are required. Column naming follows the Neo4j bulk import convention.

### Vertex files

```
id:ID(Person),firstName:STRING,lastName:STRING,age:INT
1,Alice,Smith,30
2,Bob,Jones,25
```

- `:ID(TYPE)` — unique vertex ID within the type
- Remaining columns are vertex properties

### Edge files (forward)

```
:START_ID(Person),:END_ID(Person),since:INT
1,2,2020
```

- `:START_ID(TYPE)` — source vertex ID
- `:END_ID(TYPE)` — target vertex ID
- Remaining columns are edge properties

### Edge files (backward / reverse)

For reverse adjacency lists, swap the ID columns:

```
:END_ID(Person),:START_ID(Person),since:INT
2,1,2020
```

## JSON

A JSON file is a list of objects. Each object must have consistent labels and unique properties.

```json
[
  { "id": 1, "firstName": "Alice", "lastName": "Smith", "age": 30 },
  { "id": 2, "firstName": "Bob",   "lastName": "Jones", "age": 25 }
]
```

## Directory Layout

Place vertex and edge files in a flat directory:

```
dataset/
├── person.csv       ← vertex file
├── comment.csv      ← vertex file
├── knows.csv        ← edge file (forward)
└── knows_rev.csv    ← edge file (backward, optional)
```

Pass the directory to `bulkload`:

```bash
./tools/bulkload --workspace /path/to/db --data /path/to/dataset
```

## Catalog Persistence

After loading, the schema is persisted to `<workspace>/catalog.bin` and loaded automatically on the next startup. No need to re-run `bulkload` unless the dataset changes.
