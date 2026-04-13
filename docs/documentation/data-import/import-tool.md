# Import Tool

`turbolynx import` bulk-loads graph data from CSV (or JSON) files into a TurboLynx workspace.

---

## Synopsis

```bash
turbolynx import [options]
```

## Options

| Option | Description |
|---|---|
| `--workspace <path>` | **(required)** Directory where the database files (`store.db`, `catalog.bin`) will be written. Created automatically if it does not exist. |
| `--nodes <Label> <file>` | Vertex CSV/JSON file with the given label. Repeatable. |
| `--nodes <Label:Parent> <file>` | Vertex file with a sub-label. The vertex belongs to both `Label` and `Parent` partitions, so edge files referencing either label can resolve its IDs. |
| `--relationships <Type> <file>` | Edge CSV/JSON file with the given relationship type. Repeatable. |
| `--incremental <true\|false>` | When `true`, append edges to an existing database without clearing vertex data. Only edge files may be specified in incremental mode. Default: `false`. |
| `--skip-histogram` | Skip post-load histogram generation. Useful for faster iteration during development; not recommended for production. |
| `--log-level <level>` | Set log verbosity: `trace`, `debug`, `info`, `warn`, `error`, `critical`, `off`. Default: `info`. |

---

## Basic Usage

Load vertices first, then edges:

```bash
turbolynx import \
    --workspace /data/mydb \
    --nodes Person  data/person.csv \
    --nodes Company data/company.csv \
    --relationships KNOWS     data/person_knows_person.csv \
    --relationships WORKS_AT  data/person_works_at_company.csv
```

Each `--nodes` pair is `<Label> <file>`. Each `--relationships` pair is `<Type> <file>`.

---

## Multi-label Vertices

When a vertex file should be accessible under multiple labels, use the `Label:Parent` syntax:

```bash
--nodes Comment:Message  data/comment.csv \
--nodes Post:Message     data/post.csv
```

Both `Comment` and `Post` vertices are registered under the `Message` parent label. Edge files with `:START_ID(Message)` or `:END_ID(Message)` can then reference vertices from either file.

---

## Full Example — LDBC Social Network

```bash
DATA=/source-data/ldbc/sf1
WS=/data/ldbc/sf1

turbolynx import \
    --workspace $WS \
    --nodes Person            $DATA/dynamic/Person.csv \
    --nodes Comment:Message   $DATA/dynamic/Comment.csv \
    --nodes Post:Message      $DATA/dynamic/Post.csv \
    --nodes Forum             $DATA/dynamic/Forum.csv \
    --nodes Organisation      $DATA/static/Organisation.csv \
    --nodes Place             $DATA/static/Place.csv \
    --nodes Tag               $DATA/static/Tag.csv \
    --nodes TagClass          $DATA/static/TagClass.csv \
    --relationships HAS_CREATOR    $DATA/dynamic/Comment_hasCreator_Person.csv \
    --relationships HAS_CREATOR    $DATA/dynamic/Post_hasCreator_Person.csv \
    --relationships IS_LOCATED_IN  $DATA/dynamic/Person_isLocatedIn_Place.csv \
    --relationships KNOWS          $DATA/dynamic/Person_knows_Person.csv \
    --relationships LIKES          $DATA/dynamic/Person_likes_Comment.csv \
    --relationships LIKES          $DATA/dynamic/Person_likes_Post.csv \
    --relationships REPLY_OF       $DATA/dynamic/Comment_replyOf_Post.csv \
    --relationships REPLY_OF       $DATA/dynamic/Comment_replyOf_Comment.csv \
    --relationships HAS_TAG        $DATA/dynamic/Comment_hasTag_Tag.csv \
    --relationships HAS_TAG        $DATA/dynamic/Post_hasTag_Tag.csv
```

---

## Pipeline Stages

When `turbolynx import` runs, it executes four stages in order:

1. **Initialize workspace** — create the output directory and database files.
2. **Load vertices** — read each `--nodes` CSV, create vertex extents, and build LID-to-PID mappings for edge resolution.
3. **Load edges** — read each `--relationships` CSV, resolve source/destination IDs via the vertex mappings, and create forward and backward adjacency lists.
4. **Post-processing** — generate query-optimizer histograms (unless `--skip-histogram`), flush metadata, create unified partitions, and persist the catalog.

---

## Output Files

After a successful import, the workspace directory contains:

| File | Description |
|---|---|
| `store.db` | Main data store (vertex extents, edge adjacency lists, properties) |
| `catalog.bin` | Schema catalog (vertex labels, edge types, property schemas) |
| `store.db.meta` | Chunk metadata index |

---

## Tips

- **Edge type names** in `--relationships` should be consistent across files. Multiple files can share the same type (e.g., two `HAS_CREATOR` files for `Comment` and `Post`).
- **Order matters**: vertex files must be listed before edge files, because edge loading resolves vertex IDs built during vertex loading.
- For file format details (CSV header annotations, JSON structure, supported types), see [File Formats](formats.md).
