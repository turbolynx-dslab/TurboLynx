# Node.js API

The Node.js bindings wrap the TurboLynx WebAssembly runtime, so they install
without any native build step. Read-only Cypher query execution against
pre-built TurboLynx workspaces.

## Install

See the [Node.js installation guide](../../../installation/overview.md?environment=node)
for build-from-source instructions. A pre-built `npm install turbolynx` on the
npm registry is on the roadmap.

## Quick start

```js
const { TurboLynx } = require('turbolynx');

(async () => {
  const db = await TurboLynx.open('/path/to/workspace');

  const r = await db.query(
    'MATCH (n:Person) RETURN n.firstName, n.lastName LIMIT 5'
  );
  console.log(r.columns); // [ 'n.firstName', 'n.lastName' ]
  console.log(r.rows);    // [ [ 'Marc', 'Ravalomanana' ], ... ]

  console.log(await db.labels());
  console.log(await db.schema('Person'));

  db.close();
})();
```

## API reference

### `TurboLynx.open(workspacePath: string): Promise<TurboLynx>`

Open a workspace directory in read-only mode. The directory must contain a
TurboLynx workspace (`catalog.bin`, `store.db`, `.store_meta`,
`catalog_version`).

### `TurboLynx.version(): Promise<string>`

TurboLynx library version string.

### `db.query(cypher: string): Promise<QueryResult>`

Execute a Cypher query and return the result.

```ts
interface QueryResult {
  columns: string[];   // column names
  types:   string[];   // logical type names per column
  rows:    unknown[][];
}
```

### `db.labels(): Promise<LabelInfo[]>`

List all node and edge labels in the database.

```ts
interface LabelInfo {
  name: string;
  type: 'node' | 'edge';
}
```

### `db.schema(label: string, isEdge?: boolean): Promise<Record<string, string>>`

Property schema for a label, returned as a `propertyName -> typeName` map.

### `db.close(): void`

Close the connection and release the underlying WASM resources. Idempotent.

## Errors

All API methods reject with a `TurboLynxError` on failure (workspace open
failures, query parse/execution errors, closed-connection access, etc.).

```js
const { TurboLynx, TurboLynxError } = require('turbolynx');

try {
  const db = await TurboLynx.open('/nonexistent');
} catch (e) {
  if (e instanceof TurboLynxError) console.error('TurboLynx:', e.message);
  else throw e;
}
```

## Status

Alpha. The bindings expose a minimal read-only surface (open, query, schema
inspection). Write operations, prepared statements, and streaming results are
on the roadmap. Track issues at the
[GitHub repository](https://github.com/turbolynx-dslab/TurboLynx).
