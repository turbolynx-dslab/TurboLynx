# turbolynx (Node.js)

TurboLynx Node.js bindings, built on top of the TurboLynx WebAssembly runtime.
Read-only Cypher query execution against pre-built TurboLynx workspaces.

## Install

The package bundles the TurboLynx WASM binary, so no native build step is
required at install time. From this directory:

```bash
npm install
# or, to package a tarball you can `npm install` elsewhere:
npm pack
```

A pre-built `npm install turbolynx` on the npm registry is on the roadmap.

## Usage

```js
const { TurboLynx } = require('turbolynx');

(async () => {
  const db = await TurboLynx.open('/path/to/workspace');

  const r = await db.query(
    'MATCH (n:Person) RETURN n.firstName, n.lastName LIMIT 5'
  );
  console.log(r.columns); // [ 'n.firstName', 'n.lastName' ]
  console.log(r.rows);    // [ [ ... ], ... ]

  console.log(await db.labels());
  console.log(await db.schema('Person'));

  db.close();
})();
```

## API

| Method | Description |
| --- | --- |
| `TurboLynx.open(path)` | Open a workspace directory (read-only). |
| `TurboLynx.version()` | TurboLynx library version. |
| `db.query(cypher)` | Run a Cypher query, returns `{columns, types, rows}`. |
| `db.labels()` | List node/edge labels. |
| `db.schema(label, isEdge?)` | Property schema for a label. |
| `db.close()` | Close the connection. Idempotent. |

See the [TurboLynx documentation](https://turbolynx.io) for the full Cypher
surface and workspace format.
