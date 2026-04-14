// Smoke test for the Node.js bindings.
// Run with: WORKSPACE=/data/ldbc/mini node test.js
'use strict';

const { TurboLynx, TurboLynxError } = require('./index.js');

const ws = process.env.WORKSPACE || '/data/ldbc/mini';

(async () => {
  console.log('TurboLynx version:', await TurboLynx.version());

  const db = await TurboLynx.open(ws);
  console.log('Opened:', ws);

  const labels = await db.labels();
  console.log('Labels:', labels.map((l) => l.name + '(' + l.type + ')').join(', '));

  if (labels.find((l) => l.name === 'Person')) {
    console.log('Person schema:', await db.schema('Person'));
    const r = await db.query(
      'MATCH (n:Person) RETURN n.firstName, n.lastName LIMIT 3',
    );
    console.log('Query columns:', r.columns);
    console.log('Query rows:', r.rows);
  }

  db.close();
  console.log('OK');
})().catch((e) => {
  if (e instanceof TurboLynxError) {
    console.error('TurboLynxError:', e.message);
  } else {
    console.error(e);
  }
  process.exit(1);
});
