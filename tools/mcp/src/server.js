#!/usr/bin/env node
/**
 * TurboLynx MCP server (read-only, stdio transport).
 *
 * Exposes a TurboLynx workspace to MCP-compatible agents (Claude Desktop,
 * Cursor, etc.) via a small set of tools and resources.
 *
 * Configuration:
 *   TURBOLYNX_WORKSPACE   Path to a TurboLynx workspace directory (required).
 *
 * Example Claude Desktop config:
 *   {
 *     "mcpServers": {
 *       "turbolynx": {
 *         "command": "npx",
 *         "args": ["-y", "turbolynx-mcp"],
 *         "env": { "TURBOLYNX_WORKSPACE": "/path/to/workspace" }
 *       }
 *     }
 *   }
 */
'use strict';

const { Server } = require('@modelcontextprotocol/sdk/server/index.js');
const { StdioServerTransport } = require('@modelcontextprotocol/sdk/server/stdio.js');
const {
  CallToolRequestSchema,
  ListToolsRequestSchema,
  ListResourcesRequestSchema,
  ListResourceTemplatesRequestSchema,
  ReadResourceRequestSchema,
} = require('@modelcontextprotocol/sdk/types.js');

const { TurboLynx, TurboLynxError } = require('turbolynx');

const WORKSPACE = process.env.TURBOLYNX_WORKSPACE;
if (!WORKSPACE) {
  process.stderr.write(
    'turbolynx-mcp: TURBOLYNX_WORKSPACE env var is required.\n'
  );
  process.exit(2);
}

// Opt-in gate. Unset or "0" → read-only server (legacy behavior). Any other
// non-empty value opens the workspace writable and registers `mutate_cypher`.
// We require an explicit opt-in because WASM writes through NODEFS land on
// the host filesystem and there's no concurrency protection against another
// process opening the same workspace.
const WRITES_ENABLED = (() => {
  const v = process.env.TURBOLYNX_ALLOW_WRITES;
  return !!(v && v !== '0' && v.toLowerCase() !== 'false');
})();

let _dbPromise = null;
function getDb() {
  if (!_dbPromise) {
    _dbPromise = TurboLynx.open(WORKSPACE, { writable: WRITES_ENABLED });
  }
  return _dbPromise;
}

/* ───── Tools ──────────────────────────────────────────────────── */

const READ_TOOLS = [
  {
    name: 'query_cypher',
    description:
      'Execute a read-only Cypher query against the TurboLynx workspace. ' +
      'Returns columns, types, and rows. Use MATCH / WITH / RETURN — write ' +
      'operations (CREATE, MERGE, SET, DELETE, REMOVE, DROP) are rejected. ' +
      'Use the `mutate_cypher` tool (when exposed) to write. Parameters may ' +
      'be passed via `params` and referenced in the query as $name (string ' +
      'values are auto-quoted).',
    inputSchema: {
      type: 'object',
      properties: {
        cypher: { type: 'string', description: 'Cypher query text.' },
        params: {
          type: 'object',
          description:
            'Optional map of parameters substituted into $name placeholders. ' +
            'Values may be string, number, boolean, or null.',
          additionalProperties: true,
        },
        limit: {
          type: 'integer',
          minimum: 1,
          description:
            'Optional cap on rows returned to the agent (post-execution). ' +
            'Prefer a LIMIT clause in the query for server-side truncation.',
        },
      },
      required: ['cypher'],
    },
  },
  {
    name: 'explain_cypher',
    description:
      'Return the physical query plan for a Cypher statement WITHOUT executing ' +
      'it. Useful for previewing cost / shape before running. Same parameter ' +
      'substitution as query_cypher.',
    inputSchema: {
      type: 'object',
      properties: {
        cypher: { type: 'string', description: 'Cypher query text.' },
        params: {
          type: 'object',
          description: 'Optional parameter map ($name substitution).',
          additionalProperties: true,
        },
      },
      required: ['cypher'],
    },
  },
  {
    name: 'list_labels',
    description:
      'List all node and edge labels in the workspace, so the agent can ' +
      'discover the schema before writing queries.',
    inputSchema: { type: 'object', properties: {} },
  },
  {
    name: 'describe_label',
    description:
      'Return the property schema (propertyName → typeName) for a node or ' +
      'edge label.',
    inputSchema: {
      type: 'object',
      properties: {
        label: { type: 'string', description: 'Label name (case-sensitive).' },
        is_edge: {
          type: 'boolean',
          default: false,
          description: 'True if this label is an edge / relationship type.',
        },
      },
      required: ['label'],
    },
  },
  {
    name: 'sample_label',
    description:
      'Return a small sample of rows for a node label so the agent can see ' +
      'real values. Equivalent to `MATCH (n:<label>) RETURN n LIMIT n`.',
    inputSchema: {
      type: 'object',
      properties: {
        label: { type: 'string' },
        n: { type: 'integer', default: 5, minimum: 1, maximum: 100 },
      },
      required: ['label'],
    },
  },
  // Type-based schema tools. These are the right fit for a truly schemaless
  // property graph (e.g. DBpedia loaded under a single NODE label, with
  // class membership expressed via rdf:type edges). The label-based tools
  // above still work for labelled datasets like LDBC — agents pick whichever
  // vocabulary matches the workspace they're on.
  {
    name: 'list_types',
    description:
      'Schemaless view of the catalog: enumerate rdf:type-style peer nodes ' +
      'and the number of entities pointing at each one. Works when every ' +
      'node lives under a single partition (e.g. `NODE`) and type ' +
      'membership is an edge, not a label. Edge-label defaults to `type`.',
    inputSchema: {
      type: 'object',
      properties: {
        edge: {
          type: 'string',
          default: 'type',
          description: 'The edge label that expresses "is-a"/rdf:type. ' +
            'Override if your dataset uses something else.',
        },
        limit: {
          type: 'integer',
          default: 200,
          minimum: 1,
          maximum: 2000,
          description: 'Cap on type rows returned.',
        },
      },
    },
  },
  {
    name: 'describe_type',
    description:
      'For a given type URI, return the property-coverage histogram over ' +
      'entities of that type: how many nodes carry each property, as an ' +
      'absolute count and a fraction of the type total. This surfaces ' +
      'schema heterogeneity (e.g. 90% of Films have a director, 40% have ' +
      'runtime, 5% have budget) that a label-based system would hide.',
    inputSchema: {
      type: 'object',
      properties: {
        type_uri: { type: 'string', description: 'URI of the type node.' },
        edge:     { type: 'string', default: 'type' },
        limit:    { type: 'integer', default: 100, minimum: 1, maximum: 1000 },
      },
      required: ['type_uri'],
    },
  },
  {
    name: 'sample_type',
    description:
      'Return a small sample of nodes of a given type so the agent can see ' +
      'real values. Equivalent to `MATCH (n)-[:<edge>]->(t {uri:$type_uri}) ' +
      'RETURN n LIMIT $n`.',
    inputSchema: {
      type: 'object',
      properties: {
        type_uri: { type: 'string' },
        edge:     { type: 'string', default: 'type' },
        n:        { type: 'integer', default: 5, minimum: 1, maximum: 100 },
      },
      required: ['type_uri'],
    },
  },
];

// `mutate_cypher` is registered only when the server was launched with
// TURBOLYNX_ALLOW_WRITES set. Separating the destructive path into its own
// tool means the agent has to deliberately pick it — a read-only agent
// prompt never even sees the mutation surface.
const WRITE_TOOLS = [
  {
    name: 'mutate_cypher',
    description:
      'Execute a Cypher statement that may create, update, or delete graph ' +
      'data (CREATE / MERGE / SET / DELETE / DETACH DELETE / REMOVE / DROP). ' +
      'Returns columns, types, and rows the same way as query_cypher. Only ' +
      'exposed when the server was started with TURBOLYNX_ALLOW_WRITES=1.',
    inputSchema: {
      type: 'object',
      properties: {
        cypher: { type: 'string', description: 'Cypher statement text.' },
        params: {
          type: 'object',
          description:
            'Optional map of parameters substituted into $name placeholders.',
          additionalProperties: true,
        },
        limit: {
          type: 'integer',
          minimum: 1,
          description: 'Optional cap on rows returned to the agent.',
        },
      },
      required: ['cypher'],
    },
    annotations: {
      // MCP tool annotations let clients render a confirmation prompt before
      // invoking destructive tools. Setting the hints conservatively even if
      // individual statements may not mutate — agent safety overrides strict
      // accuracy here.
      destructiveHint: true,
      readOnlyHint: false,
      idempotentHint: false,
    },
  },
];

const TOOLS = WRITES_ENABLED ? READ_TOOLS.concat(WRITE_TOOLS) : READ_TOOLS;

// Coarse guard that rejects obvious write statements reaching `query_cypher`
// and `explain_cypher`. The real isolation for pure-read deployments is the
// read-only connect path inside the WASM runtime; this guard just fails fast
// with a friendly message and keeps the read-tool contract explicit even on
// a writable server.
function blockedWriteCheck(cypher) {
  const stripped = cypher.replace(/\/\*[\s\S]*?\*\/|\/\/.*$/gm, ' ');
  if (/\b(CREATE|MERGE|DELETE|DETACH|SET|REMOVE|DROP|LOAD\s+CSV)\b/i.test(stripped)) {
    return (
      'Write operations (CREATE / MERGE / DELETE / SET / REMOVE / DROP) are ' +
      'not allowed via query_cypher. Use mutate_cypher instead.'
    );
  }
  return null;
}

async function runTool(name, args) {
  const db = await getDb();

  if (name === 'query_cypher') {
    const cypher = String(args.cypher || '').trim();
    if (!cypher) throw new Error('cypher argument must be a non-empty string');
    const blocked = blockedWriteCheck(cypher);
    if (blocked) throw new Error(blocked);
    const params = args.params && typeof args.params === 'object' ? args.params : undefined;
    const r = await db.query(cypher, params);
    let rows = r.rows || [];
    if (Number.isInteger(args.limit) && args.limit > 0) {
      rows = rows.slice(0, args.limit);
    }
    return { columns: r.columns, types: r.types, row_count: rows.length, rows };
  }

  if (name === 'mutate_cypher') {
    if (!WRITES_ENABLED) {
      throw new Error(
        'mutate_cypher is disabled. Start the server with ' +
        'TURBOLYNX_ALLOW_WRITES=1 to enable writes.');
    }
    const cypher = String(args.cypher || '').trim();
    if (!cypher) throw new Error('cypher argument must be a non-empty string');
    const params = args.params && typeof args.params === 'object' ? args.params : undefined;
    const r = await db.query(cypher, params);
    let rows = r.rows || [];
    if (Number.isInteger(args.limit) && args.limit > 0) {
      rows = rows.slice(0, args.limit);
    }
    return { columns: r.columns, types: r.types, row_count: rows.length, rows };
  }

  if (name === 'explain_cypher') {
    const cypher = String(args.cypher || '').trim();
    if (!cypher) throw new Error('cypher argument must be a non-empty string');
    const blocked = blockedWriteCheck(cypher);
    if (blocked) throw new Error(blocked);
    const params = args.params && typeof args.params === 'object' ? args.params : undefined;
    const r = await db.explain(cypher, params);
    return { plan: r.plan };
  }

  if (name === 'list_labels') {
    return { labels: await db.labels() };
  }

  if (name === 'describe_label') {
    const label = String(args.label || '');
    if (!label) throw new Error('label argument is required');
    const props = await db.schema(label, !!args.is_edge);
    return { label, is_edge: !!args.is_edge, properties: props };
  }

  if (name === 'sample_label') {
    const label = String(args.label || '');
    if (!label) throw new Error('label argument is required');
    const n = Number.isInteger(args.n) ? args.n : 5;
    // Identifier escaping: reject any label with a backtick to keep this simple.
    if (/`/.test(label)) throw new Error('label may not contain backticks');
    const r = await db.query('MATCH (n:`' + label + '`) RETURN n LIMIT ' + n);
    return r;
  }

  // ---- Type-based (schemaless) schema tools -------------------------------

  if (name === 'list_types') {
    const edge = String(args.edge || 'type');
    if (/`/.test(edge)) throw new Error('edge may not contain backticks');
    const limit = Number.isInteger(args.limit) ? args.limit : 200;
    const cypher =
      'MATCH (n)-[:`' + edge + '`]->(t) ' +
      'RETURN t.uri AS uri, count(n) AS n_nodes ' +
      'ORDER BY n_nodes DESC LIMIT ' + limit;
    const r = await db.query(cypher);
    return { edge, types: r.rows || [] };
  }

  if (name === 'describe_type') {
    const edge = String(args.edge || 'type');
    const uri = String(args.type_uri || '');
    if (!uri) throw new Error('type_uri argument is required');
    if (/`/.test(edge)) throw new Error('edge may not contain backticks');
    const limit = Number.isInteger(args.limit) ? args.limit : 100;
    // Two queries so we can compute coverage = present / total.
    const totalR = await db.query(
      'MATCH (n)-[:`' + edge + '`]->(t {uri: $uri}) RETURN count(n) AS total',
      { uri },
    );
    const total = (totalR.rows && totalR.rows[0] && totalR.rows[0].total) || 0;
    if (!total) {
      return { type_uri: uri, total: 0, properties: [] };
    }
    const propR = await db.query(
      'MATCH (n)-[:`' + edge + '`]->(t {uri: $uri}) ' +
      'UNWIND keys(n) AS k ' +
      'RETURN k AS property, count(*) AS present ' +
      'ORDER BY present DESC LIMIT ' + limit,
      { uri },
    );
    const props = (propR.rows || []).map((row) => ({
      property: row.property,
      present: row.present,
      coverage: Number((row.present / total).toFixed(4)),
    }));
    return { type_uri: uri, total, properties: props };
  }

  if (name === 'sample_type') {
    const edge = String(args.edge || 'type');
    const uri = String(args.type_uri || '');
    if (!uri) throw new Error('type_uri argument is required');
    if (/`/.test(edge)) throw new Error('edge may not contain backticks');
    const n = Number.isInteger(args.n) ? args.n : 5;
    const cypher =
      'MATCH (n)-[:`' + edge + '`]->(t {uri: $uri}) RETURN n LIMIT ' + n;
    const r = await db.query(cypher, { uri });
    return r;
  }

  throw new Error('Unknown tool: ' + name);
}

/* ───── Resources ──────────────────────────────────────────────── */

const RESOURCES = [
  {
    uri: 'turbolynx://schema',
    name: 'Full schema',
    description: 'Every node/edge label with its property schema.',
    mimeType: 'application/json',
  },
];

// Parameterised resources — advertised separately via resources/templates/list
// so MCP clients know how to build per-label URIs. Without this, the handler
// in readResource() below was unreachable from standard clients.
const RESOURCE_TEMPLATES = [
  {
    uriTemplate: 'turbolynx://label/{name}',
    name: 'Per-label schema',
    description:
      'Property schema for a single label. `{name}` is URL-encoded; pass ' +
      'the label exactly as it appears in list_labels / turbolynx://schema.',
    mimeType: 'application/json',
  },
];

async function readResource(uri) {
  const db = await getDb();

  if (uri === 'turbolynx://schema') {
    const labels = await db.labels();
    const out = { nodes: {}, edges: {} };
    for (const l of labels) {
      const bucket = l.type === 'edge' ? out.edges : out.nodes;
      try { bucket[l.name] = await db.schema(l.name, l.type === 'edge'); }
      catch (e) { bucket[l.name] = { _error: e.message }; }
    }
    return { contents: [{ uri, mimeType: 'application/json', text: JSON.stringify(out, null, 2) }] };
  }

  const m = /^turbolynx:\/\/label\/(.+)$/.exec(uri);
  if (m) {
    const label = decodeURIComponent(m[1]);
    const labels = await db.labels();
    const found = labels.find((l) => l.name === label);
    if (!found) throw new Error('Unknown label: ' + label);
    const props = await db.schema(label, found.type === 'edge');
    const body = { label, type: found.type, properties: props };
    return { contents: [{ uri, mimeType: 'application/json', text: JSON.stringify(body, null, 2) }] };
  }

  throw new Error('Unknown resource: ' + uri);
}

/* ───── Server wiring ──────────────────────────────────────────── */

const server = new Server(
  { name: 'turbolynx', version: '0.0.1' },
  { capabilities: { tools: {}, resources: {} } },
);

server.setRequestHandler(ListToolsRequestSchema, async () => ({ tools: TOOLS }));

server.setRequestHandler(CallToolRequestSchema, async (req) => {
  try {
    const result = await runTool(req.params.name, req.params.arguments || {});
    return { content: [{ type: 'text', text: JSON.stringify(result, null, 2) }] };
  } catch (e) {
    const msg = e instanceof TurboLynxError ? 'TurboLynx: ' + e.message : String(e.message || e);
    return { isError: true, content: [{ type: 'text', text: msg }] };
  }
});

server.setRequestHandler(ListResourcesRequestSchema, async () => ({ resources: RESOURCES }));

server.setRequestHandler(ListResourceTemplatesRequestSchema, async () => ({
  resourceTemplates: RESOURCE_TEMPLATES,
}));

server.setRequestHandler(ReadResourceRequestSchema, async (req) =>
  readResource(req.params.uri),
);

async function main() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
}

main().catch((e) => {
  process.stderr.write('turbolynx-mcp fatal: ' + (e && e.stack || e) + '\n');
  process.exit(1);
});
