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
