# turbolynx-mcp

MCP server for [TurboLynx](https://turbolynx.io). Exposes a TurboLynx
workspace to MCP-compatible agents — Claude Desktop, Cursor, Codex, and any
other client that speaks the [Model Context
Protocol](https://modelcontextprotocol.io).

The server is **read-only** in this v0 release. Writes (CREATE / MERGE /
DELETE / SET / REMOVE / DROP) are rejected.

## Install

From a TurboLynx checkout:

```bash
cd tools/mcp
npm install
npm pack                         # produces turbolynx-mcp-0.0.1.tgz
npm install -g ./turbolynx-mcp-0.0.1.tgz
```

The package bundles the TurboLynx WASM runtime (via the `turbolynx`
Node.js bindings), so **no native build is required**. A published
`turbolynx-mcp` on the npm registry is on the roadmap.

## Configure your client

### Claude Desktop

Edit `~/Library/Application Support/Claude/claude_desktop_config.json`
(macOS) or `%APPDATA%\Claude\claude_desktop_config.json` (Windows):

```json
{
  "mcpServers": {
    "turbolynx": {
      "command": "turbolynx-mcp",
      "env": {
        "TURBOLYNX_WORKSPACE": "/absolute/path/to/workspace"
      }
    }
  }
}
```

If you haven't installed globally, replace `"command": "turbolynx-mcp"`
with `"command": "node"` and
`"args": ["/absolute/path/to/tools/mcp/src/server.js"]`.

Restart Claude Desktop. The agent will now see the tools below.

### Cursor / other clients

Most clients use the same shape (`command`, `args`, `env`). Check each
client's docs for the config file location.

## Tools

| Tool | Description |
|------|-------------|
| `query_cypher` | Run a read-only Cypher query; returns columns, types, rows. |
| `list_labels` | List every node and edge label in the workspace. |
| `describe_label` | Property schema (`propertyName → typeName`) for a label. |
| `sample_label` | Return a handful of real rows for a node label. |

## Resources

| URI | Description |
|-----|-------------|
| `turbolynx://schema` | Full schema dump (all nodes + edges, JSON). |
| `turbolynx://label/{name}` | Per-label schema for a single label. |

## Manual test (stdio JSON-RPC)

```bash
TURBOLYNX_WORKSPACE=/data/ldbc/mini bash test.sh
```

## Status

Alpha. See the [TurboLynx documentation](https://turbolynx.io) for the
query surface. Planned follow-ups: write tools, bulk-load, SSE transport
for remote agents, npm registry release.
