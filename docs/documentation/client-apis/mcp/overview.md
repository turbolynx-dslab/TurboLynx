# MCP server

TurboLynx ships a [Model Context
Protocol](https://modelcontextprotocol.io) server so MCP-compatible agents
— Claude Desktop, Cursor, Codex, and others — can query a TurboLynx
workspace directly. The server wraps the WASM-based Node.js bindings, so
there is no native build step at install time.

The v0 server is **read-only**. Write operations (CREATE / MERGE / DELETE
/ SET / REMOVE / DROP) are rejected up front.

## Install

From a TurboLynx checkout:

```bash
cd tools/mcp
npm install
npm pack                                       # turbolynx-mcp-0.0.1.tgz
npm install -g ./turbolynx-mcp-0.0.1.tgz
```

A published `turbolynx-mcp` on the npm registry is on the roadmap.

## Configure Claude Desktop

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

Restart Claude Desktop. The agent will see four tools and one resource
namespace.

## Tools

| Tool | Arguments | Description |
|------|-----------|-------------|
| `query_cypher` | `cypher: string`, `limit?: int` | Execute a read-only Cypher query. |
| `list_labels` | — | List all node and edge labels. |
| `describe_label` | `label: string`, `is_edge?: bool` | Property schema for a label. |
| `sample_label` | `label: string`, `n?: int` | A small sample of real rows. |

`query_cypher` returns a JSON object with `columns`, `types`,
`row_count`, and `rows` fields.

## Resources

| URI | Returns |
|-----|---------|
| `turbolynx://schema` | Full schema dump — all nodes + edges with properties. |
| `turbolynx://label/{name}` | Per-label schema for a single label. |

## Example session

With the server configured, an agent might hold a conversation like:

> **User:** Who are the most-connected people in this dataset?
>
> **Agent** (calls `list_labels` → sees `Person`, `KNOWS` → calls
> `describe_label(Person)` → writes the query, calls `query_cypher`):
>
> ```cypher
> MATCH (p:Person)-[:KNOWS]->()
> RETURN p.firstName, p.lastName, count(*) AS friends
> ORDER BY friends DESC LIMIT 10
> ```
>
> **Agent:** The most-connected people are Marc Ravalomanana (42 friends),
> K. Sen (41), …

## Security

The server is scoped to one workspace (`TURBOLYNX_WORKSPACE`). It is
intended for local, trusted use. The v0 release does not support remote
agents or authentication — that will come with the SSE transport on the
roadmap.

## Status

Alpha. Source at
[`tools/mcp/`](https://github.com/turbolynx-dslab/TurboLynx/tree/main/tools/mcp).
Planned follow-ups: write tools, bulk-load, SSE transport, npm registry
release.
