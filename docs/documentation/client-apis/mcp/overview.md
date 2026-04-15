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

Restart Claude Desktop. The agent will see five tools and one resource
namespace.

## Tools

| Tool | Arguments | Description |
|------|-----------|-------------|
| `query_cypher` | `cypher: string`, `params?: object`, `limit?: int` | Execute a read-only Cypher query. |
| `explain_cypher` | `cypher: string`, `params?: object` | Return the physical plan without executing. |
| `list_labels` | — | List all node and edge labels. |
| `describe_label` | `label: string`, `is_edge?: bool` | Property schema for a label. |
| `sample_label` | `label: string`, `n?: int` | A small sample of real rows. |

`query_cypher` returns a JSON object with `columns`, `types`,
`row_count`, and `rows` fields. `explain_cypher` returns `{ "plan": "…" }`.

### Parameterized queries

Both `query_cypher` and `explain_cypher` accept a `params` map whose keys
are referenced in the Cypher text as `$name`. String values are
auto-quoted; numbers, booleans, and `null` are substituted literally.

```json
{
  "cypher": "MATCH (p:Person) WHERE p.firstName = $name RETURN count(p)",
  "params": { "name": "Jack" }
}
```

## Not supported in v0

The alpha MCP server deliberately omits the following — they are tracked
on the roadmap:

- Write operations (CREATE / MERGE / DELETE / SET / REMOVE / DROP / LOAD CSV)
- Bulk load from CSV / JSONL
- Remote transport (SSE, HTTP) and auth — stdio only
- Streaming / paged results — the full rowset is returned in one response
- Multiple workspaces — one `TURBOLYNX_WORKSPACE` per server

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
