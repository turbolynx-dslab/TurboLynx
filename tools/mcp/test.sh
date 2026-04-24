#!/bin/bash
# Smoke-test the MCP server by piping JSON-RPC requests through stdio.
#
# Default mode (read-only) exercises tool list, resource list, resource
# templates list, and verifies that write statements through query_cypher
# are rejected.
#
# Pass --write (or set TURBOLYNX_ALLOW_WRITES=1 before invoking) to also
# launch the server in writable mode and exercise mutate_cypher.
set -e
export TURBOLYNX_WORKSPACE="${TURBOLYNX_WORKSPACE:-/data/ldbc/mini}"

WRITE_MODE=0
if [ "$1" = "--write" ] || [ "$TURBOLYNX_ALLOW_WRITES" = "1" ]; then
    WRITE_MODE=1
fi

REQ_INIT='{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"0"}}}'
REQ_TOOLS='{"jsonrpc":"2.0","id":2,"method":"tools/list"}'
REQ_LABELS='{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"list_labels","arguments":{}}}'
REQ_DESC='{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"describe_label","arguments":{"label":"Person"}}}'
REQ_QUERY='{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"query_cypher","arguments":{"cypher":"MATCH (n:Person) RETURN n.firstName, n.lastName LIMIT 3"}}}'
REQ_RES='{"jsonrpc":"2.0","id":6,"method":"resources/list"}'
REQ_TEMPLATES='{"jsonrpc":"2.0","id":7,"method":"resources/templates/list"}'
REQ_WRITE_BLOCK='{"jsonrpc":"2.0","id":8,"method":"tools/call","params":{"name":"query_cypher","arguments":{"cypher":"CREATE (x:Foo)"}}}'

# Writable smoke: SET a property on an existing Person (schema-safe even
# on a strict-schema workspace like LDBC), then restore the original value.
# The cleanup query relies on id uniqueness; picking id=65 (always present
# in /data/ldbc/mini). Callers against other workspaces should edit this.
REQ_MUTATE='{"jsonrpc":"2.0","id":9,"method":"tools/call","params":{"name":"mutate_cypher","arguments":{"cypher":"MATCH (p:Person {id:65}) SET p.browserUsed = '"'"'McpSmoke'"'"' RETURN p.id, p.browserUsed"}}}'
REQ_MUTATE_CLEANUP='{"jsonrpc":"2.0","id":10,"method":"tools/call","params":{"name":"mutate_cypher","arguments":{"cypher":"MATCH (p:Person {id:65}) SET p.browserUsed = '"'"'Firefox'"'"' RETURN p.id, p.browserUsed"}}}'

run_server() {
    local env_prefix="$1"; shift
    { echo "$REQ_INIT"; echo "$REQ_TOOLS"; echo "$REQ_LABELS"; echo "$REQ_DESC"; \
      echo "$REQ_QUERY"; echo "$REQ_RES"; echo "$REQ_TEMPLATES"; echo "$REQ_WRITE_BLOCK"; \
      if [ "$WRITE_MODE" = "1" ]; then echo "$REQ_MUTATE"; echo "$REQ_MUTATE_CLEANUP"; fi
      sleep 8; } \
      | env $env_prefix node src/server.js 2>/tmp/mcp.err \
      > /tmp/mcp.out
}

if [ "$WRITE_MODE" = "1" ]; then
    echo "=== writable mode (TURBOLYNX_ALLOW_WRITES=1) ==="
    run_server "TURBOLYNX_ALLOW_WRITES=1"
else
    echo "=== read-only mode ==="
    run_server ""
fi

echo "=== stdout (JSON-RPC responses) ==="
cat /tmp/mcp.out
echo "=== stderr (last 3 lines) ==="
tail -3 /tmp/mcp.err
