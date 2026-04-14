#!/bin/bash
# Smoke-test the MCP server by piping JSON-RPC init + tools/call through stdio.
set -e
export TURBOLYNX_WORKSPACE="${TURBOLYNX_WORKSPACE:-/data/ldbc/mini}"

REQ_INIT='{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"0"}}}'
REQ_LIST='{"jsonrpc":"2.0","id":2,"method":"tools/list"}'
REQ_LABELS='{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"list_labels","arguments":{}}}'
REQ_DESC='{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"describe_label","arguments":{"label":"Person"}}}'
REQ_QUERY='{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"query_cypher","arguments":{"cypher":"MATCH (n:Person) RETURN n.firstName, n.lastName LIMIT 3"}}}'
REQ_RES='{"jsonrpc":"2.0","id":6,"method":"resources/list"}'
REQ_WRITE='{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"query_cypher","arguments":{"cypher":"CREATE (x:Foo)"}}}'

{ echo "$REQ_INIT"; echo "$REQ_LIST"; echo "$REQ_LABELS"; echo "$REQ_DESC"; echo "$REQ_QUERY"; echo "$REQ_RES"; echo "$REQ_WRITE"; sleep 8; } \
  | node src/server.js 2>/tmp/mcp.err \
  > /tmp/mcp.out
echo "=== stdout (JSON-RPC responses) ==="
cat /tmp/mcp.out
echo "=== stderr (last 3 lines) ==="
tail -3 /tmp/mcp.err
