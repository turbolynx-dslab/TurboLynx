#!/bin/bash
# Serve docs locally with WASM playground + LDBC workspace
# Usage: bash docs/serve-playground.sh [port]
set -e

PORT=${1:-8080}
DOCS_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKSPACE_DIR="/data/ldbc/sf1"

# Create symlink for workspace access
mkdir -p "$DOCS_DIR/assets/workspace"
for f in catalog.bin catalog_version .store_meta store.db; do
    ln -sf "$WORKSPACE_DIR/$f" "$DOCS_DIR/assets/workspace/$f" 2>/dev/null || true
done

echo "Serving docs at http://localhost:$PORT"
echo "WASM assets: $DOCS_DIR/assets/wasm/"
echo "Workspace: $DOCS_DIR/assets/workspace/ -> $WORKSPACE_DIR"
echo ""
echo "Open http://localhost:$PORT/index.html in your browser"
echo ""

cd "$DOCS_DIR"
python3 -m http.server "$PORT" --bind 0.0.0.0
