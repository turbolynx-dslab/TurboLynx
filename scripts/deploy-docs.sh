#!/bin/bash
set -e
cd "$(git -C "$(dirname "$0")/.." rev-parse --show-toplevel)"

echo "=== 1. Build mkdocs ==="
mkdocs build

echo "=== 2. Build demo (Next.js static export) ==="
(cd demo/app && npx next build)
cp -r demo/app/out/ site/demo/
touch site/.nojekyll

echo "=== 3. Deploy to gh-pages (turbolynx remote) ==="
TMPDIR=$(mktemp -d)
cp -r site/* "$TMPDIR/"
cp site/.nojekyll "$TMPDIR/"

cd "$TMPDIR"
git init
git checkout -b gh-pages
git add -A
git commit -m "deploy: docs + demo app"
git remote add turbolynx git@github.com:turbolynx-dslab/TurboLynx.git
git push turbolynx gh-pages --force

rm -rf "$TMPDIR"
echo "=== Done! Site: https://turbolynx.io/ ==="
