#!/usr/bin/env bash
# Download a TurboLynx benchmark dataset from HuggingFace.
#
# Usage:
#   ./scripts/download_test_data.sh <hf_repo> <target_dir>
#
# Requires: huggingface_hub Python package
#   pip3 install huggingface_hub
#   huggingface-cli login   (or set HF_TOKEN env var)
#
# Example:
#   ./scripts/download_test_data.sh HuggignHajae/TurboLynx-LDBC-SF1 /source-data/ldbc/sf1

set -euo pipefail

HF_REPO="${1:?Usage: $0 <hf_repo> <target_dir>}"
TARGET_DIR="${2:?Usage: $0 <hf_repo> <target_dir>}"

echo "[download] Fetching ${HF_REPO} -> ${TARGET_DIR}"
mkdir -p "${TARGET_DIR}"

python3 - <<EOF
from huggingface_hub import snapshot_download
snapshot_download(
    repo_id="${HF_REPO}",
    repo_type="dataset",
    local_dir="${TARGET_DIR}",
    ignore_patterns=["*.git*", "*.md"],
)
print("[download] Done: ${TARGET_DIR}")
EOF
