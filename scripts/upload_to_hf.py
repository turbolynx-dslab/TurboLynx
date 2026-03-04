#!/usr/bin/env python3
"""
Upload TurboLynx benchmark datasets to Hugging Face.

Usage:
    python3 scripts/upload_to_hf.py

Requires:
    huggingface_hub installed and logged in:
        pip3 install huggingface_hub
        huggingface-cli login   (or set HF_TOKEN env var)

Datasets uploaded:
    HuggignHajae/TurboLynx-TPCH-SF1
    HuggignHajae/TurboLynx-TPCH-SF10
    HuggignHajae/TurboLynx-LDBC-SF1
    HuggignHajae/TurboLynx-LDBC-SF10
    HuggignHajae/TurboLynx-LDBC-SF100
    HuggignHajae/TurboLynx-DBPEDIA
"""

import os
import sys
from pathlib import Path
from huggingface_hub import HfApi, login

# ── Configuration ──────────────────────────────────────────────────────────────

SOURCE_DATA_ROOT = Path("/mnt/sdc/jhha/source-data")

DATASETS = [
    {
        "repo_id":    "HuggignHajae/TurboLynx-TPCH-SF1",
        "local_dir":  SOURCE_DATA_ROOT / "tpch" / "sf1",
        "description": "TPC-H benchmark data Scale Factor 1, preprocessed for TurboLynx graph format.",
    },
    {
        "repo_id":    "HuggignHajae/TurboLynx-TPCH-SF10",
        "local_dir":  SOURCE_DATA_ROOT / "tpch" / "sf10",
        "description": "TPC-H benchmark data Scale Factor 10, preprocessed for TurboLynx graph format.",
    },
    {
        "repo_id":    "HuggignHajae/TurboLynx-LDBC-SF1",
        "local_dir":  SOURCE_DATA_ROOT / "ldbc" / "sf1",
        "description": "LDBC Social Network Benchmark data Scale Factor 1, preprocessed for TurboLynx graph format.",
    },
    {
        "repo_id":    "HuggignHajae/TurboLynx-LDBC-SF10",
        "local_dir":  SOURCE_DATA_ROOT / "ldbc" / "sf10",
        "description": "LDBC Social Network Benchmark data Scale Factor 10, preprocessed for TurboLynx graph format.",
    },
    {
        "repo_id":    "HuggignHajae/TurboLynx-LDBC-SF100",
        "local_dir":  SOURCE_DATA_ROOT / "ldbc" / "sf100",
        "description": "LDBC Social Network Benchmark data Scale Factor 100, preprocessed for TurboLynx graph format.",
    },
    {
        "repo_id":    "HuggignHajae/TurboLynx-DBPEDIA",
        "local_dir":  SOURCE_DATA_ROOT / "dbpedia",
        "description": "DBpedia dataset preprocessed for TurboLynx graph format.",
    },
]

# ── Helpers ────────────────────────────────────────────────────────────────────

def sizeof_fmt(path: Path) -> str:
    total = sum(f.stat().st_size for f in path.rglob("*") if f.is_file())
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if total < 1024:
            return f"{total:.1f} {unit}"
        total /= 1024
    return f"{total:.1f} PB"

def create_repo_if_not_exists(api: HfApi, repo_id: str, description: str):
    try:
        api.repo_info(repo_id=repo_id, repo_type="dataset")
        print(f"  [exists] {repo_id}")
    except Exception:
        print(f"  [create] {repo_id}")
        api.create_repo(
            repo_id=repo_id,
            repo_type="dataset",
            private=False,
            exist_ok=True,
        )
        # Write a minimal README card
        readme = f"---\nlicense: other\n---\n\n# {repo_id.split('/')[-1]}\n\n{description}\n"
        api.upload_file(
            path_or_fileobj=readme.encode(),
            path_in_repo="README.md",
            repo_id=repo_id,
            repo_type="dataset",
        )

# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    # Allow overriding which datasets to upload via CLI args (repo name substring)
    filters = sys.argv[1:]  # e.g. "SF1" "TPCH"

    token = os.environ.get("HF_TOKEN")
    if token:
        login(token=token, add_to_git_credential=False)

    api = HfApi()
    try:
        user = api.whoami()
        print(f"Logged in as: {user['name']}\n")
    except Exception:
        print("ERROR: Not logged in. Run `huggingface-cli login` or set HF_TOKEN env var.")
        sys.exit(1)

    for ds in DATASETS:
        repo_id   = ds["repo_id"]
        local_dir = ds["local_dir"]

        # Apply filter if provided
        if filters and not any(f.lower() in repo_id.lower() for f in filters):
            continue

        if not local_dir.exists():
            print(f"[SKIP] {repo_id}: local path not found ({local_dir})")
            continue

        size = sizeof_fmt(local_dir)
        print(f"\n{'='*60}")
        print(f"Uploading: {repo_id}")
        print(f"Source:    {local_dir}  ({size})")
        print(f"{'='*60}")

        create_repo_if_not_exists(api, repo_id, ds["description"])

        api.upload_large_folder(
            repo_id=repo_id,
            repo_type="dataset",
            folder_path=str(local_dir),
            # num_workers defaults to 5; bump up for faster uploads on fast links
            num_workers=4,
        )

        print(f"[DONE] {repo_id}")

    print("\nAll uploads complete.")

if __name__ == "__main__":
    main()
