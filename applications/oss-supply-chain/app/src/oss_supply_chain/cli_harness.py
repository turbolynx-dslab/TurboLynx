"""Run Cypher through the `turbolynx` CLI and parse its CSV output.

Uses `turbolynx shell --query-file <path> --mode csv` so that scenario
`.cypher` files can be executed verbatim — the same files committed under
`applications/oss-supply-chain/queries/` also drive the differential test.
The single-query `--query` path is also exposed for inline cases (e.g. the
smoke test).

The shell interleaves connection banners, per-pipeline log lines, a
\"Time: ...\" summary, and a disconnect banner with the actual CSV rowset
on stdout (stderr is unused). `_strip_metadata` filters those lines so
the CSV reader only sees the header and data rows.
"""
from __future__ import annotations

import csv
import io
import subprocess
import tempfile
from pathlib import Path

from .loader import turbolynx_binary


_METADATA_LINE_PREFIXES = (
    "Database Connected",
    "Database Disconnected",
    "Time:",
    "[",  # timestamped spdlog lines: "[2026-...] [info] ..."
)


def _strip_metadata(stdout: str) -> str:
    """Drop log/metadata lines that the shell interleaves with result rows.

    Any CSV value that legitimately begins with \"[\" at column start
    would be CSV-quoted (\"[...\"), so the `[` prefix filter is safe for
    the query shapes this application uses.
    """
    kept = [
        line for line in stdout.splitlines()
        if line and not line.startswith(_METADATA_LINE_PREFIXES)
    ]
    return "\n".join(kept)


def _parse_csv_stdout(stdout: str) -> list[tuple[str, ...]]:
    cleaned = _strip_metadata(stdout)
    reader = csv.reader(io.StringIO(cleaned))
    rows = list(reader)
    if not rows:
        return []
    # First non-metadata line is the header; drop it.
    return [tuple(r) for r in rows[1:]]


def _run(args: list[str]) -> str:
    completed = subprocess.run(args, check=False, capture_output=True, text=True)
    if completed.returncode != 0:
        raise RuntimeError(
            "turbolynx shell failed "
            f"(exit {completed.returncode})\n"
            f"command: {' '.join(args)}\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )
    return completed.stdout


def run_query(workspace: Path, cypher: str) -> list[tuple[str, ...]]:
    """Execute a single Cypher statement via `turbolynx shell --query`."""
    statement = cypher.strip()
    if not statement.endswith(";"):
        statement += ";"
    stdout = _run([
        str(turbolynx_binary()),
        "shell",
        "--workspace", str(workspace),
        "--mode", "csv",
        "--query", statement,
    ])
    return _parse_csv_stdout(stdout)


def run_query_file(workspace: Path, cypher_path: Path) -> list[tuple[str, ...]]:
    """Execute the Cypher statements in `cypher_path` via `turbolynx shell -f`.

    The file may contain multiple `;`-terminated statements; only the
    rowset of the final statement is returned, matching how golden files
    capture the final projection of a scenario.
    """
    stdout = _run([
        str(turbolynx_binary()),
        "shell",
        "--workspace", str(workspace),
        "--mode", "csv",
        "--query-file", str(cypher_path),
    ])
    return _parse_csv_stdout(stdout)


def run_cypher(workspace: Path, cypher: str) -> list[tuple[str, ...]]:
    """Execute a Cypher block by writing it to a temp file and using `-f`.

    Prefers the file path over `--query` when the statement is multi-line
    or mixed with comments — keeps shell quoting out of the picture.
    """
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".cypher", delete=False, encoding="utf-8"
    ) as tmp:
        tmp.write(cypher)
        if not cypher.rstrip().endswith(";"):
            tmp.write(";\n")
        tmp_path = Path(tmp.name)
    try:
        return run_query_file(workspace, tmp_path)
    finally:
        tmp_path.unlink(missing_ok=True)


__all__ = ["run_query", "run_query_file", "run_cypher"]
