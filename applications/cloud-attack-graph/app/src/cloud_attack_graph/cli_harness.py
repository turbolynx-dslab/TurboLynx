"""Run Cypher through the TurboLynx CLI and parse CSV output."""

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
    "[",
)


def _strip_cypher_line_comments(cypher: str) -> str:
    lines = [
        line for line in cypher.splitlines()
        if not line.lstrip().startswith("//")
    ]
    return "\n".join(lines).strip() + "\n"


def _strip_metadata(stdout: str) -> str:
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
    cypher = _strip_cypher_line_comments(
        Path(cypher_path).read_text(encoding="utf-8")
    )
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".cypher", delete=False, encoding="utf-8"
    ) as tmp:
        tmp.write(cypher)
        tmp_path = Path(tmp.name)
    try:
        return _parse_csv_stdout(_run([
            str(turbolynx_binary()),
            "shell",
            "--workspace", str(workspace),
            "--mode", "csv",
            "--query-file", str(tmp_path),
        ]))
    finally:
        tmp_path.unlink(missing_ok=True)


__all__ = ["run_query", "run_query_file"]
