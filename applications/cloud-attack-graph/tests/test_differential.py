"""Python API ↔ CLI differential tests for cloud attack-graph scenarios."""

from __future__ import annotations

from pathlib import Path

from cloud_attack_graph.canonicalize import canonicalize
from cloud_attack_graph.cli_harness import run_query_file
from cloud_attack_graph.scenarios import credential_blast_radius


def _stringify(rows):
    return [tuple("" if v is None else str(v) for v in row) for row in rows]


def test_credential_blast_radius_python_cli_parity(workspace: Path) -> None:
    py_rows = credential_blast_radius.run(workspace, secret_name="public-api-token", max_hops=2)
    py_tuples = _stringify([(r["actor"], r["resource"], r["kind"]) for r in py_rows])

    cypher_path = (
        Path(__file__).resolve().parents[1]
        / "queries"
        / "credential_blast_radius.cypher"
    )
    cli_tuples = _stringify(run_query_file(workspace, cypher_path))

    assert canonicalize(py_tuples) == canonicalize(cli_tuples)
