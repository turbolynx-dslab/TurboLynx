"""Smoke tests for the cloud attack-graph application."""

from __future__ import annotations

from pathlib import Path

import pytest

from cloud_attack_graph.canonicalize import canonicalize
from cloud_attack_graph.cli_harness import run_query
from cloud_attack_graph.loader import connect


EXPECTED_NODE_COUNT = 15

LABEL_COUNTS = {
    "Identity": 5,
    "Resource": 4,
    "Secret": 3,
    "Workload": 3,
}


def _fetch(workspace: Path, cypher: str) -> list[tuple]:
    conn = connect(workspace)
    try:
        return [tuple(r) for r in conn.execute(cypher).fetchall()]
    finally:
        conn.close()


def _stringify(rows: list[tuple]) -> list[tuple]:
    return [tuple("" if v is None else str(v) for v in r) for r in rows]


def test_python_cli_differential_scalar(workspace: Path) -> None:
    cypher = "RETURN 7 AS seven, 'cloud' AS domain;"
    py_rows = _fetch(workspace, cypher)
    cli_rows = run_query(workspace, cypher)
    assert canonicalize(_stringify(py_rows)) == canonicalize(_stringify(cli_rows))
    assert canonicalize(_stringify(py_rows)) == [("7", "cloud")]


def test_python_api_total_count(workspace: Path) -> None:
    rows = _fetch(workspace, "MATCH (n) RETURN count(n) AS c;")
    assert len(rows) == 1
    assert int(rows[0][0]) == EXPECTED_NODE_COUNT


@pytest.mark.parametrize("label, expected", sorted(LABEL_COUNTS.items()))
def test_python_api_per_label_count(workspace: Path, label: str, expected: int) -> None:
    rows = _fetch(workspace, f"MATCH (n:{label}) RETURN count(n) AS c;")
    assert len(rows) == 1
    assert int(rows[0][0]) == expected


def test_python_cli_differential_total_count(workspace: Path) -> None:
    cypher = "MATCH (n) RETURN count(n) AS c;"
    py_rows = _fetch(workspace, cypher)
    cli_rows = run_query(workspace, cypher)
    py_normalized = canonicalize([(int(r[0]),) for r in py_rows])
    cli_normalized = canonicalize([(int(r[0]),) for r in cli_rows])
    assert py_normalized == cli_normalized
    assert py_normalized == [(EXPECTED_NODE_COUNT,)]
