"""Smoke test — proves the full M1 pipeline end-to-end on the committed fixture.

If these tests pass, the TurboLynx engine, the Python wheel, the CLI binary,
the fixture CSVs, and the harness modules all agree on a trivial count query.
Fixture-specific expected values are hand-derived from
`tests/fixtures/nodes_*.csv`.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from oss_supply_chain.canonicalize import canonicalize
from oss_supply_chain.cli_harness import run_query
from oss_supply_chain.loader import connect


# Matches the fixture under tests/fixtures/:
# Package(6) + Version(8) + Maintainer(3) + Repository(3) + License(3) + CVE(2)
EXPECTED_NODE_COUNT = 25

LABEL_COUNTS = {
    "Package": 6,
    "Version": 8,
    "Maintainer": 3,
    "Repository": 3,
    "License": 3,
    "CVE": 2,
}


def _fetch(workspace: Path, cypher: str) -> list[tuple]:
    conn = connect(workspace)
    try:
        return [tuple(r) for r in conn.execute(cypher).fetchall()]
    finally:
        conn.close()


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
