"""Smoke tests proving the M1 pipeline end-to-end on the committed fixture.

These tests exercise workspace load, Python API execution, CLI subprocess
execution, metadata-stripping stdout parsing, canonicalization, and simple
MATCH scans over the loaded graph. Fixture-specific expected values are
hand-derived from `tests/fixtures/nodes_*.csv`.
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


def _stringify(rows: list[tuple]) -> list[tuple]:
    """Normalize rowset element types to string.

    The CLI always emits strings (CSV); the Python API returns typed values.
    Stringifying both sides lets the canonicalizer compare them directly.
    """
    return [tuple("" if v is None else str(v) for v in r) for r in rows]


# ---------- Scalar storage-independent baseline ----------

def test_python_cli_differential_scalar(workspace: Path) -> None:
    """Proves workspace load → Python API → CLI → parser all agree on a scalar.

    Uses `RETURN 1 AS one, 'hi' AS greeting;` as a storage-independent
    baseline before the MATCH-based checks below.
    """
    cypher = "RETURN 1 AS one, 'hi' AS greeting;"
    py_rows = _fetch(workspace, cypher)
    cli_rows = run_query(workspace, cypher)

    assert canonicalize(_stringify(py_rows)) == canonicalize(_stringify(cli_rows))
    assert canonicalize(_stringify(py_rows)) == [("1", "hi")]


# ---------- MATCH-based target acceptance ----------

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
