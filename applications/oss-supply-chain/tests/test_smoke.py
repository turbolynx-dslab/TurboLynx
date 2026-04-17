"""Smoke tests — prove the M1 pipeline end-to-end on the committed fixture.

Two categories here:

* **Scalar** — storage-independent `RETURN` expressions. These exercise
  every harness layer (workspace load, Python API connect + execute, CLI
  subprocess, metadata-stripping stdout parser, canonicalizer) without
  touching node storage, so they pass today despite issue
  [#61](https://github.com/turbolynx-dslab/TurboLynx/issues/61) (ORCA
  teardown segfault on macOS MATCH queries).
* **MATCH-based** — count queries over the loaded graph. These are the
  originally-specified M1 acceptance tests. They are currently
  `@pytest.mark.skip`-ped pending the engine fix for issue #61; once the
  crash is resolved they should run unchanged.

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

_B1_SKIP_REASON = (
    "blocked by turbolynx-dslab/TurboLynx#61 — macOS ORCA teardown segfault on any "
    "MATCH query; unskip once the upstream fix lands and `pytest` will run unchanged."
)


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


# ---------- Scalar (storage-independent, unaffected by #61) ----------

def test_python_cli_differential_scalar(workspace: Path) -> None:
    """Proves workspace load → Python API → CLI → parser all agree on a scalar.

    Uses `RETURN 1 AS one, 'hi' AS greeting;` which never traverses node
    storage, so the crash in `gpos::CAutoMemoryPool::~CAutoMemoryPool`
    (issue #61) does not fire.
    """
    cypher = "RETURN 1 AS one, 'hi' AS greeting;"
    py_rows = _fetch(workspace, cypher)
    cli_rows = run_query(workspace, cypher)

    assert canonicalize(_stringify(py_rows)) == canonicalize(_stringify(cli_rows))
    assert canonicalize(_stringify(py_rows)) == [("1", "hi")]


# ---------- MATCH-based (target acceptance, currently blocked) ----------

@pytest.mark.skip(reason=_B1_SKIP_REASON)
def test_python_api_total_count(workspace: Path) -> None:
    rows = _fetch(workspace, "MATCH (n) RETURN count(n) AS c;")
    assert len(rows) == 1
    assert int(rows[0][0]) == EXPECTED_NODE_COUNT


@pytest.mark.skip(reason=_B1_SKIP_REASON)
@pytest.mark.parametrize("label, expected", sorted(LABEL_COUNTS.items()))
def test_python_api_per_label_count(workspace: Path, label: str, expected: int) -> None:
    rows = _fetch(workspace, f"MATCH (n:{label}) RETURN count(n) AS c;")
    assert len(rows) == 1
    assert int(rows[0][0]) == expected


@pytest.mark.skip(reason=_B1_SKIP_REASON)
def test_python_cli_differential_total_count(workspace: Path) -> None:
    cypher = "MATCH (n) RETURN count(n) AS c;"
    py_rows = _fetch(workspace, cypher)
    cli_rows = run_query(workspace, cypher)

    py_normalized = canonicalize([(int(r[0]),) for r in py_rows])
    cli_normalized = canonicalize([(int(r[0]),) for r in cli_rows])
    assert py_normalized == cli_normalized
    assert py_normalized == [(EXPECTED_NODE_COUNT,)]
