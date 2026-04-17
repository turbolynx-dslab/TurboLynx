"""Python API ↔ CLI differential tests.

Each scenario's Cypher is executed through two independent client
paths — `turbolynx.connect(...).execute(...)` and
`turbolynx shell --query-file ... --mode csv` — and the canonicalized
rowsets are compared. Any divergence points at a binding-layer issue
rather than a Cypher / planner / executor issue, since the same engine
runs both paths.

As with `test_correctness.py`, every scenario currently traverses node
storage and is therefore `@pytest.mark.skip`-ped behind issue #61
until the macOS ORCA teardown crash is fixed.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from oss_supply_chain.canonicalize import canonicalize
from oss_supply_chain.cli_harness import run_query_file
from oss_supply_chain.scenarios import blast_radius


_B1_SKIP = pytest.mark.skip(
    reason="blocked by turbolynx-dslab/TurboLynx#61 (ORCA teardown segfault on MATCH)"
)


def _stringify(rows):
    return [tuple("" if v is None else str(v) for v in r) for r in rows]


@_B1_SKIP
def test_blast_radius_python_cli_parity(workspace: Path) -> None:
    """S1.2 — Python API and CLI produce byte-identical rowsets."""
    py_rows = blast_radius.run(workspace, cve_id="CVE-2021-44228", top=10)
    py_tuples = _stringify([(r["package"], r["ecosystem"]) for r in py_rows])

    cypher_path = (
        Path(__file__).resolve().parents[1]
        / "queries"
        / "blast_radius.cypher"
    )
    cli_tuples = _stringify(run_query_file(workspace, cypher_path))

    assert canonicalize(py_tuples) == canonicalize(cli_tuples)
