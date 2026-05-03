"""Golden-file correctness tests — one assertion per SPEC §3 scenario.

Each test reads `queries/<scenario>.cypher`, executes it through the
Python API, canonicalizes the rowset, and compares it to
`queries/<scenario>.expected`. The expected file is canonical CSV (header
+ data, LF-terminated) matching what the CLI emits after `_strip_metadata`.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from oss_supply_chain.canonicalize import canonicalize
from oss_supply_chain.scenarios import blast_radius


def _read_expected(name: str) -> list[tuple[str, ...]]:
    path = (
        Path(__file__).resolve().parents[1] / "queries" / f"{name}.expected"
    )
    lines = [ln for ln in path.read_text(encoding="utf-8").splitlines() if ln]
    if not lines:
        return []
    # Drop the header row; keep data rows as string tuples.
    return [tuple(ln.split(",")) for ln in lines[1:]]


def _stringify(rows):
    return [tuple("" if v is None else str(v) for v in r) for r in rows]


@pytest.mark.xfail(
    strict=True,
    reason=(
        "Planner regression on chained MATCH + variable-length expand + "
        "vertex-targeted AdjIdxJoin. Tracked in issue #68 (bisected to "
        "commits 829799aa + fe4809ae). Remove this marker when the "
        "engine-side fix lands."
    ),
)
def test_blast_radius_golden(workspace: Path) -> None:
    """S1.1 — deterministic top-10 against the frozen fixture."""
    rows = blast_radius.run(workspace, cve_id="CVE-2021-44228", top=10)
    actual = _stringify([(r["package"], r["ecosystem"]) for r in rows])
    expected = _read_expected("blast_radius")
    assert canonicalize(actual) == canonicalize(expected)


# ---- Pure-function coverage ----


def test_blast_radius_render_substitutes_parameters() -> None:
    from oss_supply_chain.scenarios.blast_radius import _render

    cypher = _render("CVE-2099-00001", top=5)
    assert "'CVE-2099-00001'" in cypher
    assert "LIMIT 5;" in cypher
    assert "'CVE-2021-44228'" not in cypher
    assert "LIMIT 10;" not in cypher


def test_blast_radius_render_rejects_bad_args() -> None:
    from oss_supply_chain.scenarios.blast_radius import _render

    with pytest.raises(ValueError):
        _render("not-a-cve-id", top=10)
    with pytest.raises(ValueError):
        _render("CVE-2099-00001", top=0)
    with pytest.raises(ValueError):
        _render("CVE-2099-00001", top=-3)
