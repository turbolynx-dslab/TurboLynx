"""Golden-file correctness tests for committed scenarios."""

from __future__ import annotations

from pathlib import Path

import pytest

from cloud_attack_graph.canonicalize import canonicalize
from cloud_attack_graph.scenarios import credential_blast_radius


def _read_expected(name: str) -> list[tuple[str, ...]]:
    path = Path(__file__).resolve().parents[1] / "queries" / f"{name}.expected"
    lines = [ln for ln in path.read_text(encoding="utf-8").splitlines() if ln]
    if not lines:
        return []
    return [tuple(ln.split(",")) for ln in lines[1:]]


def _stringify(rows):
    return [tuple("" if v is None else str(v) for v in r) for r in rows]


def test_credential_blast_radius_golden(workspace: Path) -> None:
    rows = credential_blast_radius.run(workspace, secret_name="public-api-token", max_hops=2)
    actual = _stringify([(r["actor"], r["resource"], r["kind"]) for r in rows])
    expected = _read_expected("credential_blast_radius")
    assert canonicalize(actual) == canonicalize(expected)


def test_credential_blast_radius_render_substitutes_parameters() -> None:
    from cloud_attack_graph.scenarios.credential_blast_radius import _render

    cypher = _render("ci-bot-token", max_hops=1)
    assert "'ci-bot-token'" in cypher
    assert "*0..1" in cypher
    assert "'public-api-token'" not in cypher
    assert "*0..2" not in cypher


def test_credential_blast_radius_render_rejects_bad_args() -> None:
    from cloud_attack_graph.scenarios.credential_blast_radius import _render

    with pytest.raises(ValueError):
        _render("", max_hops=2)
    with pytest.raises(ValueError):
        _render("public-api-token", max_hops=-1)
