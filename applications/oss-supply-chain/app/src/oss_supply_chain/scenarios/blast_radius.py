"""S1 — Blast radius of a CVE.

Returns the top-N root packages whose dependency tree transitively
contains a vulnerable version of the given CVE. See `SPEC.md §3 / S1`
for the scenario definition and `queries/blast_radius.cypher` for the
Cypher template this module renders.
"""
from __future__ import annotations

from pathlib import Path
from typing import TypedDict

from ..loader import connect


class BlastRadiusRow(TypedDict):
    package: str
    ecosystem: str


_APP_ROOT = Path(__file__).resolve().parents[4]
_QUERY_PATH = _APP_ROOT / "queries" / "blast_radius.cypher"


def load_query() -> str:
    return _QUERY_PATH.read_text(encoding="utf-8")


def _render(cve_id: str, top: int) -> str:
    """Produce a runnable Cypher body with the CVE id and LIMIT substituted.

    v0 parameterization is textual: the default CVE literal
    `'CVE-2021-44228'` in `blast_radius.cypher` is replaced with the
    caller's id, and the `LIMIT 10;` clause is replaced with the chosen
    top value. Both arguments are validated below, so the substitution
    never introduces injection-style risk even on a shared workspace.
    """
    if not isinstance(cve_id, str) or not cve_id.startswith("CVE-"):
        raise ValueError(f"cve_id must be a 'CVE-...' string: {cve_id!r}")
    if not isinstance(top, int) or top <= 0:
        raise ValueError(f"top must be a positive int: {top!r}")

    template = load_query()
    if "'CVE-2021-44228'" not in template or "LIMIT 10;" not in template:
        raise RuntimeError(
            "blast_radius.cypher no longer contains the expected "
            "substitution anchors; update scenarios/blast_radius.py "
            "in lockstep with any query edits."
        )
    return (
        template
        .replace("'CVE-2021-44228'", f"'{cve_id}'")
        .replace("LIMIT 10;", f"LIMIT {top};")
    )


def run(
    workspace: Path,
    cve_id: str = "CVE-2021-44228",
    *,
    top: int = 10,
) -> list[BlastRadiusRow]:
    """Execute the blast-radius scenario and return ranked root packages.

    Rows come back sorted by `package` ascending, deterministic across
    runs (the Cypher imposes `ORDER BY package ASC LIMIT N`).
    """
    cypher = _render(cve_id, top)
    conn = connect(workspace)
    try:
        rows = [tuple(r) for r in conn.execute(cypher).fetchall()]
    finally:
        conn.close()
    return [{"package": str(r[0]), "ecosystem": str(r[1])} for r in rows]


__all__ = ["BlastRadiusRow", "load_query", "run"]
