"""S1 — blast radius from a leaked secret."""

from __future__ import annotations

from pathlib import Path
from typing import TypedDict

from ..loader import connect


class BlastRadiusRow(TypedDict):
    actor: str
    resource: str
    kind: str


_APP_ROOT = Path(__file__).resolve().parents[4]
_QUERY_PATH = _APP_ROOT / "queries" / "credential_blast_radius.cypher"


def load_query() -> str:
    return _QUERY_PATH.read_text(encoding="utf-8")


def _render(secret_name: str, *, max_hops: int = 2) -> str:
    if not isinstance(secret_name, str) or not secret_name:
        raise ValueError("secret_name must be a non-empty string")
    if not isinstance(max_hops, int) or max_hops < 0:
        raise ValueError("max_hops must be a non-negative integer")

    template = load_query()
    if "'public-api-token'" not in template or "*0..2" not in template:
        raise RuntimeError(
            "credential_blast_radius.cypher no longer contains the expected "
            "substitution anchors; update the scenario wrapper in lockstep."
        )
    return (
        template
        .replace("'public-api-token'", f"'{secret_name}'")
        .replace("*0..2", f"*0..{max_hops}")
    )


def run(workspace: Path, secret_name: str = "public-api-token", *, max_hops: int = 2) -> list[BlastRadiusRow]:
    cypher = _render(secret_name, max_hops=max_hops)
    conn = connect(workspace)
    try:
        rows = [tuple(r) for r in conn.execute(cypher).fetchall()]
    finally:
        conn.close()
    return [
        {"actor": str(r[0]), "resource": str(r[1]), "kind": str(r[2])}
        for r in rows
    ]


__all__ = ["BlastRadiusRow", "load_query", "run"]
