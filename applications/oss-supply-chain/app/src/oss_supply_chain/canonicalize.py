"""Canonicalize query rowsets for deterministic cross-run / cross-client comparison.

The Python API and the CLI do not both guarantee row order for un-ORDERed
queries and may return numeric columns with minor float noise. `canonicalize`
produces a stable, sorted list of tuples suitable for golden-file and
differential comparisons.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any, Iterable


_FLOAT_PRECISION = 9


def _canonical_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bool):
        return bool(value)
    if isinstance(value, float):
        return round(value, _FLOAT_PRECISION)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def canonical_row(row: Iterable[Any]) -> tuple:
    return tuple(_canonical_value(v) for v in row)


def _sort_key(row: tuple) -> tuple:
    # repr() gives a total order across heterogeneous Python types and
    # distinguishes None ("None") from "None" the string via the type tag.
    return tuple((type(v).__name__, repr(v)) for v in row)


def canonicalize(rows: Iterable[Iterable[Any]]) -> list[tuple]:
    """Return rows sorted deterministically and with values normalized."""
    canonized = [canonical_row(r) for r in rows]
    canonized.sort(key=_sort_key)
    return canonized
