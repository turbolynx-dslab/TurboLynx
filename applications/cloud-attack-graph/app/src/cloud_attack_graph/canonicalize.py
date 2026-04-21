"""Canonicalize query rowsets for deterministic comparison."""

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
    return tuple((type(v).__name__, repr(v)) for v in row)


def canonicalize(rows: Iterable[Iterable[Any]]) -> list[tuple]:
    canonized = [canonical_row(r) for r in rows]
    canonized.sort(key=_sort_key)
    return canonized
