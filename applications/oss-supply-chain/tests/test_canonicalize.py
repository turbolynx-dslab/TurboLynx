"""Unit tests for the canonicalizer (M1 acceptance A5)."""
from __future__ import annotations

from oss_supply_chain.canonicalize import canonicalize


def test_empty_rowset() -> None:
    assert canonicalize([]) == []


def test_null_values_normalized() -> None:
    rows = [(1, None), (None, 2), (1, None)]
    got = canonicalize(rows)
    assert got == sorted(got, key=lambda t: tuple((type(v).__name__, repr(v)) for v in t))
    assert (None, 2) in got
    assert (1, None) in got


def test_float_rounding_stable() -> None:
    rows = [(1.0000000001,), (1.0000000002,)]
    got = canonicalize(rows)
    assert got == [(1.0,), (1.0,)]


def test_mixed_int_float_columns() -> None:
    rows = [(1, 2.5), (2, 1.5), (1, 2.5)]
    got = canonicalize(rows)
    assert len(got) == 3
    assert got[0] == (1, 2.5)


def test_deterministic_sort_with_nones() -> None:
    rows_a = [(None, "x"), (1, None), ("y", None)]
    rows_b = [("y", None), (None, "x"), (1, None)]
    assert canonicalize(rows_a) == canonicalize(rows_b)


def test_bytes_decoded_to_str() -> None:
    rows = [(b"hello", 1)]
    got = canonicalize(rows)
    assert got == [("hello", 1)]
