# L1 fuzz seed corpus

Each file is one Cypher input.  libFuzzer mutates these byte-wise to drive
the parser / binder / planner / executor.  Files **need not be syntactically
valid** — well-formed seeds give the mutator a head start, and known-broken
shapes (past crashes) are the most useful starting points.

Naming convention: `<source>_<short-tag>.cypher`.

Categories:
- `regression_*` — exact (or simplified) repro of a previously closed crash.
  Always replayed first by every fuzz run so we never regress.
- `wellformed_*` — well-formed shapes (CREATE / MATCH / RETURN) used as
  mutation seeds.

Sibling directory `../known_bugs/` holds inputs that surface real ASAN
findings but where the underlying bug is **not yet fixed**.  Those are
not run by the smoke target; see `../known_bugs/README.md` for the list
and stack tips.

To add a new regression seed when a new crash is fixed:
1. Reduce the failing query to the smallest snippet that still reproduces.
2. Drop it here as `regression_<issue>.cypher`.
3. Verify `fuzz_l1` runs the corpus and the new file does not crash on
   `HEAD` (i.e. the fix is in).
