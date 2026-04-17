# TODO — OSS Supply-Chain Graph

> Derived from `plan.md`. One line per actionable item.
> Update `[ ]` → `[x]` when the owning PR merges.

## M0 — Foundation

- [x] Write `SPEC.md`
- [x] Write `tasks/plan.md`
- [x] Write `tasks/todo.md`
- [x] Write `applications/README.md`
- [x] Create empty scaffold dirs with `.gitkeep`
      (`data/`, `etl/`, `etl/cypher/`, `queries/`,
      `app/src/oss_supply_chain/`, `app/src/oss_supply_chain/scenarios/`,
      `tests/`, `tests/fixtures/`, `benchmarks/`)
- [x] Write `applications/oss-supply-chain/README.md` quickstart stub
- [x] Add `.gitignore` entries (`data/raw/`, `data/csv/`, workspace dirs,
      Python build artefacts, `.pytest_cache`; whitelist fixture CSVs)
- [x] Amend SPEC §5 to reflect real `turbolynx import --nodes ... --relationships ...`
      flags and real `turbolynx shell --query` / `.read` options
- [x] Create branch `app-oss-supply-chain` (based on `turbolynx-dslab/main`)
- [x] Commit and push to `turbolynx-dslab` remote (commit `cdc62e0f5`)
- [ ] **CHECKPOINT M0** — user approves SPEC + plan before Slice 1 starts

## M1 — Vertical smoke test

- [x] Verify macOS build — `build-portable/tools/turbolynx --help` succeeds
- [x] Verify Python wheel — `import turbolynx; turbolynx.__version__ == "0.0.1"`
      (requires app `__init__.py` TBB preload shim — see module docstring)
- [x] Author minimal fixture CSVs under `tests/fixtures/`
      (6 vertex files, 6 edge forward + 6 edge backward = 18 files, 25 nodes)
- [x] Implement `app/pyproject.toml` + package skeleton (src layout)
- [x] Implement `loader.py` — `load_fixture(workspace, fixture_dir)` builds
      the correct `turbolynx import --nodes/--relationships` invocation
- [x] Implement `canonicalize.py` with unit tests for NULL, empty, mixed numeric
      (`tests/test_canonicalize.py` — 6/6 passing)
- [x] Implement `cli_harness.py` using `turbolynx shell --query-file` path
- [x] Implement `tests/conftest.py` session-scoped workspace fixture
- [x] Implement `tests/test_smoke.py` — count + differential parity
- [x] Run `pytest applications/oss-supply-chain/tests -v` → all green
- [x] Document CLI harness decision in module docstring
- [x] Resolve SPEC OQ-2 in `data/schema.md`
- [ ] **CHECKPOINT M1** — user approves before S1 starts

## M2 — Scenario S1 blast radius

- [ ] Extend fixture with CVE chain (≥ 3 distinct root packages downstream)
- [ ] Write `queries/blast_radius.cypher`
- [ ] Generate `queries/blast_radius.expected` (review manually)
- [ ] Implement `scenarios/blast_radius.py`
- [ ] Add S1 case to `test_correctness.py` and `test_differential.py`
- [ ] S1.1 — deterministic top-10 over 3 runs
- [ ] S1.2 — Python↔CLI byte-identical after canonicalize
- [ ] S1.3 — runtime < 60 s on fixture (trivially; real gate in M6)
- [ ] **CHECKPOINT M2** — user approves

## M3 — Scenario S2 typosquat

- [ ] Extend fixture with 2–3 near-name pairs (some sharing maintainers, some not)
- [ ] Pick Levenshtein implementation (pure-Python recommended)
- [ ] Write `queries/typosquat.cypher`
- [ ] Generate `queries/typosquat.expected`
- [ ] Implement `scenarios/typosquat.py` with client-side distance filter
- [ ] Add S2 test cases
- [ ] S2.1 — deterministic result
- [ ] S2.2 — Python↔CLI parity on the Cypher prefilter rowset
- [ ] S2.3 — distance computation documented as client-side
- [ ] Resolve SPEC OQ-3 in PR description
- [ ] **CHECKPOINT M3** — user approves

## M4 — Scenario S3 bus-factor

- [ ] Add synthetic `commit_count` property on `MAINTAINED_BY` edges in fixture
- [ ] Update `data/schema.md` with new property
- [ ] Write `queries/bus_factor.cypher`
- [ ] Generate `queries/bus_factor.expected`
- [ ] Implement `scenarios/bus_factor.py`
- [ ] Add S3 test cases
- [ ] S3.1 — deterministic ranked list
- [ ] S3.2 — Python↔CLI parity
- [ ] S3.3 — empty-history slice returns empty set, not error
- [ ] **CHECKPOINT M4** — user approves

## M5 — Scenario S4 license contamination

- [ ] Extend fixture with 2–3 permissive→copyleft paths at varying depth
- [ ] Write `queries/license_contamination.cypher`
- [ ] Generate `queries/license_contamination.expected`
- [ ] Implement `scenarios/license_contamination.py`
- [ ] Add S4 test cases
- [ ] S4.1 — deterministic contaminated-root count
- [ ] S4.2 — Python↔CLI parity
- [ ] S4.3 — ten hand-picked seeds produce expected path prefixes
- [ ] **CHECKPOINT M5** — all four scenarios green on fixture

## M6 — Real data Small workspace (post-M5, optional)

- [ ] 6a. `data/fetch_osv.py` — npm advisory zip, resumable
- [ ] 6b. `data/fetch_deps_dev.py` — pick + freeze weekly snapshot date
      (resolves SPEC OQ-1 in PR description)
- [ ] 6c. `data/fetch_npm.py` — maintainer hydration for observed packages
- [ ] 6d. `etl/transform.py` — raw → label-split CSVs
- [ ] Small-scale load completes in < 2 h on M-series Mac
- [ ] Memory peak during load < 12 GB (capture and record in PR)
- [ ] Regenerate `queries/<scenario>.small.expected` for all four scenarios
- [ ] `@pytest.mark.small` gated behind `TLX_SMALL_WORKSPACE` env var
- [ ] Each scenario runs < 60 s on the Small workspace
- [ ] **CHECKPOINT M6** — user decides whether to proceed to Medium

## Backlog (not scheduled)

- [ ] Benchmarks in `benchmarks/`
- [ ] Medium-scale expansion (PyPI + Cargo)
- [ ] Stress / long-running / memory-profiled tests
- [ ] UDF-based edit-distance (revisit when engine exposes UDFs)
