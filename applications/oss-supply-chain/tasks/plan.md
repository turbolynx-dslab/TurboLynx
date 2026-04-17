# Implementation Plan — OSS Supply-Chain Graph

> Status: **DRAFT — awaiting user review**
> Derived from: `applications/oss-supply-chain/SPEC.md` v0.1
> Mode: read-only plan; no code changes until user confirms

## 1. Intent

Deliver SPEC milestones M0 → M5 via **vertical slices**. Each task is a thin but
complete end-to-end path (data → load → query → test), not a horizontal layer.
The first slice is a smoke test on a tiny hand-written fixture; every subsequent
slice adds one full scenario (Cypher + goldens + correctness test + Python↔CLI
differential test) on top of the same fixture. Real-data ingestion
(deps.dev / OSV / npm) is an **orthogonal track** gated after all four
scenarios pass on the fixture.

Rationale for this ordering:

- **Proves the full pipeline once, early.** If anything is wrong
  (`turbolynx import` CSV quirks, wheel install path on macOS, golden diff
  canonicalization), we find it on slice 1 when total investment is ~50 LOC.
- **De-risks the CLI harness.** The CLI has no `--file` flag — scripts run via
  `.read <file>` or `--query "<single>"`. The first slice forces us to commit to
  one before any scenario depends on it.
- **Keeps PRs small.** One scenario per PR, each self-contained, each mergeable
  independently.
- **Real data is decoupled.** The fixture is shaped to match the eventual
  schema, so scenario code written against the fixture keeps working when the
  Small workspace comes online.

## 2. Dependency graph

```
                         ┌────────────────────┐
                         │ A. Scaffold + spec │  ← M0 gate (this PR)
                         └─────────┬──────────┘
                                   ▼
                         ┌────────────────────┐
                         │ B. Build verified  │  macOS build-portable + wheel
                         └─────────┬──────────┘
                                   ▼
                         ┌────────────────────┐
                         │ C. Fixture CSV     │  tiny deterministic graph
                         └─────────┬──────────┘
                                   ▼
                         ┌────────────────────┐
                         │ D. Load + harness  │  import → workspace, Python +
                         │    + smoke test    │  CLI harness, canonicalizer,
                         │                    │  one trivial query green
                         └─────────┬──────────┘
                                   ▼  ← CHECKPOINT M1: vertical slice green
                ┌──────────┬───────┼───────┬──────────┐
                ▼          ▼       ▼       ▼          ▼
              S1         S2      S3      S4         (parallelizable after M1)
            blast      typo    bus    license
            radius    squat   factor  contam.
                │          │       │       │
                └──────────┴───────┴───────┘
                            │
                            ▼   ← CHECKPOINT M5: all scenarios green on fixture
                         ┌────────────────────┐
                         │ E. Real data track │  deps.dev + OSV + npm → Small
                         └─────────┬──────────┘
                                   ▼   ← optional, separate decision
                         ┌────────────────────┐
                         │ F. Benchmarks /    │  deferred per SPEC §7.4
                         │    Medium scale    │
                         └────────────────────┘
```

Notes on the graph:
- **A → B** is the only strict blocker chain before scenarios. Every scenario
  depends on the smoke test (D), not on the other scenarios.
- **S1–S4 are parallelizable** after M1, but shipped sequentially to keep PR
  review focused (SPEC §10).
- **E (real data)** runs entirely after M5. It reuses the same queries; only
  goldens need regeneration against the Small workspace.

## 3. Vertical slices

Each slice is one PR. Each PR must leave `main` green on macOS.

### Slice 0 — M0 foundation (this PR)
**Goal:** Confirm scope and lay the directory skeleton so the rest of the work
has a home.

Already landed in working tree:
- `applications/README.md`
- `applications/oss-supply-chain/SPEC.md`
- `applications/oss-supply-chain/tasks/plan.md` (this file)
- `applications/oss-supply-chain/tasks/todo.md`

Still pending for M0 close:
- Empty scaffold directories with `.gitkeep`
- `applications/oss-supply-chain/README.md` quickstart stub
- `.gitignore` entries for `data/raw/`, workspace dirs, Python build artefacts
- Branch `app-oss-supply-chain` created; first commit pushed to
  `turbolynx-dslab` remote

**Acceptance:** Repo tree matches SPEC §8 with empty subdirectories; user
approves SPEC + plan. No TurboLynx code is executed yet.

**Verification:** `tree applications/oss-supply-chain -L 2` shows full layout;
`git status` is clean after commit.

---

### Slice 1 — M1 vertical smoke test
**Goal:** Prove the full pipeline (build → import → Python query → CLI query →
golden diff → differential diff) on a committed 10–30 node fixture. No
scenario logic yet — a `MATCH (n) RETURN count(n);` style check is enough.

**Files touched**
- `applications/oss-supply-chain/tests/fixtures/`
  - `nodes_package.csv`, `nodes_version.csv`, `nodes_maintainer.csv`,
    `nodes_license.csv`, `nodes_cve.csv`, `nodes_repository.csv`
  - `edges_has_version.csv`, `edges_depends_on.csv`, `edges_maintained_by.csv`,
    `edges_declares.csv`, `edges_affected_by.csv`, `edges_hosted_at.csv`
- `applications/oss-supply-chain/app/pyproject.toml`
- `applications/oss-supply-chain/app/src/oss_supply_chain/__init__.py`
- `applications/oss-supply-chain/app/src/oss_supply_chain/loader.py`
  - `load_fixture(workspace_dir: Path, fixture_dir: Path) -> None`
  - thin `connect(workspace)` wrapper
- `applications/oss-supply-chain/app/src/oss_supply_chain/canonicalize.py`
  - `canonical(rows) -> list[tuple]`  (stable sort, numeric rounding, NULL
    normalization)
- `applications/oss-supply-chain/app/src/oss_supply_chain/cli_harness.py`
  - `run_cypher_via_shell(workspace, cypher_text) -> list[tuple]`
  - Uses `turbolynx shell --workspace W --query "..." --mode csv` when a single
    statement; `.read` via stdin piping otherwise. Decide + document in this
    slice.
- `applications/oss-supply-chain/tests/conftest.py`
  - `@pytest.fixture(scope="session")` builds a temp workspace, imports the
    fixture CSVs, yields the path; tears down on session end.
- `applications/oss-supply-chain/tests/test_smoke.py`
  - Test 1: `MATCH (n) RETURN count(n);` equals hand-computed count
  - Test 2: Differential — same query via Python API and CLI yields identical
    canonicalized rowset

**Acceptance (M1 CHECKPOINT)**
- A1. `pytest applications/oss-supply-chain/tests -v` is green on macOS.
- A2. Both Python and CLI paths return the expected count.
- A3. `conftest.py` tears down the temp workspace cleanly (no leftover dirs).
- A4. Fixture CSV headers pass `turbolynx import` without warnings at
  `--log-level warn`.
- A5. Canonicalization handles: empty rowset, NULL values, mixed int/float
  columns — each has a unit test.

**Verification steps**
1. `cmake --build build-portable` (should be no-op if already built)
2. `python3 -m pip install -e applications/oss-supply-chain/app`
3. `pytest applications/oss-supply-chain/tests -v`
4. Read `tests/conftest.py` to confirm no hard-coded `/tmp` paths that could
   collide across concurrent runs

**Resolves open questions**
- OQ-2 (SPEC §11): concrete `turbolynx import` invocation committed in
  `loader.py`. If the per-label pair form is verbose, a helper in `loader.py`
  composes it from `fixture_dir`.
- CLI harness shape: documented in `cli_harness.py` module docstring with
  the decision rationale.

---

### Slice 2 — M2 Scenario S1: blast radius
**Goal:** First real analytical scenario end-to-end on the fixture.

**Files touched**
- `applications/oss-supply-chain/queries/blast_radius.cypher`
  - Parameterized conceptually, but uses fixed literal for CVE id in v0 (no
    prepared-statement support required)
  - Uses `-[:DEPENDS_ON*1..D]-` with D=5 default
- `applications/oss-supply-chain/queries/blast_radius.expected`
  - Canonicalized output for the fixture; regenerated only on intentional
    cypher change
- `applications/oss-supply-chain/app/src/oss_supply_chain/scenarios/blast_radius.py`
  - `run(workspace, cve_id: str, top: int = 10, max_depth: int = 5) -> list[dict]`
- `applications/oss-supply-chain/tests/test_correctness.py`  (adds S1)
- `applications/oss-supply-chain/tests/test_differential.py` (adds S1)

The fixture gets a small embedded CVE chain so the answer is non-trivial
(≥ 3 distinct root packages).

**Acceptance (S1 per SPEC §3)**
- S1.1 — Deterministic top-10 across 3 Python-API runs
- S1.2 — CLI byte-identical rowset after canonicalization
- S1.3 — Runs under 60 s on fixture (trivially true; the real budget applies
  to Small workspace in Slice 6)

**Verification**
1. `pytest applications/oss-supply-chain/tests/test_correctness.py::test_blast_radius -v`
2. `pytest applications/oss-supply-chain/tests/test_differential.py::test_blast_radius -v`
3. Inspect `blast_radius.expected` diff in the PR — must be intentional.

---

### Slice 3 — M3 Scenario S2: typosquat
**Goal:** Second scenario. Introduces client-side edit-distance because the
Cypher subset (SPEC §3/S2.3) lacks a push-down UDF surface in v0.

**Files touched**
- `applications/oss-supply-chain/queries/typosquat.cypher`
  - Returns candidate name pairs + maintainer sets + publish timestamps
- `applications/oss-supply-chain/queries/typosquat.expected`
- `applications/oss-supply-chain/app/src/oss_supply_chain/scenarios/typosquat.py`
  - Applies Levenshtein ≤ 2 filter in Python after the Cypher prefilter
  - Dependency: `rapidfuzz` (small, pure Python fallback available)
- Tests additions as in S1

**Acceptance**
- S2.1–S2.3 per SPEC §3

**Verification**
- pytest additions pass.
- PR description records the OQ-3 resolution: client-side Levenshtein for v0,
  revisit when TurboLynx exposes UDFs.

---

### Slice 4 — M4 Scenario S3: bus-factor
**Goal:** Grouped aggregation + multi-pattern join.

**Files touched**
- `applications/oss-supply-chain/queries/bus_factor.cypher`
  - Uses `WITH` pipelining, `count`, `sum`, `CASE`
- `applications/oss-supply-chain/queries/bus_factor.expected`
- `applications/oss-supply-chain/app/src/oss_supply_chain/scenarios/bus_factor.py`
- Tests

Fixture enrichment: add a synthetic "commit-count" property on the
`MAINTAINED_BY` edge so the aggregation has data to chew on. Document the
property in `data/schema.md` in this slice (update, not create).

**Acceptance**
- S3.1–S3.3 per SPEC §3

**Verification** — pytest + expected-file review.

---

### Slice 5 — M5 Scenario S4: license contamination
**Goal:** Variable-length shortest path with label filters.

**Files touched**
- `applications/oss-supply-chain/queries/license_contamination.cypher`
- `applications/oss-supply-chain/queries/license_contamination.expected`
- `applications/oss-supply-chain/app/src/oss_supply_chain/scenarios/license_contamination.py`
- Tests

Fixture is extended with 2–3 permissive→copyleft paths at different depths.

**Acceptance**
- S4.1–S4.3 per SPEC §3

**Verification** — pytest + expected-file review. This is the M5 gate:
**all four scenarios green on the fixture.**

---

### Slice 6 — Real-data track (post-M5, orthogonal)
**Goal:** Exercise the same four scenarios against the Small workspace
(≈ 3–5 M nodes) loaded from deps.dev / OSV / npm.

Broken into four sub-slices (each its own PR):

- **6a. OSV fetch.** `data/fetch_osv.py` — pulls `npm` advisory zip, extracts
  to `data/raw/osv/`. Resumable via ETag/modified-since.
- **6b. deps.dev fetch.** `data/fetch_deps_dev.py` — pulls a fixed weekly GCS
  dump for npm only. Resolves SPEC OQ-1 (snapshot date) in this PR.
- **6c. npm maintainer fetch.** `data/fetch_npm.py` — queries `replicate.npmjs.com`
  for packages observed in the subset.
- **6d. Transform + load.** `etl/transform.py` emits label-split CSVs; a
  wrapper script invokes `turbolynx import` with all pairs. New goldens under
  `queries/<scenario>.small.expected` (do NOT overwrite fixture goldens).

**Acceptance**
- All four scenarios green against `small` workspace
- Each runs < 60 s on M-series Mac (SPEC S1.3 generalized)
- Memory peak < 12 GB during load; documented in PR

**Verification** — new pytest marker `@pytest.mark.small` gated behind an
env var `TLX_SMALL_WORKSPACE=/tmp/ws-small`.

---

### Deferred (not in this plan)
- Benchmarks — reserved `benchmarks/` directory stays empty.
- Medium scale — separate decision per SPEC §10/M6.
- Stress tests — per SPEC §7.4.

## 4. Checkpoints

| Checkpoint | Gate criterion | Who approves |
|---|---|---|
| M0 | SPEC + plan reviewed; scaffold tree created | user |
| M1 | Smoke test green; Python↔CLI harness decided | user |
| M2 | S1 acceptance green, PR merged | user |
| M3 | S2 acceptance green, PR merged | user |
| M4 | S3 acceptance green, PR merged | user |
| M5 | S4 acceptance green — **all scenarios on fixture** | user |
| M6 | Real data Small workspace scenarios green | user (separate decision) |

Between checkpoints: no autonomous work on the next slice without user
approval of the previous checkpoint.

## 5. Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| `turbolynx import` CSV format has an edge case that breaks on our compound schemas | M | H | Surfaced in Slice 1; fail loudly with logs, not workarounds |
| CLI stdout format drifts between versions, breaking differential test | M | M | Use `--mode csv` explicitly; snapshot header row in canonicalizer |
| Cypher subset missing a function we need (e.g. string length) | M | M | Each scenario reviews SPEC §3 feature list before writing; fall back to client-side post-processing (as with S2) |
| macOS wheel rebuild breaks after engine PR lands in `main` | L | M | CI rebuild step before test suite in each app PR |
| Fixture not representative enough, scenarios pass on fixture but fail on Small | M | L | Slice 6 is the true gate; fixture tests are necessary, not sufficient |
| SPEC §5 example commands misspec `turbolynx import` (already found `--data` drift vs. real `--nodes/--relationships` flags) | H → resolved | L | Corrected in Slice 1 via working `loader.py`; amend SPEC §5 in same PR |

## 6. What this plan deliberately does NOT cover

- Changes to TurboLynx engine code (`src/`, `tools/`). Engine bugs discovered
  here are separate issues in the main TurboLynx repo, not part of this app.
- UI / web frontend. Stdout + pytest only, per SPEC §2 non-goals.
- Mutation / CRUD scenarios. Read-only per SPEC §1.
- CI configuration changes. If/when needed, filed separately.

## 7. Open decisions requiring user answer before Slice 1 begins

1. **tasks/ location** — plan lives in `applications/oss-supply-chain/tasks/`
   (app-local). If a repo-root `tasks/` is preferred for cross-app tracking,
   we'll move.
2. **SPEC §5 amendments** — the `turbolynx import --data <dir>` placeholder was
   wrong. Amend SPEC in the M0 PR or defer to M1? Recommendation: amend in M0
   to avoid a broken example in the landed spec.
3. **CLI harness shape** — prefer `--query "<single statement>"` invocation
   (simpler, one subprocess per query) or `.read <file>` via stdin piping
   (handles multi-statement Cypher files). Recommendation: start with
   `--query`; revisit if any scenario Cypher is multi-statement.
4. **Python extras for edit-distance (S2)** — `rapidfuzz` (C extension,
   fastest) vs. pure-Python Levenshtein (zero new dep). Recommendation:
   pure-Python for v0; swap if profiling shows it matters.
