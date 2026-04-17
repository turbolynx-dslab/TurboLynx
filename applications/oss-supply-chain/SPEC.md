# SPEC — OSS Supply-Chain Security Graph

> Status: **DRAFT — awaiting user confirmation**
> Version: 0.1
> Owner: max1@dblab.postech.ac.kr
> Target platform: macOS (`build-portable/`)

## 1. Objective

Build a real-world graph application on top of TurboLynx that models the
**open-source software supply chain** — packages, versions, maintainers,
repositories, licenses, and security advisories — and use it to stress-test the
engine with four analytical scenarios that each exercise a different class of
query workload.

### Why this application

- **Schema heterogeneity is genuine.** npm, PyPI, and Cargo each carry
  disjoint metadata (`peerDependencies`, `extras_require`, `features`, etc.),
  naturally producing the "same label, very different property sets" that
  TurboLynx's schemaless storage is designed for.
- **Data is fully public.** deps.dev BigQuery, OSV.dev, and GitHub Archive
  all publish under permissive licenses.
- **Queries span distinct engine paths** — transitive closure, negative join,
  windowed aggregation, and label-restricted path search — giving four
  orthogonal stress vectors.
- **Industry relevance is high** (xz-utils, Log4Shell), so the same artefact
  doubles as a demo vehicle later.

### Primary success signal

**Robustness first**: running the full scenario suite against a Small-scale
workspace (~5 M nodes) must complete without crashes, memory errors, or
result-set divergence between the Python API and CLI. Performance and demo
polish are secondary.

### Non-goals

- Write / mutation workloads (CRUD). Read-only scenarios only in v0.
- Live ingest / streaming updates.
- A UI — stdout / Jupyter analysis is sufficient.
- Matching or competing with specialized supply-chain scanners
  (Snyk, Dependabot, etc.). This is a graph-query benchmark, not a
  security product.

---

## 2. Target users

| User | What they do with it |
|---|---|
| TurboLynx engine developers | Run the suite to catch regressions in parser, planner, executor |
| DB research students | Study query plans on realistic heterogeneous graphs |
| Demo presenters (later) | Show industry-relevant queries during talks |

Only developer-facing CLI / Python is required; no customer-facing surface.

---

## 3. Scenarios and acceptance criteria

Each scenario is implemented as a `.cypher` file with a `.expected` golden
file. Each must also be callable from the Python analytics package.

### S1 — Blast radius of a CVE
**Engine path exercised:** variable-length transitive closure, anti-join.

> Given a CVE, return the top-N root packages whose dependency tree (up to
> depth D) transitively depends on a vulnerable version.

- Cypher features: `MATCH ... -[:DEPENDS_ON*1..D]->`, aggregation, `ORDER BY`
- Acceptance:
  - S1.1 — Query for a fixed seed CVE returns deterministic top-10 root packages
    across three runs (Python API) on the frozen Small fixture.
  - S1.2 — Same query via `turbolynx` CLI produces byte-identical rowset after
    canonical sort.
  - S1.3 — Runs to completion under 60 s on the Small fixture on macOS
    (M-series).

### S2 — Typosquat detection
**Engine path exercised:** string predicates, negative pattern matching.

> Find packages whose name is edit-distance ≤ 2 from a popular package's name
> but share **no** maintainer and were published within the last 90 days.

- Cypher features: string predicates (`STARTS WITH`, length-based filters),
  `OPTIONAL MATCH`, `WHERE NOT EXISTS`, date arithmetic (via INT timestamps)
- Acceptance:
  - S2.1 — Deterministic result set on the frozen fixture.
  - S2.2 — Differential parity Python ↔ CLI.
  - S2.3 — Edit-distance ≤ 2 computed client-side (Python) when the function
    is not pushed down, to keep Cypher subset-compatible; spec documents the
    split.

### S3 — Bus-factor + unresolved CVE
**Engine path exercised:** grouping / windowed aggregation, multi-label join.

> List packages where a single maintainer is responsible for ≥ 80 % of the
> commits in the last 12 months AND the package has at least one
> HIGH/CRITICAL CVE with no fix version.

- Cypher features: aggregation (`count`, `sum`), `WITH` pipelining,
  multi-pattern join, `CASE`
- Acceptance:
  - S3.1 — Deterministic ranked list (top-50).
  - S3.2 — Python ↔ CLI parity.
  - S3.3 — Degrades gracefully when the commit history slice is empty
    (returns an empty set, not an error).

### S4 — License contamination chain
**Engine path exercised:** label-restricted path search, propagation.

> Starting from any package declared with a permissive license (MIT, BSD-*,
> Apache-2.0), find the shortest path to a transitive dependency under a
> copyleft license (GPL-*, AGPL-*, LGPL-*).

- Cypher features: variable-length shortest path, label / property filters
- Acceptance:
  - S4.1 — Deterministic count of contaminated permissive roots.
  - S4.2 — Python ↔ CLI parity.
  - S4.3 — Ten hand-picked seed packages produce the expected path prefixes.

---

## 4. Data

### Sources (v0, Small scale)

| Source | Content | Access | Subset used |
|---|---|---|---|
| deps.dev (GCS public dump) | packages, versions, dependencies, links | `gs://deps-dev-insights-v1/` | npm ecosystem only, latest weekly dump |
| OSV.dev | CVE / GHSA advisories | `github.com/google/osv-vulnerabilities/tree/main/npm` (zip) | npm advisories |
| npm registry | maintainer lists per package | `https://replicate.npmjs.com/registry/` CouchDB | only packages seen in deps.dev subset |
| SPDX license list | canonical license IDs | `https://spdx.org/licenses/licenses.json` | full |

No commercial or licensed data is used. No dataset is committed to the repo.

### Scale targets

- **Small (v0 gate):** ~3–5 M nodes, ~50 M edges, ~8 GB workspace.
  Must load on an M-series Mac with 16 GB RAM in under 2 hours.
- **Medium (stretch):** add PyPI + Cargo, ~30 M nodes, ~500 M edges.
  Added only after Small passes all acceptance criteria.

### Graph schema (v0)

```
Labels:
  Package        { name, ecosystem, first_seen, last_seen, homepage? }
  Version        { version, published_at, deprecated?, yanked? }
  Maintainer     { id, username, email? }
  Repository     { host, owner, name, stars?, archived? }
  License        { spdx_id, category ∈ {permissive, copyleft, other} }
  CVE            { id, severity, published_at, fix_version? }

Relationships:
  (Package)-[:HAS_VERSION]->(Version)
  (Version)-[:DEPENDS_ON {kind}]->(Version)       kind ∈ {runtime, dev, peer, optional}
  (Package)-[:MAINTAINED_BY]->(Maintainer)
  (Package)-[:HOSTED_AT]->(Repository)
  (Version)-[:DECLARES]->(License)
  (Version)-[:AFFECTED_BY]->(CVE)
```

Property sets intentionally vary across ecosystems in future Medium-scale
expansion (e.g. `peer`-kind edges exist mostly for npm); the schema is
documented in `data/schema.md` once implemented.

---

## 5. Commands (developer entry points)

All commands run from the TurboLynx repo root unless noted. `$APP` =
`applications/oss-supply-chain`.

### Build the engine (once)
```bash
cmake -GNinja -DCMAKE_BUILD_TYPE=Release -DBUILD_PYTHON=ON \
      -DTURBOLYNX_PORTABLE_DISK_IO=ON -DENABLE_TCMALLOC=OFF \
      -DBUILD_UNITTESTS=OFF -DTBB_TEST=OFF \
      -B build-portable
cmake --build build-portable
tools/pythonpkg/scripts/build_wheel.sh build-portable
python3 -m pip install tools/pythonpkg/dist/turbolynx-*.whl
```

### Install the application package
```bash
cd $APP && python3 -m pip install -e app
```

### Acquire and load data (Small — M6 only; earlier milestones use the fixture)
```bash
python3 $APP/data/fetch_osv.py      --out $APP/data/raw/osv
python3 $APP/data/fetch_deps_dev.py --out $APP/data/raw/depsdev
python3 $APP/data/fetch_npm.py      --out $APP/data/raw/npm
python3 $APP/etl/transform.py       --raw $APP/data/raw --out $APP/data/csv

# turbolynx import takes repeated --nodes <Label> <file> and
# --relationships <Type> <file> pairs. There is no --data <dir> flag.
./build-portable/tools/turbolynx import \
    --workspace /tmp/oss-ws \
    --nodes Package       $APP/data/csv/nodes_package.csv \
    --nodes Version       $APP/data/csv/nodes_version.csv \
    --nodes Maintainer    $APP/data/csv/nodes_maintainer.csv \
    --nodes Repository    $APP/data/csv/nodes_repository.csv \
    --nodes License       $APP/data/csv/nodes_license.csv \
    --nodes CVE           $APP/data/csv/nodes_cve.csv \
    --relationships HAS_VERSION      $APP/data/csv/edges_has_version.csv \
    --relationships DEPENDS_ON       $APP/data/csv/edges_depends_on.csv \
    --relationships MAINTAINED_BY    $APP/data/csv/edges_maintained_by.csv \
    --relationships HOSTED_AT        $APP/data/csv/edges_hosted_at.csv \
    --relationships DECLARES         $APP/data/csv/edges_declares.csv \
    --relationships AFFECTED_BY      $APP/data/csv/edges_affected_by.csv
```

### Run a scenario (Python API)
```bash
python3 -m oss_supply_chain.scenarios.blast_radius \
    --workspace /tmp/oss-ws --cve CVE-2021-44228 --top 10
```

### Run a scenario (CLI)
`turbolynx shell` has no `--file` flag. Use `--query` for a single
statement (the form used by the differential test harness):

```bash
./build-portable/tools/turbolynx shell \
    --workspace /tmp/oss-ws --mode csv \
    --query "$(cat $APP/queries/blast_radius.cypher)"
```

For a multi-statement script, pipe into the interactive shell and use
the `.read` dot command:

```bash
./build-portable/tools/turbolynx shell --workspace /tmp/oss-ws \
    <<EOF
.read $APP/queries/blast_radius.cypher
EOF
```

### Run the test suite
```bash
pytest $APP/tests -v
```

---

## 6. Code style

- Python: `black` formatted, `ruff` for linting, type hints on public APIs.
- Python ≥ 3.10. No compiled extensions in this package.
- Cypher files: trailing newline, lowercase keywords avoided (use canonical
  uppercase `MATCH`, `WHERE`, `RETURN`), one logical clause per line.
- English-only in code, comments, commit messages, PR descriptions.
- No `Co-Authored-By: Claude` (per `CLAUDE.md`). Commits authored solely by the
  human user.
- Follow existing TurboLynx C++17 conventions if any helper binaries are added
  later (none in v0).

---

## 7. Testing strategy

### 7.1 Correctness (golden files)
Every scenario has `queries/<name>.cypher` and `queries/<name>.expected`.
`tests/test_correctness.py` loads each pair, executes through the Python API
against the small fixture workspace, canonicalizes output (stable sort,
float rounding), and diffs against the expected file.

### 7.2 Differential (Python API ↔ CLI)
`tests/test_differential.py` runs each `.cypher` through:
1. `turbolynx.connect(workspace).execute(query)` → rowset A
2. `turbolynx shell --workspace ... --file <query>` → rowset B
then asserts `A == B` after canonicalization. This catches divergence in the
Python binding vs. shell frontends.

### 7.3 Fixture workspace
A tiny deterministic fixture (~10 k nodes, committed under
`tests/fixtures/mini.csv`) is regenerated from a frozen seed. The Small
production workspace is NOT committed; CI downloads it on demand or uses the
mini fixture only.

### 7.4 What is NOT in v0
- Stress tests (long-running loops, memory profiling) — deferred to
  Medium-scale milestone.
- Property-based testing / fuzzing — out of scope here; the engine already has
  its own fuzz suite.
- Benchmarks — a `benchmarks/` directory is reserved but empty in v0.

### 7.5 Test execution gate
A scenario PR is mergeable only when:
- `pytest applications/oss-supply-chain/tests -v` passes on macOS
- Cypher golden file exists
- Differential test passes for that scenario

---

## 8. Project structure

```
applications/oss-supply-chain/
├── SPEC.md                   ← this file
├── README.md                 ← quickstart (added with scaffold)
├── data/
│   ├── fetch_osv.py          ← OSV advisories, idempotent + resumable
│   ├── fetch_deps_dev.py     ← deps.dev weekly snapshot
│   ├── fetch_npm.py          ← npm maintainer hydration
│   ├── schema.md             ← source schema docs
│   └── raw/                  ← .gitignored (data/raw/, data/csv/ both ignored)
├── etl/
│   ├── transform.py          ← raw → turbolynx-import CSV
│   └── cypher/
│       └── post_import.cypher  ← optional post-load indexing / stats
├── queries/
│   ├── blast_radius.cypher
│   ├── blast_radius.expected
│   ├── typosquat.cypher
│   ├── typosquat.expected
│   ├── bus_factor.cypher
│   ├── bus_factor.expected
│   ├── license_contamination.cypher
│   └── license_contamination.expected
├── app/
│   ├── pyproject.toml
│   └── src/oss_supply_chain/
│       ├── __init__.py
│       ├── loader.py         ← thin wrapper over turbolynx.connect
│       ├── canonicalize.py   ← rowset canonicalization for tests
│       └── scenarios/
│           ├── blast_radius.py
│           ├── typosquat.py
│           ├── bus_factor.py
│           └── license_contamination.py
├── tests/
│   ├── conftest.py
│   ├── fixtures/
│   │   └── mini.csv          ← tiny deterministic graph
│   ├── test_correctness.py
│   └── test_differential.py
└── benchmarks/               ← reserved, empty in v0
```

---

## 9. Boundaries

### Always do
- Write English-only code and comments.
- Regenerate goldens only when a Cypher file intentionally changes, and review
  the diff manually before committing.
- Keep scenario PRs small — one scenario per PR after the scaffold lands.
- Run `pytest applications/oss-supply-chain/tests -v` before pushing.
- Store large data under `data/raw/` and ensure it is `.gitignore`d.

### Ask first
- Before bumping the scale target from Small to Medium (disk + time budget).
- Before adding a new data source not listed in §4.
- Before adding new labels / relationship types to the schema in §4.
- Before adding any compiled Python extension, new third-party dependency,
  or new build flag to TurboLynx.
- Before committing any file larger than 1 MB.

### Never do
- Modify failing tests to make them pass — fix the implementation instead
  (`CLAUDE.md` rule).
- Add `Co-Authored-By: Claude` or any AI co-author trailer to commits.
- Commit raw data dumps, API keys, or credentials.
- Rely on network access during test execution.
- Refactor TurboLynx core (`src/`) as a side effect of this application.
  Engine bugs discovered here are filed as separate issues and fixed in
  dedicated PRs.
- Pin to a specific TurboLynx build artefact path outside `build-portable/`
  (keep macOS as the default).

---

## 10. Milestones

| # | Deliverable | Gate |
|---|---|---|
| M0 | This SPEC + `applications/README.md` + empty scaffold | user confirms spec |
| M1 | Data loaders (fetch + transform) + schema docs + tiny fixture | `make load` on Small completes on macOS |
| M2 | Scenario S1 (blast radius) + goldens + tests pass | S1 acceptance all green |
| M3 | Scenario S2 (typosquat) | S2 acceptance all green |
| M4 | Scenario S3 (bus-factor) | S3 acceptance all green |
| M5 | Scenario S4 (license) | S4 acceptance all green |
| M6 | Medium-scale expansion (PyPI + Cargo) — optional | separate decision |

Each milestone is one PR against `main` via branch `app-oss-supply-chain`.

---

## 11. Open questions

Mark any open issue here before implementation. Current:

- **OQ-1**: Which specific deps.dev weekly snapshot date is the "frozen"
  fixture bound to? Pick once during M1 and record in `data/schema.md`.
- **OQ-2**: Does `turbolynx import` accept our chosen CSV naming scheme
  across 6 vertex labels + 5 edge labels? Verify during M1 by dry-running
  the import tool; if not, the ETL emits a per-label file pair.
- **OQ-3**: Is edit-distance pushable into Cypher, or must it remain
  client-side? Assumed client-side for v0 (see §3 / S2.3) — revisit if
  TurboLynx exposes a UDF surface later.
