# TurboLynx Applications

Real-world applications built on TurboLynx. Each subdirectory is a self-contained
application used to stress-test the engine end-to-end (robustness first,
performance second, demo visibility third). Applications deliberately sit
outside `test/` and `demo/` so they can own their own data, ETL, queries, and
Python/CLI harness without interfering with unit tests or the VLDB demo.

## Directory layout convention

Every application MUST follow this layout:

```
applications/<app-name>/
├── SPEC.md            # spec-driven-development output (objective, scenarios,
│                       # acceptance criteria, boundaries)
├── README.md          # quickstart: install, load data, run a query
├── data/              # data acquisition
│   ├── fetch.py       # or fetch.sh — idempotent, resumable download scripts
│   ├── schema.md      # source schemas and mapping to TurboLynx labels
│   └── raw/           # .gitignored — downloaded source data
├── etl/               # raw → TurboLynx workspace
│   ├── transform.py   # raw → CSV conforming to turbolynx import format
│   └── cypher/        # DDL (.cypher) applied post-import, if any
├── queries/           # one .cypher per scenario
│   ├── <scenario>.cypher
│   └── <scenario>.expected  # golden output for regression
├── app/               # Python package — analytics, CLI wrappers, orchestration
│   ├── pyproject.toml
│   └── src/<app_name>/
├── tests/             # pytest
│   ├── conftest.py
│   ├── test_correctness.py
│   └── test_differential.py   # Python API ↔ CLI result parity
└── benchmarks/        # perf scripts (optional, added when Medium scale is hit)
```

`tests/` uses `pytest`. Golden-file tests read `queries/*.cypher` and compare
against `queries/*.expected`. Differential tests execute the same Cypher through
the Python API and the `turbolynx` CLI shell, then assert equal rowsets.

## Shared conventions

- Code and comments in English only
- Python code formatted with `black`; Cypher files are hand-formatted
- Python ≥ 3.10; type hints required on public functions
- Data downloads land under `data/raw/` and are `.gitignore`d
- No dataset larger than 1 MB is committed; use fixture generators instead
- Tests must not require network access — use a cached fixture slice

## Adding a new application

1. Copy `applications/_template/` (add one when we have a second app) into
   `applications/<new-app>/`
2. Write `SPEC.md` via the `spec-driven-development` skill
3. Land SPEC + convention scaffold as a separate PR before implementation
4. Implement one scenario at a time, each behind its own PR

## Platform

Applications target **macOS (`build-portable/`)** as the primary development
environment. Linux is supported via the standard `build/` layout but is not a
gating requirement for landing application code.
