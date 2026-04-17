# Data Schema

## v0 fixture schema

The committed fixture under `tests/fixtures/` uses a trimmed subset of the
full schema described in `SPEC.md §4`. Properties marked optional in the
spec are omitted from the fixture to keep it readable; they are re-introduced
when the Small workspace (M6) loads real data.

### Vertex labels

| Label | File | Properties (fixture) |
|---|---|---|
| `Package` | `nodes_package.csv` | `name:STRING`, `ecosystem:STRING` |
| `Version` | `nodes_version.csv` | `version:STRING`, `published_at:BIGINT` (epoch seconds) |
| `Maintainer` | `nodes_maintainer.csv` | `username:STRING` |
| `Repository` | `nodes_repository.csv` | `host:STRING`, `owner:STRING`, `name:STRING` |
| `License` | `nodes_license.csv` | `spdx_id:STRING`, `category:STRING` |
| `CVE` | `nodes_cve.csv` | `id:STRING`, `severity:STRING`, `published_at:BIGINT` |

### Relationship types

| Type | Forward file | Endpoints | Properties |
|---|---|---|---|
| `HAS_VERSION` | `edges_has_version.csv` | `Package → Version` | (none) |
| `DEPENDS_ON` | `edges_depends_on.csv` | `Version → Version` | `kind:STRING` |
| `MAINTAINED_BY` | `edges_maintained_by.csv` | `Package → Maintainer` | (none) |
| `HOSTED_AT` | `edges_hosted_at.csv` | `Package → Repository` | (none) |
| `DECLARES` | `edges_declares.csv` | `Version → License` | (none) |
| `AFFECTED_BY` | `edges_affected_by.csv` | `Version → CVE` | (none) |

Every edge file has a sibling `*.csv.backward` with the ID columns swapped
and rows re-sorted by the new first column, per the `turbolynx import`
convention documented in `docs/documentation/data-import/formats.md`.

## Fixture graph — node inventory

| Label | IDs | Description |
|---|---|---|
| `Package` | 1–6 | log4j (maven), log4js (npm), lodash (npm), lodashx (npm), awesome-lib (npm), webapp (npm) |
| `Version` | 1–8 | v1 log4j@2.14.0, v2 log4j@2.16.0, v3 log4js@1.0.0, v4 lodash@4.17.20, v5 lodash@4.17.21, v6 lodashx@0.1.0, v7 awesome-lib@1.0.0, v8 webapp@0.5.0 |
| `Maintainer` | 1–3 | alice, bob, carol |
| `Repository` | 1–3 | GitHub repos for log4j, log4js, lodash |
| `License` | 1–3 | MIT, Apache-2.0, GPL-3.0 |
| `CVE` | 1–2 | CVE-2021-44228 (Log4Shell), CVE-2021-23337 (lodash cmd injection) |

Total: **25 nodes**, **33 directed edges**.

## Fixture coverage per scenario

| Scenario (SPEC §3) | Fixture ingredients |
|---|---|
| S1 blast radius | `CVE-2021-44228` → `log4j@2.14.0` → `awesome-lib@1.0.0` → `webapp@0.5.0` (2 root packages, 1–2 hops) |
| S2 typosquat | `log4j`↔`log4js` and `lodash`↔`lodashx` are name-close but share no maintainer |
| S3 bus-factor | `log4js` and `lodashx` each maintained by `carol` only (commit_count property added in M4) |
| S4 license contamination | `webapp(MIT)` → `awesome-lib(MIT)` → `lodashx(GPL-3.0)` via DEPENDS_ON at depth 2 |

## Type caveats on the CSV path

TurboLynx's CSV importer does **not** currently accept the `BOOLEAN` column
annotation — the documented restriction lives in
`docs/documentation/data-import/formats.md §Property Column Types`. Boolean
properties listed in `SPEC.md §4` (`deprecated`, `yanked`, `archived`) are
therefore absent from the fixture. When these fields are added in M6
alongside the real deps.dev / OSV / npm data, encode them as either
`INT` (`0` / `1`) or `STRING` (`true` / `false`) rather than `BOOLEAN`.

## Open questions resolved in this milestone

- **OQ-2 (SPEC §11)** — `turbolynx import`'s CSV format accepts our per-label
  pair scheme. Each vertex label takes one `--nodes <Label> <file>` pair;
  each edge type takes one `--relationships <Type> <file>` pair and
  auto-resolves the `.backward` sibling by filename convention. No ETL
  per-label split is needed beyond what the fixture already does.
