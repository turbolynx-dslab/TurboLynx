# Third-Party Notices

This file is a high-level inventory of vendored or adapted third-party code in
this repository, especially code that lives outside `third_party/`.

For libraries vendored under `third_party/`, see the license file shipped in
each component directory.

## Scope & Design Notes

- **Per-subdirectory LICENSE policy**: For components under `third_party/`,
  each vendored subtree ships with its own `LICENSE` (and `NOTICE` /
  `COPYRIGHT` where the upstream provides one). This file intentionally does
  **not** re-enumerate them. This mirrors DuckDB's upstream layout and is
  legally sufficient for source distribution: MIT / BSD copyright-preservation
  and Apache-2.0 §4(d) NOTICE-propagation are satisfied because the license
  texts travel with the code. A consolidated NOTICE would only be required for
  *binary-only* distributions that strip `third_party/` sources.
- **Verbatim vendored components** (e.g., `utf8proc`, GPORCA `gporca/`
  subtree): upstream `LICENSE` / `COPYRIGHT` files are preserved in place
  without edit. No additional annotation is added or needed on top of them.
- **GPORCA + PostgreSQL-derived headers**: GPORCA's own `COPYRIGHT` already
  disclaims that "This product may include a number of subcomponents with
  separate copyright notices and license terms." File-level attribution in
  `postgres.h`, `c.h`, `pg_config*.h`, etc. is authoritative; the
  directory-level Apache-2.0 label is a simplification, not an override.
- **DuckDB MIT attribution is file-header based, not directory based**: files
  meaningfully derived from DuckDB carry the standard DuckDB block comment at
  the top. Absence of that header under the directories listed below indicates
  TurboLynx-original work. This has been audited; there are no known cases of
  DuckDB-derived files missing the header.

## GPORCA / Greenplum Query Optimizer

- Component: GPORCA (Greenplum Query Optimizer) source, plus Greenplum /
  Postgres-derived compatibility, glue, and integration code used to embed
  GPORCA into TurboLynx.
- Upstream sources:
  - https://github.com/greenplum-db/gporca-archive (GPORCA itself)
  - Greenplum / Postgres compatibility headers and utilities
- License: Apache License 2.0
- Vendored locations:
  - `src/optimizer/orca/` — implementation
    - `gporca/` : vendored upstream GPORCA source
    - `cdb/`, `gpopt/`, `nodes/`, `utils/` : Greenplum-derived glue / translator
      code (Apache 2.0)
  - `src/include/optimizer/orca/` — headers
    - `gporca/` : vendored upstream GPORCA headers
    - `cdb/`, `common/`, `gpopt/`, `nodes/`, `port/`, `storage/`, `utils/`,
      and top-level Postgres headers (`c.h`, `postgres.h`, `pg_config*.h`) :
      Greenplum / Postgres-derived compatibility headers (Apache 2.0)
- Notes:
  - The `gporca` subtree is vendored verbatim from upstream GPORCA with its
    own LICENSE and COPYRIGHT preserved in place.
  - The surrounding directories (`cdb`, `gpopt`, `nodes`, `utils`, etc.) are
    Greenplum / Postgres-derived integration code, also distributed under
    Apache 2.0. File-level copyright and attribution notices have been
    retained where present.
- Local license files:
  - `src/optimizer/orca/gporca/LICENSE`
  - `src/optimizer/orca/gporca/COPYRIGHT`
  - `src/include/optimizer/orca/LICENSE`

## DuckDB MIT — partial attribution

- Component: Portions of the database/runtime code in this repository were
  originally adapted from DuckDB, with credit to the DuckDB project.
- Upstream source: https://github.com/duckdb/duckdb
- License: MIT License
- Local license file: `licenses/duckdb-MIT.txt`
- Directories that contain *some* code in this category (alongside
  TurboLynx-original code):
  - `src/common` / `src/include/common`
  - `src/main` / `src/include/main`
  - `src/parser` / `src/include/parser`
  - `src/planner` / `src/include/planner`
  - `src/function` / `src/include/function`
  - `src/catalog` / `src/include/catalog`
  - `src/execution` / `src/include/execution`
  - `src/storage` / `src/include/storage`
- Notes:
  - The directories above are *mixed*: TurboLynx-original sources and
    DuckDB-attributed sources live side-by-side, and over time many files
    have been rewritten, merged, or split. Because a precise file-by-file
    catalogue is not practical, this notice is intentionally coarse — we
    identify the directories where such code can occur rather than
    enumerating individual files.
  - On a best-effort basis, files whose content meaningfully traces back to
    DuckDB carry the standard DuckDB block-comment header at the top,
    preserving the MIT copyright notice and the corresponding upstream path.
    Files without that header under the directories above are TurboLynx
    work (including, but not limited to, graph/Cypher, extent storage, and
    disk AIO).

This notice file is informational and does not replace the original license
files shipped with each vendored component.
