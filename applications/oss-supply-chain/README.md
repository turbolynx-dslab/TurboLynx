# OSS Supply-Chain Graph

A TurboLynx application that models the open-source software supply chain
(packages, versions, maintainers, licenses, CVEs) and exercises the engine
with four analytical scenarios. See [`SPEC.md`](SPEC.md) for the full
specification and [`tasks/plan.md`](tasks/plan.md) for the implementation plan.

Status: **M0 — scaffold only.** No loaders, no queries, no tests yet.

## Quickstart (planned — not functional until M1)

### 1. Build TurboLynx on macOS

```bash
# From the TurboLynx repo root:
xcode-select --install
brew install cmake ninja
python3 -m pip install pybind11 wheel

cmake -GNinja -DCMAKE_BUILD_TYPE=Release -DBUILD_PYTHON=ON \
      -DTURBOLYNX_PORTABLE_DISK_IO=ON -DENABLE_TCMALLOC=OFF \
      -DBUILD_UNITTESTS=OFF -DTBB_TEST=OFF \
      -B build-portable
cmake --build build-portable
tools/pythonpkg/scripts/build_wheel.sh build-portable
python3 -m pip install tools/pythonpkg/dist/turbolynx-*.whl
```

### 2. Install this application (editable)

```bash
python3 -m pip install -e applications/oss-supply-chain/app
```

### 3. Run the test suite against the tiny fixture

```bash
pytest applications/oss-supply-chain/tests -v
```

### 4. (M6 only) Load the Small real-data workspace

```bash
python3 applications/oss-supply-chain/data/fetch_osv.py      --out applications/oss-supply-chain/data/raw/osv
python3 applications/oss-supply-chain/data/fetch_deps_dev.py --out applications/oss-supply-chain/data/raw/depsdev
python3 applications/oss-supply-chain/data/fetch_npm.py      --out applications/oss-supply-chain/data/raw/npm
python3 applications/oss-supply-chain/etl/transform.py \
    --raw applications/oss-supply-chain/data/raw \
    --out applications/oss-supply-chain/data/csv
./build-portable/tools/turbolynx import \
    --workspace /tmp/oss-ws-small \
    --nodes Package       applications/oss-supply-chain/data/csv/nodes_package.csv \
    --nodes Version       applications/oss-supply-chain/data/csv/nodes_version.csv \
    --nodes Maintainer    applications/oss-supply-chain/data/csv/nodes_maintainer.csv \
    --nodes Repository    applications/oss-supply-chain/data/csv/nodes_repository.csv \
    --nodes License       applications/oss-supply-chain/data/csv/nodes_license.csv \
    --nodes CVE           applications/oss-supply-chain/data/csv/nodes_cve.csv \
    --relationships HAS_VERSION      applications/oss-supply-chain/data/csv/edges_has_version.csv \
    --relationships DEPENDS_ON       applications/oss-supply-chain/data/csv/edges_depends_on.csv \
    --relationships MAINTAINED_BY    applications/oss-supply-chain/data/csv/edges_maintained_by.csv \
    --relationships HOSTED_AT        applications/oss-supply-chain/data/csv/edges_hosted_at.csv \
    --relationships DECLARES         applications/oss-supply-chain/data/csv/edges_declares.csv \
    --relationships AFFECTED_BY      applications/oss-supply-chain/data/csv/edges_affected_by.csv
```

Then point tests at it:

```bash
TLX_SMALL_WORKSPACE=/tmp/oss-ws-small pytest applications/oss-supply-chain/tests -v -m small
```

## Directory layout

See [`../README.md`](../README.md) for the cross-application convention and
[`SPEC.md §8`](SPEC.md#8-project-structure) for the per-app tree.

## Scenarios

| # | Scenario | Engine path exercised |
|---|---|---|
| S1 | Blast radius of a CVE | variable-length transitive closure, aggregation |
| S2 | Typosquat detection | string predicates, anti-pattern, client-side edit-distance |
| S3 | Bus-factor + unresolved CVE | grouped aggregation, multi-pattern join |
| S4 | License contamination chain | variable-length shortest path with label filter |

Full scenario definitions and acceptance criteria live in
[`SPEC.md §3`](SPEC.md#3-scenarios-and-acceptance-criteria).

## Milestones

Tracked in [`tasks/todo.md`](tasks/todo.md). Current: **M0**.
