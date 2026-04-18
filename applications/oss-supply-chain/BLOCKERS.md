# Known Blockers

## B1 — Anonymous `:ID(Label)` fixture headers can crash MATCH on macOS

**Tracking:** [turbolynx-dslab/TurboLynx#61](https://github.com/turbolynx-dslab/TurboLynx/issues/61)
**Discovered:** M1 smoke-test attempt on `app-m1-smoke-test` branch,
2026-04-17.
**App status:** mitigated for `oss-supply-chain` by using named import keys.

### Symptom

On macOS, a workspace imported from vertex CSVs whose first column is the
anonymous Neo4j-style header `:ID(Label)` can segfault on a later MATCH query.
A minimal reproducer is:

```bash
printf ':ID(Person)|name:STRING\n1|Alice\n2|Bob\n' > person.csv
./build-portable/tools/turbolynx import --workspace /tmp/ws \
    --log-level warn --nodes Person person.csv
./build-portable/tools/turbolynx shell --workspace /tmp/ws \
    --query "MATCH (n:Person) RETURN n.name;"
# May terminate with SIGSEGV / exit 139 on macOS.
```

The same fixture shape with a named import key, for example
`uid:ID(Person)|name:STRING`, runs through MATCH normally in both the CLI and
Python API paths.

### Application Mitigation

The committed fixture under `tests/fixtures/` now uses `uid:ID(<Label>)` for
every vertex label. `uid` is only the fixture-local relationship lookup key;
scenario code uses domain properties such as `Package.name`, `Version.version`,
and `CVE.id`.

With that fixture shape:

```bash
pytest applications/oss-supply-chain/tests -v
# 19 passed
```

The M1 smoke tests and S1 blast-radius golden/differential tests are no longer
blocked at the application layer.

### Remaining Engine Note

The anonymous-header crash is still useful as an engine regression test, but it
does not currently block this application. If TurboLynx later fully supports
anonymous `:ID(Label)` headers on macOS, this file can be retired or moved into
engine-level regression documentation.
