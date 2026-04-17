# Known Blockers

## B1 — macOS `MATCH` queries segfault in ORCA teardown (engine-side)

**Tracking:** [turbolynx-dslab/TurboLynx#61](https://github.com/turbolynx-dslab/TurboLynx/issues/61)
**Discovered:** M1 smoke-test attempt on `app-m1-smoke-test` branch, 2026-04-17.

**Symptom.** Any Cypher query of shape `MATCH (n:<Label>) RETURN ...;`
terminates with `SIGSEGV` (exit 139) on the current macOS (`build-portable`)
build. Trivial queries that do not touch storage (e.g. `RETURN 1 AS one;`)
return normally, and the bulk `turbolynx import` step completes cleanly —
the crash is isolated to the execute path of pattern-matching queries.

**Reproduction.** With a minimal fixture:

```
printf ':ID(Person)|name:STRING\n1|Alice\n2|Bob\n' > person.csv
./build-portable/tools/turbolynx import --workspace /tmp/ws \
    --log-level warn --nodes Person person.csv
./build-portable/tools/turbolynx shell --workspace /tmp/ws \
    --query "MATCH (n:Person) RETURN n.name;"
# → Database Connected ... EXC_BAD_ACCESS / exit 139
```

`--explain` returns cleanly; `--compile-only` still crashes.

**Backtrace (lldb).**

```
thread #1, stop reason = EXC_BAD_ACCESS (code=1, address=0xfffffffffffffff0)
  frame #0  libc++abi.dylib`__cxxabiv1::(anonymous namespace)::
                            dyn_cast_get_derived_info + 8
  frame #1  libc++abi.dylib`__dynamic_cast + 56
  frame #2  libturbolynx.dylib`gpos::CAutoMemoryPool::~CAutoMemoryPool()
  frame #3  libturbolynx.dylib`gpos_exec + 1504
  frame #4  libturbolynx.dylib`turbolynx::Planner::execute(
                                   duckdb::BoundRegularQuery*)
  frame #5  libturbolynx.dylib`turbolynx_compile_query
  frame #6  libturbolynx.dylib`turbolynx_prepare
  frame #7  turbolynx`RunQuery
  frame #8  turbolynx`RunShell
  frame #9  turbolynx`main
```

The crash lives in the C++ ABI `__dynamic_cast` during destruction of
`gpos::CAutoMemoryPool`. Classic signature of either:

1. A vtable-missing teardown path (ORCA memory-pool auto-guard being run
   after the underlying pool singleton has already shut down), or
2. A dylib-RTTI mismatch (two copies of the GPOS type info linked into
   different shared libraries on macOS arm64).

Both interpretations are consistent with the `-fvisibility=hidden` +
shared-library split used by the `build-portable` target; Linux builds
don't exhibit this because GPOS lives in a single translation unit.

**Impact on this application.** M1 (vertical smoke test) cannot pass on
macOS until the engine is fixed. The fixture loads, the harness code
runs, the canonicalizer unit tests pass (6/6), but every Python-API or
CLI query that touches node storage terminates the process.

**Workarounds attempted.**

| Attempt | Result |
|---|---|
| `--mode table`, `--mode box`, `--mode json` | all crash |
| `--join-order-optimizer greedy` / `query` | all crash |
| `--compile-only` | crash |
| `--explain` | succeeds (different tear-down path) |
| Minimal 2-node Person fixture (no edges, no types besides INT/STRING) | still crashes |
| Python binding (`turbolynx.connect(...).execute(...)`) | same crash, same stack |

**Attempted in follow-up:**

- `-DCMAKE_BUILD_TYPE=RelWithDebInfo -DBUILD_UNITTESTS=ON` → same crash with
  symbolicated backtrace; rules out release-only optimization.
- Running `build-debug/test/bulkload/bulkload_test` → one failing case at
  `test/bulkload/test_smoke_cli.cpp:160` on `MATCH (n:Person {id: 1})
  RETURN n.firstName, n.city;`. The existing in-tree test suite already
  flags this regression on macOS, so the fix can be gated on that binary
  returning to green. Full diagnostics posted as a comment on issue #61.
- Source-level fault: `src/include/optimizer/orca/gporca/libgpos/gpos/task/
  CTask.h:288` — `dynamic_cast<CTask *>(ITask::Self())` reached during
  `~CAutoMemoryPool → CAutoSuspendAbort`. Hypothesis: stale `IWorker` /
  `ITask` pointer from `CWorkerPoolManager` at macOS teardown time.

**Not attempted** (require deeper engine work or user decision):

- `build-release` (non-portable) on macOS — not supported; macOS only
  has the portable DISK IO path.
- Building on Linux to confirm Linux-only working baseline.
- Bisecting across commits since `9d84bc10a` (portable macOS build landed).

**App-side status.** All M1 source code is complete and committed on this
branch (`app-m1-smoke-test`). Tests that don't touch the storage path
(the `test_canonicalize.py` unit tests) pass; `test_smoke.py` is expected
to fail until the engine is fixed. No production-ready PR can be opened
from this branch yet.

**Suggested next step.** Engine owner triages whether this is:
(a) a macOS-only ORCA shutdown regression to fix in `src/`, or
(b) a build-portable-specific link/RTTI issue to fix in the CMake target
    setup.

Until resolved, M1 remains blocked.
