# Code Review Priorities

TurboLynx review findings use priority tags to make the expected response clear.
The tag describes the risk of leaving the issue in the codebase, not the size of
the patch required to fix it.

| Tag | Meaning | Typical examples |
|---|---|---|
| `[P0]` | Critical issue that should be handled immediately. | Database cannot start, broad data corruption, or an urgent security vulnerability. |
| `[P1]` | Serious bug that should be fixed before release or merge. | Data loss, crash, use-after-free, failed WAL recovery, wrong mutation target, injection risk, or broken I/O completion guarantees. |
| `[P2]` | Important issue with narrower scope or more conditional impact. | Semantic mismatch for a specific query pattern, memory leak, inaccurate watermark, or a heuristic that works only for the current fixture. |
| `[P3]` | Quality, maintainability, or defensive improvement. | Better error messages, removing duplication, simplifying structure, or hardening an edge case. |

## Overfitting Check

Every code review should also check whether the implementation is overfitted to
the current tests, sample data, or a narrow query shape. Be especially careful
with code that assumes:

- a specific column name or column order
- the first partition, first property, first label, first edge type, or first
  numeric column is the intended one
- a parser can be replaced with regular expressions or ad-hoc string splitting
- vectors are always flat and can be read with raw `GetData()[row]`
- there are no selection vectors, constant vectors, dictionary vectors, or nulls
- a query pattern from the fixture represents the full Cypher semantics

If a finding is caused by one of these assumptions, call that out explicitly in
the review. These issues often pass narrow tests while failing on real schemas,
different data layouts, or slightly more general queries.

## Current-Implementation Verification

Review findings must be checked against the current implementation before they
are reported or turned into GitHub issues. A review note, design document,
benchmark comment, or older finding is only evidence that the area is worth
checking; it is not proof that the bug still exists.

For every correctness finding, verify the current `HEAD` behavior directly:

- inspect the implementation paths that apply the state change
- inspect the read/recovery paths that observe that state
- check nearby regression tests when they exist
- use `git blame` or `git log` when a finding may have been fixed after the
  review document was written
- distinguish implementation bugs from stale documentation or stale benchmark
  wording

Do not re-open an old correctness concern just because older documentation still
describes the old behavior. If the code has already changed, classify the result
as one of:

- **implemented/resolved**: the current implementation and tests cover the
  original concern
- **documentation drift**: docs or benchmark descriptions no longer match the
  implementation
- **residual correctness gap**: the main fix exists, but a current path still
  violates the intended semantics
- **performance/statistics follow-up**: the semantics are correct, but the
  current design may still have planner, statistics, or high-degree workload
  costs that need measurement

### Example: DELETE vs DETACH DELETE

Be especially careful with mutation semantics that have changed over time.
For example, earlier CRUD review notes and benchmark text assumed plain node
`DELETE` could behave like a cascading delete and leave or remove incident
edges implicitly. The current semantics are different:

- plain `DELETE` must fail when live relationships still exist
- `DETACH DELETE` is the operation that removes incident edges before deleting
  the node
- edge deletions performed by `DETACH DELETE` must also be checked in the WAL
  and recovery paths before reporting a durability issue

If the current code already enforces this split, do not file a new correctness
issue saying that node deletion leaves dangling edges. Instead, file a
documentation issue if docs still describe plain `DELETE` as cascading, and file
a separate performance or statistics issue only if the current implementation
still needs measurement or planner support.

This distinction matters because a stale review note can be true historically
and false for the current codebase. The priority tag should describe the risk in
the code that exists now, not the risk that existed before a later fix landed.
