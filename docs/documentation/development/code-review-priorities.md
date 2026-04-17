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
