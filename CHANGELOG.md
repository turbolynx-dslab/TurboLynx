# Changelog

All notable releases of TurboLynx are recorded here.

## Unreleased — since v1.0.0

### Added
- Multi-layer fuzz harness (L1 crash/sanitizer, L2 metamorphic, L3
  differential vs Neo4j, L4 stateful CRUD), 35 tests across the four
  layers. (#242)
- ASan / sanitizer CI lane gated on PRs. (#252, #260, #263)
- Pattern comprehension end-to-end (`[(a)-[r]->(b) WHERE … | a.x]`).
  (#231, #244, #246, #247, #248)
- DBpedia mini fixture for bulkload regression. (#243)
- Typemod encoding refactor and widening for 64-bit IDs. (#250, #251)

### Fixed
- Fuzz-found bugs:
  - #236 — UNION ALL + ORDER BY pipeline wiring + leaf/non-leaf source
    dispatch. (#272)
  - #238 — Empty-bootstrap multi-label CREATE registers every label.
    (#265)
  - #240 — Multi-WITH chain that drops the bound variable no longer
    drops rows; ORCA `CColumnDescriptor::m_prop_id`/`m_node_id` now
    initialized. (#267)
  - #270 — VLE / shortestPath SEGV on CREATE-only data after
    checkpoint; iterators now route through `graph_storage_wrapper`
    instead of bypassing it. (#269)
- IdSeek wrapper cache re-prime per Execute (multi-IdSeek + delta).
  (#254 / #235)
- `AdjListDelta` shadow edge tagged by direction; directed forward
  `MATCH (a)-[r:KNOWS]->(b)` no longer double-counts on delta data.
  (#255 / #237, #239)
- Multi-PS BOTH edge pattern comprehension. (#259 / #245)
- Filter / list passthrough / variable-length path edge cases.
  (#256, #257, #258 / #253)
- AdjListDelta empty-bootstrap merge label. (#266 / #241)
- ORCA stack handling under ASan; expression depth UAF.
  (#261, #262, #264)
- Schema-evolved PS `SET` property OOB. (#273)

### Refactored
- Delete `SchemaFlowGraph` and the parallel planner-side state machine
  (~760 LOC net removal); the assertion it was guarding moved into
  pipeline/source dispatch and is now expressed directly. (#268)

## v1.0.0 — 2025-12-02

Initial public release. State of the codebase at the time of the VLDB 2026
paper submission ("TurboLynx: an analytical graph database for schemaless
property graphs").
