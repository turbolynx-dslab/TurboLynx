// S1 — Blast radius of a CVE
//
// Given a CVE id, return the root packages (distinct by name) whose
// dependency tree transitively contains at least one vulnerable version,
// up to a depth of 5 hops over DEPENDS_ON.
//
// v0 parameterization: the CVE literal and the LIMIT value are edited
// in-place by `scenarios.blast_radius._render`. Until the Python API
// exposes prepared-statement parameters, these anchors ('CVE-...' and
// 'LIMIT 10;') are load-bearing — do not reformat without updating
// `blast_radius.py` in lockstep.
//
// Engine features exercised:
//   - incoming-edge traversal   : <-[:AFFECTED_BY]-
//   - variable-length path      : -[:DEPENDS_ON*1..5]->
//   - DISTINCT projection       : RETURN DISTINCT
//   - deterministic ordering    : ORDER BY ... ASC
//   - LIMIT                     : LIMIT 10

MATCH (c:CVE {id: 'CVE-2021-44228'})<-[:AFFECTED_BY]-(vuln:Version)
MATCH (downstream:Version)-[:DEPENDS_ON*1..5]->(vuln)
MATCH (pkg:Package)-[:HAS_VERSION]->(downstream)
RETURN DISTINCT pkg.name AS package, pkg.ecosystem AS ecosystem
ORDER BY package ASC
LIMIT 10;
