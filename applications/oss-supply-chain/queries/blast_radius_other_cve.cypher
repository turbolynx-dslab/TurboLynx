// S1.2 — Blast radius of CVE-2021-23337 (lodash prototype pollution).
//
// Same shape as blast_radius.cypher but with a different CVE literal —
// exercises the same chained-MATCH + var-len + self-join Version
// topology against a non-default vulnerability to widen #68's
// regression net.

MATCH (c:CVE {id: 'CVE-2021-23337'})<-[:AFFECTED_BY]-(vuln:Version)
MATCH (downstream:Version)-[:DEPENDS_ON*1..5]->(vuln)
MATCH (pkg:Package)-[:HAS_VERSION]->(downstream)
RETURN DISTINCT pkg.name AS package, pkg.ecosystem AS ecosystem
ORDER BY package ASC
LIMIT 10;
