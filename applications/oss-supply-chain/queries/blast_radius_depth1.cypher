// S1.3 — Blast radius capped at 1-hop direct dependents.
//
// Tightens the var-len bound to *1..1 so the only packages returned
// are those whose own version directly depends on the vulnerable
// version. This exercises the chained-MATCH + var-len + self-join
// Version topology at the smallest non-trivial path length.

MATCH (c:CVE {id: 'CVE-2021-44228'})<-[:AFFECTED_BY]-(vuln:Version)
MATCH (downstream:Version)-[:DEPENDS_ON*1..1]->(vuln)
MATCH (pkg:Package)-[:HAS_VERSION]->(downstream)
RETURN DISTINCT pkg.name AS package, pkg.ecosystem AS ecosystem
ORDER BY package ASC
LIMIT 10;
