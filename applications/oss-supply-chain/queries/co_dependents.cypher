// S1.4 — Vulnerable package + each downstream package.
//
// Adds a second Package binding (vulnpkg) alongside the existing
// downstream pkg binding so the planner has to distinguish two
// Package aliases at the same time as it distinguishes the two
// Version aliases (vuln/downstream). Same chained-MATCH + var-len
// shape as blast_radius, so the regression net stays focused on #68
// without doubling the var-len pattern (which can SIGSEGV).

MATCH (c:CVE {id: 'CVE-2021-44228'})<-[:AFFECTED_BY]-(vuln:Version)<-[:HAS_VERSION]-(vulnpkg:Package)
MATCH (downstream:Version)-[:DEPENDS_ON*1..5]->(vuln)
MATCH (pkg:Package)-[:HAS_VERSION]->(downstream)
WHERE pkg.name <> vulnpkg.name
RETURN DISTINCT vulnpkg.name AS vuln_pkg, pkg.name AS affected
ORDER BY vuln_pkg ASC, affected ASC
LIMIT 10;
