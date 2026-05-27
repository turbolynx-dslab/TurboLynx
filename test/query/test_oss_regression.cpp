// Regression tests for OSS supply-chain fixture.
//
// These tests reproduce issue #63 (AdjIdxJoin mis-reads source VID after
// an aggregation in the pipeline). The first variant catches wrong
// sid_col_idx binding via a count-only query; the second catches wrong
// outer_col_map by verifying projected values.
//
#include "catch.hpp"
#include "helpers/query_runner.hpp"
#include <algorithm>
#include <string>
#include <vector>

extern std::string g_oss_path;
extern bool g_skip_requested;
extern bool g_has_oss;
extern qtest::QueryRunner* get_oss_runner();

#define SKIP_IF_NO_OSS_DB() \
    if (g_oss_path.empty()) { WARN("--oss-path not set, skipping"); g_skip_requested = true; return; } \
    if (!g_has_oss)         { WARN("DB has no OSS schema, skipping"); return; } \
    auto* qr = get_oss_runner(); \
    if (!qr) { FAIL("Cannot open DB: " << g_oss_path); return; }

// Fixture derivation for expected values:
//   MAINTAINED_BY edges per Package:
//     log4j=2, log4js=1, lodash=2, lodashx=1, awesome-lib=1, webapp=1
//   Packages with count(m) >= 2: { log4j, lodash }                       (2 rows)
//   HAS_VERSION rows:                                                    (8 rows)
//     log4j->2.14.0, log4j->2.16.0, log4js->1.0.0,
//     lodash->4.17.20, lodash->4.17.21, lodashx->0.1.0,
//     awesome-lib->1.0.0, webapp->0.5.0
//   The second MATCH does not reference `popular`, so Cypher semantics
//   produce a cross product: 2 * 8 = 16 rows.

TEST_CASE("OSS #63 cardinality after aggregation",
          "[oss][regression63]") {
    SKIP_IF_NO_OSS_DB();
    const char* query =
        "MATCH (popular:Package)-[:MAINTAINED_BY]->(m:Maintainer) "
        "WITH popular, count(m) AS n "
        "WHERE n >= 2 "
        "MATCH (suspect:Package)-[:HAS_VERSION]->(v:Version) "
        "RETURN count(v);";
    int64_t actual = qr->count(query);
    CHECK(actual == 16);
}

TEST_CASE("OSS #63 value correctness after aggregation",
          "[oss][regression63]") {
    SKIP_IF_NO_OSS_DB();
    const char* query =
        "MATCH (popular:Package)-[:MAINTAINED_BY]->(m:Maintainer) "
        "WITH popular, count(m) AS n "
        "WHERE n >= 2 "
        "MATCH (suspect:Package)-[:HAS_VERSION]->(v:Version) "
        "RETURN popular.uid, popular.name, suspect.uid, suspect.name, "
        "       v.uid, v.version "
        "ORDER BY popular.uid, suspect.uid, v.uid;";

    auto r = qr->run(query, {qtest::ColType::UINT64, qtest::ColType::STRING,
                              qtest::ColType::UINT64, qtest::ColType::STRING,
                              qtest::ColType::UINT64, qtest::ColType::STRING});
    REQUIRE(r.size() == 16);

    // Expected cross product: popular {log4j, lodash} × 8 HAS_VERSION rows.
    struct Row { int64_t p_uid; const char* p_name;
                 int64_t s_uid; const char* s_name;
                 int64_t v_uid; const char* v_version; };
    static const Row expected[] = {
        {1, "log4j",  1, "log4j",       1, "2.14.0"},
        {1, "log4j",  1, "log4j",       2, "2.16.0"},
        {1, "log4j",  2, "log4js",      3, "1.0.0"},
        {1, "log4j",  3, "lodash",      4, "4.17.20"},
        {1, "log4j",  3, "lodash",      5, "4.17.21"},
        {1, "log4j",  4, "lodashx",     6, "0.1.0"},
        {1, "log4j",  5, "awesome-lib", 7, "1.0.0"},
        {1, "log4j",  6, "webapp",      8, "0.5.0"},
        {3, "lodash", 1, "log4j",       1, "2.14.0"},
        {3, "lodash", 1, "log4j",       2, "2.16.0"},
        {3, "lodash", 2, "log4js",      3, "1.0.0"},
        {3, "lodash", 3, "lodash",      4, "4.17.20"},
        {3, "lodash", 3, "lodash",      5, "4.17.21"},
        {3, "lodash", 4, "lodashx",     6, "0.1.0"},
        {3, "lodash", 5, "awesome-lib", 7, "1.0.0"},
        {3, "lodash", 6, "webapp",      8, "0.5.0"},
    };

    for (size_t i = 0; i < r.size(); i++) {
        const auto& row = r[i];
        CHECK(row.int64_at(0) == expected[i].p_uid);
        CHECK(row.str_at(1)   == expected[i].p_name);
        CHECK(row.int64_at(2) == expected[i].s_uid);
        CHECK(row.str_at(3)   == expected[i].s_name);
        CHECK(row.int64_at(4) == expected[i].v_uid);
        CHECK(row.str_at(5)   == expected[i].v_version);
    }
}

TEST_CASE("OSS #63 issue reproduction with filtered traversal",
          "[oss][regression63]") {
    SKIP_IF_NO_OSS_DB();
    const char* query =
        "MATCH (popular:Package)-[:MAINTAINED_BY]->(popmaint:Maintainer) "
        "WITH popular, count(popmaint) AS n "
        "WHERE n >= 2 "
        "MATCH (suspect:Package)-[:HAS_VERSION]->(v:Version) "
        "WHERE suspect.name <> popular.name "
        "  AND v.published_at >= 1768694400 "
        "RETURN popular.uid, popular.name, suspect.uid, suspect.name, "
        "       v.uid, v.published_at "
        "ORDER BY popular.uid, suspect.uid, v.uid;";

    auto r = qr->run(query, {qtest::ColType::UINT64, qtest::ColType::STRING,
                              qtest::ColType::UINT64, qtest::ColType::STRING,
                              qtest::ColType::UINT64, qtest::ColType::INT64});
    REQUIRE(r.size() == 2);

    CHECK(r[0].int64_at(0) == 1);
    CHECK(r[0].str_at(1) == "log4j");
    CHECK(r[0].int64_at(2) == 4);
    CHECK(r[0].str_at(3) == "lodashx");
    CHECK(r[0].int64_at(4) == 6);
    CHECK(r[0].int64_at(5) == 1771200000);

    CHECK(r[1].int64_at(0) == 3);
    CHECK(r[1].str_at(1) == "lodash");
    CHECK(r[1].int64_at(2) == 4);
    CHECK(r[1].str_at(3) == "lodashx");
    CHECK(r[1].int64_at(4) == 6);
    CHECK(r[1].int64_at(5) == 1771200000);
}

// Note: direct edge-traversal sanity checks (e.g. `count(v)` over
// HAS_VERSION) are not included here because the OSS fixture also trips
// issue #32 (ID seek mixes rows across partitions with the same local
// extent seqno), which corrupts the first vertex of most partitions.
// Vertex-count integrity is already verified by the query_test harness
// in probe_and_verify().

// Two bindings of the same label (vuln:Version, downstream:Version) share
// table MDId + column name `_id`; before the NodeId-aware fix the lineage
// walker collapsed them, so HAS_VERSION traversed from vuln._id instead of
// downstream._id and returned the vulnerable package itself (#68).

TEST_CASE("OSS #68 blast-radius default", "[oss][regression68]") {
    SKIP_IF_NO_OSS_DB();
    const char* query =
        "MATCH (c:CVE {id: 'CVE-2021-44228'})<-[:AFFECTED_BY]-(vuln:Version) "
        "MATCH (downstream:Version)-[:DEPENDS_ON*1..5]->(vuln) "
        "MATCH (pkg:Package)-[:HAS_VERSION]->(downstream) "
        "RETURN DISTINCT pkg.name AS package, pkg.ecosystem AS ecosystem "
        "ORDER BY package ASC;";
    auto r = qr->run(query, {qtest::ColType::STRING, qtest::ColType::STRING});
    REQUIRE(r.size() == 2);
    CHECK(r[0].str_at(0) == "awesome-lib");
    CHECK(r[0].str_at(1) == "npm");
    CHECK(r[1].str_at(0) == "webapp");
    CHECK(r[1].str_at(1) == "npm");
}

TEST_CASE("OSS #68 blast-radius alternate CVE", "[oss][regression68]") {
    SKIP_IF_NO_OSS_DB();
    const char* query =
        "MATCH (c:CVE {id: 'CVE-2021-23337'})<-[:AFFECTED_BY]-(vuln:Version) "
        "MATCH (downstream:Version)-[:DEPENDS_ON*1..5]->(vuln) "
        "MATCH (pkg:Package)-[:HAS_VERSION]->(downstream) "
        "RETURN DISTINCT pkg.name AS package, pkg.ecosystem AS ecosystem "
        "ORDER BY package ASC;";
    auto r = qr->run(query, {qtest::ColType::STRING, qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == "webapp");
    CHECK(r[0].str_at(1) == "npm");
}

TEST_CASE("OSS #68 vulnpkg vs affected pkg (Package self-join layer)",
          "[oss][regression68]") {
    SKIP_IF_NO_OSS_DB();
    // Adds a second Package binding (`vulnpkg`) so both Package and
    // Version bindings appear twice in the same plan.
    const char* query =
        "MATCH (c:CVE {id: 'CVE-2021-44228'})"
        "<-[:AFFECTED_BY]-(vuln:Version)<-[:HAS_VERSION]-(vulnpkg:Package) "
        "MATCH (downstream:Version)-[:DEPENDS_ON*1..5]->(vuln) "
        "MATCH (pkg:Package)-[:HAS_VERSION]->(downstream) "
        "WHERE pkg.name <> vulnpkg.name "
        "RETURN DISTINCT vulnpkg.name AS vp, pkg.name AS ap "
        "ORDER BY vp ASC, ap ASC;";
    auto r = qr->run(query, {qtest::ColType::STRING, qtest::ColType::STRING});
    REQUIRE(r.size() == 2);
    CHECK(r[0].str_at(0) == "log4j");
    CHECK(r[0].str_at(1) == "awesome-lib");
    CHECK(r[1].str_at(0) == "log4j");
    CHECK(r[1].str_at(1) == "webapp");
}

// Double VarLen with both ends self-joined (#181). Two layered fixes:
//
//  (a) Operator: when ORCA prunes the path output (count(*)) the second
//      VarLen's `inner_col_map` was empty, so `tgtColIdx = inner_col_map[0]`
//      walked off the vector and SIGSEGVed in ExecuteNaiveInput.
//
//  (b) Converter: PlanRegularMatch processed edges in syntax order. When
//      the first edge has both endpoints unbound (e.g. `(p1)-[r]->(v1)`)
//      and a later edge joins onto a bound anchor (`(v1)-[*]->(vuln)`),
//      the cartProd-merge in line 2076 inflated the fresh subtree against
//      qg_plan. Neo4j on the imported OSS fixture returned 4 paths; we
//      were emitting 36 (9× inflation). PlanOptionalMatch already had
//      the edge-reorder; PlanRegularMatch now mirrors it.
//
// Oracles below are Neo4j-verified against the imported OSS fixture.

TEST_CASE("OSS #181 double var-len count matches Neo4j",
          "[oss][regression181]") {
    SKIP_IF_NO_OSS_DB();
    CHECK(qr->count(
        "MATCH (c:CVE {id: 'CVE-2021-44228'})<-[:AFFECTED_BY]-(vuln:Version) "
        "MATCH (p1:Package)-[:HAS_VERSION]->(v1:Version)"
        "-[:DEPENDS_ON*1..3]->(vuln) "
        "MATCH (p2:Package)-[:HAS_VERSION]->(v2:Version)"
        "-[:DEPENDS_ON*1..3]->(vuln) "
        "RETURN count(*)") == 4);
}

TEST_CASE("OSS #181 double var-len ordered pairs match Neo4j",
          "[oss][regression181]") {
    SKIP_IF_NO_OSS_DB();
    auto r = qr->run(
        "MATCH (c:CVE {id: 'CVE-2021-44228'})<-[:AFFECTED_BY]-(vuln:Version) "
        "MATCH (p1:Package)-[:HAS_VERSION]->(v1:Version)"
        "-[:DEPENDS_ON*1..3]->(vuln) "
        "MATCH (p2:Package)-[:HAS_VERSION]->(v2:Version)"
        "-[:DEPENDS_ON*1..3]->(vuln) "
        "WHERE p1.name < p2.name "
        "RETURN p1.name AS p1, p2.name AS p2 "
        "ORDER BY p1 ASC, p2 ASC",
        {qtest::ColType::STRING, qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == "awesome-lib");
    CHECK(r[0].str_at(1) == "webapp");
}

// Cypher relationship-isomorphism: edges of the same query graph must bind
// distinct relationships. The single inbound HAS_VERSION to each Version
// fixes r1 == r2 when both endpoints are Package, so Neo4j returns 0 rows.
TEST_CASE("OSS #199 single-MATCH multi-edge enforces edge-iso",
          "[oss][regression199]") {
    SKIP_IF_NO_OSS_DB();
    CHECK(qr->count(
        "MATCH (c:CVE {id: 'CVE-2021-44228'})<-[:AFFECTED_BY]-(vuln:Version) "
        "MATCH (p:Package)-[:HAS_VERSION]->(vuln)"
        "<-[:HAS_VERSION]-(q:Package) "
        "RETURN count(*)") == 0);
}

TEST_CASE("OSS #199 single-MATCH multi-edge enforces edge-iso (projected)",
          "[oss][regression199]") {
    SKIP_IF_NO_OSS_DB();
    auto r = qr->run(
        "MATCH (c:CVE {id: 'CVE-2021-44228'})<-[:AFFECTED_BY]-(vuln:Version) "
        "MATCH (p:Package)-[:HAS_VERSION]->(vuln)"
        "<-[:HAS_VERSION]-(q:Package) "
        "RETURN p.name AS p, q.name AS q",
        {qtest::ColType::STRING, qtest::ColType::STRING});
    CHECK(r.size() == 0);
}

// Three-edge same-type chain: exercises the AND-chain across multiple
// prior edges. Each Version has exactly one inbound HAS_VERSION, so r1==r2
// is forced; iso rejects every row.
TEST_CASE("OSS #199 three-edge HAS_VERSION enforces transitive iso",
          "[oss][regression199]") {
    SKIP_IF_NO_OSS_DB();
    CHECK(qr->count(
        "MATCH (p:Package)-[:HAS_VERSION]->(v:Version)"
        "<-[:HAS_VERSION]-(q:Package)-[:HAS_VERSION]->(v2:Version) "
        "RETURN count(*)") == 0);
}

// Control for the two cases above: Cypher iso applies only within a single
// MATCH's query graph. Splitting the pattern into separate MATCHes bypasses
// iso, so the same pattern returns a non-zero count. Neo4j oracles:
//   2-edge split → 1, 3-edge split → 12. The 0-vs-N gap proves the new
//   iso predicate is what's rejecting rows above, not a join misfire.
TEST_CASE("OSS #199 split-MATCH control matches Neo4j non-iso count",
          "[oss][regression199]") {
    SKIP_IF_NO_OSS_DB();
    CHECK(qr->count(
        "MATCH (c:CVE {id: 'CVE-2021-44228'})<-[:AFFECTED_BY]-(vuln:Version) "
        "MATCH (p:Package)-[:HAS_VERSION]->(vuln) "
        "MATCH (q:Package)-[:HAS_VERSION]->(vuln) "
        "RETURN count(*)") == 1);
    CHECK(qr->count(
        "MATCH (p:Package)-[:HAS_VERSION]->(v:Version) "
        "MATCH (q:Package)-[:HAS_VERSION]->(v) "
        "MATCH (q)-[:HAS_VERSION]->(v2:Version) "
        "RETURN count(*)") == 12);
}

// a-b-a sequence (HAS_VERSION, DEPENDS_ON, HAS_VERSION). The partition-
// overlap filter must SKIP iso between HV and DEPENDS_ON (cross-type) and
// only enforce r1 != r3 between the two HV edges. Both HV edges connect
// distinct (package, version) pairs so iso passes; cardinality must
// match Neo4j (= 4).
TEST_CASE("OSS #199 mixed-type sequence skips cross-type iso",
          "[oss][regression199]") {
    SKIP_IF_NO_OSS_DB();
    auto r = qr->run(
        "MATCH (p1:Package)-[:HAS_VERSION]->(v1:Version)"
        "<-[:DEPENDS_ON]-(v2:Version)<-[:HAS_VERSION]-(p2:Package) "
        "RETURN p1.name AS p1, v1.uid AS v1, v2.uid AS v2, p2.name AS p2 "
        "ORDER BY p1, p2",
        {qtest::ColType::STRING, qtest::ColType::UINT64,
         qtest::ColType::UINT64, qtest::ColType::STRING});
    REQUIRE(r.size() == 4);
    CHECK(r[0].str_at(0) == "awesome-lib");
    CHECK(r[0].int64_at(1) == 7);
    CHECK(r[0].int64_at(2) == 8);
    CHECK(r[0].str_at(3) == "webapp");
    CHECK(r[1].str_at(0) == "lodash");
    CHECK(r[1].int64_at(1) == 4);
    CHECK(r[1].int64_at(2) == 8);
    CHECK(r[1].str_at(3) == "webapp");
    CHECK(r[2].str_at(0) == "lodashx");
    CHECK(r[2].int64_at(1) == 6);
    CHECK(r[2].int64_at(2) == 7);
    CHECK(r[2].str_at(3) == "awesome-lib");
    CHECK(r[3].str_at(0) == "log4j");
    CHECK(r[3].int64_at(1) == 1);
    CHECK(r[3].int64_at(2) == 7);
    CHECK(r[3].str_at(3) == "awesome-lib");
}
