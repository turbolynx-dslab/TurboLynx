// Regression tests for OSS supply-chain fixture.
//
// These tests reproduce issue #63 (AdjIdxJoin mis-reads source VID after
// an aggregation in the pipeline). The first variant catches wrong
// sid_col_idx binding via a count-only query; the second catches wrong
// outer_col_map by verifying projected values.
//
// Tagged [!mayfail] because the underlying planner bug is not yet fixed:
//   - Release build: cardinality query returns 8 instead of 16.
//   - Debug build:   adjidxjoin.cpp:977 assertion aborts the process.
// When issue #63 is resolved, remove the [!mayfail] tag.

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
          "[oss][regression63][!mayfail]") {
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
          "[oss][regression63][!mayfail]") {
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

// Note: direct edge-traversal sanity checks (e.g. `count(v)` over
// HAS_VERSION) are not included here because the OSS fixture also trips
// issue #32 (ID seek mixes rows across partitions with the same local
// extent seqno), which corrupts the first vertex of most partitions.
// Vertex-count integrity is already verified by the query_test harness
// in probe_and_verify().
