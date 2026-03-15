// Stage 7 — Multi-partition vertex tests (M28)
// Tests for queries using super-labels (e.g., :Message → Comment + Post)
// that span multiple vertex partitions.
// All expected values verified against Neo4j 5.24.0 with LDBC SF1.

#include "catch.hpp"
#include "helpers/query_runner.hpp"
#include <vector>

extern std::string g_db_path;
extern bool g_skip_requested;

extern qtest::QueryRunner* get_runner();

#define SKIP_IF_NO_DB() \
    if (g_db_path.empty()) { WARN("--db-path not set, skipping"); g_skip_requested = true; return; } \
    auto* qr = get_runner(); \
    if (!qr) { FAIL("Cannot open DB: " << g_db_path); return; }

// MPV-01: Count Comments via HAS_CREATOR using :Message label
// Message maps to both Comment and Post partitions.
// HAS_CREATOR only connects Comment→Person, so only Comment matches.
TEST_CASE("Q7-MPV-01 Message via HAS_CREATOR count (Comment only)", "[q7][mpv]") {
    SKIP_IF_NO_DB();
    // Same as: MATCH (:Person {id: 933})<-[:HAS_CREATOR]-(c:Comment) RETURN count(c)
    // Person 933 has authored some comments via HAS_CREATOR.
    auto r = qr->run(
        "MATCH (:Person {id: 933})<-[:HAS_CREATOR]-(message:Message) "
        "RETURN count(message)",
        {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    // Verify against explicit Comment query
    auto r2 = qr->run(
        "MATCH (:Person {id: 933})<-[:HAS_CREATOR]-(c:Comment) "
        "RETURN count(c)",
        {qtest::ColType::INT64});
    REQUIRE(r2.size() == 1);
    CHECK(r[0].int64_at(0) == r2[0].int64_at(0));
}

// MPV-02: Count all Messages via REPLY_OF (multi-partition edge + multi-partition vertex)
// REPLY_OF connects Comment→Post and Comment→Comment.
// :Message should capture both Post and Comment destinations.
// TODO: Requires multi-schema outer support in IdSeek (REPLY_OF has multi-edge partitions
// from M27, so AdjIdxJoin outputs with schema_idx > 0, but IdSeek asserts num_outer_schemas=1).
TEST_CASE("Q7-MPV-02 REPLY_OF to Message count", "[q7][mpv][!mayfail]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (c:Comment)-[:REPLY_OF]->(m:Message) "
        "RETURN count(m)",
        {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    // Should equal REPLY_OF→Post count + REPLY_OF→Comment count
    auto r_post = qr->run(
        "MATCH (c:Comment)-[:REPLY_OF]->(p:Post) "
        "RETURN count(p)",
        {qtest::ColType::INT64});
    auto r_comment = qr->run(
        "MATCH (c:Comment)-[:REPLY_OF]->(c2:Comment) "
        "RETURN count(c2)",
        {qtest::ColType::INT64});
    if (r_post.size() == 1 && r_comment.size() == 1) {
        CHECK(r[0].int64_at(0) == r_post[0].int64_at(0) + r_comment[0].int64_at(0));
    }
}

// MPV-03: Message properties — firstName from Comment via HAS_CREATOR
TEST_CASE("Q7-MPV-03 Message properties via HAS_CREATOR", "[q7][mpv]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (:Person {id: 933})<-[:HAS_CREATOR]-(message:Message) "
        "RETURN message.id "
        "ORDER BY message.id ASC LIMIT 3",
        {qtest::ColType::INT64});
    REQUIRE(r.size() >= 1);
    // Verify against explicit Comment query
    auto r2 = qr->run(
        "MATCH (:Person {id: 933})<-[:HAS_CREATOR]-(c:Comment) "
        "RETURN c.id "
        "ORDER BY c.id ASC LIMIT 3",
        {qtest::ColType::INT64});
    REQUIRE(r2.size() >= 1);
    CHECK(r[0].int64_at(0) == r2[0].int64_at(0));
}
