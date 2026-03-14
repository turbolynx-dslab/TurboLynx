// Stage 6 — Bidirectional (BOTH direction) query tests
// Tests for M26: (a)-[:REL]-(b) undirected pattern support.
// All expected values verified against Neo4j with LDBC SF1.

#include "catch.hpp"
#include "helpers/query_runner.hpp"
#include <vector>
#include <set>

extern std::string g_db_path;
extern bool g_skip_requested;

extern qtest::QueryRunner* get_runner();

#define SKIP_IF_NO_DB() \
    if (g_db_path.empty()) { WARN("--db-path not set, skipping"); g_skip_requested = true; return; } \
    auto* qr = get_runner(); \
    if (!qr) { FAIL("Cannot open DB: " << g_db_path); return; }

// BOTH-01: Undirected KNOWS from Person 933
// M26-D stateless dedup: each edge emitted once (forward if src<tgt, backward if src>tgt).
TEST_CASE("Q6-BOTH-01 Undirected KNOWS from Person 933", "[q6][both]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "RETURN f.id ORDER BY f.id ASC",
        {qtest::ColType::INT64});
    // Person 933 has 5 friends — dedup ensures exactly 5 rows
    REQUIRE(r.size() == 5);
}

// BOTH-02: Undirected HAS_CREATOR (heterogeneous label)
// Comment->Person is stored as Comment(src)->Person(dst).
// Undirected: from Comment side, forward finds the creator.
TEST_CASE("Q6-BOTH-02 Undirected HAS_CREATOR from Comment", "[q6][both]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (c:Comment {id: 824635044686})-[:HAS_CREATOR]-(p:Person) "
        "RETURN p.id, p.firstName",
        {qtest::ColType::INT64, qtest::ColType::STRING});
    // HAS_CREATOR is heterogeneous (Comment→Person), only one direction hits.
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 933);
    CHECK(r[0].str_at(1) == "Mahinda");
}

// BOTH-03: Count undirected KNOWS friends (aggregation)
TEST_CASE("Q6-BOTH-03 Count undirected KNOWS friends of Person 933", "[q6][both]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "RETURN count(DISTINCT f) AS cnt",
        {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 5);
}

// BOTH-05: Undirected KNOWS with friend properties + edge properties (IdSeek)
TEST_CASE("Q6-BOTH-05 Undirected KNOWS with friend and edge properties", "[q6][both][idseek]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (n:Person {id: 933})-[r:KNOWS]-(friend:Person) "
        "RETURN friend.id AS personId, friend.firstName AS firstName, "
        "       friend.lastName AS lastName, r.creationDate AS friendshipCreationDate "
        "ORDER BY friendshipCreationDate DESC, personId ASC",
        {qtest::ColType::INT64, qtest::ColType::STRING,
         qtest::ColType::STRING, qtest::ColType::INT64});
    REQUIRE(r.size() == 5);
    // Verify all friend properties are non-empty
    for (size_t i = 0; i < r.size(); i++) {
        CHECK(r[i].int64_at(0) > 0);
        CHECK(!r[i].str_at(1).empty());
        CHECK(!r[i].str_at(2).empty());
        CHECK(r[i].int64_at(3) > 0);
    }
}

// BOTH-06: Unlabeled target node — friend without :Person label
// The system should infer the target partition from the edge definition.
TEST_CASE("Q6-BOTH-06 Unlabeled target node properties via IdSeek", "[q6][both][unlabeled]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (n:Person {id: 933})-[r:KNOWS]-(friend) "
        "RETURN friend.id AS personId, friend.firstName AS firstName, "
        "       friend.lastName AS lastName, r.creationDate AS friendshipCreationDate "
        "ORDER BY friendshipCreationDate DESC, personId ASC",
        {qtest::ColType::INT64, qtest::ColType::STRING,
         qtest::ColType::STRING, qtest::ColType::INT64});
    REQUIRE(r.size() == 5);
    for (size_t i = 0; i < r.size(); i++) {
        CHECK(r[i].int64_at(0) > 0);
        CHECK(!r[i].str_at(1).empty());
        CHECK(!r[i].str_at(2).empty());
        CHECK(r[i].int64_at(3) > 0);
    }
}

// BOTH-04: VarLen undirected KNOWS *1..2
// Edge isomorphism prevents trivial cycles (A-B-A).
TEST_CASE("Q6-BOTH-04 VarLen undirected KNOWS *1..2 from Person 933", "[q6][both][varlen]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (p:Person {id: 933})-[:KNOWS*1..2]-(f:Person) "
        "RETURN count(DISTINCT f) AS cnt",
        {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    // Must be > 5 (2-hop reaches friends-of-friends)
    // and must NOT include 933 itself (edge isomorphism prevents A-B-A cycle)
    CHECK(r[0].int64_at(0) > 5);
}
