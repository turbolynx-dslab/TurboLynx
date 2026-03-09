// Stage 1 — Node / Edge count smoke tests
// All expected values verified against Neo4j 5.24.0 with LDBC SF1.

#include "catch.hpp"
#include "helpers/query_runner.hpp"
#include <iostream>

extern std::string g_db_path;
extern bool g_skip_requested;

extern qtest::QueryRunner* get_runner();

#define SKIP_IF_NO_DB() \
    if (g_db_path.empty()) { WARN("--db-path not set, skipping"); g_skip_requested = true; return; } \
    auto* qr = get_runner(); \
    if (!qr) { FAIL("Cannot open DB: " << g_db_path); return; }

// ---------------------------------------------------------------------------
// Node counts
// ---------------------------------------------------------------------------

TEST_CASE("Q1-01 Person count", "[q1][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (p:Person) RETURN count(p)") == 9892);
}

TEST_CASE("Q1-02 Comment count", "[q1][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (c:Comment) RETURN count(c)") == 2052169);
}

TEST_CASE("Q1-03 Post count", "[q1][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (p:Post) RETURN count(p)") == 1003605);
}

TEST_CASE("Q1-04 Forum count", "[q1][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (f:Forum) RETURN count(f)") == 90492);
}

TEST_CASE("Q1-05 Tag count", "[q1][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (t:Tag) RETURN count(t)") == 16080);
}

TEST_CASE("Q1-06 TagClass count", "[q1][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (tc:TagClass) RETURN count(tc)") == 71);
}

TEST_CASE("Q1-07 Place count", "[q1][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (pl:Place) RETURN count(pl)") == 1460);
}

TEST_CASE("Q1-08 Organisation count", "[q1][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (o:Organisation) RETURN count(o)") == 7955);
}

// ---------------------------------------------------------------------------
// Edge counts
// ---------------------------------------------------------------------------

// Edge counts use named src/dst variables (anonymous typed node patterns not supported).
// Each edge type is stored for exactly one src/dst type pair, so omitting src/dst
// labels gives the same count.

TEST_CASE("Q1-09 KNOWS edge count", "[q1][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN count(r)") == 2889968);
}

TEST_CASE("Q1-10 HAS_CREATOR (Comment->Person) count", "[q1][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (a:Comment)-[r:HAS_CREATOR]->(b:Person) RETURN count(r)") == 2052169);
}

TEST_CASE("Q1-11 POST_HAS_CREATOR (Post->Person) count", "[q1][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (a:Post)-[r:POST_HAS_CREATOR]->(b:Person) RETURN count(r)") == 1003605);
}

TEST_CASE("Q1-12 LIKES (Person->Comment) count", "[q1][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (a:Person)-[r:LIKES]->(b:Comment) RETURN count(r)") == 1438418);
}

TEST_CASE("Q1-13 LIKES_POST (Person->Post) count", "[q1][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (a:Person)-[r:LIKES_POST]->(b:Post) RETURN count(r)") == 751677);
}

TEST_CASE("Q1-14 CONTAINER_OF (Forum->Post) count", "[q1][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (a:Forum)-[r:CONTAINER_OF]->(b:Post) RETURN count(r)") == 1003605);
}

TEST_CASE("Q1-15 REPLY_OF (Comment->Post) count", "[q1][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (a:Comment)-[r:REPLY_OF]->(b:Post) RETURN count(r)") == 1011420);
}

TEST_CASE("Q1-16 REPLY_OF_COMMENT (Comment->Comment) count", "[q1][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (a:Comment)-[r:REPLY_OF_COMMENT]->(b:Comment) RETURN count(r)") == 1040749);
}

TEST_CASE("Q1-17 HAS_TAG (Comment->Tag) count", "[q1][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (a:Comment)-[r:HAS_TAG]->(b:Tag) RETURN count(r)") == 2698393);
}

TEST_CASE("Q1-18 POST_HAS_TAG (Post->Tag) count", "[q1][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (a:Post)-[r:POST_HAS_TAG]->(b:Tag) RETURN count(r)") == 713258);
}

TEST_CASE("Q1-19 FORUM_HAS_TAG (Forum->Tag) count", "[q1][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (a:Forum)-[r:FORUM_HAS_TAG]->(b:Tag) RETURN count(r)") == 309766);
}

TEST_CASE("Q1-20 HAS_TYPE (Tag->TagClass) count", "[q1][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (a:Tag)-[r:HAS_TYPE]->(b:TagClass) RETURN count(r)") == 16080);
}

TEST_CASE("Q1-21 IS_PART_OF (Place->Place) count", "[q1][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (a:Place)-[r:IS_PART_OF]->(b:Place) RETURN count(r)") == 1454);
}
