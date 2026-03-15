// Stage 2 — Property filter + 1-hop traversal
// All expected values verified against Neo4j 5.24.0 with LDBC SF1.

#include "catch.hpp"
#include "helpers/query_runner.hpp"

extern std::string g_db_path;
extern bool g_skip_requested;

extern qtest::QueryRunner* get_runner();

#define SKIP_IF_NO_DB() \
    if (g_db_path.empty()) { WARN("--db-path not set, skipping"); g_skip_requested = true; return; } \
    auto* qr = get_runner(); \
    if (!qr) { FAIL("Cannot open DB: " << g_db_path); return; }

TEST_CASE("Q2-01 Person 933 firstName/lastName", "[q2][filter]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (p:Person) WHERE p.id = 933 "
        "RETURN p.firstName, p.lastName",
        {qtest::ColType::STRING, qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == "Mahinda");
    CHECK(r[0].str_at(1) == "Perera");
}

TEST_CASE("Q2-02 Person 933 outgoing KNOWS count", "[q2][filter]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count(
        "MATCH (p:Person {id: 933})-[:KNOWS]->(friend:Person) "
        "RETURN count(friend)") == 5);
}

TEST_CASE("Q2-03 Person 933 IS_LOCATED_IN city", "[q2][filter]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (p:Person {id: 933})-[:IS_LOCATED_IN]->(pl:Place) "
        "RETURN pl.id, pl.name",
        {qtest::ColType::INT64, qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 1353);
    CHECK(r[0].str_at(1) == "Kelaniya");
}

TEST_CASE("Q2-04 Person 933 properties", "[q2][filter]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (p:Person {id: 933}) "
        "RETURN p.gender, p.birthday, p.locationIP, p.browserUsed",
        {qtest::ColType::STRING, qtest::ColType::INT64,
         qtest::ColType::STRING, qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == "male");
    CHECK(r[0].int64_at(1) == 628646400000LL);
    CHECK(r[0].str_at(2) == "119.235.7.103");
    CHECK(r[0].str_at(3) == "Firefox");
}

TEST_CASE("Q2-05 Comment 1236950581249 creator", "[q2][filter]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (c:Comment)-[:HAS_CREATOR]->(p:Person) "
        "WHERE c.id = 1236950581249 "
        "RETURN p.id, p.firstName",
        {qtest::ColType::INT64, qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 10995116284808LL);
    CHECK(r[0].str_at(1) == "Andrei");
}

TEST_CASE("Q2-06 Forum 77644 post count", "[q2][filter]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count(
        "MATCH (f:Forum)-[:CONTAINER_OF]->(p:Post) WHERE f.id = 77644 "
        "RETURN count(p)") == 1208);
}

TEST_CASE("Q2-07 Post count with Tag Genghis_Khan", "[q2][filter]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count(
        "MATCH (p:Post)-[:HAS_TAG]->(t:Tag) WHERE t.name = 'Genghis_Khan' "
        "RETURN count(p)") == 3715);
}

TEST_CASE("Q2-08 Person 933 liked Comments count", "[q2][filter]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count(
        "MATCH (p:Person {id: 933})-[:LIKES]->(c:Comment) "
        "RETURN count(c)") == 12);
}

TEST_CASE("Q2-09 Person 933 liked Posts count", "[q2][filter]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count(
        "MATCH (p:Person {id: 933})-[:LIKES]->(po:Post) "
        "RETURN count(po)") == 5);
}

// ---------------------------------------------------------------------------
// Multi-partition vertex/edge tests (M28 — :Message = Comment + Post)
// ---------------------------------------------------------------------------

TEST_CASE("Q2-10 Person 933 Messages via HAS_CREATOR count", "[q2][filter][mpe]") {
    SKIP_IF_NO_DB();
    // Person 933 authored 57 comments + 313 posts = 370 messages
    REQUIRE(qr->count(
        "MATCH (:Person {id: 933})<-[:HAS_CREATOR]-(m:Message) "
        "RETURN count(m)") == 370);
}

TEST_CASE("Q2-11 Person 933 liked Messages count", "[q2][filter][mpe]") {
    SKIP_IF_NO_DB();
    // Person 933 liked 12 comments + 5 posts = 17 messages
    REQUIRE(qr->count(
        "MATCH (p:Person {id: 933})-[:LIKES]->(m:Message) "
        "RETURN count(m)") == 17);
}

TEST_CASE("Q2-12 Message count with Tag Genghis_Khan", "[q2][filter][mpe]") {
    SKIP_IF_NO_DB();
    // Genghis_Khan tag: Comment HAS_TAG 5267 + Post HAS_TAG 3715 = 8982
    REQUIRE(qr->count(
        "MATCH (m:Message)-[:HAS_TAG]->(t:Tag) WHERE t.name = 'Genghis_Khan' "
        "RETURN count(m)") == 8982);
}
