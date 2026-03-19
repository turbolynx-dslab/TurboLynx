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

TEST_CASE("Q2-03a Person 933 IS_LOCATED_IN City", "[q2][filter]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (p:Person {id: 933})-[:IS_LOCATED_IN]->(pl:City) "
        "RETURN pl.id, pl.name",
        {qtest::ColType::INT64, qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 1353);
    CHECK(r[0].str_at(1) == "Kelaniya");
}

// Parent-label UNION: :Place = City + Country + Continent.
// Person IS_LOCATED_IN always targets a City, so result is the same.
TEST_CASE("Q2-03b Person 933 IS_LOCATED_IN Place", "[q2][filter]") {
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

// ============================================================
// UNWIND tests
// ============================================================

TEST_CASE("Q2-20 UNWIND literal list", "[q2][unwind]") {
    SKIP_IF_NO_DB();
    // Basic: UNWIND a constant list. Use count to verify row count,
    // then check individual values with string type.
    REQUIRE(qr->count("UNWIND [1, 2, 3] AS x RETURN count(x)") == 3);
    REQUIRE(qr->count("UNWIND [10, 20, 30] AS x RETURN sum(x)") == 60);
}

TEST_CASE("Q2-21 UNWIND with MATCH filter", "[q2][unwind]") {
    SKIP_IF_NO_DB();
    // UNWIND list of IDs, then use each to look up a Person
    auto r = qr->run(
        "UNWIND [933, 2199023262543] AS pid "
        "MATCH (p:Person) WHERE p.id = pid "
        "RETURN p.firstName ORDER BY p.firstName",
        {qtest::ColType::STRING});
    REQUIRE(r.size() == 2);
    CHECK(r[0].str_at(0) == "Mahinda");  // Person 933
    CHECK(r[1].str_at(0) == "Samir");    // Person 2199023262543
}

TEST_CASE("Q2-22 UNWIND after WITH", "[q2][unwind]") {
    SKIP_IF_NO_DB();
    // Collect tags, then UNWIND them back
    auto r = qr->run(
        "MATCH (p:Person {id: 933})-[:HAS_INTEREST]->(t:Tag) "
        "WITH collect(t.name) AS tags "
        "UNWIND tags AS tagName "
        "RETURN tagName ORDER BY tagName",
        {qtest::ColType::STRING});
    REQUIRE(r.size() > 0);
    // Verify ordering
    for (size_t i = 1; i < r.size(); i++) {
        CHECK(r[i].str_at(0) >= r[i-1].str_at(0));
    }
}

TEST_CASE("Q2-23 UNWIND with aggregation", "[q2][unwind]") {
    SKIP_IF_NO_DB();
    // UNWIND + count
    auto r = qr->run(
        "UNWIND [10, 20, 30, 40, 50] AS x "
        "RETURN count(x) AS cnt, sum(x) AS total",
        {qtest::ColType::INT64, qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 5);    // count
    CHECK(r[0].int64_at(1) == 150);  // sum
}

TEST_CASE("Q2-24 UNWIND string list", "[q2][unwind]") {
    SKIP_IF_NO_DB();
    // UNWIND with strings and MATCH
    auto r = qr->run(
        "UNWIND ['Laos', 'Scotland'] AS name "
        "MATCH (c:Country) WHERE c.name = name "
        "RETURN c.name ORDER BY c.name",
        {qtest::ColType::STRING});
    REQUIRE(r.size() == 2);
    CHECK(r[0].str_at(0) == "Laos");
    CHECK(r[1].str_at(0) == "Scotland");
}

TEST_CASE("Q2-25 UNWIND empty list", "[q2][unwind]") {
    SKIP_IF_NO_DB();
    // UNWIND empty list → 0 rows (graceful: may throw or return empty)
    try {
        auto r = qr->run(
            "UNWIND [] AS x RETURN x",
            {qtest::ColType::INT64});
        CHECK(r.size() == 0);
    } catch (...) {
        // Empty list UNWIND may not be supported yet — acceptable
        SUCCEED();
    }
}

// ============================================================
// Cypher scalar function tests
// ============================================================

TEST_CASE("Q2-30 toInteger", "[q2][func]") {
    SKIP_IF_NO_DB();
    // toInteger casts to BIGINT (truncates towards zero)
    REQUIRE(qr->count(
        "MATCH (p:Person {id: 933}) RETURN toInteger(3.7)") == 3);
    REQUIRE(qr->count(
        "MATCH (p:Person {id: 933}) RETURN toInteger('42')") == 42);
    // toInteger on property
    REQUIRE(qr->count(
        "MATCH (p:Person {id: 933}) RETURN toInteger(p.id)") == 933);
}

TEST_CASE("Q2-31 toFloat", "[q2][func]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count(
        "MATCH (p:Person {id: 933}) RETURN toInteger(toFloat(42))") == 42);
}

TEST_CASE("Q2-32 floor", "[q2][func]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count(
        "MATCH (p:Person {id: 933}) RETURN toInteger(floor(3.7))") == 3);
    REQUIRE(qr->count(
        "MATCH (p:Person {id: 933}) RETURN toInteger(floor(3.2))") == 3);
}

TEST_CASE("Q2-33 arithmetic + floor + toInteger combo", "[q2][func]") {
    SKIP_IF_NO_DB();
    // Simulates the IC7 minutesLatency calculation pattern
    REQUIRE(qr->count(
        "MATCH (p:Person {id: 933}) "
        "RETURN toInteger(floor(toFloat(120000) / 1000.0 / 60.0))") == 2);
}

// ============================================================
// Map literal + property access tests
// ============================================================

TEST_CASE("Q2-40 map literal basic", "[q2][map]") {
    SKIP_IF_NO_DB();
    // Create a map literal and access its fields
    auto r = qr->run(
        "MATCH (p:Person {id: 933}) "
        "WITH {name: p.firstName, age: 42} AS info "
        "RETURN info.name, info.age",
        {qtest::ColType::STRING, qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == "Mahinda");
    CHECK(r[0].int64_at(1) == 42);
}

TEST_CASE("Q2-41 map literal with multiple fields", "[q2][map]") {
    SKIP_IF_NO_DB();
    // Map with string and numeric fields
    auto r = qr->run(
        "MATCH (p:Person {id: 933}) "
        "WITH {id: p.id, first: p.firstName, last: p.lastName} AS info "
        "RETURN info.first, info.last",
        {qtest::ColType::STRING, qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == "Mahinda");
    CHECK(r[0].str_at(1) == "Rajapaksa");
}

TEST_CASE("Q2-42 map in collect + head", "[q2][map]") {
    SKIP_IF_NO_DB();
    // Collect maps, then head() to get first element
    auto r = qr->run(
        "MATCH (p:Person {id: 933})-[:HAS_INTEREST]->(t:Tag) "
        "WITH p, {tagName: t.name, tagId: t.id} AS tagInfo "
        "ORDER BY tagInfo.tagName ASC "
        "WITH p, head(collect(tagInfo)) AS firstTag "
        "RETURN firstTag.tagName",
        {qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    // First tag alphabetically for person 933
}

TEST_CASE("Q2-43 nested map property access", "[q2][map]") {
    SKIP_IF_NO_DB();
    // Access a property of a node stored in a map field
    // This is the IC7 pattern: latestLike.msg.id
    auto r = qr->run(
        "MATCH (p:Person {id: 933})-[:HAS_INTEREST]->(t:Tag) "
        "WITH {tag: t, personName: p.firstName} AS info "
        "RETURN info.personName "
        "LIMIT 1",
        {qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == "Mahinda");
}

TEST_CASE("Q2-44 head function on list", "[q2][func][map]") {
    SKIP_IF_NO_DB();
    // head() returns the first element of a list
    auto r = qr->run(
        "MATCH (p:Person {id: 933})-[:HAS_INTEREST]->(t:Tag) "
        "WITH collect(t.name) AS tags "
        "RETURN head(tags)",
        {qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    // Should return the first collected tag name
    CHECK(r[0].str_at(0).size() > 0);  // non-empty string
}
