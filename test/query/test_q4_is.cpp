// Stage 4 — LDBC IS queries (q1–q7 from queries/ldbc/sf1/)
// All expected values verified against Neo4j 5.24.0 with LDBC SF1.
// Edge names translated to our system's fine-grained types.

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

// IS1 (q1.cql) — Person basic info with city
TEST_CASE("Q4-IS1 Person 35184372099695 basic info", "[q4][is]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (n:Person {id: 35184372099695})-[:IS_LOCATED_IN]->(p:Place) "
        "RETURN n.firstName, n.lastName, n.birthday, n.locationIP, "
        "       n.browserUsed, p.id, n.gender, n.creationDate",
        {qtest::ColType::STRING, qtest::ColType::STRING,
         qtest::ColType::INT64,  qtest::ColType::STRING,
         qtest::ColType::STRING, qtest::ColType::INT64,
         qtest::ColType::STRING, qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == "Changpeng");
    CHECK(r[0].str_at(1) == "Wei");
    CHECK(r[0].int64_at(2) == 337132800000LL);
    CHECK(r[0].str_at(3) == "1.1.39.242");
    CHECK(r[0].str_at(4) == "Internet Explorer");
    CHECK(r[0].int64_at(5) == 367);
    CHECK(r[0].str_at(6) == "female");
    CHECK(r[0].int64_at(7) == 1347431652132LL);
}

// IS2 (q2.cql) — Recent 10 messages by Person 933 with originating post
TEST_CASE("Q4-IS2 Person 933 recent 10 comments with post info", "[q4][is]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (:Person {id: 933})<-[:HAS_CREATOR]-(message:Comment) "
        "WITH message ORDER BY message.creationDate DESC, message.id ASC LIMIT 10 "
        "MATCH (message)-[:REPLY_OF|REPLY_OF_COMMENT*1..8]->(:Post)"
        "      <-[:POST_HAS_CREATOR]-(person:Person) "
        "RETURN message.id, message.creationDate, person.id "
        "ORDER BY message.creationDate DESC, message.id ASC",
        {qtest::ColType::INT64, qtest::ColType::INT64, qtest::ColType::INT64});
    REQUIRE(r.size() == 10);
    // Verify first and last rows
    CHECK(r[0].int64_at(0) == 2199027727462LL);
    CHECK(r[0].int64_at(1) == 1347156463979LL);
    CHECK(r[0].int64_at(2) == 32985348833579LL);

    CHECK(r[9].int64_at(0) == 1786707216224LL);
    CHECK(r[9].int64_at(1) == 1335668918002LL);
    CHECK(r[9].int64_at(2) == 10995116284808LL);
}

// IS3 (q3.cql) — Friend list for Person 933
TEST_CASE("Q4-IS3 Person 933 friend list", "[q4][is]") {
    SKIP_IF_NO_DB();
    // KNOWS is bidirectional in LDBC data (both directions stored)
    // Use outgoing + DISTINCT to get canonical 5 friends
    auto r = qr->run(
        "MATCH (n:Person {id: 933})-[r:KNOWS]->(friend:Person) "
        "RETURN DISTINCT friend.id, friend.firstName, friend.lastName, "
        "       r.creationDate "
        "ORDER BY r.creationDate DESC, friend.id ASC LIMIT 20",
        {qtest::ColType::INT64, qtest::ColType::STRING,
         qtest::ColType::STRING, qtest::ColType::INT64});
    // Person 933 has 5 distinct friends
    REQUIRE(r.size() == 5);
    CHECK(r[0].int64_at(0) == 32985348833579LL);
    CHECK(r[0].str_at(1) == "Otto");
    CHECK(r[0].str_at(2) == "Becker");
    CHECK(r[0].int64_at(3) == 1346980290195LL);

    CHECK(r[4].int64_at(0) == 4139);
    CHECK(r[4].str_at(1) == "Baruch");
    CHECK(r[4].str_at(2) == "Dego");
    CHECK(r[4].int64_at(3) == 1268465841718LL);
}

// IS4 (q4.cql) — Post content
TEST_CASE("Q4-IS4 Post 2199029886840 content", "[q4][is]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (m:Post {id: 2199029886840}) "
        "RETURN m.creationDate, m.imageFile",
        {qtest::ColType::INT64, qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 1347463431887LL);
    CHECK(r[0].str_at(1) == "photo2199029886840.jpg");
}

// IS5 (q5.cql) — Comment creator
TEST_CASE("Q4-IS5 Comment 824635044686 creator", "[q4][is]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (m:Comment {id: 824635044686})-[:HAS_CREATOR]->(p:Person) "
        "RETURN p.id, p.firstName, p.lastName",
        {qtest::ColType::INT64, qtest::ColType::STRING, qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 933);
    CHECK(r[0].str_at(1) == "Mahinda");
    CHECK(r[0].str_at(2) == "Perera");
}

// IS6 (q6.cql) — Forum containing the message (via REPLY_OF chain)
TEST_CASE("Q4-IS6 Forum of Comment 824635044686", "[q4][is]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (m:Comment {id: 824635044686})"
        "      -[:REPLY_OF|REPLY_OF_COMMENT*0..8]->(:Post)"
        "      <-[:CONTAINER_OF]-(f:Forum)"
        "      -[:HAS_MODERATOR]->(mod:Person) "
        "RETURN f.id, f.title, mod.id, mod.firstName, mod.lastName",
        {qtest::ColType::INT64, qtest::ColType::STRING,
         qtest::ColType::INT64, qtest::ColType::STRING, qtest::ColType::STRING});
    REQUIRE(r.size() >= 1);
    CHECK(r[0].int64_at(0) == 412317916558LL);
    CHECK(r[0].str_at(1) == "Wall of Fritz Muller");
    CHECK(r[0].int64_at(2) == 6597069777240LL);
    CHECK(r[0].str_at(3) == "Fritz");
    CHECK(r[0].str_at(4) == "Muller");
}

// IS7 (q7.cql) — Replies to Comment 824635044682
TEST_CASE("Q4-IS7 Replies to Comment 824635044682", "[q4][is]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (m:Comment {id: 824635044682})<-[:REPLY_OF_COMMENT]-(c:Comment)"
        "      -[:HAS_CREATOR]->(p:Person) "
        "RETURN c.id, c.content, c.creationDate, p.id, p.firstName, p.lastName "
        "ORDER BY c.creationDate DESC, p.id ASC",
        {qtest::ColType::INT64, qtest::ColType::STRING, qtest::ColType::INT64,
         qtest::ColType::INT64, qtest::ColType::STRING, qtest::ColType::STRING});
    REQUIRE(r.size() == 2);
    // row 0: commentId=824635044685, "great", 1295218398759, 2738, "Eden", "Atias"
    CHECK(r[0].int64_at(0) == 824635044685LL);
    CHECK(r[0].str_at(1) == "great");
    CHECK(r[0].int64_at(2) == 1295218398759LL);
    CHECK(r[0].int64_at(3) == 2738);
    CHECK(r[0].str_at(4) == "Eden");
    CHECK(r[0].str_at(5) == "Atias");
    // row 1: commentId=824635044686, "cool", 1295203653676, 933, "Mahinda", "Perera"
    CHECK(r[1].int64_at(0) == 824635044686LL);
    CHECK(r[1].str_at(1) == "cool");
    CHECK(r[1].int64_at(2) == 1295203653676LL);
    CHECK(r[1].int64_at(3) == 933);
    CHECK(r[1].str_at(4) == "Mahinda");
    CHECK(r[1].str_at(5) == "Perera");
}
