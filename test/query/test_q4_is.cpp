// Stage 4 — LDBC Interactive Short (IS) queries (IS1–IS7)
// All expected values verified against Neo4j 5.24.0 with LDBC SF1.
// Queries match Neo4j exactly (Message label, OPTIONAL MATCH, CASE, etc.)

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

// IS1 — Person basic info with city
TEST_CASE("Q4-IS1 Person 35184372099695 basic info", "[q4][is]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (n:Person {id: 35184372099695})-[r:IS_LOCATED_IN]->(p:Place) "
        "RETURN n.firstName AS firstName, n.lastName AS lastName, "
        "       n.birthday AS birthday, n.locationIP AS locationIP, "
        "       n.browserUsed AS browserUsed, p.id AS cityId, "
        "       n.gender AS gender, n.creationDate AS creationDate",
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

// IS2 — Recent 10 comments by Person 933 with originating post info
TEST_CASE("Q4-IS2 Person 933 recent 10 comments with post info", "[q4][is]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (:Person {id: 933})<-[:HAS_CREATOR]-(message:Comment) "
        "WITH message, message.id AS messageId, "
        "     message.creationDate AS messageCreationDate "
        "ORDER BY messageCreationDate DESC, messageId ASC "
        "LIMIT 10 "
        "MATCH (message)-[:REPLY_OF*0..]->(post:Post), "
        "      (post)-[:HAS_CREATOR]->(person) "
        "RETURN messageId, message.content AS messageContent, "
        "       messageCreationDate, post.id AS postId, "
        "       person.id AS personId, person.firstName AS personFirstName, "
        "       person.lastName AS personLastName "
        "ORDER BY messageCreationDate DESC, messageId ASC",
        {qtest::ColType::INT64, qtest::ColType::STRING, qtest::ColType::INT64,
         qtest::ColType::INT64, qtest::ColType::INT64, qtest::ColType::STRING,
         qtest::ColType::STRING});
    REQUIRE(r.size() == 10);
    // row 0
    CHECK(r[0].int64_at(0) == 2199027727462LL);
    CHECK(r[0].str_at(1) == "good");
    CHECK(r[0].int64_at(2) == 1347156463979LL);
    CHECK(r[0].int64_at(3) == 2061588773973LL);
    CHECK(r[0].int64_at(4) == 32985348833579LL);
    CHECK(r[0].str_at(5) == "Otto");
    CHECK(r[0].str_at(6) == "Becker");
    // row 1
    CHECK(r[1].int64_at(0) == 2061588773980LL);
    CHECK(r[1].str_at(1) == "no way!");
    CHECK(r[1].int64_at(2) == 1347020779693LL);
    CHECK(r[1].int64_at(3) == 2061588773973LL);
    CHECK(r[1].int64_at(4) == 32985348833579LL);
    CHECK(r[1].str_at(5) == "Otto");
    CHECK(r[1].str_at(6) == "Becker");
    // row 2
    CHECK(r[2].int64_at(0) == 2061584946139LL);
    CHECK(r[2].str_at(1) == "yes");
    CHECK(r[2].int64_at(2) == 1343987237082LL);
    CHECK(r[2].int64_at(3) == 2061584946128LL);
    CHECK(r[2].int64_at(4) == 4139);
    CHECK(r[2].str_at(5) == "Baruch");
    CHECK(r[2].str_at(6) == "Dego");
    // row 3
    CHECK(r[3].int64_at(0) == 2061585616327LL);
    CHECK(r[3].str_at(1).find("About Arnold Schwarzenegger") == 0);
    CHECK(r[3].int64_at(2) == 1343919397609LL);
    CHECK(r[3].int64_at(3) == 2061585616321LL);
    CHECK(r[3].int64_at(4) == 6597069777240LL);
    CHECK(r[3].str_at(5) == "Fritz");
    CHECK(r[3].str_at(6) == "Muller");
    // row 4
    CHECK(r[4].int64_at(0) == 2061585618894LL);
    CHECK(r[4].str_at(1) == "thanks");
    CHECK(r[4].int64_at(2) == 1343106691147LL);
    CHECK(r[4].int64_at(3) == 2061585618887LL);
    CHECK(r[4].int64_at(4) == 6597069777240LL);
    CHECK(r[4].str_at(5) == "Fritz");
    CHECK(r[4].str_at(6) == "Muller");
    // row 5
    CHECK(r[5].int64_at(0) == 2061585619578LL);
    CHECK(r[5].str_at(1).find("About Josip Broz Tito") == 0);
    CHECK(r[5].int64_at(2) == 1342380120292LL);
    CHECK(r[5].int64_at(3) == 2061585619561LL);
    CHECK(r[5].int64_at(4) == 6597069777240LL);
    CHECK(r[5].str_at(5) == "Fritz");
    CHECK(r[5].str_at(6) == "Muller");
    // row 6
    CHECK(r[6].int64_at(0) == 1786707214487LL);
    CHECK(r[6].str_at(1) == "maybe");
    CHECK(r[6].int64_at(2) == 1335712528590LL);
    CHECK(r[6].int64_at(3) == 1786707214481LL);
    CHECK(r[6].int64_at(4) == 10995116284808LL);
    CHECK(r[6].str_at(5) == "Andrei");
    CHECK(r[6].str_at(6) == "Condariuc");
    // row 7
    CHECK(r[7].int64_at(0) == 1786707214957LL);
    CHECK(r[7].str_at(1).find("About Lady Gaga") == 0);
    CHECK(r[7].int64_at(2) == 1335694024103LL);
    CHECK(r[7].int64_at(3) == 1786707214955LL);
    CHECK(r[7].int64_at(4) == 10995116284808LL);
    CHECK(r[7].str_at(5) == "Andrei");
    CHECK(r[7].str_at(6) == "Condariuc");
    // row 8
    CHECK(r[8].int64_at(0) == 1786707214469LL);
    CHECK(r[8].str_at(1).find("About Enrique Iglesias") == 0);
    CHECK(r[8].int64_at(2) == 1335678484226LL);
    CHECK(r[8].int64_at(3) == 1786707214468LL);
    CHECK(r[8].int64_at(4) == 10995116284808LL);
    CHECK(r[8].str_at(5) == "Andrei");
    CHECK(r[8].str_at(6) == "Condariuc");
    // row 9
    CHECK(r[9].int64_at(0) == 1786707216224LL);
    CHECK(r[9].str_at(1) == "no way!");
    CHECK(r[9].int64_at(2) == 1335668918002LL);
    CHECK(r[9].int64_at(3) == 1786707216218LL);
    CHECK(r[9].int64_at(4) == 10995116284808LL);
    CHECK(r[9].str_at(5) == "Andrei");
    CHECK(r[9].str_at(6) == "Condariuc");
}

// IS3 — Friend list for Person 933
TEST_CASE("Q4-IS3 Person 933 friend list", "[q4][is]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (n:Person {id: 933})-[r:KNOWS]-(friend) "
        "RETURN friend.id AS personId, friend.firstName AS firstName, "
        "       friend.lastName AS lastName, "
        "       r.creationDate AS friendshipCreationDate "
        "ORDER BY friendshipCreationDate DESC, personId ASC",
        {qtest::ColType::INT64, qtest::ColType::STRING,
         qtest::ColType::STRING, qtest::ColType::INT64});
    REQUIRE(r.size() == 5);
    // row 0
    CHECK(r[0].int64_at(0) == 32985348833579LL);
    CHECK(r[0].str_at(1) == "Otto");
    CHECK(r[0].str_at(2) == "Becker");
    CHECK(r[0].int64_at(3) == 1346980290195LL);
    // row 1
    CHECK(r[1].int64_at(0) == 32985348838375LL);
    CHECK(r[1].str_at(1) == "Otto");
    CHECK(r[1].str_at(2) == "Richter");
    CHECK(r[1].int64_at(3) == 1342512289463LL);
    // row 2
    CHECK(r[2].int64_at(0) == 10995116284808LL);
    CHECK(r[2].str_at(1) == "Andrei");
    CHECK(r[2].str_at(2) == "Condariuc");
    CHECK(r[2].int64_at(3) == 1293950621955LL);
    // row 3
    CHECK(r[3].int64_at(0) == 6597069777240LL);
    CHECK(r[3].str_at(1) == "Fritz");
    CHECK(r[3].str_at(2) == "Muller");
    CHECK(r[3].int64_at(3) == 1284975763187LL);
    // row 4
    CHECK(r[4].int64_at(0) == 4139);
    CHECK(r[4].str_at(1) == "Baruch");
    CHECK(r[4].str_at(2) == "Dego");
    CHECK(r[4].int64_at(3) == 1268465841718LL);
}

// IS4 — Message content (uses Message super-label, not Post/Comment)
TEST_CASE("Q4-IS4 Message 2199029886840 content", "[q4][is]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (m:Message {id: 2199029886840}) "
        "RETURN m.creationDate AS messageCreationDate, "
        "       m.imageFile AS messageContent",
        {qtest::ColType::INT64, qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 1347463431887LL);
    CHECK(r[0].str_at(1) == "photo2199029886840.jpg");
}

// IS5 — Message creator (uses Message super-label)
TEST_CASE("Q4-IS5 Message 824635044686 creator", "[q4][is]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (m:Message {id: 824635044686})-[r:HAS_CREATOR]->(p:Person) "
        "RETURN p.id AS personId, p.firstName AS firstName, "
        "       p.lastName AS lastName",
        {qtest::ColType::INT64, qtest::ColType::STRING, qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 933);
    CHECK(r[0].str_at(1) == "Mahinda");
    CHECK(r[0].str_at(2) == "Perera");
}

// IS6 — Forum containing the message (uses Message super-label, REPLY_OF chain)
TEST_CASE("Q4-IS6 Forum of Message 824635044686", "[q4][is]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (m:Message {id: 824635044686})-[:REPLY_OF*0..]->(p:Post)"
        "      <-[:CONTAINER_OF]-(f:Forum)"
        "      -[:HAS_MODERATOR]->(mod:Person) "
        "RETURN f.id AS forumId, f.title AS forumTitle, "
        "       mod.id AS moderatorId, mod.firstName AS moderatorFirstName, "
        "       mod.lastName AS moderatorLastName",
        {qtest::ColType::INT64, qtest::ColType::STRING,
         qtest::ColType::INT64, qtest::ColType::STRING, qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 412317916558LL);
    CHECK(r[0].str_at(1) == "Wall of Fritz Muller");
    CHECK(r[0].int64_at(2) == 6597069777240LL);
    CHECK(r[0].str_at(3) == "Fritz");
    CHECK(r[0].str_at(4) == "Muller");
}

// IS7 — Replies to message with OPTIONAL MATCH + CASE (uses Message super-label)
TEST_CASE("Q4-IS7 Replies to Message 824635044682", "[q4][is]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (m:Message {id: 824635044682})<-[:REPLY_OF]-(c:Comment)"
        "      -[:HAS_CREATOR]->(p:Person) "
        "OPTIONAL MATCH (m)-[:HAS_CREATOR]->(a:Person)-[r:KNOWS]-(p) "
        "RETURN c.id AS commentId, c.content AS commentContent, "
        "       c.creationDate AS commentCreationDate, "
        "       p.id AS replyAuthorId, "
        "       p.firstName AS replyAuthorFirstName, "
        "       p.lastName AS replyAuthorLastName, "
        "       CASE r._id WHEN null THEN false ELSE true END "
        "           AS replyAuthorKnowsOriginalMessageAuthor "
        "ORDER BY commentCreationDate DESC, replyAuthorId ASC",
        {qtest::ColType::INT64, qtest::ColType::STRING, qtest::ColType::INT64,
         qtest::ColType::INT64, qtest::ColType::STRING, qtest::ColType::STRING,
         qtest::ColType::BOOL});
    REQUIRE(r.size() == 2);
    // row 0: 824635044685, "great", 1295218398759, 2738, "Eden", "Atias", true
    CHECK(r[0].int64_at(0) == 824635044685LL);
    CHECK(r[0].str_at(1) == "great");
    CHECK(r[0].int64_at(2) == 1295218398759LL);
    CHECK(r[0].int64_at(3) == 2738);
    CHECK(r[0].str_at(4) == "Eden");
    CHECK(r[0].str_at(5) == "Atias");
    CHECK(r[0].bool_at(6) == true);
    // row 1: 824635044686, "cool", 1295203653676, 933, "Mahinda", "Perera", true
    CHECK(r[1].int64_at(0) == 824635044686LL);
    CHECK(r[1].str_at(1) == "cool");
    CHECK(r[1].int64_at(2) == 1295203653676LL);
    CHECK(r[1].int64_at(3) == 933);
    CHECK(r[1].str_at(4) == "Mahinda");
    CHECK(r[1].str_at(5) == "Perera");
    CHECK(r[1].bool_at(6) == true);
}
