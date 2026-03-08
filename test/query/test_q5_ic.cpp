// Stage 5 — LDBC IC queries (q9, q11–q13, q15–q16, q18–q19)
// All expected values verified against Neo4j 5.24.0 with LDBC SF1.
// Edge names translated to our system's fine-grained types.

#include "catch.hpp"
#include "helpers/query_runner.hpp"
#include <vector>

extern std::string g_db_path;
extern bool g_skip_requested;

static qtest::QueryRunner* get_runner() {
    static qtest::QueryRunner* runner = nullptr;
    if (!runner) {
        if (g_db_path.empty()) return nullptr;
        runner = new qtest::QueryRunner(g_db_path);
    }
    return runner;
}

#define SKIP_IF_NO_DB() \
    if (g_db_path.empty()) { WARN("--db-path not set, skipping"); g_skip_requested = true; return; } \
    auto* qr = get_runner(); \
    if (!qr) { FAIL("Cannot open DB: " << g_db_path); return; }

// IC9 (q9.cql) — Friends' messages before a date, sorted by date DESC
TEST_CASE("Q5-IC9 Person 17592186052613 friends messages before date", "[q5][ic]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (n:Person {id: 17592186052613})-[:KNOWS]->(friend:Person)"
        "      <-[:HAS_CREATOR]-(message:Comment) "
        "WHERE message.creationDate <= 1354060800000 "
        "RETURN DISTINCT friend.id, friend.firstName, friend.lastName, "
        "       message.id, message.content, message.creationDate "
        "ORDER BY message.creationDate DESC, message.id ASC LIMIT 20",
        {qtest::ColType::INT64, qtest::ColType::STRING, qtest::ColType::STRING,
         qtest::ColType::INT64, qtest::ColType::STRING, qtest::ColType::INT64});
    REQUIRE(r.size() == 20);
    // Top result
    CHECK(r[0].int64_at(0) == 6597069773262LL);
    CHECK(r[0].str_at(1) == "Bill");
    CHECK(r[0].str_at(2) == "Moore");
    CHECK(r[0].int64_at(3) == 2199023411115LL);
    CHECK(r[0].int64_at(5) == 1347527459367LL);
}

// IC11 (q11.cql) — Tag statistics for friends' posts in a date window
TEST_CASE("Q5-IC11 Person 21990232559429 tag statistics", "[q5][ic]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (person:Person {id: 21990232559429})-[:KNOWS]->(friend:Person)"
        "      <-[:POST_HAS_CREATOR]-(post:Post)-[:POST_HAS_TAG]->(tag:Tag) "
        "WITH DISTINCT tag, post "
        "WITH tag, "
        "  CASE WHEN post.creationDate >= 1335830400000 AND post.creationDate < 1339027200000 "
        "       THEN 1 ELSE 0 END AS valid, "
        "  CASE WHEN post.creationDate < 1335830400000 THEN 1 ELSE 0 END AS inValid "
        "WITH tag.name AS tagName, sum(valid) AS postCount, sum(inValid) AS inValidPostCount "
        "WHERE postCount > 0 AND inValidPostCount = 0 "
        "RETURN tagName, postCount "
        "ORDER BY postCount DESC, tagName ASC LIMIT 10",
        {qtest::ColType::STRING, qtest::ColType::INT64});
    REQUIRE(r.size() == 5);
    CHECK(r[0].str_at(0) == "Hassan_II_of_Morocco");
    CHECK(r[0].int64_at(1) == 2);
    CHECK(r[1].str_at(0) == "Appeal_to_Reason");
    CHECK(r[2].str_at(0) == "Principality_of_Littoral_Croatia");
    CHECK(r[3].str_at(0) == "Rivers_of_Babylon");
    CHECK(r[4].str_at(0) == "Van_Morrison");
}

// IC12 (q12.cql) — Forum post counts by 2-hop friends who joined after date
TEST_CASE("Q5-IC12 Person 28587302325306 forum posts by 2-hop friends", "[q5][ic]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (person:Person {id: 28587302325306})-[:KNOWS*1..2]-(friend:Person) "
        "WHERE NOT person = friend "
        "WITH DISTINCT friend "
        "MATCH (friend)<-[membership:HAS_MEMBER]-(forum:Forum) "
        "WHERE membership.joinDate > 1343088000000 "
        "WITH forum, friend "
        "MATCH (friend)<-[:POST_HAS_CREATOR]-(post:Post)<-[:CONTAINER_OF]-(forum) "
        "WITH forum, count(post) AS postCount "
        "RETURN forum.title, postCount, forum.id "
        "ORDER BY postCount DESC, forum.id ASC LIMIT 20",
        {qtest::ColType::STRING, qtest::ColType::INT64, qtest::ColType::INT64});
    REQUIRE(r.size() == 20);
    CHECK(r[0].str_at(0) == "Group for She_Blinded_Me_with_Science in Antofagasta");
    CHECK(r[0].int64_at(1) == 10);
    CHECK(r[0].int64_at(2) == 1236950612644LL);
}

// IC13 (q13.cql) — Co-occurring tags with 'Angola' on friends-of-friends' posts
TEST_CASE("Q5-IC13 Person 30786325583618 Angola co-tags", "[q5][ic]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (knownTag:Tag {name: 'Angola'}) "
        "WITH knownTag "
        "MATCH (person:Person {id: 30786325583618})-[:KNOWS*1..2]-(friend:Person) "
        "WHERE NOT person=friend "
        "WITH DISTINCT friend, knownTag "
        "MATCH (friend)<-[:POST_HAS_CREATOR]-(post:Post), "
        "      (post)-[:POST_HAS_TAG]->(knownTag), "
        "      (post)-[:POST_HAS_TAG]->(tag:Tag) "
        "WHERE NOT knownTag = tag "
        "WITH tag.name AS tagName, count(post) AS postCount "
        "RETURN tagName, postCount "
        "ORDER BY postCount DESC, tagName ASC LIMIT 10",
        {qtest::ColType::STRING, qtest::ColType::INT64});
    REQUIRE(r.size() == 10);
    CHECK(r[0].str_at(0) == "Tom_Gehrels");
    CHECK(r[0].int64_at(1) == 28);
    CHECK(r[1].str_at(0) == "Sammy_Sosa");
    CHECK(r[1].int64_at(1) == 9);
}

// IC15 (q15.cql) — Replies to comments authored by a specific Person
TEST_CASE("Q5-IC15 Replies to comments of Person 24189255818757", "[q5][ic]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (s:Person {id: 24189255818757})<-[:HAS_CREATOR]-(p:Comment)"
        "      <-[:REPLY_OF_COMMENT]-(comment:Comment)-[:HAS_CREATOR]->(person:Person) "
        "RETURN person.id, person.firstName, person.lastName, "
        "       comment.creationDate, comment.id, comment.content "
        "ORDER BY comment.creationDate DESC, comment.id ASC LIMIT 20",
        {qtest::ColType::INT64, qtest::ColType::STRING, qtest::ColType::STRING,
         qtest::ColType::INT64, qtest::ColType::INT64, qtest::ColType::STRING});
    REQUIRE(r.size() == 3);
    CHECK(r[0].int64_at(0) == 28587302328958LL);
    CHECK(r[0].str_at(1) == "Jessica");
    CHECK(r[0].str_at(2) == "Castillo");
    CHECK(r[0].int64_at(3) == 1341921296480LL);
    CHECK(r[0].int64_at(4) == 2061588598034LL);

    CHECK(r[1].int64_at(0) == 24189255812556LL);
    CHECK(r[1].str_at(1) == "Naresh");
    CHECK(r[1].str_at(2) == "Sharma");
    CHECK(r[1].int64_at(3) == 1341888221696LL);

    CHECK(r[2].int64_at(0) == 6597069770791LL);
    CHECK(r[2].str_at(1) == "Roberto");
    CHECK(r[2].int64_at(3) == 1341854866309LL);
}

// IC16 (q16.cql) — Friends' comments before a date (2-hop)
TEST_CASE("Q5-IC16 Person 13194139542834 2-hop friends recent comments", "[q5][ic]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (root:Person {id: 13194139542834})-[:KNOWS*1..2]->(friend:Person) "
        "WHERE NOT friend = root "
        "WITH DISTINCT friend "
        "MATCH (friend)<-[:HAS_CREATOR]-(message:Comment) "
        "WHERE message.creationDate < 1324080000000 "
        "RETURN friend.id, friend.firstName, friend.lastName, "
        "       message.id, message.content, message.creationDate "
        "ORDER BY message.creationDate DESC, message.id ASC LIMIT 20",
        {qtest::ColType::INT64, qtest::ColType::STRING, qtest::ColType::STRING,
         qtest::ColType::INT64, qtest::ColType::STRING, qtest::ColType::INT64});
    REQUIRE(r.size() == 20);
    CHECK(r[0].int64_at(0) == 2199023260919LL);
    CHECK(r[0].str_at(1) == "Xiaolu");
    CHECK(r[0].str_at(2) == "Wang");
    CHECK(r[0].int64_at(3) == 1511829711860LL);
    CHECK(r[0].int64_at(5) == 1324079889425LL);
}

// IC18 (q18.cql) — Friends working at companies in a country
TEST_CASE("Q5-IC18 Person 30786325583618 friends at Laos companies", "[q5][ic]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (person:Person {id: 30786325583618})-[:KNOWS*1..2]->(friend:Person) "
        "WHERE NOT person = friend "
        "WITH DISTINCT friend "
        "MATCH (friend)-[workAt:WORK_AT]->(company:Organisation)"
        "      -[:ORG_IS_LOCATED_IN]->(country:Place {name: 'Laos'}) "
        "WHERE workAt.workFrom < 2010 "
        "RETURN friend.id, friend.firstName, friend.lastName, "
        "       company.name, workAt.workFrom "
        "ORDER BY workAt.workFrom ASC, friend.id ASC, company.name DESC LIMIT 10",
        {qtest::ColType::INT64, qtest::ColType::STRING, qtest::ColType::STRING,
         qtest::ColType::STRING, qtest::ColType::INT64});
    REQUIRE(r.size() == 10);
    CHECK(r[0].int64_at(0) == 6597069767125LL);
    CHECK(r[0].str_at(3) == "Lao_Airlines");
    CHECK(r[0].int64_at(4) == 2002);

    CHECK(r[9].int64_at(0) == 2199023258003LL);
    CHECK(r[9].str_at(2) == "Achiou");
    CHECK(r[9].int64_at(4) == 2009);
}

// IC19 (q19.cql) — Friends' replies to posts tagged with BasketballPlayer
TEST_CASE("Q5-IC19 Person 17592186052613 friends replies to BasketballPlayer posts", "[q5][ic]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (tag:Tag)-[:HAS_TYPE*0..5]->(baseTagClass:TagClass) "
        "WHERE tag.name = 'BasketballPlayer' OR baseTagClass.name = 'BasketballPlayer' "
        "WITH DISTINCT tag "
        "MATCH (person:Person {id: 17592186052613})<-[:KNOWS]-(friend:Person)"
        "      <-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->(post:Post)"
        "      -[:POST_HAS_TAG]->(tag) "
        "RETURN friend.id, friend.firstName, friend.lastName, count(comment) AS replyCount "
        "ORDER BY replyCount DESC, friend.id ASC LIMIT 20",
        {qtest::ColType::INT64, qtest::ColType::STRING, qtest::ColType::STRING,
         qtest::ColType::INT64});
    REQUIRE(r.size() == 6);
    CHECK(r[0].int64_at(0) == 8796093029854LL);
    CHECK(r[0].str_at(1) == "Zaenal");
    CHECK(r[0].int64_at(3) == 40);

    CHECK(r[1].int64_at(0) == 8796093031506LL);
    CHECK(r[1].str_at(1) == "Gheorghe");
    CHECK(r[1].int64_at(3) == 32);
}
