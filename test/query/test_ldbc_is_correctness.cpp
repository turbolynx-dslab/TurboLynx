// Stage 4 — LDBC Interactive Short (IS) queries (IS1–IS7)
//
// Two fixture sizes are supported via `helpers/ldbc_expected_counts.hpp`,
// which dispatches between SF1 (default) and SF0.003 (mini, set with
// `cmake -DTURBOLYNX_LDBC_FIXTURE_MINI=ON`). All SF0.003 expected
// values are Neo4j-verified against the committed mini fixture.

#include "catch.hpp"
#include "helpers/query_runner.hpp"
#include "helpers/ldbc_expected_counts.hpp"
#include <string>
#include <vector>

extern std::string g_ldbc_path;
extern bool g_skip_requested;
extern bool g_has_ldbc;

extern qtest::QueryRunner* get_ldbc_runner();

#define SKIP_IF_NO_DB() \
    if (g_ldbc_path.empty()) { WARN("--ldbc-path not set, skipping"); g_skip_requested = true; return; } \
    if (!g_has_ldbc) { WARN("DB has no LDBC schema, skipping"); return; } \
    auto* qr = get_ldbc_runner(); \
    if (!qr) { FAIL("Cannot open DB: " << g_ldbc_path); return; }

static std::string sample_person_id_str() {
    return std::to_string(ldbc::SAMPLE_PERSON_ID);
}

// IS1 — Person basic info with city.
//
// `:City` is a sub-label SF1's load script tags but `load-ldbc-mini.sh`
// does not (Place.csv `type` column is not split into separate label
// files). IS1a uses :City and is therefore SF1-only; IS1b uses the
// parent :Place label and exercises the same query path on both
// fixtures.
#ifndef TURBOLYNX_LDBC_FIXTURE_MINI
TEST_CASE("IS1a sample person basic info (City)", "[ldbc][is]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (n:Person {id: " + std::to_string(ldbc::IS1_PERSON_ID) +
             "})-[r:IS_LOCATED_IN]->(p:City) "
             "RETURN n.firstName AS firstName, n.lastName AS lastName, "
             "       n.birthday AS birthday, n.locationIP AS locationIP, "
             "       n.browserUsed AS browserUsed, p.id AS cityId, "
             "       n.gender AS gender, n.creationDate AS creationDate";
    auto r = qr->run(q.c_str(),
        {qtest::ColType::STRING, qtest::ColType::STRING,
         qtest::ColType::INT64,  qtest::ColType::STRING,
         qtest::ColType::STRING, qtest::ColType::INT64,
         qtest::ColType::STRING, qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == ldbc::IS1_FIRST_NAME);
    CHECK(r[0].str_at(1) == ldbc::IS1_LAST_NAME);
    CHECK(r[0].int64_at(2) == ldbc::IS1_BIRTHDAY_MS);
    CHECK(r[0].str_at(3) == ldbc::IS1_LOCATION_IP);
    CHECK(r[0].str_at(4) == ldbc::IS1_BROWSER_USED);
    CHECK(r[0].int64_at(5) == ldbc::IS1_CITY_ID);
    CHECK(r[0].str_at(6) == ldbc::IS1_GENDER);
    CHECK(r[0].int64_at(7) == ldbc::IS1_CREATION_DATE_MS);
}
#endif

TEST_CASE("IS1b sample person basic info (Place)", "[ldbc][is]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (n:Person {id: " + std::to_string(ldbc::IS1_PERSON_ID) +
             "})-[r:IS_LOCATED_IN]->(p:Place) "
             "RETURN n.firstName AS firstName, n.lastName AS lastName, "
             "       n.birthday AS birthday, n.locationIP AS locationIP, "
             "       n.browserUsed AS browserUsed, p.id AS cityId, "
             "       n.gender AS gender, n.creationDate AS creationDate";
    auto r = qr->run(q.c_str(),
        {qtest::ColType::STRING, qtest::ColType::STRING,
         qtest::ColType::INT64,  qtest::ColType::STRING,
         qtest::ColType::STRING, qtest::ColType::INT64,
         qtest::ColType::STRING, qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == ldbc::IS1_FIRST_NAME);
    CHECK(r[0].str_at(1) == ldbc::IS1_LAST_NAME);
    CHECK(r[0].int64_at(2) == ldbc::IS1_BIRTHDAY_MS);
    CHECK(r[0].str_at(3) == ldbc::IS1_LOCATION_IP);
    CHECK(r[0].str_at(4) == ldbc::IS1_BROWSER_USED);
    CHECK(r[0].int64_at(5) == ldbc::IS1_CITY_ID);
    CHECK(r[0].str_at(6) == ldbc::IS1_GENDER);
    CHECK(r[0].int64_at(7) == ldbc::IS1_CREATION_DATE_MS);
}

// IS2 — Recent 10 comments by SAMPLE_PERSON with originating post info
TEST_CASE("IS2 sample person recent 10 comments with post info", "[ldbc][is]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (:Person {id: " + sample_person_id_str() +
             "})<-[:HAS_CREATOR]-(message:Comment) "
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
             "ORDER BY messageCreationDate DESC, messageId ASC";
    auto r = qr->run(q.c_str(),
        {qtest::ColType::INT64, qtest::ColType::STRING, qtest::ColType::INT64,
         qtest::ColType::INT64, qtest::ColType::INT64, qtest::ColType::STRING,
         qtest::ColType::STRING});
    constexpr size_t N = sizeof(ldbc::IS2_RECENT_COMMENTS) / sizeof(ldbc::IS2_RECENT_COMMENTS[0]);
    REQUIRE(r.size() == N);
    for (size_t i = 0; i < N; ++i) {
        INFO("row " << i);
        const auto& exp = ldbc::IS2_RECENT_COMMENTS[i];
        CHECK(r[i].int64_at(0) == exp.message_id);
        // Some content strings are very long ("About …" prefixes); use
        // startswith semantics to keep the assertion robust to the
        // 256-char chunk truncation the legacy test relied on.
        CHECK(r[i].str_at(1).find(exp.content) == 0);
        CHECK(r[i].int64_at(2) == exp.message_creation_ms);
        CHECK(r[i].int64_at(3) == exp.post_id);
        CHECK(r[i].int64_at(4) == exp.person_id);
        CHECK(r[i].str_at(5) == exp.first_name);
        CHECK(r[i].str_at(6) == exp.last_name);
    }
}

// IS3 — Friend list for SAMPLE_PERSON
TEST_CASE("IS3 sample person friend list", "[ldbc][is]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (n:Person {id: " + sample_person_id_str() +
             "})-[r:KNOWS]-(friend) "
             "RETURN friend.id AS personId, friend.firstName AS firstName, "
             "       friend.lastName AS lastName, "
             "       r.creationDate AS friendshipCreationDate "
             "ORDER BY friendshipCreationDate DESC, personId ASC";
    auto r = qr->run(q.c_str(),
        {qtest::ColType::INT64, qtest::ColType::STRING,
         qtest::ColType::STRING, qtest::ColType::INT64});
    constexpr size_t N = sizeof(ldbc::IS3_FRIENDS) / sizeof(ldbc::IS3_FRIENDS[0]);
    REQUIRE(r.size() == N);
    for (size_t i = 0; i < N; ++i) {
        INFO("row " << i);
        const auto& exp = ldbc::IS3_FRIENDS[i];
        CHECK(r[i].int64_at(0) == exp.person_id);
        CHECK(r[i].str_at(1) == exp.first_name);
        CHECK(r[i].str_at(2) == exp.last_name);
        CHECK(r[i].int64_at(3) == exp.friendship_ms);
    }
}

// IS4 — Message content (uses Message super-label, not Post/Comment)
TEST_CASE("IS4 sample post content", "[ldbc][is]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (m:Message {id: " + std::to_string(ldbc::IS4_POST_ID) + "}) "
             "RETURN m.creationDate AS messageCreationDate, "
             "       m.imageFile AS messageContent";
    auto r = qr->run(q.c_str(),
        {qtest::ColType::INT64, qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == ldbc::IS4_CREATION_MS);
    CHECK(r[0].str_at(1) == ldbc::IS4_IMAGE_FILE);
}

// IS5 — Message creator (uses Message super-label)
TEST_CASE("IS5 sample comment creator", "[ldbc][is]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (m:Message {id: " + std::to_string(ldbc::SAMPLE_COMMENT_ID) +
             "})-[r:HAS_CREATOR]->(p:Person) "
             "RETURN p.id AS personId, p.firstName AS firstName, "
             "       p.lastName AS lastName";
    auto r = qr->run(q.c_str(),
        {qtest::ColType::INT64, qtest::ColType::STRING, qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == ldbc::SAMPLE_COMMENT_CREATOR_ID);
    CHECK(r[0].str_at(1) == ldbc::SAMPLE_COMMENT_CREATOR_FIRSTNAME);
    CHECK(r[0].str_at(2) == ldbc::SAMPLE_COMMENT_CREATOR_LASTNAME);
}

// IS6 — Forum containing the message (uses Message super-label, REPLY_OF chain)
TEST_CASE("IS6 forum of sample comment", "[ldbc][is]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (m:Message {id: " + std::to_string(ldbc::SAMPLE_COMMENT_ID) +
             "})-[:REPLY_OF*0..]->(p:Post)"
             "      <-[:CONTAINER_OF]-(f:Forum)"
             "      -[:HAS_MODERATOR]->(mod:Person) "
             "RETURN f.id AS forumId, f.title AS forumTitle, "
             "       mod.id AS moderatorId, mod.firstName AS moderatorFirstName, "
             "       mod.lastName AS moderatorLastName";
    auto r = qr->run(q.c_str(),
        {qtest::ColType::INT64, qtest::ColType::STRING,
         qtest::ColType::INT64, qtest::ColType::STRING, qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == ldbc::IS6_FORUM_ID);
    CHECK(r[0].str_at(1) == ldbc::IS6_FORUM_TITLE);
    CHECK(r[0].int64_at(2) == ldbc::IS6_MOD_ID);
    CHECK(r[0].str_at(3) == ldbc::IS6_MOD_FIRST);
    CHECK(r[0].str_at(4) == ldbc::IS6_MOD_LAST);
}

// IS7 — Replies to message with OPTIONAL MATCH + CASE (uses Message super-label)
TEST_CASE("IS7 replies to sample message", "[ldbc][is]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (m:Message {id: " + std::to_string(ldbc::IS7_MESSAGE_ID) +
             "})<-[:REPLY_OF]-(c:Comment)"
             "      -[:HAS_CREATOR]->(p:Person) "
             "OPTIONAL MATCH (m)-[:HAS_CREATOR]->(a:Person)-[r:KNOWS]-(p) "
             "RETURN c.id AS commentId, c.content AS commentContent, "
             "       c.creationDate AS commentCreationDate, "
             "       p.id AS replyAuthorId, "
             "       p.firstName AS replyAuthorFirstName, "
             "       p.lastName AS replyAuthorLastName, "
             "       CASE r._id WHEN null THEN false ELSE true END "
             "           AS replyAuthorKnowsOriginalMessageAuthor "
             "ORDER BY commentCreationDate DESC, replyAuthorId ASC";
    auto r = qr->run(q.c_str(),
        {qtest::ColType::INT64, qtest::ColType::STRING, qtest::ColType::INT64,
         qtest::ColType::INT64, qtest::ColType::STRING, qtest::ColType::STRING,
         qtest::ColType::BOOL});
    constexpr size_t N = sizeof(ldbc::IS7_REPLIES) / sizeof(ldbc::IS7_REPLIES[0]);
    REQUIRE(r.size() == N);
    for (size_t i = 0; i < N; ++i) {
        INFO("row " << i);
        const auto& exp = ldbc::IS7_REPLIES[i];
        CHECK(r[i].int64_at(0) == exp.comment_id);
        CHECK(r[i].str_at(1) == exp.content);
        CHECK(r[i].int64_at(2) == exp.creation_ms);
        CHECK(r[i].int64_at(3) == exp.reply_author_id);
        CHECK(r[i].str_at(4) == exp.reply_author_first);
        CHECK(r[i].str_at(5) == exp.reply_author_last);
        CHECK(r[i].bool_at(6) == exp.reply_author_knows_orig);
    }
}
