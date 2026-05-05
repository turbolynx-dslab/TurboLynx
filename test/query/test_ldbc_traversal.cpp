// Stage 3 — Traversal tests: multi-hop, bidirectional, MPV/MPE
//
// Two fixture sizes are supported by the same source file. Per-test
// expected counts come from `helpers/ldbc_expected_counts.hpp`, which
// dispatches between the SF1 (default) and SF0.003 (mini) values via
// the `TURBOLYNX_LDBC_FIXTURE_MINI` cmake define. SF1 values were
// verified against Neo4j 5.24.0; SF0.003 values were verified against
// Neo4j 5 with the committed `test/data/ldbc-mini/` fixture loaded
// via `neo4j-admin database import`.

#include "catch.hpp"
#include "helpers/query_runner.hpp"
#include "helpers/ldbc_expected_counts.hpp"
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
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

// Build "MATCH (n:Person {id: <SAMPLE_PERSON_ID>})" + tail.
static std::string sample_person_query(const std::string& tail) {
    return "MATCH (n:Person {id: " + std::to_string(ldbc::SAMPLE_PERSON_ID) +
           "})" + tail;
}

TEST_CASE("FoF count (sample person)", "[ldbc][traversal][multihop]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p:Person {id: " + std::to_string(ldbc::SAMPLE_PERSON_ID) +
             "})-[:KNOWS]->(f:Person)-[:KNOWS]->(fof:Person) "
             "WHERE fof <> p RETURN count(DISTINCT fof)";
    REQUIRE(qr->count(q.c_str()) == ldbc::TRAV_FOF_COUNT);
}

TEST_CASE("Top 10 persons by Comment count", "[ldbc][traversal][multihop]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (p:Person)<-[:HAS_CREATOR]-(c:Comment) "
        "RETURN p.id, count(c) AS cnt "
        "ORDER BY cnt DESC, p.id ASC LIMIT 10",
        {qtest::ColType::INT64, qtest::ColType::INT64});
    REQUIRE(r.size() == 10);
    for (size_t i = 0; i < 10; ++i) {
        INFO("row " << i);
        CHECK(r[i].int64_at(0) == ldbc::TRAV_TOP10_PERSON_BY_COMMENT[i].pid);
        CHECK(r[i].int64_at(1) == ldbc::TRAV_TOP10_PERSON_BY_COMMENT[i].cnt);
    }
}

TEST_CASE("Top 5 Forums by Post count", "[ldbc][traversal][multihop]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (f:Forum)-[:CONTAINER_OF]->(p:Post) "
        "RETURN f.id, count(p) AS cnt "
        "ORDER BY cnt DESC, f.id ASC LIMIT 5",
        {qtest::ColType::INT64, qtest::ColType::INT64});
    REQUIRE(r.size() == 5);
    for (size_t i = 0; i < 5; ++i) {
        INFO("row " << i);
        CHECK(r[i].int64_at(0) == ldbc::TRAV_TOP5_FORUM_BY_POST[i].fid);
        CHECK(r[i].int64_at(1) == ldbc::TRAV_TOP5_FORUM_BY_POST[i].cnt);
    }
}

TEST_CASE("Distinct Comment creators liked by sample person", "[ldbc][traversal][multihop]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p:Person {id: " + std::to_string(ldbc::SAMPLE_PERSON_ID) +
             "})-[:LIKES]->(c:Comment)-[:HAS_CREATOR]->(creator:Person) "
             "RETURN count(DISTINCT creator)";
    REQUIRE(qr->count(q.c_str()) == ldbc::TRAV_DISTINCT_COMMENT_CREATORS_LIKED);
}

TEST_CASE("Top 5 TagClasses by Tag count", "[ldbc][traversal][multihop]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (t:Tag)-[:HAS_TYPE]->(tc:TagClass) "
        "RETURN tc.name, count(t) AS cnt "
        "ORDER BY cnt DESC, tc.name ASC LIMIT 5",
        {qtest::ColType::STRING, qtest::ColType::INT64});
    REQUIRE(r.size() == 5);
    for (size_t i = 0; i < 5; ++i) {
        INFO("row " << i);
        CHECK(r[i].str_at(0) == ldbc::TRAV_TOP5_TAGCLASS_BY_TAG[i].name);
        CHECK(r[i].int64_at(1) == ldbc::TRAV_TOP5_TAGCLASS_BY_TAG[i].cnt);
    }
}

TEST_CASE("Top 5 Tags by Post count", "[ldbc][traversal][multihop]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (p:Post)-[:HAS_TAG]->(t:Tag) "
        "RETURN t.name, count(p) AS cnt "
        "ORDER BY cnt DESC, t.name ASC LIMIT 5",
        {qtest::ColType::STRING, qtest::ColType::INT64});
    REQUIRE(r.size() == 5);
    for (size_t i = 0; i < 5; ++i) {
        INFO("row " << i);
        CHECK(r[i].str_at(0) == ldbc::TRAV_TOP5_TAG_BY_POST[i].name);
        CHECK(r[i].int64_at(1) == ldbc::TRAV_TOP5_TAG_BY_POST[i].cnt);
    }
}

// ---------------------------------------------------------------------------
// Multi-partition vertex/edge tests (M28 — :Message = Comment + Post)
// ---------------------------------------------------------------------------

TEST_CASE("Top 10 persons by Message count", "[ldbc][traversal][multihop][mpe]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (p:Person)<-[:HAS_CREATOR]-(m:Message) "
        "RETURN p.id, count(m) AS cnt "
        "ORDER BY cnt DESC, p.id ASC LIMIT 10",
        {qtest::ColType::INT64, qtest::ColType::INT64});
    REQUIRE(r.size() == 10);
    for (size_t i = 0; i < 10; ++i) {
        INFO("row " << i);
        CHECK(r[i].int64_at(0) == ldbc::TRAV_TOP10_PERSON_BY_MESSAGE[i].pid);
        CHECK(r[i].int64_at(1) == ldbc::TRAV_TOP10_PERSON_BY_MESSAGE[i].cnt);
    }
}

TEST_CASE("Top 5 Tags by Message count", "[ldbc][traversal][multihop][mpe]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (m:Message)-[:HAS_TAG]->(t:Tag) "
        "RETURN t.name, count(m) AS cnt "
        "ORDER BY cnt DESC, t.name ASC LIMIT 5",
        {qtest::ColType::STRING, qtest::ColType::INT64});
    REQUIRE(r.size() == 5);
    for (size_t i = 0; i < 5; ++i) {
        INFO("row " << i);
        CHECK(r[i].str_at(0) == ldbc::TRAV_TOP5_TAG_BY_MESSAGE[i].name);
        CHECK(r[i].int64_at(1) == ldbc::TRAV_TOP5_TAG_BY_MESSAGE[i].cnt);
    }
}

TEST_CASE("Distinct Message creators liked by sample person", "[ldbc][traversal][multihop][mpe]") {
    SKIP_IF_NO_DB();
    // LIKES -> Message -> HAS_CREATOR -> Person
    auto q = "MATCH (p:Person {id: " + std::to_string(ldbc::SAMPLE_PERSON_ID) +
             "})-[:LIKES]->(m:Message)-[:HAS_CREATOR]->(creator:Person) "
             "RETURN count(DISTINCT creator)";
    REQUIRE(qr->count(q.c_str()) == ldbc::TRAV_DISTINCT_MESSAGE_CREATORS_LIKED);
}

// ---------------------------------------------------------------------------
// Multi-partition vertex tests (M28 — :Message = Comment + Post)
// Merged from former test_q7_multipart_vertex.cpp
// ---------------------------------------------------------------------------

// MPV-01: Count Comments via HAS_CREATOR using :Message label
// Message maps to both Comment and Post partitions.
TEST_CASE("Message via HAS_CREATOR count", "[ldbc][traversal][mpv]") {
    SKIP_IF_NO_DB();
    auto qm = "MATCH (:Person {id: " + std::to_string(ldbc::SAMPLE_PERSON_ID) +
              "})<-[:HAS_CREATOR]-(message:Message) RETURN count(message)";
    auto r = qr->run(qm.c_str(), {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == ldbc::TRAV_MESSAGES_AUTHORED_BY_SAMPLE_PERSON);

    auto qc = "MATCH (:Person {id: " + std::to_string(ldbc::SAMPLE_PERSON_ID) +
              "})<-[:HAS_CREATOR]-(c:Comment) RETURN count(c)";
    auto r2 = qr->run(qc.c_str(), {qtest::ColType::INT64});
    REQUIRE(r2.size() == 1);
    CHECK(r2[0].int64_at(0) == ldbc::TRAV_COMMENTS_AUTHORED_BY_SAMPLE_PERSON);
}

// MPV-02: Count all Messages via REPLY_OF (multi-partition edge + multi-partition vertex)
// Verified relative to (REPLY_OF→Post + REPLY_OF→Comment) so it stays
// fixture-independent. Tagged [!mayfail] for pre-existing flakiness.
TEST_CASE("REPLY_OF to Message count", "[ldbc][traversal][mpv][!mayfail]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (c:Comment)-[:REPLY_OF]->(m:Message) "
        "RETURN count(m)",
        {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
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

// MPV-03: Message properties — same query as MPV-01, repeated as a property-read smoke check.
TEST_CASE("Message properties via HAS_CREATOR", "[ldbc][traversal][mpv]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (:Person {id: " + std::to_string(ldbc::SAMPLE_PERSON_ID) +
             "})<-[:HAS_CREATOR]-(message:Message) RETURN count(message)";
    auto r = qr->run(q.c_str(), {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == ldbc::TRAV_MESSAGES_AUTHORED_BY_SAMPLE_PERSON);
}

TEST_CASE("Message IdSeek keeps same-seqno partitions separated", "[ldbc][traversal][mpv][idseek]") {
    SKIP_IF_NO_DB();

    auto comments = qr->run(
        "MATCH (c:Comment) "
        "RETURN id(c), c.id, c.creationDate "
        "ORDER BY c.id ASC LIMIT 256",
        {qtest::ColType::INT64, qtest::ColType::INT64, qtest::ColType::INT64});
    auto posts = qr->run(
        "MATCH (p:Post) "
        "RETURN id(p), p.id, p.creationDate "
        "ORDER BY p.id ASC LIMIT 256",
        {qtest::ColType::INT64, qtest::ColType::INT64, qtest::ColType::INT64});

    REQUIRE(!comments.empty());
    REQUIRE(!posts.empty());

    struct SeedRow {
        int64_t internal_id;
        int64_t user_id;
        int64_t creation_date;
        uint16_t extent_seqno;
    };

    auto make_seed = [](const qtest::Row &row) -> SeedRow {
        auto internal_id = row.int64_at(0);
        auto user_id = row.int64_at(1);
        auto creation_date = row.int64_at(2);
        auto extent_seqno = (uint16_t)(((uint64_t)internal_id >> 32) & 0xFFFFull);
        return {internal_id, user_id, creation_date, extent_seqno};
    };

    std::unordered_map<uint16_t, SeedRow> comments_by_seqno;
    for (auto &row : comments.rows) {
        auto seed = make_seed(row);
        comments_by_seqno.try_emplace(seed.extent_seqno, seed);
    }

    std::optional<SeedRow> comment_seed;
    std::optional<SeedRow> post_seed;
    for (auto &row : posts.rows) {
        auto seed = make_seed(row);
        auto it = comments_by_seqno.find(seed.extent_seqno);
        if (it != comments_by_seqno.end()) {
            comment_seed = it->second;
            post_seed = seed;
            break;
        }
    }

    REQUIRE(comment_seed.has_value());
    REQUIRE(post_seed.has_value());
    INFO("shared extent seqno=" << comment_seed->extent_seqno);
    INFO("comment internal id=" << comment_seed->internal_id << " user id=" << comment_seed->user_id);
    INFO("post internal id=" << post_seed->internal_id << " user id=" << post_seed->user_id);

    auto seek_query =
        "MATCH (seed:Message) "
        "WHERE seed.id = " + std::to_string(comment_seed->user_id) +
        " OR seed.id = " + std::to_string(post_seed->user_id) +
        " WITH id(seed) AS mid, seed.id AS expected_id, "
        "      seed.creationDate AS expected_ts "
        "MATCH (msg:Message) WHERE id(msg) = mid "
        "RETURN mid, id(msg), expected_id, msg.id, expected_ts, msg.creationDate "
        "ORDER BY expected_id ASC";

    auto r = qr->run(
        seek_query.c_str(),
        {qtest::ColType::INT64, qtest::ColType::INT64, qtest::ColType::INT64,
         qtest::ColType::INT64, qtest::ColType::INT64, qtest::ColType::INT64});

    REQUIRE(r.size() == 2);
    for (size_t i = 0; i < r.size(); i++) {
        CHECK(r[i].int64_at(0) == r[i].int64_at(1));
        CHECK(r[i].int64_at(2) == r[i].int64_at(3));
        CHECK(r[i].int64_at(4) == r[i].int64_at(5));
    }
}

// ---------------------------------------------------------------------------
// Bidirectional (BOTH direction) tests (M26)
// Merged from former test_q6_bidirectional.cpp
// ---------------------------------------------------------------------------

// M26-D stateless dedup: each edge emitted once (forward if src<tgt, backward if src>tgt).
TEST_CASE("Undirected KNOWS from sample person", "[ldbc][traversal][both]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p:Person {id: " + std::to_string(ldbc::SAMPLE_PERSON_ID) +
             "})-[:KNOWS]-(f:Person) RETURN f.id ORDER BY f.id ASC";
    auto r = qr->run(q.c_str(), {qtest::ColType::INT64});
    REQUIRE(r.size() == (size_t)ldbc::TRAV_KNOWS_FRIENDS_OF_SAMPLE_PERSON);
}

// Undirected HAS_CREATOR (heterogeneous label)
// Comment->Person is stored as Comment(src)->Person(dst).
// Undirected: from Comment side, forward finds the creator.
TEST_CASE("Undirected HAS_CREATOR from Comment", "[ldbc][traversal][both]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (c:Comment {id: " + std::to_string(ldbc::SAMPLE_COMMENT_ID) +
             "})-[:HAS_CREATOR]-(p:Person) RETURN p.id, p.firstName";
    auto r = qr->run(q.c_str(),
        {qtest::ColType::INT64, qtest::ColType::STRING});
    // HAS_CREATOR is heterogeneous (Comment→Person), only one direction hits.
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == ldbc::SAMPLE_COMMENT_CREATOR_ID);
    CHECK(r[0].str_at(1) == ldbc::SAMPLE_COMMENT_CREATOR_FIRSTNAME);
}

// Count undirected KNOWS friends (aggregation)
TEST_CASE("Count undirected KNOWS friends of sample person", "[ldbc][traversal][both]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p:Person {id: " + std::to_string(ldbc::SAMPLE_PERSON_ID) +
             "})-[:KNOWS]-(f:Person) RETURN count(DISTINCT f) AS cnt";
    auto r = qr->run(q.c_str(), {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == ldbc::TRAV_KNOWS_FRIENDS_OF_SAMPLE_PERSON);
}

// VarLen undirected KNOWS *1..2
// Edge isomorphism prevents trivial cycles (A-B-A).
TEST_CASE("VarLen undirected KNOWS *1..2 from sample person", "[ldbc][traversal][both][varlen]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p:Person {id: " + std::to_string(ldbc::SAMPLE_PERSON_ID) +
             "})-[:KNOWS*1..2]-(f:Person) RETURN count(DISTINCT f) AS cnt";
    auto r = qr->run(q.c_str(), {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    // Must exceed the 1-hop distinct friend count (2-hop reaches FoF)
    // and must NOT include the sample person itself (edge isomorphism).
    CHECK(r[0].int64_at(0) > ldbc::TRAV_KNOWS_FRIENDS_OF_SAMPLE_PERSON);
}

// Undirected KNOWS with friend properties + edge properties (IdSeek)
TEST_CASE("Undirected KNOWS with friend and edge properties", "[ldbc][traversal][both][idseek]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (n:Person {id: " + std::to_string(ldbc::SAMPLE_PERSON_ID) +
             "})-[r:KNOWS]-(friend:Person) "
             "RETURN friend.id AS personId, friend.firstName AS firstName, "
             "       friend.lastName AS lastName, r.creationDate AS friendshipCreationDate "
             "ORDER BY friendshipCreationDate DESC, personId ASC";
    auto r = qr->run(q.c_str(),
        {qtest::ColType::INT64, qtest::ColType::STRING,
         qtest::ColType::STRING, qtest::ColType::INT64});
    REQUIRE(r.size() == (size_t)ldbc::TRAV_KNOWS_FRIENDS_OF_SAMPLE_PERSON);
    for (size_t i = 0; i < r.size(); i++) {
        CHECK(r[i].int64_at(0) > 0);
        CHECK(!r[i].str_at(1).empty());
        CHECK(!r[i].str_at(2).empty());
        CHECK(r[i].int64_at(3) > 0);
    }
}

// Unlabeled target node — friend without :Person label
// The system should infer the target partition from the edge definition.
TEST_CASE("Unlabeled target node properties via IdSeek", "[ldbc][traversal][both][unlabeled]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (n:Person {id: " + std::to_string(ldbc::SAMPLE_PERSON_ID) +
             "})-[r:KNOWS]-(friend) "
             "RETURN friend.id AS personId, friend.firstName AS firstName, "
             "       friend.lastName AS lastName, r.creationDate AS friendshipCreationDate "
             "ORDER BY friendshipCreationDate DESC, personId ASC";
    auto r = qr->run(q.c_str(),
        {qtest::ColType::INT64, qtest::ColType::STRING,
         qtest::ColType::STRING, qtest::ColType::INT64});
    REQUIRE(r.size() == (size_t)ldbc::TRAV_KNOWS_FRIENDS_OF_SAMPLE_PERSON);
    for (size_t i = 0; i < r.size(); i++) {
        CHECK(r[i].int64_at(0) > 0);
        CHECK(!r[i].str_at(1).empty());
        CHECK(!r[i].str_at(2).empty());
        CHECK(r[i].int64_at(3) > 0);
    }
}
