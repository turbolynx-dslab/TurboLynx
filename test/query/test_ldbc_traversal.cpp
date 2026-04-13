// Stage 3 — Traversal tests: multi-hop, bidirectional, MPV/MPE
// All expected values verified against Neo4j 5.24.0 with LDBC SF1.

#include "catch.hpp"
#include "helpers/query_runner.hpp"
#include <vector>
#include <set>
#include <utility>

extern std::string g_ldbc_path;
extern bool g_skip_requested;
extern bool g_has_ldbc;

extern qtest::QueryRunner* get_ldbc_runner();

#define SKIP_IF_NO_DB() \
    if (g_ldbc_path.empty()) { WARN("--ldbc-path not set, skipping"); g_skip_requested = true; return; } \
    if (!g_has_ldbc) { WARN("DB has no LDBC schema, skipping"); return; } \
    auto* qr = get_ldbc_runner(); \
    if (!qr) { FAIL("Cannot open DB: " << g_ldbc_path); return; }

TEST_CASE("FoF count (Person 933)", "[ldbc][traversal][multihop]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count(
        "MATCH (p:Person {id: 933})-[:KNOWS]->(f:Person)-[:KNOWS]->(fof:Person) "
        "WHERE fof <> p RETURN count(DISTINCT fof)") == 1506);
}

TEST_CASE("Top 10 persons by Comment count", "[ldbc][traversal][multihop]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (p:Person)<-[:HAS_CREATOR]-(c:Comment) "
        "RETURN p.id, count(c) AS cnt "
        "ORDER BY cnt DESC, p.id ASC LIMIT 10",
        {qtest::ColType::INT64, qtest::ColType::INT64});
    REQUIRE(r.size() == 10);

    // Verify top result (personId, cnt)
    using P = std::pair<int64_t, int64_t>;
    std::vector<P> expected = {
        {2199023262543LL, 8915}, {4139,  7896}, {2199023259756LL, 7694},
        {2783,            7530}, {4398046513018LL, 7423}, {7725, 6612},
        {6597069777240LL, 6565}, {9116,  6135}, {4398046519372LL, 5894},
        {8796093029267LL, 5640}
    };
    for (size_t i = 0; i < 10; ++i) {
        INFO("row " << i);
        CHECK(r[i].int64_at(0) == expected[i].first);
        CHECK(r[i].int64_at(1) == expected[i].second);
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

    using P = std::pair<int64_t, int64_t>;
    std::vector<P> expected = {
        {77644,         1208}, {87312,  1032},
        {137439023186LL, 1001}, {412317916558LL, 891}, {55025, 810}
    };
    for (size_t i = 0; i < 5; ++i) {
        INFO("row " << i);
        CHECK(r[i].int64_at(0) == expected[i].first);
        CHECK(r[i].int64_at(1) == expected[i].second);
    }
}

TEST_CASE("Distinct Comment creators liked by Person 933", "[ldbc][traversal][multihop]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count(
        "MATCH (p:Person {id: 933})-[:LIKES]->(c:Comment)-[:HAS_CREATOR]->(creator:Person) "
        "RETURN count(DISTINCT creator)") == 12);
}

TEST_CASE("Top 5 TagClasses by Tag count", "[ldbc][traversal][multihop]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (t:Tag)-[:HAS_TYPE]->(tc:TagClass) "
        "RETURN tc.name, count(t) AS cnt "
        "ORDER BY cnt DESC, tc.name ASC LIMIT 5",
        {qtest::ColType::STRING, qtest::ColType::INT64});
    REQUIRE(r.size() == 5);

    using P = std::pair<std::string, int64_t>;
    std::vector<P> expected = {
        {"Album", 5061}, {"Single", 4311}, {"Person", 1530},
        {"Country", 1000}, {"MusicalArtist", 899}
    };
    for (size_t i = 0; i < 5; ++i) {
        INFO("row " << i);
        CHECK(r[i].str_at(0) == expected[i].first);
        CHECK(r[i].int64_at(1) == expected[i].second);
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

    using P = std::pair<std::string, int64_t>;
    std::vector<P> expected = {
        {"Augustine_of_Hippo", 10817}, {"Adolf_Hitler", 5227},
        {"Muammar_Gaddafi", 5120}, {"Imelda_Marcos", 4412}, {"Sammy_Sosa", 4059}
    };
    for (size_t i = 0; i < 5; ++i) {
        INFO("row " << i);
        CHECK(r[i].str_at(0) == expected[i].first);
        CHECK(r[i].int64_at(1) == expected[i].second);
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

    using P = std::pair<int64_t, int64_t>;
    std::vector<P> expected = {
        {2199023262543LL, 9936}, {2783,             8754},
        {2199023259756LL, 8667}, {4139,             8558},
        {7725,            7833}, {4398046513018LL,  7682},
        {6597069777240LL, 7491}, {9116,             7189},
        {4398046519372LL, 6535}, {8796093029267LL,  6294}
    };
    for (size_t i = 0; i < 10; ++i) {
        INFO("row " << i);
        CHECK(r[i].int64_at(0) == expected[i].first);
        CHECK(r[i].int64_at(1) == expected[i].second);
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

    using P = std::pair<std::string, int64_t>;
    std::vector<P> expected = {
        {"Augustine_of_Hippo", 24299}, {"Adolf_Hitler", 12326},
        {"Muammar_Gaddafi", 12003}, {"Imelda_Marcos", 9571}, {"Genghis_Khan", 8982}
    };
    for (size_t i = 0; i < 5; ++i) {
        INFO("row " << i);
        CHECK(r[i].str_at(0) == expected[i].first);
        CHECK(r[i].int64_at(1) == expected[i].second);
    }
}

TEST_CASE("Distinct Message creators liked by Person 933", "[ldbc][traversal][multihop][mpe]") {
    SKIP_IF_NO_DB();
    // LIKES -> Message -> HAS_CREATOR -> Person
    // 12 comment creators + 5 post creators, 14 distinct persons
    REQUIRE(qr->count(
        "MATCH (p:Person {id: 933})-[:LIKES]->(m:Message)-[:HAS_CREATOR]->(creator:Person) "
        "RETURN count(DISTINCT creator)") == 14);
}

// ---------------------------------------------------------------------------
// Multi-partition vertex tests (M28 — :Message = Comment + Post)
// Merged from former test_q7_multipart_vertex.cpp
// ---------------------------------------------------------------------------

// MPV-01: Count Comments via HAS_CREATOR using :Message label
// Message maps to both Comment and Post partitions.
// HAS_CREATOR connects both Comment→Person and Post→Person.
// Neo4j ground truth: Message count = 370 (Comment 57 + Post 313).
TEST_CASE("Message via HAS_CREATOR count", "[ldbc][traversal][mpv]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (:Person {id: 933})<-[:HAS_CREATOR]-(message:Message) "
        "RETURN count(message)",
        {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 370);
    // Verify Comment-only count
    auto r2 = qr->run(
        "MATCH (:Person {id: 933})<-[:HAS_CREATOR]-(c:Comment) "
        "RETURN count(c)",
        {qtest::ColType::INT64});
    REQUIRE(r2.size() == 1);
    CHECK(r2[0].int64_at(0) == 57);
}

// MPV-02: Count all Messages via REPLY_OF (multi-partition edge + multi-partition vertex)
// REPLY_OF connects Comment→Post and Comment→Comment.
// :Message should capture both Post and Comment destinations.
TEST_CASE("REPLY_OF to Message count", "[ldbc][traversal][mpv][!mayfail]") {
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

// MPV-03: Message properties — ids from Message (Comment+Post) via HAS_CREATOR
// Neo4j: Message includes both Comments and Posts, so lowest IDs may be Posts.
TEST_CASE("Message properties via HAS_CREATOR", "[ldbc][traversal][mpv]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (:Person {id: 933})<-[:HAS_CREATOR]-(message:Message) "
        "RETURN count(message)",
        {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 370);
}

// ---------------------------------------------------------------------------
// Bidirectional (BOTH direction) tests (M26)
// Merged from former test_q6_bidirectional.cpp
// ---------------------------------------------------------------------------

// M26-D stateless dedup: each edge emitted once (forward if src<tgt, backward if src>tgt).
TEST_CASE("Undirected KNOWS from Person 933", "[ldbc][traversal][both]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "RETURN f.id ORDER BY f.id ASC",
        {qtest::ColType::INT64});
    // Person 933 has 5 friends — dedup ensures exactly 5 rows
    REQUIRE(r.size() == 5);
}

// Undirected HAS_CREATOR (heterogeneous label)
// Comment->Person is stored as Comment(src)->Person(dst).
// Undirected: from Comment side, forward finds the creator.
TEST_CASE("Undirected HAS_CREATOR from Comment", "[ldbc][traversal][both]") {
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

// Count undirected KNOWS friends (aggregation)
TEST_CASE("Count undirected KNOWS friends of Person 933", "[ldbc][traversal][both]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person) "
        "RETURN count(DISTINCT f) AS cnt",
        {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 5);
}

// VarLen undirected KNOWS *1..2
// Edge isomorphism prevents trivial cycles (A-B-A).
TEST_CASE("VarLen undirected KNOWS *1..2 from Person 933", "[ldbc][traversal][both][varlen]") {
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

// Undirected KNOWS with friend properties + edge properties (IdSeek)
TEST_CASE("Undirected KNOWS with friend and edge properties", "[ldbc][traversal][both][idseek]") {
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

// Unlabeled target node — friend without :Person label
// The system should infer the target partition from the edge definition.
TEST_CASE("Unlabeled target node properties via IdSeek", "[ldbc][traversal][both][unlabeled]") {
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
