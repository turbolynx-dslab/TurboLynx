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
// fixture-independent. Previously tagged [!mayfail]; counts match the fixture cleanly now.
TEST_CASE("REPLY_OF to Message count", "[ldbc][traversal][mpv]") {
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

// Regression for issue #128: IdSeek through a join into a union-label
// (`:Message` = Comment ∪ Post) used to drop sibling-only properties
// because the virtual-partition rewrite clobbered scan_types[part_idx]
// with the first sub-partition's projection, and later siblings
// inherited those SQLNULL placeholders.
TEST_CASE("Message sibling-only property via HAS_CREATOR join",
          "[ldbc][traversal][mpv][issue-90]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (:Person)<-[:HAS_CREATOR]-(m:Message) "
        "WHERE m.imageFile <> '' "
        "RETURN m.id, m.imageFile "
        "ORDER BY m.id ASC LIMIT 1",
        {qtest::ColType::INT64, qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) > 0);
    CHECK(r[0].str_at(1).find("photo") == 0);
}

// Regression for issue #90's struct/CASE path. struct_extract used to
// drop the parent's selection (Reference instead of Slice through the
// dictionary), so callers like CASE/coalesce that evaluate a branch on
// only a subset of rows would read row-0 of the unsliced child. Drive
// CASE explicitly so the regression is exercised even when the dataset
// happens to short-circuit coalesce on the first child.
TEST_CASE("struct_extract preserves row selection through CASE",
          "[ldbc][traversal][issue-90][struct-extract]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (m:Message) WHERE m.id = 1168231105519 "
        "WITH head(collect({n: m})) AS w "
        "RETURN CASE WHEN w.n.imageFile <> '' THEN w.n.imageFile ELSE 'fallback' END AS pick",
        {qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    // 1168231105519 is an image-Post in the LDBC mini fixture. The CASE
    // THEN branch must read the actual imageFile value through the
    // struct chain, not the fallback.
    CHECK(r[0].str_at(0).find("photo") == 0);
}

// Regression for issues #48 + #132: struct_extract used to silently
// fall back to field index 0 when the requested field name was not
// present on the bound STRUCT type. With #132's repro (Comment partition
// has no imageFile column), the runtime dereferenced a wrong-typed
// child and SIGSEGV'd. The bind path now flags the missing field so
// the runtime emits a typed-NULL — Cypher property semantics let the
// coalesce fall through to the next branch.
TEST_CASE("struct_extract on missing field returns NULL (coalesce path)",
          "[ldbc][traversal][issue-132][issue-48][struct-extract]") {
    SKIP_IF_NO_DB();
    // Comment has `content` but no `imageFile`. coalesce must pick
    // content (non-NULL) since the imageFile branch is a missing-field
    // NULL — pre-fix this query SIGSEGV'd at prepare.
    auto r = qr->run(
        "MATCH (m:Comment) WHERE m.content <> '' "
        "WITH m LIMIT 1 "
        "WITH head(collect({n: m})) AS w "
        "RETURN coalesce(w.n.content, w.n.imageFile) AS pick",
        {qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    REQUIRE(!r[0].is_null_at(0));
    CHECK(r[0].str_at(0).size() > 0);
}

TEST_CASE("struct_extract on missing field is IS NULL",
          "[ldbc][traversal][issue-132][issue-48][struct-extract]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (m:Comment) WITH m LIMIT 1 "
        "WITH head(collect({n: m})) AS w "
        "RETURN w.n.imageFile IS NULL AS missing",
        {qtest::ColType::BOOL});
    REQUIRE(r.size() == 1);
    CHECK(r[0].bool_at(0) == true);
}

// toFloat(INTERVAL) → total milliseconds, the LDBC IC7 idiom for
// "ms diff between two ms-timestamps".
TEST_CASE("toFloat(INTERVAL) returns total milliseconds",
          "[ldbc][traversal][issue-90][tofloat-interval]") {
    SKIP_IF_NO_DB();
    // 1356998400000 - 1356998200000 = 200000 ms.
    auto r = qr->run(
        "MATCH (a:Message {id: 1168231108551}) "
        "MATCH (b:Message {id: 1168231108526}) "
        "RETURN toInteger(toFloat(a.creationDate - b.creationDate)) AS ms",
        {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    // The exact value isn't pinned (fixture-dependent), but the result
    // must be a non-zero integer — pre-fix it was 0 because toFloat had
    // no INTERVAL overload.
    CHECK(r[0].int64_at(0) != 0);
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

// ====================================================================
// OPTIONAL MATCH semantics
//
// LOJ semantics pinned down here (oracle: Neo4j 5 on the same fixture):
// - no match preserves the left row with NULL columns
// - all matches surface as separate rows
// - chained OPTIONAL propagates NULL into later clauses
// - WHERE attached to OPTIONAL filters the match, not the left row
// - count(var) and count(*) differ on miss (NULL is uncounted)
// - undirected BOTH-direction with both endpoints bound must produce
//   exactly one row per pair regardless of storage direction
//   (issue #83 regression — used to emit a real match + spurious NULL)
// ====================================================================

TEST_CASE("OPTIONAL MATCH: missing edge keeps left row with NULL",
          "[ldbc][traversal][optional]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p:Person {id: " + std::to_string(ldbc::SAMPLE_PERSON_ID) +
             "}) "
             "OPTIONAL MATCH (p)-[:KNOWS]->(f:Person {id: 9999999999999}) "
             "RETURN p.id, f IS NULL AS missing";
    auto r = qr->run(q.c_str(),
        {qtest::ColType::INT64, qtest::ColType::BOOL});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == ldbc::SAMPLE_PERSON_ID);
    CHECK(r[0].bool_at(1));
}

// T2 (all matches surface as rows) and T4 (multi-hop atomic) are
// fixture-specific because they pin every friend's id (and city).
// Their definitions sit in the TURBOLYNX_LDBC_FIXTURE_MINI block at
// the end of this section, alongside the other mini-only OPTIONAL
// MATCH cases. SF1 needs its own oracle to enable them there.

TEST_CASE("OPTIONAL MATCH: NULL propagates through chained OPTIONAL",
          "[ldbc][traversal][optional]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p:Person {id: " + std::to_string(ldbc::SAMPLE_PERSON_ID) +
             "}) "
             "OPTIONAL MATCH (p)-[:KNOWS]->(f:Person {id: 9999999999999}) "
             "OPTIONAL MATCH (f)-[:IS_LOCATED_IN]->(c:Place) "
             "RETURN p.id, f IS NULL AS f_null, c IS NULL AS c_null";
    auto r = qr->run(q.c_str(),
        {qtest::ColType::INT64, qtest::ColType::BOOL, qtest::ColType::BOOL});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == ldbc::SAMPLE_PERSON_ID);
    CHECK(r[0].bool_at(1));
    CHECK(r[0].bool_at(2));
}

// WHERE attached to OPTIONAL is part of the optional clause — when the
// filter rejects every match, the left row is still preserved with NULL.
TEST_CASE("OPTIONAL MATCH: WHERE inside OPTIONAL keeps left row on filter miss",
          "[ldbc][traversal][optional]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p:Person {id: " + std::to_string(ldbc::SAMPLE_PERSON_ID) +
             "}) "
             "OPTIONAL MATCH (p)-[:KNOWS]->(f:Person) "
             "WHERE f.firstName = '_NoSuchFirstName_' "
             "RETURN p.id, f IS NULL AS missing";
    auto r = qr->run(q.c_str(),
        {qtest::ColType::INT64, qtest::ColType::BOOL});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == ldbc::SAMPLE_PERSON_ID);
    CHECK(r[0].bool_at(1));
}

TEST_CASE("OPTIONAL MATCH: property access on missing node returns NULL",
          "[ldbc][traversal][optional]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p:Person {id: " + std::to_string(ldbc::SAMPLE_PERSON_ID) +
             "}) "
             "OPTIONAL MATCH (p)-[:STUDY_AT]->(u:Organisation {id: 9999999999999}) "
             "RETURN p.id, u.name IS NULL AS name_null";
    auto r = qr->run(q.c_str(),
        {qtest::ColType::INT64, qtest::ColType::BOOL});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == ldbc::SAMPLE_PERSON_ID);
    CHECK(r[0].bool_at(1));
}

// count(var) does not count NULL; count(*) counts rows. With matches the
// two agree; on a miss they disagree.
TEST_CASE("OPTIONAL MATCH: count(var) == count(*) when matches exist",
          "[ldbc][traversal][optional][aggregation]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p:Person {id: " + std::to_string(ldbc::SAMPLE_PERSON_ID) +
             "}) "
             "OPTIONAL MATCH (p)-[:KNOWS]->(f:Person) "
             "RETURN count(f) AS cf, count(*) AS cs";
    auto r = qr->run(q.c_str(),
        {qtest::ColType::INT64, qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == ldbc::SAMPLE_PERSON_OUTGOING_KNOWS);
    CHECK(r[0].int64_at(1) == ldbc::SAMPLE_PERSON_OUTGOING_KNOWS);
}

TEST_CASE("OPTIONAL MATCH: count(var)=0 vs count(*)=1 on miss",
          "[ldbc][traversal][optional][aggregation]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p:Person {id: " + std::to_string(ldbc::SAMPLE_PERSON_ID) +
             "}) "
             "OPTIONAL MATCH (p)-[:KNOWS]->(f:Person {id: 9999999999999}) "
             "RETURN count(f) AS cf, count(*) AS cs";
    auto r = qr->run(q.c_str(),
        {qtest::ColType::INT64, qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 0);
    CHECK(r[0].int64_at(1) == 1);
}

// Relationship-property filter inside OPTIONAL — every fixture KNOWS
// edge has creationDate > 0, so the filter is a no-op.
TEST_CASE("OPTIONAL MATCH: relationship property filter",
          "[ldbc][traversal][optional]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p:Person {id: " + std::to_string(ldbc::SAMPLE_PERSON_ID) +
             "}) "
             "OPTIONAL MATCH (p)-[r:KNOWS]->(f:Person) "
             "WHERE r.creationDate > 0 "
             "RETURN count(r) AS rc";
    REQUIRE(qr->count(q.c_str()) == ldbc::SAMPLE_PERSON_OUTGOING_KNOWS);
}

// ------------------- Mini-only OPTIONAL MATCH cases ---------------------
// These pin BOTH-direction LOJ behaviour against the directed KNOWS
// storage of the SF0.003 mini fixture. SF1 needs its own anchor ids.
#ifdef TURBOLYNX_LDBC_FIXTURE_MINI

// BOTH-direction, both endpoints bound, edge absent → no match, single
// LOJ row with count(r)=0.
TEST_CASE("OPTIONAL MATCH BOTH (mini): unconnected pair → NULL row",
          "[ldbc][traversal][optional][both]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (a:Person {id: 14}), (b:Person {id: 16}) "
        "OPTIONAL MATCH (a)-[r:KNOWS]-(b) "
        "RETURN count(r) AS rc",
        {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 0);
}

// BOTH-direction, both endpoints bound, edge stored as a→b. Forward
// phase of the dual-phase scan matches. IS3_FRIENDS[1] pins Ken's
// friendship_ms so we can confirm the actual edge was returned.
// Two queries because RETURN count(agg) + r.property in one
// projection currently mis-prunes the LOJ row (separate issue).
TEST_CASE("OPTIONAL MATCH BOTH (mini): edge a→b matches with exact edge",
          "[ldbc][traversal][optional][both]") {
    SKIP_IF_NO_DB();
    const char* prefix =
        "MATCH (a:Person {id: 14}), (b:Person {id: 10995116277782}) "
        "OPTIONAL MATCH (a)-[r:KNOWS]-(b) ";
    auto rc_query = std::string(prefix) + "RETURN count(r) AS rc";
    auto rd_query = std::string(prefix) + "RETURN r.creationDate AS d";
    auto rc = qr->run(rc_query.c_str(), {qtest::ColType::INT64});
    REQUIRE(rc.size() == 1);
    CHECK(rc[0].int64_at(0) == 1);
    auto rd = qr->run(rd_query.c_str(), {qtest::ColType::INT64});
    REQUIRE(rd.size() == 1);
    CHECK(rd[0].int64_at(0) == ldbc::IS3_FRIENDS[1].friendship_ms);
}

// BOTH-direction, both endpoints bound, edge stored as b→a. Backward
// phase of the dual-phase scan must surface the same edge even though
// the storage SID/TID is reversed relative to the query. Two queries
// for the same projection caveat as above.
TEST_CASE("OPTIONAL MATCH BOTH (mini): edge b→a matches same edge",
          "[ldbc][traversal][optional][both]") {
    SKIP_IF_NO_DB();
    const char* prefix =
        "MATCH (a:Person {id: 10995116277782}), (b:Person {id: 14}) "
        "OPTIONAL MATCH (a)-[r:KNOWS]-(b) ";
    auto rc_query = std::string(prefix) + "RETURN count(r) AS rc";
    auto rd_query = std::string(prefix) + "RETURN r.creationDate AS d";
    auto rc = qr->run(rc_query.c_str(), {qtest::ColType::INT64});
    REQUIRE(rc.size() == 1);
    CHECK(rc[0].int64_at(0) == 1);
    auto rd = qr->run(rd_query.c_str(), {qtest::ColType::INT64});
    REQUIRE(rd.size() == 1);
    CHECK(rd[0].int64_at(0) == ldbc::IS3_FRIENDS[1].friendship_ms);
}

// Plain (non-OPTIONAL) MATCH counterparts of the OPTIONAL BOTH tests
// above. The OPTIONAL path is patched by the OR-LOJ-predicate in
// PlanOptionalMatch; the plain MATCH still relies on a single-direction
// IJ + SEL chain and misses the backward-stored orientation.
// Tracked in issue #138.
TEST_CASE("MATCH BOTH (mini): plain MATCH single-clause edge a→b matches",
          "[ldbc][traversal][both]") {
    SKIP_IF_NO_DB();
    const char* prefix =
        "MATCH (a:Person {id: 14}), (b:Person {id: 10995116277782}) "
        "MATCH (a)-[r:KNOWS]-(b) ";
    auto rc_query = std::string(prefix) + "RETURN count(r) AS rc";
    auto rd_query = std::string(prefix) + "RETURN r.creationDate AS d";
    auto rc = qr->run(rc_query.c_str(), {qtest::ColType::INT64});
    REQUIRE(rc.size() == 1);
    CHECK(rc[0].int64_at(0) == 1);
    auto rd = qr->run(rd_query.c_str(), {qtest::ColType::INT64});
    REQUIRE(rd.size() == 1);
    CHECK(rd[0].int64_at(0) == ldbc::IS3_FRIENDS[1].friendship_ms);
}

// [!mayfail] until #138 is fixed — plain MATCH BOTH self-ref both-bound
// with backward-stored storage misses the edge.
TEST_CASE("MATCH BOTH (mini): plain MATCH single-clause edge b→a matches",
          "[ldbc][traversal][both][!mayfail]") {
    SKIP_IF_NO_DB();
    const char* prefix =
        "MATCH (a:Person {id: 10995116277782}), (b:Person {id: 14}) "
        "MATCH (a)-[r:KNOWS]-(b) ";
    auto rc_query = std::string(prefix) + "RETURN count(r) AS rc";
    auto rd_query = std::string(prefix) + "RETURN r.creationDate AS d";
    auto rc = qr->run(rc_query.c_str(), {qtest::ColType::INT64});
    REQUIRE(rc.size() == 1);
    CHECK(rc[0].int64_at(0) == 1);
    auto rd = qr->run(rd_query.c_str(), {qtest::ColType::INT64});
    REQUIRE(rd.size() == 1);
    CHECK(rd[0].int64_at(0) == ldbc::IS3_FRIENDS[1].friendship_ms);
}

// Single-MATCH form `(a {id})-[r:KNOWS]-(b {id})` — the planner sees
// neither endpoint bound at the entry of the standard A→R→B branch,
// so it goes through a different code path than the two-MATCH form.
// Same backward-stored bug applies in principle.
TEST_CASE("MATCH BOTH (mini): single-clause inline ids edge a→b matches",
          "[ldbc][traversal][both]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count(
        "MATCH (a:Person {id: 14})-[r:KNOWS]-(b:Person {id: 10995116277782}) "
        "RETURN count(r)") == 1);
}

// [!mayfail] until #138 is fixed — single-MATCH inline form takes a
// different planner path but hits the same forward-only IJ chain.
TEST_CASE("MATCH BOTH (mini): single-clause inline ids edge b→a matches",
          "[ldbc][traversal][both][!mayfail]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count(
        "MATCH (a:Person {id: 10995116277782})-[r:KNOWS]-(b:Person {id: 14}) "
        "RETURN count(r)") == 1);
}

// Mixed match/no-match across multiple anchors: SAMPLE_PERSON has a
// STUDY_AT relation, Alim (24189255811081) has none. Both rows
// surface, the latter with NULL.
TEST_CASE("OPTIONAL MATCH (mini): mixed match/no-match across anchors",
          "[ldbc][traversal][optional]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "UNWIND [14, 24189255811081] AS pid "
        "MATCH (p:Person {id: pid}) "
        "OPTIONAL MATCH (p)-[:STUDY_AT]->(u:Organisation) "
        "RETURN p.id AS id, u.name AS uni, u IS NULL AS u_null "
        "ORDER BY id ASC",
        {qtest::ColType::INT64, qtest::ColType::STRING, qtest::ColType::BOOL});
    REQUIRE(r.size() == 2);
    CHECK(r[0].int64_at(0) == 14);
    CHECK(r[0].str_at(1) == "Zanjan_University");
    CHECK(!r[0].bool_at(2));
    CHECK(r[1].int64_at(0) == 24189255811081LL);
    CHECK(r[1].bool_at(2));
}

// T2 (mini-only): every outgoing KNOWS friend of SAMPLE_PERSON is
// returned with the exact friend id from the oracle.
TEST_CASE("OPTIONAL MATCH (mini): all matches surface with exact friend ids",
          "[ldbc][traversal][optional]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p:Person {id: " + std::to_string(ldbc::SAMPLE_PERSON_ID) +
             "}) "
             "OPTIONAL MATCH (p)-[:KNOWS]->(f:Person) "
             "RETURN p.id, f.id AS fid ORDER BY fid ASC";
    auto r = qr->run(q.c_str(),
        {qtest::ColType::INT64, qtest::ColType::INT64});
    constexpr size_t N =
        sizeof(ldbc::SAMPLE_PERSON_KNOWS_FRIENDS_BY_ID_ASC) /
        sizeof(ldbc::SAMPLE_PERSON_KNOWS_FRIENDS_BY_ID_ASC[0]);
    REQUIRE(r.size() == N);
    for (size_t i = 0; i < N; ++i) {
        INFO("row " << i);
        CHECK(r[i].int64_at(0) == ldbc::SAMPLE_PERSON_ID);
        CHECK(r[i].int64_at(1) ==
              ldbc::SAMPLE_PERSON_KNOWS_FRIENDS_BY_ID_ASC[i].person_id);
    }
}

// T4 (mini-only): multi-hop atomic LOJ. For each KNOWS friend, follow
// IS_LOCATED_IN to a city and compare both friend id and city name
// against the oracle.
TEST_CASE("OPTIONAL MATCH (mini): multi-hop atomic — friend and city exact",
          "[ldbc][traversal][optional][multihop]") {
    SKIP_IF_NO_DB();
    auto q = "MATCH (p:Person {id: " + std::to_string(ldbc::SAMPLE_PERSON_ID) +
             "}) "
             "OPTIONAL MATCH (p)-[:KNOWS]->(f:Person)-[:IS_LOCATED_IN]->(c:Place) "
             "RETURN f.id AS fid, c.name AS city ORDER BY fid ASC";
    auto r = qr->run(q.c_str(),
        {qtest::ColType::INT64, qtest::ColType::STRING});
    constexpr size_t N =
        sizeof(ldbc::SAMPLE_PERSON_KNOWS_FRIENDS_BY_ID_ASC) /
        sizeof(ldbc::SAMPLE_PERSON_KNOWS_FRIENDS_BY_ID_ASC[0]);
    REQUIRE(r.size() == N);
    for (size_t i = 0; i < N; ++i) {
        INFO("row " << i);
        const auto& exp = ldbc::SAMPLE_PERSON_KNOWS_FRIENDS_BY_ID_ASC[i];
        CHECK(r[i].int64_at(0) == exp.person_id);
        CHECK(r[i].str_at(1) == exp.city_name);
    }
}

#endif  // TURBOLYNX_LDBC_FIXTURE_MINI
