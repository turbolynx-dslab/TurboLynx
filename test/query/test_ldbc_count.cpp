// Stage 1 — Node / Edge count smoke tests
//
// Two fixture sizes are supported by the same source file. The
// expected counts are pulled from `helpers/ldbc_expected_counts.hpp`,
// which selects between the SF1 (default) and SF0.003 (mini) values
// based on the `TURBOLYNX_LDBC_FIXTURE_MINI` cmake define. SF1 values
// were verified against Neo4j 5.24.0 originally; SF0.003 values were
// verified against Neo4j 5 with the same `test/data/ldbc-mini/`
// fixture loaded via `neo4j-admin database import`.

#include "catch.hpp"
#include "helpers/query_runner.hpp"
#include "helpers/ldbc_expected_counts.hpp"

extern std::string g_ldbc_path;
extern bool g_skip_requested;
extern bool g_has_ldbc;

extern qtest::QueryRunner* get_ldbc_runner();

#define SKIP_IF_NO_DB() \
    if (g_ldbc_path.empty()) { WARN("--ldbc-path not set, skipping"); g_skip_requested = true; return; } \
    if (!g_has_ldbc) { WARN("DB has no LDBC schema, skipping"); return; } \
    auto* qr = get_ldbc_runner(); \
    if (!qr) { FAIL("Cannot open DB: " << g_ldbc_path); return; }

// ---------------------------------------------------------------------------
// Node counts
// ---------------------------------------------------------------------------

TEST_CASE("Person count", "[ldbc][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (p:Person) RETURN count(p)") == ldbc::PERSON_COUNT);
}

TEST_CASE("MPV count(*) — no-label MATCH", "[ldbc][count][bug-a2]") {
    SKIP_IF_NO_DB();
    // Pre-fix this aborted in ORCA's normalizer because column pruning over
    // a multi-vertex-partition NodeScan + count(*) collapsed every branch's
    // ProjectColumnar to NULL. The regression is "plans + executes without
    // aborting"; the exact total is left unpinned to avoid coupling to a
    // specific scale factor — we just sanity-check it strictly exceeds the
    // largest single-label count we already verify (Person).
    auto rows = qr->run("MATCH (n) RETURN count(*) AS cnt",
                        {qtest::ColType::INT64});
    REQUIRE(rows.size() == 1);
    CHECK(rows[0].int64_at(0) > ldbc::COUNT_STAR_LOWER_BOUND);
}

TEST_CASE("Comment count", "[ldbc][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (c:Comment) RETURN count(c)") == ldbc::COMMENT_COUNT);
}

TEST_CASE("Post count", "[ldbc][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (p:Post) RETURN count(p)") == ldbc::POST_COUNT);
}

TEST_CASE("Forum count", "[ldbc][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (f:Forum) RETURN count(f)") == ldbc::FORUM_COUNT);
}

TEST_CASE("Tag count", "[ldbc][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (t:Tag) RETURN count(t)") == ldbc::TAG_COUNT);
}

TEST_CASE("TagClass count", "[ldbc][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (tc:TagClass) RETURN count(tc)") == ldbc::TAGCLASS_COUNT);
}

TEST_CASE("Place count", "[ldbc][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (pl:Place) RETURN count(pl)") == ldbc::PLACE_COUNT);
}

TEST_CASE("Organisation count", "[ldbc][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (o:Organisation) RETURN count(o)") == ldbc::ORGANISATION_COUNT);
}

// ---------------------------------------------------------------------------
// Single-partition edge counts
// ---------------------------------------------------------------------------

TEST_CASE("KNOWS count", "[ldbc][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN count(r)") == ldbc::KNOWS_COUNT);
}

TEST_CASE("HAS_CREATOR (Comment->Person) count", "[ldbc][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (a:Comment)-[r:HAS_CREATOR]->(b:Person) RETURN count(r)") == ldbc::HAS_CREATOR_COMMENT_COUNT);
}

TEST_CASE("HAS_CREATOR (Post->Person) count", "[ldbc][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (a:Post)-[r:HAS_CREATOR]->(b:Person) RETURN count(r)") == ldbc::HAS_CREATOR_POST_COUNT);
}

TEST_CASE("LIKES (Person->Comment) count", "[ldbc][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (a:Person)-[r:LIKES]->(b:Comment) RETURN count(r)") == ldbc::LIKES_COMMENT_COUNT);
}

TEST_CASE("LIKES (Person->Post) count", "[ldbc][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (a:Person)-[r:LIKES]->(b:Post) RETURN count(r)") == ldbc::LIKES_POST_COUNT);
}

TEST_CASE("CONTAINER_OF count", "[ldbc][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (a:Forum)-[r:CONTAINER_OF]->(b:Post) RETURN count(r)") == ldbc::CONTAINER_OF_COUNT);
}

TEST_CASE("REPLY_OF (Comment->Post) count", "[ldbc][count][!mayfail]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (a:Comment)-[r:REPLY_OF]->(b:Post) RETURN count(r)") == ldbc::REPLY_OF_POST_COUNT);
}

TEST_CASE("REPLY_OF (Comment->Comment) count", "[ldbc][count][!mayfail]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (a:Comment)-[r:REPLY_OF]->(b:Comment) RETURN count(r)") == ldbc::REPLY_OF_COMMENT_COUNT);
}

TEST_CASE("HAS_TAG (Comment->Tag) count", "[ldbc][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (a:Comment)-[r:HAS_TAG]->(b:Tag) RETURN count(r)") == ldbc::HAS_TAG_COMMENT_COUNT);
}

TEST_CASE("HAS_TAG (Post->Tag) count", "[ldbc][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (a:Post)-[r:HAS_TAG]->(b:Tag) RETURN count(r)") == ldbc::HAS_TAG_POST_COUNT);
}

TEST_CASE("HAS_TAG (Forum->Tag) count", "[ldbc][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (a:Forum)-[r:HAS_TAG]->(b:Tag) RETURN count(r)") == ldbc::HAS_TAG_FORUM_COUNT);
}

TEST_CASE("HAS_TYPE count", "[ldbc][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (a:Tag)-[r:HAS_TYPE]->(b:TagClass) RETURN count(r)") == ldbc::HAS_TYPE_COUNT);
}

TEST_CASE("IS_PART_OF count", "[ldbc][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (a:Place)-[r:IS_PART_OF]->(b:Place) RETURN count(r)") == ldbc::IS_PART_OF_COUNT);
}

// ---------------------------------------------------------------------------
// Multi-partition vertex (MPV) — :Message = Comment + Post
// ---------------------------------------------------------------------------

TEST_CASE("Message count", "[ldbc][count][mpv][!mayfail]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (m:Message) RETURN count(m)") == ldbc::MESSAGE_COUNT);
}

// ---------------------------------------------------------------------------
// Multi-partition edge (MPE) — unified edge types across partitions
// ---------------------------------------------------------------------------

TEST_CASE("HAS_CREATOR (Message->Person) count", "[ldbc][count][mpe]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count(
        "MATCH (m:Message)-[r:HAS_CREATOR]->(p:Person) RETURN count(r)") == ldbc::HAS_CREATOR_MESSAGE_COUNT);
}

TEST_CASE("LIKES (Person->Message) count", "[ldbc][count][mpe]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count(
        "MATCH (p:Person)-[r:LIKES]->(m:Message) RETURN count(r)") == ldbc::LIKES_MESSAGE_COUNT);
}

TEST_CASE("HAS_TAG (Message->Tag) count", "[ldbc][count][mpe]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count(
        "MATCH (m:Message)-[r:HAS_TAG]->(t:Tag) RETURN count(r)") == ldbc::HAS_TAG_MESSAGE_COUNT);
}

TEST_CASE("IS_LOCATED_IN (Message->Place) count", "[ldbc][count][mpe]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count(
        "MATCH (m:Message)-[r:IS_LOCATED_IN]->(pl:Place) RETURN count(r)") == ldbc::IS_LOCATED_IN_MESSAGE_COUNT);
}

TEST_CASE("IS_LOCATED_IN total count", "[ldbc][count][mpe]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count(
        "MATCH (a)-[r:IS_LOCATED_IN]->(b:Place) RETURN count(r)") == ldbc::IS_LOCATED_IN_TOTAL_COUNT);
}

// ---------------------------------------------------------------------------
// count(*) regression — zero-column chunk path
// ---------------------------------------------------------------------------
// A pattern whose closing edge rebinds an already-bound variable produces an
// AdjIdxJoin whose output schema is empty (ORCA drops all columns because the
// aggregate above only needs row counts). The result is a 0-column DataChunk.
// Previously this hit `!types.empty()` in DataChunk::Initialize and aborted;
// now DataChunk accepts zero-column Initialize and the count(*) pipeline runs
// to completion.

TEST_CASE("count(*) on triangle pattern runs without assertion",
          "[ldbc][count][regression]") {
    SKIP_IF_NO_DB();
    // The exact query does not need to have a non-zero answer at any scale —
    // what matters is that the pipeline completes without hitting the
    // `!types.empty()` assertion at data_chunk.cpp:40. We just require a
    // single row (the count scalar) to come back.
    auto r = qr->run(
        "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)-[:KNOWS]->(a) "
        "RETURN count(*);");
    REQUIRE(r.size() == 1);
    REQUIRE(r[0].int64_at(0) >= 0);
}

TEST_CASE("count(*) on 2-cycle pattern runs without assertion",
          "[ldbc][count][regression]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(a) "
        "RETURN count(*);");
    REQUIRE(r.size() == 1);
    REQUIRE(r[0].int64_at(0) >= 0);
}
