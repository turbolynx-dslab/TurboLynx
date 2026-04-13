// Stage 1 — Node / Edge count smoke tests
// All expected values verified against Neo4j 5.24.0 with LDBC SF1.

#include "catch.hpp"
#include "helpers/query_runner.hpp"

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
    REQUIRE(qr->count("MATCH (p:Person) RETURN count(p)") == 9892);
}

TEST_CASE("Comment count", "[ldbc][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (c:Comment) RETURN count(c)") == 2052169);
}

TEST_CASE("Post count", "[ldbc][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (p:Post) RETURN count(p)") == 1003605);
}

TEST_CASE("Forum count", "[ldbc][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (f:Forum) RETURN count(f)") == 90492);
}

TEST_CASE("Tag count", "[ldbc][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (t:Tag) RETURN count(t)") == 16080);
}

TEST_CASE("TagClass count", "[ldbc][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (tc:TagClass) RETURN count(tc)") == 71);
}

TEST_CASE("Place count", "[ldbc][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (pl:Place) RETURN count(pl)") == 1460);
}

TEST_CASE("Organisation count", "[ldbc][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (o:Organisation) RETURN count(o)") == 7955);
}

// ---------------------------------------------------------------------------
// Single-partition edge counts
// ---------------------------------------------------------------------------

TEST_CASE("KNOWS count", "[ldbc][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN count(r)") == 180623);
}

TEST_CASE("HAS_CREATOR (Comment->Person) count", "[ldbc][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (a:Comment)-[r:HAS_CREATOR]->(b:Person) RETURN count(r)") == 2052169);
}

TEST_CASE("HAS_CREATOR (Post->Person) count", "[ldbc][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (a:Post)-[r:HAS_CREATOR]->(b:Person) RETURN count(r)") == 1003605);
}

TEST_CASE("LIKES (Person->Comment) count", "[ldbc][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (a:Person)-[r:LIKES]->(b:Comment) RETURN count(r)") == 1438418);
}

TEST_CASE("LIKES (Person->Post) count", "[ldbc][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (a:Person)-[r:LIKES]->(b:Post) RETURN count(r)") == 751677);
}

TEST_CASE("CONTAINER_OF count", "[ldbc][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (a:Forum)-[r:CONTAINER_OF]->(b:Post) RETURN count(r)") == 1003605);
}

TEST_CASE("REPLY_OF (Comment->Post) count", "[ldbc][count][!mayfail]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (a:Comment)-[r:REPLY_OF]->(b:Post) RETURN count(r)") == 1011420);
}

TEST_CASE("REPLY_OF (Comment->Comment) count", "[ldbc][count][!mayfail]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (a:Comment)-[r:REPLY_OF]->(b:Comment) RETURN count(r)") == 1040749);
}

TEST_CASE("HAS_TAG (Comment->Tag) count", "[ldbc][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (a:Comment)-[r:HAS_TAG]->(b:Tag) RETURN count(r)") == 2698393);
}

TEST_CASE("HAS_TAG (Post->Tag) count", "[ldbc][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (a:Post)-[r:HAS_TAG]->(b:Tag) RETURN count(r)") == 713258);
}

TEST_CASE("HAS_TAG (Forum->Tag) count", "[ldbc][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (a:Forum)-[r:HAS_TAG]->(b:Tag) RETURN count(r)") == 309766);
}

TEST_CASE("HAS_TYPE count", "[ldbc][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (a:Tag)-[r:HAS_TYPE]->(b:TagClass) RETURN count(r)") == 16080);
}

TEST_CASE("IS_PART_OF count", "[ldbc][count]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (a:Place)-[r:IS_PART_OF]->(b:Place) RETURN count(r)") == 1454);
}

// ---------------------------------------------------------------------------
// Multi-partition vertex (MPV) — :Message = Comment + Post
// ---------------------------------------------------------------------------

TEST_CASE("Message count", "[ldbc][count][mpv][!mayfail]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count("MATCH (m:Message) RETURN count(m)") == 3055774);
}

// ---------------------------------------------------------------------------
// Multi-partition edge (MPE) — unified edge types across partitions
// ---------------------------------------------------------------------------

TEST_CASE("HAS_CREATOR (Message->Person) count", "[ldbc][count][mpe]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count(
        "MATCH (m:Message)-[r:HAS_CREATOR]->(p:Person) RETURN count(r)") == 3055774);
}

TEST_CASE("LIKES (Person->Message) count", "[ldbc][count][mpe]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count(
        "MATCH (p:Person)-[r:LIKES]->(m:Message) RETURN count(r)") == 2190095);
}

TEST_CASE("HAS_TAG (Message->Tag) count", "[ldbc][count][mpe]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count(
        "MATCH (m:Message)-[r:HAS_TAG]->(t:Tag) RETURN count(r)") == 3411651);
}

TEST_CASE("IS_LOCATED_IN (Message->Place) count", "[ldbc][count][mpe]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count(
        "MATCH (m:Message)-[r:IS_LOCATED_IN]->(pl:Place) RETURN count(r)") == 3055774);
}

// Person(9892) + Comment(2052169) + Post(1003605) + Organisation(7955) = 3073621
TEST_CASE("IS_LOCATED_IN total count", "[ldbc][count][mpe]") {
    SKIP_IF_NO_DB();
    REQUIRE(qr->count(
        "MATCH (a)-[r:IS_LOCATED_IN]->(b:Place) RETURN count(r)") == 3073621);
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
    // The exact query does not need to have a non-zero answer on SF1 — what
    // matters is that the pipeline completes without hitting the
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
