// LDBC Hadoop datagen SF=0.1 smoke regression.
//
// Cross-checks the engine against a fixture produced by the *Hadoop*
// variant of the LDBC SNB datagen (vs the Spark mini fixture used by
// the rest of the [ldbc] suite). The two datagens diverge in entity
// column layout, multi-valued field encoding, and datetime format —
// so the same query stack should be exercised on both to catch
// datagen-shaped regressions.
//
// This suite is intentionally narrow: a handful of CSV-verifiable
// counts (scale 1528 Persons, 286k Messages) and one regression query
// per recently fixed path (#90, #128). No external reference like
// Neo4j is required — every expected value is either a row count we
// derived directly from the generated CSVs or a property whose value
// is known from the schema (e.g. imageFile must start with `photo`
// for any Post that has one).
//
// Local-only. Skip if `--ldbc-hadoop-path` isn't provided. Generation
// recipe is in scripts/preprocessors/ldbc-preprocessors/README.md.

#include "catch.hpp"
#include "helpers/query_runner.hpp"
#include <cstdint>
#include <string>

extern std::string g_ldbc_hadoop_path;
extern bool g_skip_requested;
extern bool g_has_ldbc_hadoop;

extern qtest::QueryRunner* get_ldbc_hadoop_runner();

#define SKIP_IF_NO_DB() \
    if (g_ldbc_hadoop_path.empty()) { \
        WARN("--ldbc-hadoop-path not set, skipping"); \
        g_skip_requested = true; return; \
    } \
    if (!g_has_ldbc_hadoop) { \
        WARN("--ldbc-hadoop-path given but DB has no LDBC schema, skipping"); \
        return; \
    } \
    auto* qr = get_ldbc_hadoop_runner(); \
    if (!qr) { FAIL("Cannot open DB: " << g_ldbc_hadoop_path); return; }


// ---------------------------------------------------------------------------
// Count sanity — values match the row counts of the generated CSVs and
// are also checked by the integrity probe in query_test_main.cpp. They
// re-appear here so a [ldbc-hadoop]-only run still asserts them.
// ---------------------------------------------------------------------------

TEST_CASE("Hadoop SF=0.1: Person count", "[ldbc-hadoop][count]") {
    SKIP_IF_NO_DB();
    CHECK(qr->count("MATCH (n:Person) RETURN count(n)") == 1528);
}

TEST_CASE("Hadoop SF=0.1: Message (Comment+Post) union count",
          "[ldbc-hadoop][count][mpv]") {
    SKIP_IF_NO_DB();
    CHECK(qr->count("MATCH (n:Comment) RETURN count(n)") == 151043);
    CHECK(qr->count("MATCH (n:Post) RETURN count(n)") == 135701);
    CHECK(qr->count("MATCH (n:Message) RETURN count(n)") == 286744);
}

TEST_CASE("Hadoop SF=0.1: KNOWS edge count",
          "[ldbc-hadoop][count][traversal]") {
    SKIP_IF_NO_DB();
    CHECK(qr->count("MATCH (:Person)-[:KNOWS]->(:Person) RETURN count(*)") == 14073);
}


// ---------------------------------------------------------------------------
// Regression for #128 — IdSeek MPV scan through a multi-hop join used
// to drop sibling-only properties because the planner clobbered the
// scan_types for non-primary partitions. Hadoop SF=0.1 has both image
// Posts (imageFile populated) and text Comments/Posts, so this query
// exercises the same code path on Hadoop-shaped data.
// ---------------------------------------------------------------------------

TEST_CASE("Hadoop SF=0.1: sibling-only imageFile via HAS_CREATOR join",
          "[ldbc-hadoop][issue-128][mpv]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (:Person)<-[:HAS_CREATOR]-(m:Message) "
        "WHERE m.imageFile <> '' "
        "RETURN m.id, m.imageFile "
        "ORDER BY m.id ASC LIMIT 3",
        {qtest::ColType::INT64, qtest::ColType::STRING});
    REQUIRE(r.size() == 3);
    for (size_t i = 0; i < r.size(); i++) {
        INFO("row " << i);
        // Every imageFile on an LDBC SNB image Post is "photo<post_id>.jpg".
        CHECK(r[i].str_at(1).find("photo") == 0);
        CHECK(r[i].int64_at(0) > 0);
    }
}


// ---------------------------------------------------------------------------
// Regression for #90 — struct_extract through a CASE / coalesce path.
// Hadoop SF=0.1 has 151k Comments with real content, so a single-row
// collect+head over an arbitrary Comment must surface the underlying
// string through `coalesce(w.n.content, w.n.imageFile)`.
// ---------------------------------------------------------------------------

TEST_CASE("Hadoop SF=0.1: struct_extract on collect+head over Message union",
          "[ldbc-hadoop][issue-90][struct-extract]") {
    SKIP_IF_NO_DB();
    // Specific image-Post in the SF=0.1 output. collect a struct over
    // that single row, head() it, then read the imageFile through the
    // struct chain — this is the per-row path #90's struct_extract
    // fix addressed.
    //
    // (Comment-only WITH→collect+coalesce over a partition that lacks
    // the imageFile column entirely is a separate code path that
    // currently SIGSEGVs — tracked under #132.)
    auto r = qr->run(
        "MATCH (m:Message) WHERE m.id = 481036337184 "
        "WITH head(collect({n: m})) AS w "
        "RETURN w.n.id AS mid, w.n.imageFile AS img",
        {qtest::ColType::INT64, qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 481036337184LL);
    CHECK(r[0].str_at(1).find("photo481036337184") == 0);
}


// ---------------------------------------------------------------------------
// toFloat(INTERVAL) — TIMESTAMP - TIMESTAMP returns INTERVAL by SQL
// standard; the overload added with #90 makes toFloat over that
// interval return total milliseconds. Verify on two real messages.
// ---------------------------------------------------------------------------

TEST_CASE("Hadoop SF=0.1: toFloat(INTERVAL) sanity",
          "[ldbc-hadoop][issue-90][tofloat-interval]") {
    SKIP_IF_NO_DB();
    // Two specific messages whose IDs are stable in the SF=0.1 output;
    // any two messages with different creationDate would do. Drive the
    // subtraction directly instead of indexing a collected node list —
    // node-list indexing through `ms[i].prop` is a separate code path
    // not relevant to the toFloat(INTERVAL) regression.
    auto r = qr->run(
        "MATCH (a:Message {id: 148281}) MATCH (b:Message {id: 230037}) "
        "RETURN toInteger(toFloat(b.creationDate - a.creationDate)) AS ms_diff",
        {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) > 0);
}
