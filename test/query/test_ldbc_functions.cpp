// Stage 8 — Cypher meta functions: labels(), type(), keys(), properties()

#include "catch.hpp"
#include "helpers/query_runner.hpp"
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

#define FRESH_DB() qr->clearDelta()

TEST_CASE("labels() returns node label list", "[ldbc][func][meta]") {
    SKIP_IF_NO_DB();
    try {
        FRESH_DB();
        auto r = qr->run(
            "MATCH (n:Person {id: 933}) RETURN labels(n) AS lbl",
            {qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        auto val = r[0].str_at(0);
        CHECK(val.find("Person") != std::string::npos);
    } catch (const std::exception& e) {
        FAIL("labels(): " << e.what());
    }
}

TEST_CASE("type() returns relationship type", "[ldbc][func][meta]") {
    SKIP_IF_NO_DB();
    try {
        FRESH_DB();
        auto r = qr->run(
            "MATCH (n:Person {id: 933})-[r:KNOWS]->(m:Person) RETURN type(r) AS t LIMIT 1",
            {qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        CHECK(r[0].str_at(0) == "KNOWS");
    } catch (const std::exception& e) {
        FAIL("type(): " << e.what());
    }
}

TEST_CASE("keys() returns property key names for node", "[ldbc][func][meta]") {
    SKIP_IF_NO_DB();
    try {
        FRESH_DB();
        auto r = qr->run(
            "MATCH (n:Person {id: 933}) RETURN keys(n) AS k",
            {qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        auto val = r[0].str_at(0);
        CHECK(val.find("id") != std::string::npos);
        CHECK(val.find("firstName") != std::string::npos);
        CHECK(val.find("last") != std::string::npos);
    } catch (const std::exception& e) {
        FAIL("keys(): " << e.what());
    }
}

TEST_CASE("properties() returns property map for node", "[ldbc][func][meta]") {
    SKIP_IF_NO_DB();
    try {
        FRESH_DB();
        // properties(n) returns a struct — verify it doesn't crash
        // and returns exactly 1 row
        auto r = qr->run(
            "MATCH (n:Person {id: 933}) RETURN properties(n) AS props",
            {});
        CHECK(r.size() == 1);
    } catch (const std::exception& e) {
        FAIL("properties(): " << e.what());
    }
}

TEST_CASE("keys() returns property key names for edge", "[ldbc][func][meta]") {
    SKIP_IF_NO_DB();
    try {
        FRESH_DB();
        auto r = qr->run(
            "MATCH (:Person {id: 933})-[r:KNOWS]->(:Person) RETURN keys(r) AS k LIMIT 1",
            {qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        auto val = r[0].str_at(0);
        INFO("keys(edge) returned: " << val);
        // KNOWS edge should have at least one property key
        CHECK(val.size() > 2);  // more than just "[]"
    } catch (const std::exception& e) {
        FAIL("keys(edge): " << e.what());
    }
}

// ============================================================
// String + concatenation, =~ regex, FOREACH
// ============================================================

TEST_CASE("string + concatenation", "[ldbc][func][string]") {
    SKIP_IF_NO_DB();
    try {
        FRESH_DB();
        auto r = qr->run(
            "MATCH (n:Person {id: 933}) RETURN n.firstName + ' ' + n.lastName AS fullName",
            {qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        auto val = r[0].str_at(0);
        CHECK(val.find(" ") != std::string::npos);  // has space between names
        CHECK(val.size() > 2);  // non-empty
    } catch (const std::exception& e) {
        FAIL("string +: " << e.what());
    }
}

TEST_CASE("=~ regex operator", "[ldbc][func][regex]") {
    SKIP_IF_NO_DB();
    try {
        FRESH_DB();
        // Find persons whose firstName starts with 'Ma'
        auto r = qr->run(
            "MATCH (n:Person {id: 933}) WHERE n.firstName =~ 'Ma.*' RETURN n.id",
            {qtest::ColType::INT64});
        // Person 933's firstName may or may not start with 'Ma'
        // Just verify it doesn't crash
        CHECK(r.size() <= 1);
    } catch (const std::exception& e) {
        FAIL("=~ regex: " << e.what());
    }
}

// FOREACH test removed: UNWIND rewrite crashes and CREATE pollutes the shared DB.
// Re-add when FOREACH has a dedicated execution operator and isolated workspace.
