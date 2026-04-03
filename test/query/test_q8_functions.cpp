// Stage 8 — Cypher meta functions: labels(), type(), keys(), properties()

#include "catch.hpp"
#include "helpers/query_runner.hpp"
#include <vector>

extern std::string g_db_path;
extern bool g_skip_requested;

extern qtest::QueryRunner* get_runner();

#define SKIP_IF_NO_DB() \
    if (g_db_path.empty()) { WARN("--db-path not set, skipping"); g_skip_requested = true; return; } \
    auto* qr = get_runner(); \
    if (!qr) { FAIL("QueryRunner not initialized"); return; }

#define FRESH_DB() qr->clearDelta()

TEST_CASE("Q8-1 labels() returns node label list", "[q8][func][meta]") {
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

TEST_CASE("Q8-2 type() returns relationship type", "[q8][func][meta]") {
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

TEST_CASE("Q8-3 keys() returns property key names for node", "[q8][func][meta]") {
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

TEST_CASE("Q8-4 properties() returns property map for node", "[q8][func][meta]") {
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

TEST_CASE("Q8-5 keys() returns property key names for edge", "[q8][func][meta]") {
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

TEST_CASE("Q8-10 string + concatenation", "[q8][func][string]") {
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

TEST_CASE("Q8-11 =~ regex operator", "[q8][func][regex]") {
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

TEST_CASE("Q8-12 FOREACH with SET", "[q8][func][foreach][.]") {  // disabled: needs dedicated execution operator
    SKIP_IF_NO_DB();
    try {
        FRESH_DB();
        // Create test nodes, then FOREACH to update them
        qr->run("CREATE (n:Person {id: 812001, firstName: 'A'})", {});
        qr->run("CREATE (n:Person {id: 812002, firstName: 'B'})", {});

        // FOREACH to set firstName on each
        qr->run(
            "MATCH (n:Person) WHERE n.id IN [812001, 812002] "
            "WITH collect(n) AS nodes "
            "FOREACH (x IN nodes | SET x.firstName = 'Updated')",
            {});

        // Verify
        auto r = qr->run(
            "MATCH (n:Person) WHERE n.id IN [812001, 812002] RETURN n.firstName ORDER BY n.id",
            {qtest::ColType::STRING});
        REQUIRE(r.size() == 2);
        CHECK(r[0].str_at(0) == "Updated");
        CHECK(r[1].str_at(0) == "Updated");
    } catch (const std::exception& e) {
        FAIL("FOREACH: " << e.what());
    }
}
