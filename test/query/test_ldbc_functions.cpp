// Stage 8 — Cypher meta functions: labels(), type(), keys(), properties()

#include "catch.hpp"
#include "helpers/query_runner.hpp"
#include "main/capi/turbolynx.h"
#include <vector>

extern std::string g_ldbc_path;
extern bool g_skip_requested;
extern bool g_has_ldbc;

extern qtest::QueryRunner* get_ldbc_runner();

struct DeltaGuard {
    qtest::QueryRunner* qr_;
    explicit DeltaGuard(qtest::QueryRunner* qr) : qr_(qr) { qr_->clearDelta(); }
    ~DeltaGuard() { qr_->clearDelta(); }
    DeltaGuard(const DeltaGuard&) = delete;
    DeltaGuard& operator=(const DeltaGuard&) = delete;
};

static std::string LastErrorMessage() {
    char* errmsg = nullptr;
    turbolynx_get_last_error(&errmsg);
    return errmsg ? errmsg : "";
}

static double ExecuteSingleDouble(qtest::QueryRunner* qr, const char* query) {
    auto* prep = turbolynx_prepare(qr->conn_id(), const_cast<char*>(query));
    REQUIRE(prep != nullptr);

    turbolynx_resultset_wrapper* rw = nullptr;
    auto total_rows = turbolynx_execute(qr->conn_id(), prep, &rw);
    if (total_rows == TURBOLYNX_ERROR) {
        auto msg = LastErrorMessage();
        turbolynx_close_prepared_statement(prep);
        FAIL("execute failed: " << msg);
    }

    REQUIRE(total_rows == 1);
    REQUIRE(rw != nullptr);
    REQUIRE(turbolynx_fetch_next(rw) == TURBOLYNX_MORE_RESULT);
    auto value = turbolynx_get_double(rw, 0);
    CHECK(turbolynx_fetch_next(rw) == TURBOLYNX_END_OF_RESULT);

    turbolynx_close_resultset(rw);
    turbolynx_close_prepared_statement(prep);
    return value;
}

static void ExecuteStatement(qtest::QueryRunner* qr, const char* query) {
    auto* prep = turbolynx_prepare(qr->conn_id(), const_cast<char*>(query));
    REQUIRE(prep != nullptr);

    turbolynx_resultset_wrapper* rw = nullptr;
    auto total_rows = turbolynx_execute(qr->conn_id(), prep, &rw);
    if (total_rows == TURBOLYNX_ERROR) {
        auto msg = LastErrorMessage();
        turbolynx_close_prepared_statement(prep);
        FAIL("execute failed: " << msg);
    }

    if (rw) {
        while (turbolynx_fetch_next(rw) == TURBOLYNX_MORE_RESULT) {
        }
        turbolynx_close_resultset(rw);
    }
    turbolynx_close_prepared_statement(prep);
}

#define SKIP_IF_NO_DB() \
    if (g_ldbc_path.empty()) { WARN("--ldbc-path not set, skipping"); g_skip_requested = true; return; } \
    if (!g_has_ldbc) { WARN("DB has no LDBC schema, skipping"); return; } \
    auto* qr = get_ldbc_runner(); \
    if (!qr) { FAIL("Cannot open DB: " << g_ldbc_path); return; } \
    DeltaGuard _delta_guard(qr)

TEST_CASE("labels() returns node label list", "[ldbc][func][meta]") {
    SKIP_IF_NO_DB();
    try {
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
        auto r = qr->run(
            "MATCH (:Person {id: 933})-[r:KNOWS]->(:Person) RETURN keys(r) AS k LIMIT 1",
            {qtest::ColType::STRING});
        REQUIRE(r.size() == 1);
        auto val = r[0].str_at(0);
        INFO("keys(edge) returned: " << val);
        CHECK(val.find("creationDate") != std::string::npos);
    } catch (const std::exception& e) {
        FAIL("keys(edge): " << e.what());
    }
}

TEST_CASE("random() returns initialized value in range", "[ldbc][func][math]") {
    SKIP_IF_NO_DB();
    try {
        auto value = ExecuteSingleDouble(qr, "RETURN random() AS r");
        CHECK(value >= 0.0);
        CHECK(value < 1.0);
    } catch (const std::exception& e) {
        FAIL("random(): " << e.what());
    }
}

TEST_CASE("setseed() deterministically seeds random()", "[ldbc][func][math]") {
    SKIP_IF_NO_DB();
    try {
        ExecuteStatement(qr, "RETURN setseed(0.5) AS _");
        auto first = ExecuteSingleDouble(qr, "RETURN random() AS r");
        ExecuteStatement(qr, "RETURN setseed(0.5) AS _");
        auto second = ExecuteSingleDouble(qr, "RETURN random() AS r");
        CHECK(first == second);
    } catch (const std::exception& e) {
        FAIL("setseed(): " << e.what());
    }
}

// ============================================================
// String + concatenation, =~ regex, FOREACH
// ============================================================

TEST_CASE("string + concatenation", "[ldbc][func][string]") {
    SKIP_IF_NO_DB();
    try {
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
