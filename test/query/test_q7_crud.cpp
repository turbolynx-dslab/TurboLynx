// Stage 7 — CRUD operations (CREATE / SET / DELETE)
// Phase 1: CREATE Node

#include "catch.hpp"
#include "helpers/query_runner.hpp"
#include <vector>

extern std::string g_db_path;
extern bool g_skip_requested;

extern qtest::QueryRunner* get_runner();

#define SKIP_IF_NO_DB() \
    if (g_db_path.empty()) { WARN("--db-path not set, skipping"); g_skip_requested = true; return; } \
    auto* qr = get_runner(); \
    if (!qr) { FAIL("Cannot open DB: " << g_db_path); return; }

// Phase 1: CREATE Node tests
TEST_CASE("Q7-01 CREATE single node", "[q7][crud][create]") {
    SKIP_IF_NO_DB();
    try {
        auto r = qr->run(
            "CREATE (n:Person {id: 99999999999999, firstName: 'TestJohn'})",
            {});
        CHECK(r.size() == 0);
    } catch (const std::exception& e) {
        FAIL("CREATE should not throw: " << e.what());
    }
}

TEST_CASE("Q7-02 CREATE then MATCH count", "[q7][crud][create]") {
    SKIP_IF_NO_DB();
    try {
        qr->run("CREATE (n:Person {id: 88888888888888, firstName: 'TestJane'})", {});
        auto r = qr->run(
            "MATCH (n:Person) RETURN count(n) AS cnt",
            {qtest::ColType::INT64});
        REQUIRE(r.size() == 1);
        // Base Person count (9892) + 2 created nodes (Q7-01 + Q7-02)
        CHECK(r[0].int64_at(0) >= 9894);
    } catch (const std::exception& e) {
        FAIL("CREATE+MATCH should not throw: " << e.what());
    }
}
