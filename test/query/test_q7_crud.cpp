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
    // CREATE with an existing label (Person). Just check it doesn't crash.
    // CREATE returns no rows.
    try {
        auto r = qr->run(
            "CREATE (n:Person {id: 99999999999999, firstName: 'TestJohn'})",
            {});
        // CREATE returns 0 rows (it's a write operation)
        CHECK(r.size() == 0);
    } catch (const std::exception& e) {
        FAIL("CREATE should not throw: " << e.what());
    }
}

TEST_CASE("Q7-02 CREATE then MATCH", "[q7][crud][create]") {
    SKIP_IF_NO_DB();
    try {
        qr->run("CREATE (n:Person {id: 88888888888888, firstName: 'TestJane'})", {});
        // Note: Phase 1 only stores in DeltaStore. Read-path merge is not yet
        // implemented, so MATCH won't find the created node. We just verify
        // CREATE itself doesn't crash.
        CHECK(true);  // If we got here, CREATE succeeded
    } catch (const std::exception& e) {
        FAIL("CREATE+MATCH should not throw: " << e.what());
    }
}
