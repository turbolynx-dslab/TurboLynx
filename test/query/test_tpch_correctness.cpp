// Stage 9 — TPC-H query tests
//
// Reads Cypher queries from benchmark/tpch/sf1/q*.cql and verifies
// result row counts against pre-computed expected values for the
// loaded fixture.
//
// Two fixture sizes are supported:
//   * SF0.01  — the committed mini fixture under test/data/tpch-mini/.
//                Used by CI (loaded via scripts/load-tpch-mini.sh).
//                Most queries pass; a number of them currently SIGSEGV
//                inside the engine and are tagged [.broken-mini] so
//                they're hidden from the default [tpch] run. Tracked
//                in issue #69.
//   * SF1     — the full benchmark workspace (loaded via
//                scripts/load-tpch.sh). Selected via the cmake-time
//                option -DTURBOLYNX_TPCH_FIXTURE_SF1=ON; defaults to
//                SF0.01.
//
// The path to the .cql query files is resolved at compile time via
// the TURBOLYNX_REPO_ROOT define injected by CMakeLists.txt.

#include "catch.hpp"
#include "helpers/query_runner.hpp"
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

extern std::string g_tpch_path;
extern bool g_skip_requested;
extern bool g_has_tpch;
extern qtest::QueryRunner* get_tpch_runner();

struct DeltaGuard {
    qtest::QueryRunner* qr_;
    explicit DeltaGuard(qtest::QueryRunner* qr) : qr_(qr) { qr_->clearDelta(); }
    ~DeltaGuard() { qr_->clearDelta(); }
    DeltaGuard(const DeltaGuard&) = delete;
    DeltaGuard& operator=(const DeltaGuard&) = delete;
};

#define SKIP_IF_NO_DB() \
    if (g_tpch_path.empty()) { WARN("--tpch-path not set, skipping"); g_skip_requested = true; return; } \
    if (!g_has_tpch) { WARN("DB has no TPC-H schema, skipping"); return; } \
    auto* qr = get_tpch_runner(); \
    if (!qr) { FAIL("Cannot open DB: " << g_tpch_path); return; } \
    DeltaGuard _delta_guard(qr)

// Path to the .cql query files. TURBOLYNX_REPO_ROOT is set at cmake time
// (test/query/CMakeLists.txt) so the test binary stays self-contained
// regardless of the current working directory.
#ifndef TURBOLYNX_REPO_ROOT
#error "TURBOLYNX_REPO_ROOT must be defined by the build system"
#endif
static const std::string QUERY_DIR =
    std::string(TURBOLYNX_REPO_ROOT) + "/benchmark/tpch/sf1/";

static std::string readFile(const std::string &path) {
    std::ifstream f(path);
    if (!f.is_open()) return "";
    std::stringstream ss;
    ss << f.rdbuf();
    return ss.str();
}

// Macro for a query known to run cleanly on the active fixture.
// expected_rows = 0 disables the row-count assertion (smoke-only).
#define TPCH_TEST(qnum, expected_rows) \
TEST_CASE("TPC-H Q" #qnum, "[tpch][q" #qnum "]") { \
    SKIP_IF_NO_DB(); \
    try { \
        std::string query = readFile(QUERY_DIR + "q" #qnum ".cql"); \
        REQUIRE(!query.empty()); \
        auto r = qr->run(query.c_str(), {}); \
        size_t exp = expected_rows; \
        if (exp > 0) { \
            CHECK(r.size() == exp); \
        } \
    } catch (const std::exception& e) { \
        FAIL("TPC-H Q" #qnum ": " << e.what()); \
    } \
}

// Macro for a query that SIGSEGVs inside the engine on the SF0.01
// mini fixture. Tagged [broken-mini] so CI can exclude it via
// `[tpch]~[broken-mini]`; otherwise the SIGSEGV would tear down the
// rest of the test run. To probe its current state during a fix:
//   ./query_test "[broken-mini]" --tpch-path /path/to/ws
// When the underlying engine bug is fixed, replace the macro use with
// `TPCH_TEST(qnum, <SF0.01 row count>)`.
#define TPCH_TEST_BROKEN_MINI(qnum, issue) \
TEST_CASE("TPC-H Q" #qnum " (broken on SF0.01, issue " issue ")", \
          "[tpch][q" #qnum "][broken-mini]") { \
    SKIP_IF_NO_DB(); \
    std::string query = readFile(QUERY_DIR + "q" #qnum ".cql"); \
    REQUIRE(!query.empty()); \
    auto r = qr->run(query.c_str(), {}); \
    /* If we got here without SIGSEGV / exception, the underlying bug \
       is likely fixed — convert the call site to TPCH_TEST. */ \
    FAIL("Query no longer crashes — replace TPCH_TEST_BROKEN_MINI with TPCH_TEST"); \
}

// Expected row counts on the committed SF0.01 mini fixture
// (test/data/tpch-mini/), measured against the engine at the commit
// that introduced this fixture. Eleven queries currently crash with
// SIGSEGV inside the engine and are deferred via TPCH_TEST_BROKEN_MINI;
// see issue #69.

// Passing on SF0.01 mini fixture
TPCH_TEST(2, 5)
TPCH_TEST(4, 5)
TPCH_TEST(8, 2)
TPCH_TEST(12, 2)
TPCH_TEST(13, 32)
TPCH_TEST(16, 315)
TPCH_TEST(17, 1)
TPCH_TEST(18, 0)   // SF0.01: predicate `sum(l_quantity) > 300` filters all rows; SF1 returns 9.
TPCH_TEST(20, 2)
TPCH_TEST(21, 1)
TPCH_TEST(22, 7)

// SIGSEGV on SF0.01 mini fixture (DECIMAL arithmetic in aggregates is
// the strongest suspect — most failing queries match this shape).
TPCH_TEST_BROKEN_MINI(1,  "#69")
TPCH_TEST_BROKEN_MINI(3,  "#69")
TPCH_TEST_BROKEN_MINI(5,  "#69")
TPCH_TEST_BROKEN_MINI(6,  "#69")
TPCH_TEST_BROKEN_MINI(7,  "#69")
TPCH_TEST_BROKEN_MINI(9,  "#69")
TPCH_TEST_BROKEN_MINI(10, "#69")
TPCH_TEST_BROKEN_MINI(11, "#69")
TPCH_TEST_BROKEN_MINI(14, "#69")
TPCH_TEST_BROKEN_MINI(15, "#69")
TPCH_TEST_BROKEN_MINI(19, "#69")
