// Stage 9 — TPC-H query tests
//
// Reads Cypher queries from `benchmark/tpch/sf1/q*.cql` and verifies
// result row counts against `helpers/tpch_expected_counts.hpp`, which
// dispatches between the SF1 (default) and SF0.01 (mini) values based
// on the `TURBOLYNX_TPCH_FIXTURE_MINI` cmake define.
//
// Two fixture sizes are supported:
//   * SF1     — the legacy benchmark fixture, loaded externally via
//                `scripts/load-tpch.sh`. Default selection. SF1 expected
//                counts come from DuckDB-generated reference CSVs.
//   * SF0.01  — the committed mini fixture under `test/data/tpch-mini/`,
//                loaded via `scripts/load-tpch-mini.sh`. CI uses this.
//                Eleven of the 22 queries currently SIGSEGV at this
//                scale (issue #69) and are wrapped with the
//                `TPCH_TEST_BROKEN_MINI` macro / `[broken-mini]` tag so
//                they don't tear down the rest of the run.
//
// The path to the .cql query files is resolved at compile time via
// the TURBOLYNX_REPO_ROOT define injected by CMakeLists.txt.

#include "catch.hpp"
#include "helpers/query_runner.hpp"
#include "helpers/tpch_expected_counts.hpp"
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
        size_t exp = (size_t)(expected_rows); \
        if (exp > 0) { \
            CHECK(r.size() == exp); \
        } \
    } catch (const std::exception& e) { \
        FAIL("TPC-H Q" #qnum ": " << e.what()); \
    } \
}

// SF0.01-only macro for queries that SIGSEGV inside the engine on the
// mini fixture. Tagged `[broken-mini]` so the CI filter excludes them.
// When the underlying engine bug is fixed, replace with `TPCH_TEST(qnum, ...)`.
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

#ifdef TURBOLYNX_TPCH_FIXTURE_MINI
// SF0.01 mini fixture: 11 passing queries + 11 broken-mini.
TPCH_TEST(2,  tpch::Q2)
TPCH_TEST(4,  tpch::Q4)
TPCH_TEST(8,  tpch::Q8)
TPCH_TEST(12, tpch::Q12)
TPCH_TEST(13, tpch::Q13)
TPCH_TEST(16, tpch::Q16)
TPCH_TEST(17, tpch::Q17)
TPCH_TEST(18, tpch::Q18)
TPCH_TEST(20, tpch::Q20)
TPCH_TEST(21, tpch::Q21)
TPCH_TEST(22, tpch::Q22)
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
#else
// SF1 (full benchmark): all 22 queries exercised.
TPCH_TEST(1,  tpch::Q1)
TPCH_TEST(2,  tpch::Q2)
TPCH_TEST(3,  tpch::Q3)
TPCH_TEST(4,  tpch::Q4)
TPCH_TEST(5,  tpch::Q5)
TPCH_TEST(6,  tpch::Q6)
TPCH_TEST(7,  tpch::Q7)
TPCH_TEST(8,  tpch::Q8)
TPCH_TEST(9,  tpch::Q9)
TPCH_TEST(10, tpch::Q10)
TPCH_TEST(11, tpch::Q11)
TPCH_TEST(12, tpch::Q12)
TPCH_TEST(13, tpch::Q13)
TPCH_TEST(14, tpch::Q14)
TPCH_TEST(15, tpch::Q15)
TPCH_TEST(16, tpch::Q16)
TPCH_TEST(17, tpch::Q17)
TPCH_TEST(18, tpch::Q18)
TPCH_TEST(19, tpch::Q19)
TPCH_TEST(20, tpch::Q20)
TPCH_TEST(21, tpch::Q21)
TPCH_TEST(22, tpch::Q22)
#endif
