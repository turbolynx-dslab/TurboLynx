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
// Mini fixture (SF0.01) loads from a parallel `benchmark/tpch/sf0.01/`
// directory because some queries need scale-tuned parameters that the
// SF1 versions can't satisfy on the smaller fixture (e.g. Q18's
// `sum_lquantity > 315` filters all mini rows since max sum is 305 →
// pinned to `> 270` in sf0.01/q18.cql so the test exercises the
// actual aggregate path). The two directories follow the same per-
// scale convention sf1/ / sf10/ / sf100/ already use.
#ifdef TURBOLYNX_TPCH_FIXTURE_MINI
static const std::string QUERY_DIR =
    std::string(TURBOLYNX_REPO_ROOT) + "/benchmark/tpch/sf0.01/";
#else
static const std::string QUERY_DIR =
    std::string(TURBOLYNX_REPO_ROOT) + "/benchmark/tpch/sf1/";
#endif

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

// Q8 — ETHIOPIA market-share within AFRICA, by ship year. Two rows
// (1995 / 1996). Values verified against DuckDB SF0.01.
TEST_CASE("TPC-H Q8 (rows)", "[tpch][q8]") {
    SKIP_IF_NO_DB();
    std::string query = readFile(QUERY_DIR + "q8.cql");
    REQUIRE(!query.empty());
    auto r = qr->run(query.c_str(),
        {qtest::ColType::INT64, qtest::ColType::AUTO});
    constexpr size_t N = sizeof(tpch::Q8_ROWS) / sizeof(tpch::Q8_ROWS[0]);
    REQUIRE(r.size() == N);
    for (size_t i = 0; i < N; ++i) {
        INFO("row " << i);
        const auto& exp = tpch::Q8_ROWS[i];
        CHECK(r[i].int64_at(0) == exp.o_year);
        CHECK(r[i].str_at(1)   == exp.mkt_share);
    }
}

// Q16 — 315-row supplier-cardinality breakdown. Pinning every row would
// be 1260+ assertions of low marginal value (311 of 315 rows have
// supplier_cnt=4 by design). Instead pin top-4 (the only cnt=8 rows),
// the first / last cnt=4 rows, and verify the full result respects
// the ORDER BY (supplier_cnt DESC, p_brand ASC, p_type ASC, p_size ASC).
TEST_CASE("TPC-H Q16 (rows)", "[tpch][q16]") {
    SKIP_IF_NO_DB();
    std::string query = readFile(QUERY_DIR + "q16.cql");
    REQUIRE(!query.empty());
    auto r = qr->run(query.c_str(),
        {qtest::ColType::STRING, qtest::ColType::STRING,
         qtest::ColType::INT64,  qtest::ColType::INT64});
    REQUIRE(r.size() == (size_t)tpch::Q16_NUM_ROWS);

    constexpr size_t TOP_N = sizeof(tpch::Q16_TOP4_ROWS) / sizeof(tpch::Q16_TOP4_ROWS[0]);
    for (size_t i = 0; i < TOP_N; ++i) {
        INFO("top row " << i);
        const auto& exp = tpch::Q16_TOP4_ROWS[i];
        CHECK(r[i].str_at(0)   == exp.p_brand);
        CHECK(r[i].str_at(1)   == exp.p_type);
        CHECK(r[i].int64_at(2) == exp.p_size);
        CHECK(r[i].int64_at(3) == exp.supplier_cnt);
    }

    // First cnt=4 tail row (index TOP_N) and last row.
    {
        const auto& exp = tpch::Q16_FIRST_TAIL_ROW;
        CHECK(r[TOP_N].str_at(0)   == exp.p_brand);
        CHECK(r[TOP_N].str_at(1)   == exp.p_type);
        CHECK(r[TOP_N].int64_at(2) == exp.p_size);
        CHECK(r[TOP_N].int64_at(3) == exp.supplier_cnt);
    }
    {
        const auto& exp = tpch::Q16_LAST_ROW;
        CHECK(r.size() > 0);
        CHECK(r[r.size()-1].str_at(0)   == exp.p_brand);
        CHECK(r[r.size()-1].str_at(1)   == exp.p_type);
        CHECK(r[r.size()-1].int64_at(2) == exp.p_size);
        CHECK(r[r.size()-1].int64_at(3) == exp.supplier_cnt);
    }

    // Ordering invariant: supplier_cnt DESC, then p_brand / p_type / p_size ASC.
    for (size_t i = 1; i < r.size(); ++i) {
        if (r[i].int64_at(3) == r[i-1].int64_at(3)) {
            // Same supplier_cnt → strict order on (p_brand, p_type, p_size).
            std::string b0 = r[i-1].str_at(0), b1 = r[i].str_at(0);
            if (b0 == b1) {
                std::string t0 = r[i-1].str_at(1), t1 = r[i].str_at(1);
                if (t0 == t1) {
                    CHECK(r[i].int64_at(2) >= r[i-1].int64_at(2));
                } else {
                    CHECK(t1 >= t0);
                }
            } else {
                CHECK(b1 >= b0);
            }
        } else {
            CHECK(r[i].int64_at(3) < r[i-1].int64_at(3));
        }
    }
}

// Q18 — large-order customer detail. Mini uses `sum_lquantity > 270`
// (sf0.01/q18.cql); SF1 uses `> 315`. 12 rows ordered by
// o_totalprice DESC, o_orderdate ASC.
TEST_CASE("TPC-H Q18 (rows)", "[tpch][q18]") {
    SKIP_IF_NO_DB();
    std::string query = readFile(QUERY_DIR + "q18.cql");
    REQUIRE(!query.empty());
    auto r = qr->run(query.c_str(),
        {qtest::ColType::STRING, qtest::ColType::INT64, qtest::ColType::INT64,
         qtest::ColType::INT64,  qtest::ColType::AUTO,  qtest::ColType::AUTO});
    constexpr size_t N = sizeof(tpch::Q18_ROWS) / sizeof(tpch::Q18_ROWS[0]);
    REQUIRE(r.size() == N);
    for (size_t i = 0; i < N; ++i) {
        INFO("row " << i);
        const auto& exp = tpch::Q18_ROWS[i];
        CHECK(r[i].str_at(0)   == exp.c_name);
        CHECK(r[i].int64_at(1) == exp.c_custkey);
        CHECK(r[i].int64_at(2) == exp.o_orderkey);
        CHECK(r[i].int64_at(3) == exp.o_orderdate_ms);
        CHECK(r[i].str_at(4)   == exp.o_totalprice);
        CHECK(r[i].str_at(5)   == exp.sum_l_quantity);
    }
}

// Q13 — 32-bucket customer order-count histogram. The OPTIONAL MATCH
// makes the c_count=0 / custdist=500 row (= customers with no orders)
// the largest bucket. Ordered by custdist DESC, c_count DESC.
TEST_CASE("TPC-H Q13 (rows)", "[tpch][q13]") {
    SKIP_IF_NO_DB();
    std::string query = readFile(QUERY_DIR + "q13.cql");
    REQUIRE(!query.empty());
    auto r = qr->run(query.c_str(),
        {qtest::ColType::INT64, qtest::ColType::INT64});
    constexpr size_t N = sizeof(tpch::Q13_ROWS) / sizeof(tpch::Q13_ROWS[0]);
    REQUIRE(r.size() == N);
    for (size_t i = 0; i < N; ++i) {
        INFO("row " << i);
        const auto& exp = tpch::Q13_ROWS[i];
        CHECK(r[i].int64_at(0) == exp.c_count);
        CHECK(r[i].int64_at(1) == exp.custdist);
    }
    // Verify ordering: custdist DESC, c_count DESC.
    for (size_t i = 1; i < r.size(); ++i) {
        if (r[i].int64_at(1) == r[i-1].int64_at(1)) {
            CHECK(r[i].int64_at(0) <= r[i-1].int64_at(0));
        } else {
            CHECK(r[i].int64_at(1) <  r[i-1].int64_at(1));
        }
    }
}

// Q2 — 5-row top-K supplier listing with 8 columns each.
TEST_CASE("TPC-H Q2 (rows)", "[tpch][q2]") {
    SKIP_IF_NO_DB();
    std::string query = readFile(QUERY_DIR + "q2.cql");
    REQUIRE(!query.empty());
    auto r = qr->run(query.c_str(),
        {qtest::ColType::AUTO,   qtest::ColType::STRING, qtest::ColType::STRING,
         qtest::ColType::INT64,  qtest::ColType::STRING, qtest::ColType::STRING,
         qtest::ColType::STRING, qtest::ColType::STRING});
    constexpr size_t N = sizeof(tpch::Q2_ROWS) / sizeof(tpch::Q2_ROWS[0]);
    REQUIRE(r.size() == N);
    for (size_t i = 0; i < N; ++i) {
        INFO("row " << i);
        const auto& exp = tpch::Q2_ROWS[i];
        CHECK(r[i].str_at(0)   == exp.s_acctbal);
        CHECK(r[i].str_at(1)   == exp.s_name);
        CHECK(r[i].str_at(2)   == exp.n_name);
        CHECK(r[i].int64_at(3) == exp.p_partkey);
        CHECK(r[i].str_at(4)   == exp.p_mfgr);
        CHECK(r[i].str_at(5)   == exp.s_address);
        CHECK(r[i].str_at(6)   == exp.s_phone);
        CHECK(r[i].str_at(7).find(exp.s_comment_prefix) == 0);
    }
}

// Q4 — 5-row order priority breakdown.
TEST_CASE("TPC-H Q4 (rows)", "[tpch][q4]") {
    SKIP_IF_NO_DB();
    std::string query = readFile(QUERY_DIR + "q4.cql");
    REQUIRE(!query.empty());
    auto r = qr->run(query.c_str(),
        {qtest::ColType::STRING, qtest::ColType::INT64});
    constexpr size_t N = sizeof(tpch::Q4_ROWS) / sizeof(tpch::Q4_ROWS[0]);
    REQUIRE(r.size() == N);
    for (size_t i = 0; i < N; ++i) {
        INFO("row " << i);
        const auto& exp = tpch::Q4_ROWS[i];
        CHECK(r[i].str_at(0)   == exp.o_orderpriority);
        CHECK(r[i].int64_at(1) == exp.order_count);
    }
}

// Q20 — 2-row BRAZIL supplier list.
TEST_CASE("TPC-H Q20 (rows)", "[tpch][q20]") {
    SKIP_IF_NO_DB();
    std::string query = readFile(QUERY_DIR + "q20.cql");
    REQUIRE(!query.empty());
    auto r = qr->run(query.c_str(),
        {qtest::ColType::STRING, qtest::ColType::STRING});
    constexpr size_t N = sizeof(tpch::Q20_ROWS) / sizeof(tpch::Q20_ROWS[0]);
    REQUIRE(r.size() == N);
    for (size_t i = 0; i < N; ++i) {
        INFO("row " << i);
        const auto& exp = tpch::Q20_ROWS[i];
        CHECK(r[i].str_at(0) == exp.s_name);
        CHECK(r[i].str_at(1) == exp.s_address);
    }
}

// Q12 — 2-row priority breakdown by ship mode.
TEST_CASE("TPC-H Q12 (rows)", "[tpch][q12]") {
    SKIP_IF_NO_DB();
    std::string query = readFile(QUERY_DIR + "q12.cql");
    REQUIRE(!query.empty());
    auto r = qr->run(query.c_str(),
        {qtest::ColType::STRING, qtest::ColType::INT64, qtest::ColType::INT64});
    constexpr size_t N = sizeof(tpch::Q12_ROWS) / sizeof(tpch::Q12_ROWS[0]);
    REQUIRE(r.size() == N);
    for (size_t i = 0; i < N; ++i) {
        INFO("row " << i);
        const auto& exp = tpch::Q12_ROWS[i];
        CHECK(r[i].str_at(0)   == exp.l_shipmode);
        CHECK(r[i].int64_at(1) == exp.high_line_count);
        CHECK(r[i].int64_at(2) == exp.low_line_count);
    }
}

// Q17 — scalar 1-row value check (DuckDB-verified).
TEST_CASE("TPC-H Q17 (rows)", "[tpch][q17]") {
    SKIP_IF_NO_DB();
    std::string query = readFile(QUERY_DIR + "q17.cql");
    REQUIRE(!query.empty());
    auto r = qr->run(query.c_str(), {qtest::ColType::AUTO});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == tpch::Q17_AVG_YEARLY_STR);
}

// Q21 — supplier ranking, 1-row result on mini.
TEST_CASE("TPC-H Q21 (rows)", "[tpch][q21]") {
    SKIP_IF_NO_DB();
    std::string query = readFile(QUERY_DIR + "q21.cql");
    REQUIRE(!query.empty());
    auto r = qr->run(query.c_str(),
        {qtest::ColType::STRING, qtest::ColType::INT64});
    constexpr size_t N = sizeof(tpch::Q21_ROWS) / sizeof(tpch::Q21_ROWS[0]);
    REQUIRE(r.size() == N);
    for (size_t i = 0; i < N; ++i) {
        INFO("row " << i);
        const auto& exp = tpch::Q21_ROWS[i];
        CHECK(r[i].str_at(0)   == exp.s_name);
        CHECK(r[i].int64_at(1) == exp.numwait);
    }
}

// Q22 — strengthened to row-by-row value comparison against DuckDB
// reference (see tpch_expected_counts.hpp Q22_ROWS for the oracle).
// First TPC-H query to move beyond pure row-count checking; sets the
// pattern for the other ten passing-on-mini queries to follow.
TEST_CASE("TPC-H Q22 (rows)", "[tpch][q22]") {
    SKIP_IF_NO_DB();
    std::string query = readFile(QUERY_DIR + "q22.cql");
    REQUIRE(!query.empty());
    auto r = qr->run(query.c_str(),
        {qtest::ColType::STRING, qtest::ColType::INT64, qtest::ColType::AUTO});
    constexpr size_t N = sizeof(tpch::Q22_ROWS) / sizeof(tpch::Q22_ROWS[0]);
    REQUIRE(r.size() == N);
    for (size_t i = 0; i < N; ++i) {
        INFO("row " << i);
        const auto& exp = tpch::Q22_ROWS[i];
        CHECK(r[i].str_at(0)   == exp.cntrycode);
        CHECK(r[i].int64_at(1) == exp.numcust);
        CHECK(r[i].str_at(2)   == exp.totacctbal);
    }
}
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
