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

// Issue #106 — broader func-over-agg coverage. Each of these has at
// least one projection element where containsAgg=true (the aggregate
// is nested inside a scalar expression at RETURN/WITH level), so they
// hit the extractAggs path. Pre-fix all six leaked the synthetic
// `_agg_N` intermediates as extra output columns; the fix makes the
// emitted column count match the user's RETURN list. Oracles cross-
// checked against DuckDB v1.5.2 (`CALL dbgen(sf=0.01)` on the same
// LINEITEM table).
TEST_CASE("Issue #106 — passthrough col + nested agg/agg ratio",
          "[tpch][issue106][i106-1]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (l:LINEITEM) WHERE l.L_RETURNFLAG = \"R\" "
        "RETURN l.L_LINESTATUS AS status, "
        "sum(l.L_QUANTITY) * 1.0 / count(*) AS ratio "
        "ORDER BY status",
        {qtest::ColType::STRING, qtest::ColType::AUTO});
    REQUIRE(r.size() == 1);
    CHECK(r[0].cols.size() == 2);
    CHECK(r[0].str_at(0) == "F");
    CHECK(r[0].str_at(1) == "25.597168");
}

TEST_CASE("Issue #106 — nested agg/agg ratio + passthrough col (reverse order)",
          "[tpch][issue106][i106-2]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (l:LINEITEM) WHERE l.L_RETURNFLAG = \"R\" "
        "RETURN sum(l.L_QUANTITY) * 1.0 / count(*) AS ratio, "
        "l.L_LINESTATUS AS status "
        "ORDER BY status",
        {qtest::ColType::AUTO, qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].cols.size() == 2);
    CHECK(r[0].str_at(0) == "25.597168");
    CHECK(r[0].str_at(1) == "F");
}

TEST_CASE("Issue #106 — passthrough cols sandwiching nested agg expr",
          "[tpch][issue106][i106-3]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (l:LINEITEM) "
        "RETURN l.L_RETURNFLAG AS rf, "
        "sum(l.L_EXTENDEDPRICE) * 1.0 / count(*) AS avg_price, "
        "l.L_LINESTATUS AS ls "
        "ORDER BY rf, ls",
        {qtest::ColType::STRING, qtest::ColType::AUTO, qtest::ColType::STRING});
    struct Row { const char* rf; const char* avg_price; const char* ls; };
    constexpr Row expected[] = {
        {"A", "35785.709307", "F"},
        {"N", "35588.509684", "F"},
        {"N", "35703.760594", "O"},
        {"R", "35874.006533", "F"},
    };
    REQUIRE(r.size() == 4);
    for (size_t i = 0; i < 4; ++i) {
        INFO("row " << i);
        CHECK(r[i].cols.size() == 3);
        CHECK(r[i].str_at(0) == expected[i].rf);
        CHECK(r[i].str_at(1) == expected[i].avg_price);
        CHECK(r[i].str_at(2) == expected[i].ls);
    }
}

TEST_CASE("Issue #106 — passthrough col + two distinct nested agg exprs",
          "[tpch][issue106][i106-4]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (l:LINEITEM) "
        "RETURN l.L_LINESTATUS AS status, "
        "sum(l.L_QUANTITY) * 1.0 / count(*) AS avg_qty, "
        "sum(l.L_EXTENDEDPRICE) * 1.0 / count(*) AS avg_price "
        "ORDER BY status",
        {qtest::ColType::STRING, qtest::ColType::AUTO, qtest::ColType::AUTO});
    struct Row { const char* status; const char* avg_qty; const char* avg_price; };
    constexpr Row expected[] = {
        {"F", "25.588395", "35827.108092"},
        {"O", "25.466771", "35703.760594"},
    };
    REQUIRE(r.size() == 2);
    for (size_t i = 0; i < 2; ++i) {
        INFO("row " << i);
        CHECK(r[i].cols.size() == 3);
        CHECK(r[i].str_at(0) == expected[i].status);
        CHECK(r[i].str_at(1) == expected[i].avg_qty);
        CHECK(r[i].str_at(2) == expected[i].avg_price);
    }
}

TEST_CASE("Issue #106 — three aggregates inside one scalar expression",
          "[tpch][issue106][i106-5]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (l:LINEITEM) "
        "RETURN l.L_LINESTATUS AS status, "
        "(sum(l.L_QUANTITY) + sum(l.L_EXTENDEDPRICE)) * 1.0 / sum(l.L_QUANTITY) "
        "AS combined "
        "ORDER BY status",
        {qtest::ColType::STRING, qtest::ColType::AUTO});
    struct Row { const char* status; const char* combined; };
    constexpr Row expected[] = {
        {"F", "1401.131095"},
        {"O", "1402.974388"},
    };
    REQUIRE(r.size() == 2);
    for (size_t i = 0; i < 2; ++i) {
        INFO("row " << i);
        CHECK(r[i].cols.size() == 2);
        CHECK(r[i].str_at(0) == expected[i].status);
        CHECK(r[i].str_at(1) == expected[i].combined);
    }
}

TEST_CASE("Issue #106 — leading constant scaling a nested agg ratio",
          "[tpch][issue106][i106-6]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (l:LINEITEM) "
        "RETURN l.L_LINESTATUS AS status, "
        "100.0 * sum(l.L_QUANTITY) / sum(l.L_EXTENDEDPRICE) AS pct "
        "ORDER BY status",
        {qtest::ColType::STRING, qtest::ColType::AUTO});
    struct Row { const char* status; const char* pct; };
    constexpr Row expected[] = {
        {"F", "0.071422"},
        {"O", "0.071328"},
    };
    REQUIRE(r.size() == 2);
    for (size_t i = 0; i < 2; ++i) {
        INFO("row " << i);
        CHECK(r[i].cols.size() == 2);
        CHECK(r[i].str_at(0) == expected[i].status);
        CHECK(r[i].str_at(1) == expected[i].pct);
    }
}

// ----------------------------------------------------------------------
// Issue #106 — non-sum aggregates (avg / min / max / count(distinct))
// follow the same extractAggs path; verify the fix is agg-agnostic.
// Pre-fix every one of these leaked the synthetic `_agg_N` columns.
// ----------------------------------------------------------------------

TEST_CASE("Issue #106 — passthrough col + nested avg expression",
          "[tpch][issue106][i106-7]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (l:LINEITEM) RETURN l.L_LINESTATUS AS status, "
        "avg(l.L_QUANTITY) * 2 AS double_avg ORDER BY status",
        {qtest::ColType::STRING, qtest::ColType::AUTO});
    struct Row { const char* status; const char* double_avg; };
    constexpr Row expected[] = {{"F", "51.176791"}, {"O", "50.933542"}};
    REQUIRE(r.size() == 2);
    for (size_t i = 0; i < 2; ++i) {
        INFO("row " << i);
        CHECK(r[i].cols.size() == 2);
        CHECK(r[i].str_at(0) == expected[i].status);
        CHECK(r[i].str_at(1) == expected[i].double_avg);
    }
}

TEST_CASE("Issue #106 — max minus min in one expression",
          "[tpch][issue106][i106-8]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (l:LINEITEM) RETURN l.L_LINESTATUS AS status, "
        "max(l.L_QUANTITY) - min(l.L_QUANTITY) AS spread ORDER BY status",
        {qtest::ColType::STRING, qtest::ColType::AUTO});
    REQUIRE(r.size() == 2);
    CHECK(r[0].cols.size() == 2);
    CHECK(r[0].str_at(0) == "F"); CHECK(r[0].str_at(1) == "49.00");
    CHECK(r[1].cols.size() == 2);
    CHECK(r[1].str_at(0) == "O"); CHECK(r[1].str_at(1) == "49.00");
}

TEST_CASE("Issue #106 — count(DISTINCT) inside scalar expression",
          "[tpch][issue106][i106-9]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (l:LINEITEM) RETURN l.L_LINESTATUS AS status, "
        "count(DISTINCT l.L_PARTKEY) * 2 AS dpk_doubled ORDER BY status",
        {qtest::ColType::STRING, qtest::ColType::INT64});
    REQUIRE(r.size() == 2);
    CHECK(r[0].cols.size() == 2);
    CHECK(r[0].str_at(0) == "F"); CHECK(r[0].int64_at(1) == 4000);
    CHECK(r[1].cols.size() == 2);
    CHECK(r[1].str_at(0) == "O"); CHECK(r[1].int64_at(1) == 4000);
}

TEST_CASE("Issue #106 — three heterogeneous aggs in one expression "
          "(max - min) / avg",
          "[tpch][issue106][i106-10]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (l:LINEITEM) RETURN l.L_LINESTATUS AS status, "
        "(max(l.L_QUANTITY) - min(l.L_QUANTITY)) * 1.0 / avg(l.L_QUANTITY) "
        "AS norm_range ORDER BY status",
        {qtest::ColType::STRING, qtest::ColType::AUTO});
    struct Row { const char* status; const char* norm_range; };
    constexpr Row expected[] = {{"F", "1.914931"}, {"O", "1.924076"}};
    REQUIRE(r.size() == 2);
    for (size_t i = 0; i < 2; ++i) {
        INFO("row " << i);
        CHECK(r[i].cols.size() == 2);
        CHECK(r[i].str_at(0) == expected[i].status);
        CHECK(r[i].str_at(1) == expected[i].norm_range);
    }
}

TEST_CASE("Issue #106 — avg / avg ratio across two columns",
          "[tpch][issue106][i106-11]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (l:LINEITEM) RETURN l.L_RETURNFLAG AS rf, "
        "avg(l.L_QUANTITY) / avg(l.L_EXTENDEDPRICE) AS qpp ORDER BY rf",
        {qtest::ColType::STRING, qtest::ColType::AUTO});
    struct Row { const char* rf; const char* qpp; };
    constexpr Row expected[] = {
        {"A", "0.000715"}, {"N", "0.000713"}, {"R", "0.000714"},
    };
    REQUIRE(r.size() == 3);
    for (size_t i = 0; i < 3; ++i) {
        INFO("row " << i);
        CHECK(r[i].cols.size() == 2);
        CHECK(r[i].str_at(0) == expected[i].rf);
        CHECK(r[i].str_at(1) == expected[i].qpp);
    }
}

// ----------------------------------------------------------------------
// Issue #106 — harder shapes: deeper nesting / many aggs / mixing.
// ----------------------------------------------------------------------

TEST_CASE("Issue #106 — four mixed aggs in nested arithmetic "
          "(sum + max) * (count - min)",
          "[tpch][issue106][i106-12]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (l:LINEITEM) RETURN l.L_LINESTATUS AS status, "
        "(sum(l.L_QUANTITY) + max(l.L_QUANTITY)) * "
        "(count(*) - min(l.L_QUANTITY)) AS v ORDER BY status",
        {qtest::ColType::STRING, qtest::ColType::AUTO});
    struct Row { const char* status; const char* v; };
    constexpr Row expected[] = {
        {"F", "23224145750.0000"}, {"O", "22995764448.0000"},
    };
    REQUIRE(r.size() == 2);
    for (size_t i = 0; i < 2; ++i) {
        INFO("row " << i);
        CHECK(r[i].cols.size() == 2);
        CHECK(r[i].str_at(0) == expected[i].status);
        CHECK(r[i].str_at(1) == expected[i].v);
    }
}

TEST_CASE("Issue #106 — aggregate result fed into scalar function abs(...)",
          "[tpch][issue106][i106-13]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (l:LINEITEM) RETURN l.L_LINESTATUS AS status, "
        "abs(sum(l.L_EXTENDEDPRICE) - sum(l.L_QUANTITY) * 1000.0) AS v "
        "ORDER BY status",
        {qtest::ColType::STRING, qtest::ColType::AUTO});
    struct Row { const char* status; const char* v; };
    constexpr Row expected[] = {
        {"F", "308451458.370000"}, {"O", "307611302.100000"},
    };
    REQUIRE(r.size() == 2);
    for (size_t i = 0; i < 2; ++i) {
        INFO("row " << i);
        CHECK(r[i].cols.size() == 2);
        CHECK(r[i].str_at(0) == expected[i].status);
        CHECK(r[i].str_at(1) == expected[i].v);
    }
}

TEST_CASE("Issue #106 — two passthroughs + three nested-agg expressions "
          "(GROUP BY rf, ls; sum/count, max-min, avg*100)",
          "[tpch][issue106][i106-14]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (l:LINEITEM) RETURN l.L_RETURNFLAG AS rf, l.L_LINESTATUS AS ls, "
        "sum(l.L_QUANTITY) * 1.0 / count(*) AS r1, "
        "max(l.L_QUANTITY) - min(l.L_QUANTITY) AS r2, "
        "avg(l.L_EXTENDEDPRICE) * 100 AS r3 "
        "ORDER BY rf, ls",
        {qtest::ColType::STRING, qtest::ColType::STRING,
         qtest::ColType::AUTO, qtest::ColType::AUTO, qtest::ColType::AUTO});
    struct Row { const char* rf; const char* ls; const char* r1; const char* r2; const char* r3; };
    constexpr Row expected[] = {
        {"A", "F", "25.575155", "49.00", "3578570.930694"},
        {"N", "F", "25.778736", "49.00", "3558850.968391"},
        {"N", "O", "25.466771", "49.00", "3570376.059436"},
        {"R", "F", "25.597168", "49.00", "3587400.653268"},
    };
    REQUIRE(r.size() == 4);
    for (size_t i = 0; i < 4; ++i) {
        INFO("row " << i);
        CHECK(r[i].cols.size() == 5);
        CHECK(r[i].str_at(0) == expected[i].rf);
        CHECK(r[i].str_at(1) == expected[i].ls);
        CHECK(r[i].str_at(2) == expected[i].r1);
        CHECK(r[i].str_at(3) == expected[i].r2);
        CHECK(r[i].str_at(4) == expected[i].r3);
    }
}

TEST_CASE("Issue #106 — depth-3 nested arithmetic with four aggs "
          "((sum-min)*2 + max) / (count + 1)",
          "[tpch][issue106][i106-15]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (l:LINEITEM) RETURN l.L_LINESTATUS AS status, "
        "((sum(l.L_QUANTITY) - min(l.L_QUANTITY)) * 2 + max(l.L_QUANTITY)) "
        "* 1.0 / (count(*) + 1) AS v ORDER BY status",
        {qtest::ColType::STRING, qtest::ColType::AUTO});
    struct Row { const char* status; const char* v; };
    constexpr Row expected[] = {{"F", "51.176685"}, {"O", "50.933444"}};
    REQUIRE(r.size() == 2);
    for (size_t i = 0; i < 2; ++i) {
        INFO("row " << i);
        CHECK(r[i].cols.size() == 2);
        CHECK(r[i].str_at(0) == expected[i].status);
        CHECK(r[i].str_at(1) == expected[i].v);
    }
}

TEST_CASE("Issue #106 — count(DISTINCT)/count(*) selectivity ratio",
          "[tpch][issue106][i106-16]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (l:LINEITEM) RETURN l.L_LINESTATUS AS status, "
        "count(DISTINCT l.L_PARTKEY) * 1.0 / count(*) AS selectivity "
        "ORDER BY status",
        {qtest::ColType::STRING, qtest::ColType::AUTO});
    struct Row { const char* status; const char* selectivity; };
    constexpr Row expected[] = {{"F", "0.066388"}, {"O", "0.066558"}};
    REQUIRE(r.size() == 2);
    for (size_t i = 0; i < 2; ++i) {
        INFO("row " << i);
        CHECK(r[i].cols.size() == 2);
        CHECK(r[i].str_at(0) == expected[i].status);
        CHECK(r[i].str_at(1) == expected[i].selectivity);
    }
}

TEST_CASE("Issue #106 — five aggs (max,avg,count(DISTINCT),sum,count) "
          "in depth-3 mixed expression with abs(...)",
          "[tpch][issue106][i106-17]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (l:LINEITEM) RETURN l.L_LINESTATUS AS status, "
        "abs((max(l.L_QUANTITY) - avg(l.L_QUANTITY)) * 1.0 / "
        "count(DISTINCT l.L_PARTKEY)) - sum(l.L_QUANTITY) * 1.0 / count(*) "
        "AS v ORDER BY status",
        {qtest::ColType::STRING, qtest::ColType::AUTO});
    struct Row { const char* status; const char* v; };
    constexpr Row expected[] = {{"F", "-25.576190"}, {"O", "-25.454504"}};
    REQUIRE(r.size() == 2);
    for (size_t i = 0; i < 2; ++i) {
        INFO("row " << i);
        CHECK(r[i].cols.size() == 2);
        CHECK(r[i].str_at(0) == expected[i].status);
        CHECK(r[i].str_at(1) == expected[i].v);
    }
}

TEST_CASE("Issue #106 — five aggs (max,min,count(DISTINCT),sum,count) "
          "via two parenthesized subexpressions",
          "[tpch][issue106][i106-18]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (l:LINEITEM) RETURN l.L_LINESTATUS AS status, "
        "((max(l.L_QUANTITY) + min(l.L_QUANTITY)) * count(DISTINCT l.L_PARTKEY)) "
        "- (sum(l.L_QUANTITY) + count(*)) AS v ORDER BY status",
        {qtest::ColType::STRING, qtest::ColType::AUTO});
    struct Row { const char* status; const char* v; };
    constexpr Row expected[] = {{"F", "-699002.00"}, {"O", "-693300.00"}};
    REQUIRE(r.size() == 2);
    for (size_t i = 0; i < 2; ++i) {
        INFO("row " << i);
        CHECK(r[i].cols.size() == 2);
        CHECK(r[i].str_at(0) == expected[i].status);
        CHECK(r[i].str_at(1) == expected[i].v);
    }
}

// ----------------------------------------------------------------------
// Issue #111 — global aggregates (0 grouping keys) on TPC-H tables.
// Bug 1 (count(*) / sum(1) / count(1) — ORCA Normalizer crash from a
// NULL relational child) and Bug 2 (Sink SIGSEGV for sum/min/max/avg
// on Linux x86_64). The cases below re-run the verbatim issue
// reproducers, with oracles cross-checked against DuckDB v1.5.2 over
// the same .tbl files.
// ----------------------------------------------------------------------

TEST_CASE("Issue #111 — global sum / min / max / avg over LINEITEM.L_QUANTITY",
          "[tpch][issue111][global-agg]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (l:LINEITEM) RETURN sum(l.L_QUANTITY) AS s, "
        "min(l.L_QUANTITY) AS mn, max(l.L_QUANTITY) AS mx, "
        "avg(l.L_QUANTITY) AS a",
        {qtest::ColType::AUTO, qtest::ColType::AUTO,
         qtest::ColType::AUTO, qtest::ColType::AUTO});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == "1536127.00");
    CHECK(r[0].str_at(1) == "1.00");
    CHECK(r[0].str_at(2) == "50.00");
    CHECK(r[0].str_at(3) == "25.527661");
}

TEST_CASE("Issue #111 — global sum over LINEITEM bare-INT property",
          "[tpch][issue111][global-agg]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (l:LINEITEM) RETURN sum(l.L_LINENUMBER) AS s, "
        "sum(l.L_EXTENDEDPRICE) AS p, count(l.L_QUANTITY) AS c",
        {qtest::ColType::INT64, qtest::ColType::AUTO, qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 180782);
    CHECK(r[0].str_at(1) == "2152189760.47");
    CHECK(r[0].int64_at(2) == 60175);
}

TEST_CASE("Issue #111 — global aggregates over NATION",
          "[tpch][issue111][global-agg]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (n:NATION) RETURN count(n) AS c, sum(n.N_NATIONKEY) AS s, "
        "min(n.N_NAME) AS mn, max(n.N_NAME) AS mx",
        {qtest::ColType::INT64, qtest::ColType::INT64,
         qtest::ColType::STRING, qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 25);
    CHECK(r[0].int64_at(1) == 300);
    CHECK(r[0].str_at(2) == "ALGERIA");
    CHECK(r[0].str_at(3) == "VIETNAM");
}

TEST_CASE("Issue #111 — global count(*) / sum(1) / count(1) "
          "(no input columns referenced)",
          "[tpch][issue111][global-agg]") {
    SKIP_IF_NO_DB();
    // NATION (small) and LINEITEM (large multi-partition) both exercise
    // the ProjectColumnar-pruned-to-zero path that pre-fix returned a
    // NULL relational child to the parent GbAgg.
    auto r1 = qr->run(
        "MATCH (n:NATION) RETURN count(*) AS cs, sum(1) AS s1, "
        "count(1) AS c1",
        {qtest::ColType::INT64, qtest::ColType::INT64, qtest::ColType::INT64});
    REQUIRE(r1.size() == 1);
    CHECK(r1[0].int64_at(0) == 25);
    CHECK(r1[0].int64_at(1) == 25);
    CHECK(r1[0].int64_at(2) == 25);

    auto r2 = qr->run(
        "MATCH (l:LINEITEM) RETURN count(*) AS cs, sum(1) AS s1",
        {qtest::ColType::INT64, qtest::ColType::INT64});
    REQUIRE(r2.size() == 1);
    CHECK(r2[0].int64_at(0) == 60175);
    CHECK(r2[0].int64_at(1) == 60175);
}

// Range cmp on UBIGINT :ID columns. Pre-fix `<` / `<=` returned 0
// because the planner derived range bounds from the literal's INT type,
// and INT_MIN reinterpreted in the UBIGINT domain wiped the entire
// match set.
TEST_CASE("Range cmp on UBIGINT :ID column",
          "[tpch][cmp][cmp-int]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (n:NATION) WHERE n.N_NATIONKEY <= 5 RETURN count(n) AS v",
        {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 6);

    r = qr->run(
        "MATCH (n:NATION) WHERE n.N_NATIONKEY < 5 RETURN count(n) AS v",
        {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 5);

    r = qr->run(
        "MATCH (n:NATION) WHERE n.N_NATIONKEY > 5 RETURN count(n) AS v",
        {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 19);

    // LINEITEM is multi-partition over UBIGINT L_ORDERKEY — exercises
    // the multi-partition NodeScan range-filter path.
    r = qr->run(
        "MATCH (l:LINEITEM) WHERE l.L_ORDERKEY <= 5 RETURN count(l) AS v",
        {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 17);
}

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
// Previously SIGSEGV'd on the SF0.01 fixture (#69) — all 11 unblocked
// by the converter type-resolution fix; now exercised as count-only
// smoke tests. Future PRs can promote individual queries to row-by-row
// value tests against the DuckDB oracle (already done for Q8 / Q12 /
// Q13 / Q16-Q22).
TPCH_TEST(1,  tpch::Q1)
TPCH_TEST(3,  tpch::Q3)
TPCH_TEST(5,  tpch::Q5)
TPCH_TEST(6,  tpch::Q6)
TPCH_TEST(7,  tpch::Q7)
TPCH_TEST(9,  tpch::Q9)
TPCH_TEST(10, tpch::Q10)
TPCH_TEST(11, tpch::Q11)
TPCH_TEST(15, tpch::Q15)
TPCH_TEST(19, tpch::Q19)

// Q14 — value test: 100*SUM(CASE...) / SUM(...) PROMO revenue.
// Pre-fix this returned 1734.31 (100x) because CScalarIf had no
// type_modifier so DECIMAL(15,4) result fell back to DECIMAL(12,2).
TEST_CASE("TPC-H Q14 (rows)", "[tpch][q14]") {
    SKIP_IF_NO_DB();
    std::string query = readFile(QUERY_DIR + "q14.cql");
    REQUIRE(!query.empty());
    auto r = qr->run(query.c_str(), {qtest::ColType::AUTO});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == tpch::Q14_PROMO_REVENUE_STR);
}

// ----------------------------------------------------------------------
// Issue (CScalarIf type_modifier) — exercise CASE-inside-SUM with a
// variety of THEN/ELSE type combinations beyond DECIMAL(12,2). All
// values cross-checked against DuckDB v1.5.2 dbgen(sf=0.01).
// ----------------------------------------------------------------------

TEST_CASE("CASE-in-SUM — INTEGER literal branches (1/0)",
          "[tpch][issue110][i110-1]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (l:LINEITEM) RETURN sum(CASE WHEN l.L_RETURNFLAG = \"R\" "
        "THEN 1 ELSE 0 END) AS v",
        {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 14902);
}

TEST_CASE("CASE-in-SUM — DECIMAL THEN with wider scale "
          "(L_EXTENDEDPRICE * L_QUANTITY)",
          "[tpch][issue110][i110-2]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (l:LINEITEM) RETURN sum(CASE WHEN l.L_RETURNFLAG = \"R\" "
        "THEN l.L_EXTENDEDPRICE * l.L_QUANTITY ELSE 0 END) AS v",
        {qtest::ColType::AUTO});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == "17993342580.7300");
}

TEST_CASE("CASE-in-SUM — DOUBLE branch (tofloat / 2)",
          "[tpch][issue110][i110-3]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (l:LINEITEM) RETURN sum(CASE WHEN l.L_RETURNFLAG = \"R\" "
        "THEN tofloat(l.L_QUANTITY) / 2 ELSE 0 END) AS v",
        {qtest::ColType::AUTO});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == "190724.500000");
}

TEST_CASE("CASE-in-SUM — DATE-predicate WHEN, DECIMAL THEN",
          "[tpch][issue110][i110-4]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (l:LINEITEM) RETURN sum(CASE WHEN l.L_SHIPDATE >= "
        "date(\"1997-01-01\") THEN l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT) "
        "ELSE 0 END) AS v",
        {qtest::ColType::AUTO});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == "541382560.7292");
}

TEST_CASE("CASE-in-SUM — DECIMAL THEN with non-zero DECIMAL ELSE",
          "[tpch][issue110][i110-5]") {
    SKIP_IF_NO_DB();
    auto r = qr->run(
        "MATCH (l:LINEITEM) RETURN sum(CASE WHEN l.L_RETURNFLAG = \"R\" "
        "THEN l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT) ELSE 0.5 END) AS v",
        {qtest::ColType::AUTO});
    REQUIRE(r.size() == 1);
    // DuckDB oracle 508019090.9067; QueryRunner formats DOUBLE/DECIMAL
    // with 6 decimal digits, so trailing 99 reflects rounding of the
    // DECIMAL(.,6)-ish output.
    CHECK(r[0].str_at(0) == "508019090.906699");
}

TEST_CASE("CASE-in-SUM-over-SUM ratio — DECIMAL Q14-shape (sanity)",
          "[tpch][issue110][i110-6]") {
    SKIP_IF_NO_DB();
    // Same shape as Q14 but with a different selectivity (R-flag) so
    // a regression here is independent of Q14's specific date filter.
    auto r = qr->run(
        "MATCH (l:LINEITEM) RETURN 100 * sum(CASE WHEN l.L_RETURNFLAG = \"R\" "
        "THEN l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT) ELSE 0 END) "
        "/ sum(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS v",
        {qtest::ColType::AUTO});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == "24.839263");
}
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
