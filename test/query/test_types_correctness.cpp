// types-mini fixture: a single :TypeRow label with one column per
// loader-supported scalar type. Tests target type-promotion edges
// (CASE LCT, scalar-arithmetic widening, aggregate output type)
// rather than any benchmark workload.
//
// Bugs in #111 (global aggregate / sum(bare INT)) are intentionally
// avoided here so the suite stays green; the same fixture will host
// regression tests once #111 ships.

#include "catch.hpp"
#include "helpers/query_runner.hpp"
#include <string>

extern std::string g_types_path;
extern bool g_skip_requested;
extern bool g_has_types;
extern qtest::QueryRunner* get_types_runner();

#define SKIP_IF_NO_TYPES_DB() \
    if (g_types_path.empty()) { WARN("--types-path not set, skipping"); g_skip_requested = true; return; } \
    if (!g_has_types)        { WARN("DB has no :TypeRow label, skipping"); return; } \
    auto* qr = get_types_runner(); \
    if (!qr) { FAIL("Cannot open DB: " << g_types_path); return; }

// ---------- baseline: per-type passthrough -----------------------------

// Note: range comparisons on `t.id` (ID-typed primary key) currently
// return zero rows on this fixture; equality works. Tests therefore
// filter on `t.t_int` / `t.t_str` instead, which are regular property
// columns. (This is an orthogonal issue tracked separately.)

TEST_CASE("types: integer columns round-trip", "[types][passthru]") {
    SKIP_IF_NO_TYPES_DB();
    auto r = qr->run(
        "MATCH (t:TypeRow) WHERE t.t_str = \"ten\" "
        "RETURN t.t_int, t.t_long, t.t_ulong",
        {qtest::ColType::INT64, qtest::ColType::INT64, qtest::ColType::UINT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 10);
    CHECK(r[0].int64_at(1) == 10);
    CHECK(r[0].int64_at(2) == 10);
}

TEST_CASE("types: float / double round-trip", "[types][passthru]") {
    SKIP_IF_NO_TYPES_DB();
    auto r = qr->run(
        "MATCH (t:TypeRow) WHERE t.t_str = \"max-ish\" "
        "RETURN t.t_float, t.t_double",
        {qtest::ColType::AUTO, qtest::ColType::AUTO});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == "3.141593");
    CHECK(r[0].str_at(1) == "3.141593");
}

TEST_CASE("types: decimal at three widths/scales", "[types][passthru]") {
    SKIP_IF_NO_TYPES_DB();
    auto r = qr->run(
        "MATCH (t:TypeRow) WHERE t.t_str = \"ten\" "
        "RETURN t.t_dec_small, t.t_dec_default, t.t_dec_wide",
        {qtest::ColType::AUTO, qtest::ColType::AUTO, qtest::ColType::AUTO});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == "10.50");
    CHECK(r[0].str_at(1) == "10.50");
    CHECK(r[0].str_at(2) == "10.5000");
}

TEST_CASE("types: string round-trip", "[types][passthru]") {
    SKIP_IF_NO_TYPES_DB();
    auto r = qr->run(
        "MATCH (t:TypeRow) WHERE t.t_str = \"zero\" RETURN t.t_str",
        {qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == "zero");
}

// ---------- arithmetic widening between types --------------------------

TEST_CASE("types: INT + DOUBLE promotes to DOUBLE", "[types][arith]") {
    SKIP_IF_NO_TYPES_DB();
    auto r = qr->run(
        "MATCH (t:TypeRow) WHERE t.t_str = \"hundred\" "
        "RETURN t.t_int + t.t_double AS v",
        {qtest::ColType::AUTO});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == "200.500000");
}

TEST_CASE("types: INT * DECIMAL preserves DECIMAL scale", "[types][arith]") {
    SKIP_IF_NO_TYPES_DB();
    auto r = qr->run(
        "MATCH (t:TypeRow) WHERE t.t_str = \"ten\" "
        "RETURN t.t_int * t.t_dec_default AS v",
        {qtest::ColType::AUTO});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == "105.00");
}

TEST_CASE("types: DECIMAL(5,2) * DECIMAL(12,2) widens", "[types][arith]") {
    SKIP_IF_NO_TYPES_DB();
    auto r = qr->run(
        "MATCH (t:TypeRow) WHERE t.t_str = \"ten\" "
        "RETURN t.t_dec_small * t.t_dec_default AS v",
        {qtest::ColType::AUTO});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == "110.2500");
}

// ---------- CASE LCT across types --------------------------------------

TEST_CASE("types: CASE INTEGER vs INTEGER returns INTEGER",
          "[types][case]") {
    SKIP_IF_NO_TYPES_DB();
    // category='A' has rows id=1..4 with t_int = 0,1,-1,10. The CASE
    // returns t_int when ≥0 else 0 → 0+1+0+10 = 11.
    auto r = qr->run(
        "MATCH (t:TypeRow) WHERE t.category = \"A\" "
        "RETURN sum(CASE WHEN t.t_int >= 0 THEN t.t_int ELSE 0 END) AS s",
        {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 11);
}

TEST_CASE("types: CASE DECIMAL THEN with INTEGER ELSE returns DECIMAL",
          "[types][case]") {
    SKIP_IF_NO_TYPES_DB();
    // category='A': rows where t_int > 0 are id=2 (1.0000) and id=4
    // (10.5000). Sum = 11.5000. Verifies #110 fix preserves DECIMAL
    // scale in CASE LCT for DECIMAL(18,4) (different width than the
    // DECIMAL(12,2) Q14 used).
    auto r = qr->run(
        "MATCH (t:TypeRow) WHERE t.category = \"A\" "
        "RETURN sum(CASE WHEN t.t_int > 0 THEN t.t_dec_wide ELSE 0 END) AS s",
        {qtest::ColType::AUTO});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == "11.5000");
}

TEST_CASE("types: CASE DOUBLE THEN with INTEGER ELSE returns DOUBLE",
          "[types][case]") {
    SKIP_IF_NO_TYPES_DB();
    // category='A' / id=2 t_double=1.0, id=4 t_double=10.5 → sum 11.5.
    auto r = qr->run(
        "MATCH (t:TypeRow) WHERE t.category = \"A\" "
        "RETURN sum(CASE WHEN t.t_int > 0 THEN t.t_double ELSE 0 END) AS s",
        {qtest::ColType::AUTO});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == "11.500000");
}

// ---------- aggregates with at least one grouping key ------------------
// (Bare-INT bug from #111 reproduces with the global form; these use
//  category as the group key so the aggregate is exercised through the
//  HashAgg-with-grouping path which works.)

TEST_CASE("types: sum DECIMAL by category", "[types][agg]") {
    SKIP_IF_NO_TYPES_DB();
    auto r = qr->run(
        "MATCH (t:TypeRow) RETURN t.category AS c, "
        "sum(t.t_dec_default) AS s ORDER BY c",
        {qtest::ColType::STRING, qtest::ColType::AUTO});
    REQUIRE(r.size() == 5);
    struct Row { const char* c; const char* s; };
    constexpr Row expected[] = {
        {"A", "10.50"},
        {"B", "100.00"},               // 100.00 + 1000.00 + (-1000.00)
        {"C", "1000000015.81"},         // 999999999.99 + 2.50 + 3.33 + 4.44 + 5.55
        {"D", "44.41"},                 // 6.66 + 7.77 + 8.88 + 9.99 + 11.11
        {"E", "40.09"},                 // 12.34 + 13.50 + 14.25
    };
    for (size_t i = 0; i < 5; ++i) {
        INFO("row " << i);
        CHECK(r[i].str_at(0) == expected[i].c);
        CHECK(r[i].str_at(1) == expected[i].s);
    }
}

TEST_CASE("types: avg DOUBLE by category", "[types][agg]") {
    SKIP_IF_NO_TYPES_DB();
    auto r = qr->run(
        "MATCH (t:TypeRow) RETURN t.category AS c, avg(t.t_double) AS a "
        "ORDER BY c",
        {qtest::ColType::STRING, qtest::ColType::AUTO});
    REQUIRE(r.size() == 5);
    CHECK(r[0].str_at(0) == "A");
    CHECK(r[0].str_at(1) == "2.625000");          // mean(0,1,-1,10.5)
    CHECK(r[2].str_at(0) == "C");
    // C = ids 8..12: (3.141593 + 2 + 3 + 4 + 5) / 5 = 3.428319.
    CHECK(r[2].str_at(1) == "3.428319");
}

TEST_CASE("types: count by category", "[types][agg]") {
    SKIP_IF_NO_TYPES_DB();
    auto r = qr->run(
        "MATCH (t:TypeRow) RETURN t.category AS c, count(*) AS n ORDER BY c",
        {qtest::ColType::STRING, qtest::ColType::INT64});
    REQUIRE(r.size() == 5);
    int64_t expected[] = {4, 3, 5, 5, 3};
    for (size_t i = 0; i < 5; ++i) {
        INFO("row " << i);
        CHECK(r[i].int64_at(1) == expected[i]);
    }
}

TEST_CASE("types: min / max DATE by category", "[types][agg]") {
    SKIP_IF_NO_TYPES_DB();
    auto r = qr->run(
        "MATCH (t:TypeRow) RETURN t.category AS c, "
        "min(t.t_date) AS mn, max(t.t_date) AS mx ORDER BY c",
        {qtest::ColType::STRING, qtest::ColType::INT64, qtest::ColType::INT64});
    REQUIRE(r.size() == 5);
    // Date values returned as int64 days-since-epoch * 86_400_000 (ms)
    // by the engine's date C-API. We just sanity-check ordering: min
    // should be the earliest, max the latest within each category.
    for (size_t i = 0; i < 5; ++i) {
        INFO("row " << i);
        CHECK(r[i].int64_at(1) <= r[i].int64_at(2));
    }
}
