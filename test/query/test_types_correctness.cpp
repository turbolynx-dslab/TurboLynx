// types-mini fixture: a single :TypeRow label with one column per
// loader-supported scalar type. Tests target type-promotion edges
// (CASE LCT, scalar-arithmetic widening, aggregate output type)
// rather than any benchmark workload.
//
// Status of #111 sub-bugs in this file:
//   Bug 2 (global sum/min/max/avg, 0 grouping keys): regression-locked
//     via the [issue111][global-agg] cases below.
//   Bug 3 (sum over bare INTEGER/UBIGINT): regression-locked via the
//     existing by-category UBIGINT case + the new global cases.
//   Bug 1 (count(*) global → ORCA Normalizer crash): still open. We
//     use count(t) as a workaround in the global-agg cases.

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

TEST_CASE("types: sum/avg over UBIGINT property by category",
          "[types][agg][issue111]") {
    SKIP_IF_NO_TYPES_DB();
    // Pre-fix `sum(t.t_ulong)` (UBIGINT) crashed: no UBIGINT sum
    // overload existed, the dispatcher fell back to DOUBLE, and
    // CastToFunctionArguments wrapped the agg child in a CAST that
    // PhysicalHashAggregate::Sink couldn't handle (#111).
    auto r = qr->run(
        "MATCH (t:TypeRow) RETURN t.category AS c, "
        "sum(t.t_ulong) AS s, avg(t.t_ulong) AS a ORDER BY c",
        {qtest::ColType::STRING, qtest::ColType::INT64, qtest::ColType::AUTO});
    REQUIRE(r.size() == 5);
    // Per typerow.tbl, t_ulong by category:
    //   A: 0,1,0,10                    → sum 11,    avg 2.75
    //   B: 100,1000,0                  → sum 1100,  avg 366.67
    //   C: 9223372036854775807,2,3,4,5 → overflows-to-hugeint (skip exact)
    //   D: 6,7,8,9,11                  → sum 41,    avg 8.20
    //   E: 12,13,14                    → sum 39,    avg 13.00
    CHECK(r[0].str_at(0) == "A");
    CHECK(r[0].int64_at(1) == 11);
    CHECK(r[1].str_at(0) == "B");
    CHECK(r[1].int64_at(1) == 1100);
    CHECK(r[3].str_at(0) == "D");
    CHECK(r[3].int64_at(1) == 41);
    CHECK(r[3].str_at(2) == "8.200000");
    CHECK(r[4].str_at(0) == "E");
    CHECK(r[4].int64_at(1) == 39);
    CHECK(r[4].str_at(2) == "13.000000");
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

// ---------- global aggregates (0 grouping keys) ------------------------
// Locks #111 Bug 2: these previously SIGSEGV'd in
// PhysicalHashAggregate::Sink under Linux x86_64 across every column /
// agg combination. Bug 1 (count(*) with 0 grouping keys) is still
// open, so the cases below use count(t) instead.

TEST_CASE("types: global count over node", "[types][agg][issue111][global-agg]") {
    SKIP_IF_NO_TYPES_DB();
    auto r = qr->run("MATCH (t:TypeRow) RETURN count(t) AS v",
                     {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 20);
}

TEST_CASE("types: global sum / min / max over INTEGER",
          "[types][agg][issue111][global-agg]") {
    SKIP_IF_NO_TYPES_DB();
    auto r = qr->run(
        "MATCH (t:TypeRow) RETURN sum(t.t_int) AS s, "
        "min(t.t_int) AS mn, max(t.t_int) AS mx",
        {qtest::ColType::INT64, qtest::ColType::INT64, qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 2147483851);
    CHECK(r[0].int64_at(1) == -1000);
    CHECK(r[0].int64_at(2) == 2147483647);
}

TEST_CASE("types: global sum / avg over DOUBLE",
          "[types][agg][issue111][global-agg]") {
    SKIP_IF_NO_TYPES_DB();
    auto r = qr->run(
        "MATCH (t:TypeRow) RETURN sum(t.t_double) AS s, avg(t.t_double) AS a",
        {qtest::ColType::AUTO, qtest::ColType::AUTO});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == "208.141593");
    CHECK(r[0].str_at(1) == "10.407080");
}

TEST_CASE("types: global sum over DECIMAL",
          "[types][agg][issue111][global-agg]") {
    SKIP_IF_NO_TYPES_DB();
    auto r = qr->run(
        "MATCH (t:TypeRow) RETURN sum(t.t_dec_default) AS s",
        {qtest::ColType::AUTO});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == "1000000210.81");
}

TEST_CASE("types: global min / max over VARCHAR",
          "[types][agg][issue111][global-agg]") {
    SKIP_IF_NO_TYPES_DB();
    auto r = qr->run(
        "MATCH (t:TypeRow) RETURN min(t.t_str) AS mn, max(t.t_str) AS mx",
        {qtest::ColType::STRING, qtest::ColType::STRING});
    REQUIRE(r.size() == 1);
    CHECK(r[0].str_at(0) == "eight");
    CHECK(r[0].str_at(1) == "zero");
}

// ---------- comparisons across the integer family ----------------------
// Locks the planner_physical range-pushdown bug: when a literal's
// narrowest fitting type (e.g. INT for `5`) was paired with a wider
// column type (e.g. UBIGINT :ID), `Value::MinimumValue(literal_type)`
// produced INT_MIN, which reinterpreted to a huge unsigned in the
// storage range comparator and wiped out `<` / `<=` (returned 0 rows).
// The fix promotes the literal up to the column's logical type before
// the cmp expression leaves the converter.

TEST_CASE("types: range cmp on :ID-typed column",
          "[types][cmp][cmp-int]") {
    SKIP_IF_NO_TYPES_DB();
    // 20 rows, t.id ∈ {1..20}. Pre-fix every `<` / `<=` returned 0.
    auto r = qr->run(
        "MATCH (t:TypeRow) WHERE t.id <= 5 RETURN count(t) AS v",
        {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 5);

    r = qr->run("MATCH (t:TypeRow) WHERE t.id < 5 RETURN count(t) AS v",
                {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 4);

    r = qr->run("MATCH (t:TypeRow) WHERE t.id > 5 RETURN count(t) AS v",
                {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 15);

    r = qr->run("MATCH (t:TypeRow) WHERE t.id >= 5 RETURN count(t) AS v",
                {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 16);
}

TEST_CASE("types: range cmp with literal on the left "
          "(swap-and-flip path)",
          "[types][cmp][cmp-int]") {
    SKIP_IF_NO_TYPES_DB();
    // 5 >= t.id  ↔  t.id <= 5 — exercises the converter's swap+flip
    // path so that const-on-left also promotes through the literal arm.
    auto r = qr->run(
        "MATCH (t:TypeRow) WHERE 5 >= t.id RETURN count(t) AS v",
        {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 5);

    r = qr->run("MATCH (t:TypeRow) WHERE 5 < t.id RETURN count(t) AS v",
                {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    CHECK(r[0].int64_at(0) == 15);
}

TEST_CASE("types: integer cmp across signed widths",
          "[types][cmp][cmp-int]") {
    SKIP_IF_NO_TYPES_DB();
    // t_long is BIGINT; literal `5` parses to INT. Pre-fix this worked
    // by accident (BIGINT range covers INT range, MaximumValue(INT)
    // didn't truncate the dataset); the case is locked here so the
    // promotion path doesn't silently degrade.
    auto r = qr->run(
        "MATCH (t:TypeRow) WHERE t.t_long <= 5 RETURN count(t) AS v",
        {qtest::ColType::INT64});
    REQUIRE(r.size() == 1);
    // t_long values <= 5: row 1=0, 2=1, 3=-1, 7=-1000, 9=2, 10=3, 11=4,
    // 12=5 → 8 rows.
    CHECK(r[0].int64_at(0) == 8);
}
