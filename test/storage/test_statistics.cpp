// =============================================================================
// [storage][statistics] Statistics unit tests
// =============================================================================
// Tag: [storage][statistics]
//
// Tests pure in-memory statistics logic:
//   [numeric]  — NumericStatistics: CheckZonemap, Merge, IsConstant
//   [validity] — ValidityStatistics: Combine, IsConstant, CanHaveNull
//   [string]   — StringStatistics: constructor state, CheckZonemap
//
// No database instance or disk I/O required.
// =============================================================================

#include "catch.hpp"
#include "test_helper.hpp"

#include "storage/statistics/numeric_statistics.hpp"
#include "storage/statistics/string_statistics.hpp"
#include "storage/statistics/validity_statistics.hpp"
#include "common/types/value.hpp"
#include "common/enums/expression_type.hpp"
#include "common/enums/filter_propagate_result.hpp"

#include <cstring>

using namespace duckdb;

// Shorthand aliases
static constexpr auto ALWAYS_TRUE  = FilterPropagateResult::FILTER_ALWAYS_TRUE;
static constexpr auto ALWAYS_FALSE = FilterPropagateResult::FILTER_ALWAYS_FALSE;
static constexpr auto NO_PRUNING   = FilterPropagateResult::NO_PRUNING_POSSIBLE;

// =============================================================================
// NumericStatistics — CheckZonemap
// =============================================================================

// Helper: create INTEGER stats with known min/max
static NumericStatistics int_stats(int32_t lo, int32_t hi) {
    return NumericStatistics(LogicalType::INTEGER,
                             Value::INTEGER(lo),
                             Value::INTEGER(hi));
}

TEST_CASE("NumericStatistics: CheckZonemap EQUAL — in range, out of range, constant segment",
          "[storage][statistics][numeric]") {
    auto stats = int_stats(10, 20);

    // constant is strictly inside → cannot prune
    REQUIRE(stats.CheckZonemap(ExpressionType::COMPARE_EQUAL, Value::INTEGER(15)) == NO_PRUNING);

    // constant is outside (too high) → always false
    REQUIRE(stats.CheckZonemap(ExpressionType::COMPARE_EQUAL, Value::INTEGER(25)) == ALWAYS_FALSE);

    // constant is outside (too low) → always false
    REQUIRE(stats.CheckZonemap(ExpressionType::COMPARE_EQUAL, Value::INTEGER(5))  == ALWAYS_FALSE);

    // boundary values are still inside
    REQUIRE(stats.CheckZonemap(ExpressionType::COMPARE_EQUAL, Value::INTEGER(10)) == NO_PRUNING);
    REQUIRE(stats.CheckZonemap(ExpressionType::COMPARE_EQUAL, Value::INTEGER(20)) == NO_PRUNING);

    // constant-segment (min == max) matches exactly → always true
    auto const_stats = int_stats(10, 10);
    REQUIRE(const_stats.CheckZonemap(ExpressionType::COMPARE_EQUAL, Value::INTEGER(10)) == ALWAYS_TRUE);
    REQUIRE(const_stats.CheckZonemap(ExpressionType::COMPARE_EQUAL, Value::INTEGER(11)) == ALWAYS_FALSE);
}

TEST_CASE("NumericStatistics: CheckZonemap GREATERTHAN",
          "[storage][statistics][numeric]") {
    auto stats = int_stats(10, 20);

    // X > 9: min(X)=10 > 9 → always true
    REQUIRE(stats.CheckZonemap(ExpressionType::COMPARE_GREATERTHAN, Value::INTEGER(9)) == ALWAYS_TRUE);

    // X > 15: max=20 > 15 but min=10 is not → cannot prune
    REQUIRE(stats.CheckZonemap(ExpressionType::COMPARE_GREATERTHAN, Value::INTEGER(15)) == NO_PRUNING);

    // X > 20: max=20 is NOT > 20 → always false
    REQUIRE(stats.CheckZonemap(ExpressionType::COMPARE_GREATERTHAN, Value::INTEGER(20)) == ALWAYS_FALSE);

    // X > 25: max=20 < 25 → always false
    REQUIRE(stats.CheckZonemap(ExpressionType::COMPARE_GREATERTHAN, Value::INTEGER(25)) == ALWAYS_FALSE);
}

TEST_CASE("NumericStatistics: CheckZonemap GREATERTHANOREQUALTO",
          "[storage][statistics][numeric]") {
    auto stats = int_stats(10, 20);

    // X >= 10: min(X)=10 >= 10 → always true
    REQUIRE(stats.CheckZonemap(ExpressionType::COMPARE_GREATERTHANOREQUALTO, Value::INTEGER(10)) == ALWAYS_TRUE);

    // X >= 5: min(X)=10 >= 5 → always true
    REQUIRE(stats.CheckZonemap(ExpressionType::COMPARE_GREATERTHANOREQUALTO, Value::INTEGER(5)) == ALWAYS_TRUE);

    // X >= 15: max=20 >= 15 but min=10 is not → cannot prune
    REQUIRE(stats.CheckZonemap(ExpressionType::COMPARE_GREATERTHANOREQUALTO, Value::INTEGER(15)) == NO_PRUNING);

    // X >= 21: max=20 < 21 → always false
    REQUIRE(stats.CheckZonemap(ExpressionType::COMPARE_GREATERTHANOREQUALTO, Value::INTEGER(21)) == ALWAYS_FALSE);
}

TEST_CASE("NumericStatistics: CheckZonemap LESSTHAN",
          "[storage][statistics][numeric]") {
    auto stats = int_stats(10, 20);

    // X < 21: max=20 < 21 → always true
    REQUIRE(stats.CheckZonemap(ExpressionType::COMPARE_LESSTHAN, Value::INTEGER(21)) == ALWAYS_TRUE);

    // X < 15: min=10 < 15 but max=20 is not → cannot prune
    REQUIRE(stats.CheckZonemap(ExpressionType::COMPARE_LESSTHAN, Value::INTEGER(15)) == NO_PRUNING);

    // X < 10: min=10 is NOT < 10 → always false
    REQUIRE(stats.CheckZonemap(ExpressionType::COMPARE_LESSTHAN, Value::INTEGER(10)) == ALWAYS_FALSE);

    // X < 5: min=10 > 5 → always false
    REQUIRE(stats.CheckZonemap(ExpressionType::COMPARE_LESSTHAN, Value::INTEGER(5)) == ALWAYS_FALSE);
}

TEST_CASE("NumericStatistics: CheckZonemap LESSTHANOREQUALTO",
          "[storage][statistics][numeric]") {
    auto stats = int_stats(10, 20);

    // X <= 20: max=20 <= 20 → always true
    REQUIRE(stats.CheckZonemap(ExpressionType::COMPARE_LESSTHANOREQUALTO, Value::INTEGER(20)) == ALWAYS_TRUE);

    // X <= 25: max=20 <= 25 → always true
    REQUIRE(stats.CheckZonemap(ExpressionType::COMPARE_LESSTHANOREQUALTO, Value::INTEGER(25)) == ALWAYS_TRUE);

    // X <= 15: min=10 <= 15 but max=20 is not → cannot prune
    REQUIRE(stats.CheckZonemap(ExpressionType::COMPARE_LESSTHANOREQUALTO, Value::INTEGER(15)) == NO_PRUNING);

    // X <= 9: min=10 > 9 → always false
    REQUIRE(stats.CheckZonemap(ExpressionType::COMPARE_LESSTHANOREQUALTO, Value::INTEGER(9)) == ALWAYS_FALSE);
}

TEST_CASE("NumericStatistics: CheckZonemap NOTEQUAL",
          "[storage][statistics][numeric]") {
    auto stats = int_stats(10, 20);

    // constant outside range → always true (no value equals constant)
    REQUIRE(stats.CheckZonemap(ExpressionType::COMPARE_NOTEQUAL, Value::INTEGER(25)) == ALWAYS_TRUE);
    REQUIRE(stats.CheckZonemap(ExpressionType::COMPARE_NOTEQUAL, Value::INTEGER(5))  == ALWAYS_TRUE);

    // constant inside range → cannot prune
    REQUIRE(stats.CheckZonemap(ExpressionType::COMPARE_NOTEQUAL, Value::INTEGER(15)) == NO_PRUNING);

    // constant-segment: all values equal constant → always false
    auto const_stats = int_stats(10, 10);
    REQUIRE(const_stats.CheckZonemap(ExpressionType::COMPARE_NOTEQUAL, Value::INTEGER(10)) == ALWAYS_FALSE);
}

TEST_CASE("NumericStatistics: CheckZonemap NULL constant → FILTER_ALWAYS_FALSE",
          "[storage][statistics][numeric]") {
    auto stats = int_stats(10, 20);
    Value null_val;  // default Value is NULL
    REQUIRE(null_val.IsNull());
    REQUIRE(stats.CheckZonemap(ExpressionType::COMPARE_EQUAL, null_val) == ALWAYS_FALSE);
    REQUIRE(stats.CheckZonemap(ExpressionType::COMPARE_GREATERTHAN, null_val) == ALWAYS_FALSE);
}

TEST_CASE("NumericStatistics: CheckZonemap BIGINT type",
          "[storage][statistics][numeric]") {
    NumericStatistics stats(LogicalType::BIGINT,
                            Value::BIGINT(100LL),
                            Value::BIGINT(200LL));

    REQUIRE(stats.CheckZonemap(ExpressionType::COMPARE_EQUAL, Value::BIGINT(150LL)) == NO_PRUNING);
    REQUIRE(stats.CheckZonemap(ExpressionType::COMPARE_EQUAL, Value::BIGINT(300LL)) == ALWAYS_FALSE);
    REQUIRE(stats.CheckZonemap(ExpressionType::COMPARE_GREATERTHAN, Value::BIGINT(50LL)) == ALWAYS_TRUE);
    REQUIRE(stats.CheckZonemap(ExpressionType::COMPARE_LESSTHAN, Value::BIGINT(201LL)) == ALWAYS_TRUE);
}

// =============================================================================
// NumericStatistics — Merge
// =============================================================================

TEST_CASE("NumericStatistics: Merge expands min/max to cover both ranges",
          "[storage][statistics][numeric]") {
    // Disjoint: [10,20] merged with [30,40] → [10,40]
    auto a = int_stats(10, 20);
    auto b = int_stats(30, 40);
    a.Merge(b);
    REQUIRE(a.min == Value::INTEGER(10));
    REQUIRE(a.max == Value::INTEGER(40));
}

TEST_CASE("NumericStatistics: Merge overlapping ranges",
          "[storage][statistics][numeric]") {
    // [10,20] merged with [5,15] → [5,20]
    auto a = int_stats(10, 20);
    auto b = int_stats(5, 15);
    a.Merge(b);
    REQUIRE(a.min == Value::INTEGER(5));
    REQUIRE(a.max == Value::INTEGER(20));
}

TEST_CASE("NumericStatistics: Merge same range is idempotent",
          "[storage][statistics][numeric]") {
    auto a = int_stats(10, 20);
    auto b = int_stats(10, 20);
    a.Merge(b);
    REQUIRE(a.min == Value::INTEGER(10));
    REQUIRE(a.max == Value::INTEGER(20));
}

TEST_CASE("NumericStatistics: Merge contained range does not change bounds",
          "[storage][statistics][numeric]") {
    // [10,20] merged with [12,18] → still [10,20]
    auto a = int_stats(10, 20);
    auto b = int_stats(12, 18);
    a.Merge(b);
    REQUIRE(a.min == Value::INTEGER(10));
    REQUIRE(a.max == Value::INTEGER(20));
}

// =============================================================================
// NumericStatistics — IsConstant
// =============================================================================

TEST_CASE("NumericStatistics: IsConstant when min == max",
          "[storage][statistics][numeric]") {
    auto const_stats = int_stats(42, 42);
    REQUIRE(const_stats.IsConstant() == true);
}

TEST_CASE("NumericStatistics: IsConstant is false when min < max",
          "[storage][statistics][numeric]") {
    auto stats = int_stats(10, 20);
    REQUIRE(stats.IsConstant() == false);
}

// =============================================================================
// ValidityStatistics — Combine
// =============================================================================

TEST_CASE("ValidityStatistics: Combine OR-merges has_null",
          "[storage][statistics][validity]") {
    // neither has null + one has null → combined has null
    unique_ptr<BaseStatistics> l = make_unique<ValidityStatistics>(false, true);
    unique_ptr<BaseStatistics> r = make_unique<ValidityStatistics>(true,  true);
    auto combined = ValidityStatistics::Combine(l, r);
    auto &res = (ValidityStatistics &)*combined;
    REQUIRE(res.has_null    == true);
    REQUIRE(res.has_no_null == true);
}

TEST_CASE("ValidityStatistics: Combine neither has null → combined has_null=false",
          "[storage][statistics][validity]") {
    unique_ptr<BaseStatistics> l = make_unique<ValidityStatistics>(false, true);
    unique_ptr<BaseStatistics> r = make_unique<ValidityStatistics>(false, true);
    auto combined = ValidityStatistics::Combine(l, r);
    auto &res = (ValidityStatistics &)*combined;
    REQUIRE(res.has_null    == false);
    REQUIRE(res.has_no_null == true);
}

TEST_CASE("ValidityStatistics: Combine both all-null → combined all-null",
          "[storage][statistics][validity]") {
    unique_ptr<BaseStatistics> l = make_unique<ValidityStatistics>(true,  false);
    unique_ptr<BaseStatistics> r = make_unique<ValidityStatistics>(true,  false);
    auto combined = ValidityStatistics::Combine(l, r);
    auto &res = (ValidityStatistics &)*combined;
    REQUIRE(res.has_null    == true);
    REQUIRE(res.has_no_null == false);
}

TEST_CASE("ValidityStatistics: Combine with null pointer → returns copy of other",
          "[storage][statistics][validity]") {
    // left null, right non-null
    unique_ptr<BaseStatistics> l = nullptr;
    unique_ptr<BaseStatistics> r = make_unique<ValidityStatistics>(true, false);
    auto combined = ValidityStatistics::Combine(l, r);
    REQUIRE(combined != nullptr);
    auto &res = (ValidityStatistics &)*combined;
    REQUIRE(res.has_null    == true);
    REQUIRE(res.has_no_null == false);
}

TEST_CASE("ValidityStatistics: Combine both null → returns nullptr",
          "[storage][statistics][validity]") {
    unique_ptr<BaseStatistics> l = nullptr;
    unique_ptr<BaseStatistics> r = nullptr;
    auto combined = ValidityStatistics::Combine(l, r);
    REQUIRE(combined == nullptr);
}

// =============================================================================
// ValidityStatistics — IsConstant
// =============================================================================

TEST_CASE("ValidityStatistics: IsConstant — all non-null",
          "[storage][statistics][validity]") {
    ValidityStatistics stats(false, true);  // has_null=false, has_no_null=true
    REQUIRE(stats.IsConstant() == true);    // all non-null is uniform → constant
}

TEST_CASE("ValidityStatistics: IsConstant — all null",
          "[storage][statistics][validity]") {
    ValidityStatistics stats(true, false);  // has_null=true, has_no_null=false
    REQUIRE(stats.IsConstant() == true);    // all null is uniform → constant
}

TEST_CASE("ValidityStatistics: IsConstant — mixed null and non-null",
          "[storage][statistics][validity]") {
    ValidityStatistics stats(true, true);   // both present
    REQUIRE(stats.IsConstant() == false);
}

// =============================================================================
// BaseStatistics — CanHaveNull / CanHaveNoNull
// =============================================================================

TEST_CASE("BaseStatistics: CanHaveNull and CanHaveNoNull via NumericStatistics",
          "[storage][statistics][validity]") {
    // 1-arg constructor sets validity_stats = ValidityStatistics(has_null=false)
    NumericStatistics stats(LogicalType::INTEGER);

    // has_null=false set in constructor
    REQUIRE(stats.CanHaveNull()   == false);
    REQUIRE(stats.CanHaveNoNull() == true);
}

TEST_CASE("BaseStatistics: CanHaveNull returns true when no validity_stats",
          "[storage][statistics][validity]") {
    // 3-arg constructor does NOT set validity_stats → unknown → returns true
    auto stats = int_stats(10, 20);
    REQUIRE(stats.CanHaveNull()   == true);   // "solid maybe"
    REQUIRE(stats.CanHaveNoNull() == true);
}

// =============================================================================
// StringStatistics — initial state and CheckZonemap
// =============================================================================

// Helper: create StringStatistics on heap with manually set min/max bytes
// (StringStatistics::Update is disabled due to utf8proc removal)
static unique_ptr<StringStatistics> make_string_stats(const char *min_str, const char *max_str) {
    auto stats = make_unique<StringStatistics>(LogicalType::VARCHAR);
    memset(stats->min, 0, StringStatistics::MAX_STRING_MINMAX_SIZE);
    memset(stats->max, 0, StringStatistics::MAX_STRING_MINMAX_SIZE);
    memcpy(stats->min, min_str, std::min(strlen(min_str), (size_t)StringStatistics::MAX_STRING_MINMAX_SIZE));
    memcpy(stats->max, max_str, std::min(strlen(max_str), (size_t)StringStatistics::MAX_STRING_MINMAX_SIZE));
    return stats;
}

TEST_CASE("StringStatistics: initial state has inverted sentinels",
          "[storage][statistics][string]") {
    // Constructor sets min=0xFF bytes, max=0x00 bytes (empty/uninitialized sentinel)
    StringStatistics stats(LogicalType::VARCHAR);

    REQUIRE(stats.min[0] == 0xFF);
    REQUIRE(stats.max[0] == 0x00);
    REQUIRE(stats.max_string_length == 0);
    REQUIRE(stats.has_unicode       == false);
    REQUIRE(stats.has_overflow_strings == false);
}

TEST_CASE("StringStatistics: CheckZonemap EQUAL — in range and out of range",
          "[storage][statistics][string]") {
    // range: ["apple", "orange"]
    auto stats = make_string_stats("apple", "orange");

    // "banana" is lexicographically in [apple, orange] → NO_PRUNING
    REQUIRE(stats->CheckZonemap(ExpressionType::COMPARE_EQUAL, "banana") == NO_PRUNING);

    // "zebra" > "orange" → ALWAYS_FALSE
    REQUIRE(stats->CheckZonemap(ExpressionType::COMPARE_EQUAL, "zebra") == ALWAYS_FALSE);

    // "aardvark" < "apple" → ALWAYS_FALSE
    REQUIRE(stats->CheckZonemap(ExpressionType::COMPARE_EQUAL, "aardvark") == ALWAYS_FALSE);

    // boundary values
    REQUIRE(stats->CheckZonemap(ExpressionType::COMPARE_EQUAL, "apple")  == NO_PRUNING);
    REQUIRE(stats->CheckZonemap(ExpressionType::COMPARE_EQUAL, "orange") == NO_PRUNING);
}

TEST_CASE("StringStatistics: CheckZonemap NOTEQUAL — outside range → ALWAYS_TRUE",
          "[storage][statistics][string]") {
    auto stats = make_string_stats("apple", "orange");

    // "zebra" > max → constant is always ≠ any value in segment → ALWAYS_TRUE
    REQUIRE(stats->CheckZonemap(ExpressionType::COMPARE_NOTEQUAL, "zebra") == ALWAYS_TRUE);

    // "aardvark" < min → ALWAYS_TRUE
    REQUIRE(stats->CheckZonemap(ExpressionType::COMPARE_NOTEQUAL, "aardvark") == ALWAYS_TRUE);

    // inside range → cannot prune
    REQUIRE(stats->CheckZonemap(ExpressionType::COMPARE_NOTEQUAL, "banana") == NO_PRUNING);
}

TEST_CASE("StringStatistics: CheckZonemap GREATERTHAN",
          "[storage][statistics][string]") {
    // range: ["apple", "orange"]
    // X > "aardvark": max="orange" > "aardvark" → NO_PRUNING
    auto stats = make_string_stats("apple", "orange");
    REQUIRE(stats->CheckZonemap(ExpressionType::COMPARE_GREATERTHAN, "aardvark") == NO_PRUNING);

    // X > "zoo": max="orange" < "zoo" → ALWAYS_FALSE
    REQUIRE(stats->CheckZonemap(ExpressionType::COMPARE_GREATERTHAN, "zoo") == ALWAYS_FALSE);
}
