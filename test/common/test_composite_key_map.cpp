// =============================================================================
// [common] EncodeNeo4jCompositeKey — composite-vertex-key encoding (#24)
// =============================================================================
// Single-column references to a composite (key1, key2) vertex follow Neo4j's
// string-concat encoding: parseUInt(str(key1) + str(key2)). Pre-fix the
// loader computed a chunk-local 10^digits(max(key2)) multiplier, so the same
// (key1, key2) could encode differently across chunks. The fixed encoder is
// per-vertex (mult = 10^digits(key2_of_this_vertex)), matching Neo4j 1:1, and
// throws InvalidInputException on uint64 overflow.

#include "catch.hpp"
#include "common/exception.hpp"
#include "loader/composite_key_map.hpp"
#include <limits>

using namespace duckdb;
using namespace turbolynx;

TEST_CASE("EncodeNeo4jCompositeKey matches Neo4j string-concat",
          "[common][composite-key][issue24]") {
    CHECK(EncodeNeo4jCompositeKey(1, 3)    == 13);     // "1"  + "3"   = "13"
    CHECK(EncodeNeo4jCompositeKey(5, 70)   == 570);    // "5"  + "70"  = "570"
    CHECK(EncodeNeo4jCompositeKey(5, 7)    == 57);     // "5"  + "7"   = "57"
    CHECK(EncodeNeo4jCompositeKey(13, 570) == 13570);  // "13" + "570" = "13570"
}

TEST_CASE("EncodeNeo4jCompositeKey treats key2==0 as digit width 1",
          "[common][composite-key][issue24]") {
    // String "0" has width 1, so (5, 0) → "5"+"0" = "50".
    CHECK(EncodeNeo4jCompositeKey(5, 0)  == 50);
    CHECK(EncodeNeo4jCompositeKey(13, 0) == 130);
    CHECK(EncodeNeo4jCompositeKey(0, 0)  == 0);
}

TEST_CASE("EncodeNeo4jCompositeKey is per-vertex, not file-wide",
          "[common][composite-key][issue24]") {
    // The pre-fix file-wide multiplier picked the digit width of max(key2)
    // across the entire file and applied it uniformly, which zero-pads
    // small key2 values. Neo4j does not zero-pad.
    //   File-wide (broken):   (1, 3)  with max(key2)=70 → 1*100+3   = 103
    //   Per-vertex (correct): (1, 3)  → 1*10+3                       = 13
    CHECK(EncodeNeo4jCompositeKey(1, 3) == 13);  // not 103
    // The encoding of (5, 70) is the same under either rule, confirming
    // they only diverge for entries whose key2 is narrower than max(key2).
    CHECK(EncodeNeo4jCompositeKey(5, 70) == 570);
}

TEST_CASE("EncodeNeo4jCompositeKey throws on uint64 overflow",
          "[common][composite-key][issue24]") {
    // key1 * 10^digits(key2) overflows: 10^19 fits in uint64 but
    // 10 * 10^19 does not.
    constexpr idx_t kMax = std::numeric_limits<idx_t>::max();
    REQUIRE_THROWS_AS(EncodeNeo4jCompositeKey(kMax, 5), InvalidInputException);
    REQUIRE_THROWS_AS(EncodeNeo4jCompositeKey(2'000'000'000ULL,
                                              10'000'000'000ULL),
                      InvalidInputException);
    // (k1 * mult) + k2 step also has a guard: max representable key1
    // alone is fine, but appending any non-zero key2 overflows.
    REQUIRE_THROWS_AS(EncodeNeo4jCompositeKey(kMax / 10, 9),
                      InvalidInputException);
}

TEST_CASE("EncodeNeo4jCompositeKey accepts non-overflowing large keys",
          "[common][composite-key][issue24]") {
    // 10^18 * 10 + 9 = 10^19 + 9, fits in uint64 (max ≈ 1.8 * 10^19).
    CHECK(EncodeNeo4jCompositeKey(1'000'000'000'000'000'000ULL, 9)
          == 10'000'000'000'000'000'009ULL);
}
