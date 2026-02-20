// =============================================================================
// [common] SimpleHistogram unit tests
// =============================================================================
// Tag: [common][histogram]
//
// Tests the SimpleHistogram struct (histogram_generator.hpp) that replaced
// boost::histogram. Covers binning correctness, edge cases, and overflow.
// =============================================================================

#include "catch.hpp"
#include "storage/statistics/histogram_generator.hpp"

using namespace duckdb;

// ---------------------------------------------------------------------------
// Helper: build a SimpleHistogram with N equal-width bins over [lo, hi]
// ---------------------------------------------------------------------------
static SimpleHistogram make_histogram(int64_t lo, int64_t hi, size_t n_bins) {
    SimpleHistogram h;
    // boundaries: n_bins + 1 edges
    for (size_t i = 0; i <= n_bins; ++i) {
        h.boundaries.push_back(lo + (int64_t)i * (hi - lo) / (int64_t)n_bins);
    }
    h.counts.resize(n_bins, 0);
    return h;
}

// ---------------------------------------------------------------------------
TEST_CASE("SimpleHistogram: fill single value into correct bin", "[common][histogram]") {
    //  bins: [0,10) [10,20) [20,30)
    auto h = make_histogram(0, 30, 3);

    h.fill(5);   // → bin 0
    h.fill(15);  // → bin 1
    h.fill(25);  // → bin 2

    REQUIRE(h.at(0) == 1);
    REQUIRE(h.at(1) == 1);
    REQUIRE(h.at(2) == 1);
}

TEST_CASE("SimpleHistogram: boundary value goes to upper bin", "[common][histogram]") {
    //  bins: [0,10) [10,20)  — half-open intervals
    auto h = make_histogram(0, 20, 2);

    // fill() uses upper_bound: for value=10, upper_bound({0,10,20}, 10) → index 2 (points to 20)
    // bin = 2, bin-- → bin = 1.  So 10 goes to bin 1 ([10,20)), which is correct for [lo,hi).
    h.fill(10);
    REQUIRE(h.at(0) == 0);
    REQUIRE(h.at(1) == 1);

    // Values below range: upper_bound({0,10,20}, -5) → index 0; bin=0; no decrement → bin=0
    h.fill(-5);
    REQUIRE(h.at(0) == 1);
}

TEST_CASE("SimpleHistogram: multiple values accumulate", "[common][histogram]") {
    //  bins: [0,100) [100,200) [200,300)
    auto h = make_histogram(0, 300, 3);

    for (int i = 0; i < 50; ++i)  h.fill(i * 1);       // 0..49 → bin 0
    for (int i = 0; i < 30; ++i)  h.fill(100 + i);     // 100..129 → bin 1
    for (int i = 0; i < 10; ++i)  h.fill(200 + i);     // 200..209 → bin 2

    REQUIRE(h.at(0) == 50);
    REQUIRE(h.at(1) == 30);
    REQUIRE(h.at(2) == 10);
}

TEST_CASE("SimpleHistogram: value beyond upper boundary is silently dropped", "[common][histogram]") {
    auto h = make_histogram(0, 10, 1);  // single bin [0,10)
    h.fill(999);  // beyond upper boundary
    // upper_bound of 999 in {0,10} → end (distance = 2)
    // bin = 2, bin-- → bin = 1; bin(1) < counts.size()(1)? No → not counted.
    // Out-of-range values are silently dropped (no crash).
    REQUIRE(h.at(0) == 0);
}

TEST_CASE("SimpleHistogram: empty boundaries is safe", "[common][histogram]") {
    SimpleHistogram h;
    // fill on empty histogram should not crash
    REQUIRE_NOTHROW(h.fill(42));
    REQUIRE(h.counts.empty());
}
