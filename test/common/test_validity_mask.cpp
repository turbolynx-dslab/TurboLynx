// Regression for issue #44: SetAllValid/SetAllInvalid only touch the
// active bits of the last validity entry, but the whole-entry fast paths
// in CheckAllValid/CheckAllInValid used to compare the full word. Counts
// not aligned to 64 spuriously failed.

#include "catch.hpp"
#include "common/types/validity_mask.hpp"

using duckdb::ValidityMask;

namespace {

ValidityMask MakeMask(duckdb::idx_t count) {
    return ValidityMask(count);
}

} // namespace

TEST_CASE("ValidityMask: SetAllValid(count) → CheckAllValid is true",
          "[common][validity-mask]") {
    // Cover full entry (64), one trailing bit (65), one trailing bit in the
    // first entry (63), and a full two-entry buffer (128).
    for (duckdb::idx_t count : {(duckdb::idx_t)1, (duckdb::idx_t)63, (duckdb::idx_t)64,
                                (duckdb::idx_t)65, (duckdb::idx_t)127,
                                (duckdb::idx_t)128, (duckdb::idx_t)129}) {
        INFO("count=" << count);
        auto m = MakeMask(count);
        m.SetAllValid(count);
        CHECK(m.CheckAllValid(count));
        CHECK_FALSE(m.CheckAllInValid(count));
    }
}

TEST_CASE("ValidityMask: SetAllInvalid(count) → CheckAllInValid is true",
          "[common][validity-mask]") {
    for (duckdb::idx_t count : {(duckdb::idx_t)1, (duckdb::idx_t)63, (duckdb::idx_t)64,
                                (duckdb::idx_t)65, (duckdb::idx_t)127,
                                (duckdb::idx_t)128, (duckdb::idx_t)129}) {
        INFO("count=" << count);
        auto m = MakeMask(count);
        m.SetAllInvalid(count);
        CHECK(m.CheckAllInValid(count));
        CHECK_FALSE(m.CheckAllValid(count));
    }
}

TEST_CASE("ValidityMask: single invalid row breaks CheckAllValid",
          "[common][validity-mask]") {
    // Confirms the partial-entry path doesn't accidentally allow an active
    // invalid bit to slip through.
    for (duckdb::idx_t count : {(duckdb::idx_t)63, (duckdb::idx_t)64,
                                (duckdb::idx_t)65, (duckdb::idx_t)128}) {
        INFO("count=" << count);
        auto m = MakeMask(count);
        m.SetAllValid(count);
        m.SetInvalid(count - 1);
        CHECK_FALSE(m.CheckAllValid(count));
        CHECK_FALSE(m.CheckAllInValid(count));
    }
}

TEST_CASE("ValidityMask: single valid row breaks CheckAllInValid",
          "[common][validity-mask]") {
    for (duckdb::idx_t count : {(duckdb::idx_t)63, (duckdb::idx_t)64,
                                (duckdb::idx_t)65, (duckdb::idx_t)128}) {
        INFO("count=" << count);
        auto m = MakeMask(count);
        m.SetAllInvalid(count);
        m.SetValid(count - 1);
        CHECK_FALSE(m.CheckAllInValid(count));
        CHECK_FALSE(m.CheckAllValid(count));
    }
}
