// Regression for issue #90: VectorOperations::Copy used to propagate the
// source's vector-wide invalid flag to the target by calling
// target.SetIsValid(false). For append-style targets (e.g. list aggregate
// state accumulating per-row inputs) this destroyed valid rows that had
// already been appended.

#include "catch.hpp"
#include "common/types/vector.hpp"
#include "common/vector_operations/vector_operations.hpp"

using duckdb::FlatVector;
using duckdb::idx_t;
using duckdb::LogicalType;
using duckdb::Value;
using duckdb::Vector;
using duckdb::VectorOperations;

TEST_CASE("VectorOperations::Copy: wholly-invalid source preserves target's valid prefix",
          "[common][vector-copy]") {
    Vector source(LogicalType::VARCHAR);
    source.SetIsValid(false);  // wholly invalid source

    Vector target(LogicalType::VARCHAR);
    target.SetValue(0, Value("first"));
    target.SetValue(1, Value("second"));
    REQUIRE(target.GetIsValid());

    // Simulate an append: copy 1 row from source into target[2..3).
    // Before the fix, this would call target.SetIsValid(false) and wipe
    // out rows 0 and 1.
    VectorOperations::Copy(source, target, /*source_count=*/1,
                           /*source_offset=*/0, /*target_offset=*/2);

    CHECK(target.GetIsValid());
    CHECK(target.GetValue(0).ToString() == "first");
    CHECK(target.GetValue(1).ToString() == "second");
    CHECK(target.GetValue(2).IsNull());
}

TEST_CASE("VectorOperations::Copy: wholly-invalid source followed by valid source",
          "[common][vector-copy]") {
    // Mirrors list_aggregate's per-row append pattern: first input is a
    // single NULL row, second input is a single valid row. The resulting
    // list child must read [NULL, valid] — not [NULL, NULL].
    Vector target(LogicalType::VARCHAR);

    Vector src_null(LogicalType::VARCHAR);
    src_null.SetIsValid(false);
    VectorOperations::Copy(src_null, target, 1, 0, 0);

    Vector src_val(LogicalType::VARCHAR);
    src_val.SetValue(0, Value("photo.jpg"));
    VectorOperations::Copy(src_val, target, 1, 0, 1);

    CHECK(target.GetIsValid());
    CHECK(target.GetValue(0).IsNull());
    CHECK_FALSE(target.GetValue(1).IsNull());
    CHECK(target.GetValue(1).ToString() == "photo.jpg");
}
