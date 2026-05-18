// Regression for issue #47. TemplatedContainsOrPosition's nested-type
// branch reads `child_value_idx` / `value_index` (already *physical*
// indexes resolved through VectorData.sel) and passes them straight
// to `Vector::GetValue`. GetValue takes a *logical* row and applies
// the vector's own selection, so DICTIONARY_VECTOR inputs would have
// the selection applied twice and read the wrong nested value.
//
// The current Cypher pipeline doesn't surface a dict-vectorized input
// into list_contains, so the bug is latent at the SQL/Cypher level.
// These tests reproduce the underlying Vector semantics directly so
// the fix is verified end-to-end at the API the function relies on,
// and pin the algorithm pattern the fix uses.

#include "catch.hpp"
#include "common/types/data_chunk.hpp"
#include "common/types/vector.hpp"
#include "execution/expression_executor.hpp"
#include "function/scalar/nested_functions.hpp"
#include "planner/expression/bound_function_expression.hpp"
#include "planner/expression/bound_reference_expression.hpp"

using duckdb::BoundFunctionExpression;
using duckdb::BoundReferenceExpression;
using duckdb::child_list_t;
using duckdb::DataChunk;
using duckdb::DictionaryVector;
using duckdb::Expression;
using duckdb::ExpressionExecutor;
using duckdb::idx_t;
using duckdb::ListContainsFun;
using duckdb::LogicalType;
using duckdb::make_unique;
using duckdb::ScalarFunction;
using duckdb::SelectionVector;
using duckdb::Value;
using duckdb::Vector;
using duckdb::VectorData;
using duckdb::VectorType;
using std::unique_ptr;
using std::vector;

TEST_CASE("Vector::GetValue on DICTIONARY_VECTOR uses the logical row",
          "[common][vector-getvalue][issue-47]") {
    // Flat backing vector with 3 distinct values.
    Vector flat(LogicalType::INTEGER);
    flat.SetValue(0, Value::INTEGER(100));
    flat.SetValue(1, Value::INTEGER(200));
    flat.SetValue(2, Value::INTEGER(300));

    // Wrap with a non-identity selection so logical → physical mapping
    // is observable: logical row [0, 1, 2] maps to physical [2, 0, 1].
    SelectionVector sel(3);
    sel.set_index(0, 2);
    sel.set_index(1, 0);
    sel.set_index(2, 1);
    flat.Slice(sel, 3);
    REQUIRE(flat.GetVectorType() == VectorType::DICTIONARY_VECTOR);

    // GetValue takes the logical row and follows the selection once.
    CHECK(flat.GetValue(0).GetValue<int32_t>() == 300);
    CHECK(flat.GetValue(1).GetValue<int32_t>() == 100);
    CHECK(flat.GetValue(2).GetValue<int32_t>() == 200);

    // Orrify exposes the physical indexes via VectorData.sel.
    VectorData data;
    flat.Orrify(3, data);
    REQUIRE(data.sel->get_index(0) == 2);
    REQUIRE(data.sel->get_index(1) == 0);
    REQUIRE(data.sel->get_index(2) == 1);

    // The buggy pattern from contains_or_position.cpp:105-106 passed
    // the physical index to GetValue, which then re-applied the
    // selection. Demonstrate that this diverges from the logical-row
    // result on every dict-mapped row:
    //   - logical row 0 should read value 300; physical idx 2 reads 200.
    //   - logical row 1 should read value 100; physical idx 0 reads 300.
    for (idx_t logical = 0; logical < 3; ++logical) {
        auto physical = data.sel->get_index(logical);
        if (physical != logical) {
            CHECK(flat.GetValue(physical).GetValue<int32_t>() !=
                  flat.GetValue(logical).GetValue<int32_t>());
        }
    }
}

TEST_CASE("list_contains nested algorithm reads correct child value via logical row",
          "[common][list-contains][issue-47]") {
    // Build a LIST(LIST(INTEGER)) value with 3 inner lists:
    //   row 0 → [10, 11]
    //   row 1 → [20, 21]
    //   row 2 → [30, 31]
    // The child vector backing those lists holds the 6 inner ints
    // [10, 11, 20, 21, 30, 31]; with a non-identity child selection we
    // can verify that the nested branch reads through the selection
    // exactly once.

    // Backing flat child of the inner-list (the integers).
    Vector flat_inner(LogicalType::INTEGER);
    flat_inner.SetValue(0, Value::INTEGER(10));
    flat_inner.SetValue(1, Value::INTEGER(11));
    flat_inner.SetValue(2, Value::INTEGER(20));
    flat_inner.SetValue(3, Value::INTEGER(21));
    flat_inner.SetValue(4, Value::INTEGER(30));
    flat_inner.SetValue(5, Value::INTEGER(31));

    // The "child vector" of the outer list is the inner Vector that
    // holds LIST(INTEGER) values at offsets [0,2,4]. Build it with
    // a non-identity selection so logical row k maps to a different
    // physical position — this is exactly what list_contains receives
    // after upstream operators slice the list column.
    Vector child_vec(LogicalType::LIST(LogicalType::INTEGER));
    // SetValue here materialises the entries 0..2 of the LIST(INT) Vector.
    child_vec.SetValue(0, Value::LIST({Value::INTEGER(10), Value::INTEGER(11)}));
    child_vec.SetValue(1, Value::LIST({Value::INTEGER(20), Value::INTEGER(21)}));
    child_vec.SetValue(2, Value::LIST({Value::INTEGER(30), Value::INTEGER(31)}));

    // Now slice the child Vector: logical [0,1,2] → physical [2,0,1].
    SelectionVector sel(3);
    sel.set_index(0, 2);
    sel.set_index(1, 0);
    sel.set_index(2, 1);
    child_vec.Slice(sel, 3);
    REQUIRE(child_vec.GetVectorType() == VectorType::DICTIONARY_VECTOR);

    // Pattern from TemplatedContainsOrPosition's nested branch:
    //   VectorData child_data; child_vec.Orrify(...);
    //   for (each inner element) {
    //     auto child_value_idx = child_data.sel->get_index(offset + child_idx);
    //     child_vec.GetValue(<index>);    // <-- the contended call
    //   }
    VectorData child_data;
    child_vec.Orrify(3, child_data);

    // Expected per-logical-row LIST values after the slice:
    //   logical 0 → [30, 31] (physical row 2)
    //   logical 1 → [10, 11] (physical row 0)
    //   logical 2 → [20, 21] (physical row 1)
    auto expect_for_logical = [](idx_t logical_row) {
        switch (logical_row) {
            case 0: return Value::LIST({Value::INTEGER(30), Value::INTEGER(31)});
            case 1: return Value::LIST({Value::INTEGER(10), Value::INTEGER(11)});
            case 2: return Value::LIST({Value::INTEGER(20), Value::INTEGER(21)});
        }
        return Value();
    };

    for (idx_t logical_row = 0; logical_row < 3; ++logical_row) {
        auto child_value_idx = child_data.sel->get_index(logical_row);  // physical
        auto buggy_read    = child_vec.GetValue(child_value_idx);       // pre-fix
        auto fixed_read    = child_vec.GetValue(logical_row);           // post-fix
        auto expected      = expect_for_logical(logical_row);

        INFO("logical_row=" << logical_row << " physical=" << child_value_idx);
        // The fixed read always matches the expected per-row value.
        CHECK(fixed_read == expected);
        // Where logical != physical, the buggy read disagrees with
        // expected — this is exactly what issue #47 describes.
        if (child_value_idx != logical_row) {
            CHECK_FALSE(buggy_read == expected);
        }
    }
}

// End-to-end check: drive the real `list_contains` ScalarFunction
// through ExpressionExecutor with a DICTIONARY_VECTOR on the value
// argument. The dict's selection [1, 0, 2] reshuffles a flat backing
// of [{i:20}, {i:10}, {i:30}] so the per-row VALUES are:
//   logical 0 → {i:10}  ∈ haystack [{i:10}, {i:30}]  → TRUE
//   logical 1 → {i:20}  ∉ haystack                   → FALSE
//   logical 2 → {i:30}  ∈ haystack                   → TRUE
//
// Pre-fix the function read value_vector.GetValue(physical_idx),
// re-applying the selection: it would have read flat[sel[sel[i]]],
// flipping the boolean on logical rows 0 and 1 (only row 2 has
// sel[i]==i and would have agreed by coincidence). Post-fix the
// function uses GetValue(i) and reads the correct value.
TEST_CASE("list_contains nested ScalarFunction honours dict selection on value",
          "[common][list-contains][issue-47]") {
    using duckdb::LogicalTypeId;

    // STRUCT child type used by the haystack list and the value column.
    child_list_t<LogicalType> struct_fields;
    struct_fields.emplace_back("i", LogicalType::INTEGER);
    auto struct_type = LogicalType::STRUCT(struct_fields);
    auto list_type   = LogicalType::LIST(struct_type);

    DataChunk input;
    input.Initialize({list_type, struct_type});
    input.SetCardinality(3);

    // Column 0 (the haystack list): same [{i:10}, {i:30}] on each row.
    auto make_struct = [&](int32_t v) {
        child_list_t<Value> kv;
        kv.emplace_back("i", Value::INTEGER(v));
        return Value::STRUCT(kv);
    };
    auto haystack = Value::LIST(struct_type, {make_struct(10), make_struct(30)});
    for (idx_t r = 0; r < 3; ++r) {
        input.data[0].SetValue(r, haystack);
    }

    // Column 1 (the value): build a flat layout in a "shifted" order
    // and Slice with a non-identity selection so logical row i reads a
    // different physical position. Identity on logical 2 keeps the
    // bug observable only on rows 0 and 1, sharpening the per-row diff.
    input.data[1].SetValue(0, make_struct(20));  // physical 0
    input.data[1].SetValue(1, make_struct(10));  // physical 1
    input.data[1].SetValue(2, make_struct(30));  // physical 2
    SelectionVector sel(3);
    sel.set_index(0, 1);  // logical 0 → physical 1 → {i:10}
    sel.set_index(1, 0);  // logical 1 → physical 0 → {i:20}
    sel.set_index(2, 2);  // logical 2 → physical 2 → {i:30}
    input.data[1].Slice(sel, 3);
    REQUIRE(input.data[1].GetVectorType() == VectorType::DICTIONARY_VECTOR);

    // Build a BoundFunctionExpression around list_contains. Bind metadata
    // isn't consulted by TemplatedContainsOrPosition, so nullptr is fine.
    vector<unique_ptr<Expression>> children;
    children.push_back(make_unique<BoundReferenceExpression>(list_type, 0));
    children.push_back(make_unique<BoundReferenceExpression>(struct_type, 1));
    auto fn = ListContainsFun::GetFunction();
    fn.arguments = {list_type, struct_type};
    fn.return_type = LogicalType::BOOLEAN;
    auto expr = make_unique<BoundFunctionExpression>(LogicalType::BOOLEAN, fn,
                                                     std::move(children), nullptr);

    ExpressionExecutor executor(*expr);
    DataChunk result;
    result.Initialize({LogicalType::BOOLEAN});
    executor.Execute(input, result);

    REQUIRE(result.size() == 3);
    CHECK(result.GetValue(0, 0).GetValue<bool>() == true);
    CHECK(result.GetValue(0, 1).GetValue<bool>() == false);
    CHECK(result.GetValue(0, 2).GetValue<bool>() == true);
}
