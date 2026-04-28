//===----------------------------------------------------------------------===//
//                         DuckDB
//
// src/function/scalar/list/range.cpp
//
//===----------------------------------------------------------------------===//

#include "function/scalar/nested_functions.hpp"
#include "common/exception.hpp"
#include "common/types/data_chunk.hpp"
#include "common/types/vector.hpp"

namespace duckdb {

// Cypher's range(start, end[, step]) is end-inclusive, e.g. range(1,3) = [1,2,3].
// 1-arg form `range(end)` follows the same end-inclusive convention starting
// from 0 (Neo4j extension; DuckDB's `range(N)` returns 0..N-1, which Cypher
// users often expect to be 0..N — we follow Cypher here so that scenarios
// like `UNWIND range(1, n) ...` produce the n elements users expect).
static int64_t ReadInt(Vector &v, idx_t row) {
	auto val = v.GetValue(row);
	if (val.IsNull()) return 0;
	return val.GetValue<int64_t>();
}

static void RangeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);
	idx_t arg_count = args.ColumnCount();
	D_ASSERT(arg_count >= 1 && arg_count <= 3);

	// Always emit FLAT — downstream FlatVector::GetData asserts on it.
	result.SetVectorType(VectorType::FLAT_VECTOR);

	auto list_data = FlatVector::GetData<list_entry_t>(result);
	auto &validity = FlatVector::Validity(result);
	idx_t out_rows = args.size();

	for (idx_t row = 0; row < out_rows; row++) {
		// Detect NULL inputs — propagate NULL.
		bool any_null = false;
		for (idx_t c = 0; c < arg_count; c++) {
			auto v = args.data[c].GetValue(row);
			if (v.IsNull()) { any_null = true; break; }
		}
		if (any_null) {
			list_data[row].offset = ListVector::GetListSize(result);
			list_data[row].length = 0;
			validity.SetInvalid(row);
			continue;
		}

		int64_t start_val, end_val, step_val;
		if (arg_count == 1) {
			start_val = 0;
			end_val   = ReadInt(args.data[0], row);
			step_val  = 1;
		} else if (arg_count == 2) {
			start_val = ReadInt(args.data[0], row);
			end_val   = ReadInt(args.data[1], row);
			step_val  = 1;
		} else {
			start_val = ReadInt(args.data[0], row);
			end_val   = ReadInt(args.data[1], row);
			step_val  = ReadInt(args.data[2], row);
		}

		if (step_val == 0) {
			throw InvalidInputException(
			    "range() step argument must not be zero");
		}

		list_data[row].offset = ListVector::GetListSize(result);
		idx_t emitted = 0;
		if (step_val > 0) {
			for (int64_t v = start_val; v <= end_val; v += step_val) {
				ListVector::PushBack(result, Value::BIGINT(v));
				emitted++;
				// Guard against overflow if v is near INT64_MAX.
				if (v > end_val - step_val) break;
			}
		} else {
			for (int64_t v = start_val; v >= end_val; v += step_val) {
				ListVector::PushBack(result, Value::BIGINT(v));
				emitted++;
				if (v < end_val - step_val) break;
			}
		}
		list_data[row].length = emitted;
	}

	result.Verify(args.size());
}

void ListRangeFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet range_set("range");
	auto list_bigint = LogicalType::LIST(LogicalType::BIGINT);
	range_set.AddFunction(ScalarFunction({LogicalType::BIGINT}, list_bigint,
	                                     RangeFunction));
	range_set.AddFunction(ScalarFunction(
	    {LogicalType::BIGINT, LogicalType::BIGINT}, list_bigint,
	    RangeFunction));
	range_set.AddFunction(ScalarFunction(
	    {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
	    list_bigint, RangeFunction));
	set.AddFunction(range_set);
}

} // namespace duckdb
