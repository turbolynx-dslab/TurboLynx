#include "function/scalar/nested_functions.hpp"
#include "common/types/data_chunk.hpp"

namespace duckdb {

static void ListSizeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &list_vec = args.data[0];
	idx_t count = args.size();
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<int64_t>(result);
	auto &result_mask = FlatVector::Validity(result);

	for (idx_t i = 0; i < count; i++) {
		auto val = list_vec.GetValue(i);
		if (val.IsNull()) {
			result_mask.SetInvalid(i);
			continue;
		}
		auto &children = ListValue::GetChildren(val);
		result_data[i] = (int64_t)children.size();
	}
}

void ListSizeFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet list_size("list_size");
	list_size.AddFunction(ScalarFunction(
	    {LogicalType::LIST(LogicalType::ANY)},
	    LogicalType::BIGINT, ListSizeFunction));
	set.AddFunction(list_size);
}

} // namespace duckdb
