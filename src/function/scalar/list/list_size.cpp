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

static void PathLengthFunction(DataChunk &args, ExpressionState &state, Vector &result) {
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
		// Path = [node, edge, node, edge, ..., node], hops = (size-1)/2
		result_data[i] = children.empty() ? 0 : (int64_t)(children.size() - 1) / 2;
	}
}

void ListSizeFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet list_size("list_size");
	list_size.AddFunction(ScalarFunction(
	    {LogicalType::LIST(LogicalType::ANY)},
	    LogicalType::BIGINT, ListSizeFunction));
	set.AddFunction(list_size);

	ScalarFunctionSet path_length("path_length");
	path_length.AddFunction(ScalarFunction(
	    {LogicalType::LIST(LogicalType::ANY)},
	    LogicalType::BIGINT, PathLengthFunction));
	set.AddFunction(path_length);
}

} // namespace duckdb
