#include "function/scalar/nested_functions.hpp"
#include "common/types/data_chunk.hpp"
#include "planner/expression/bound_function_expression.hpp"

namespace duckdb {

static void ListExtractFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	// list_extract(list, index) — 1-indexed
	auto &list_vec = args.data[0];
	auto &idx_vec = args.data[1];

	idx_t count = args.size();
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto &result_mask = FlatVector::Validity(result);

	for (idx_t i = 0; i < count; i++) {
		auto list_val = list_vec.GetValue(i);
		auto idx_val = idx_vec.GetValue(i);

		if (list_val.IsNull() || idx_val.IsNull()) {
			result_mask.SetInvalid(i);
			continue;
		}

		auto &children = ListValue::GetChildren(list_val);
		int64_t idx = idx_val.GetValue<int64_t>();
		// 0-indexed (Cypher standard), negative = from end
		if (idx < 0) idx = (int64_t)children.size() + idx;

		if (idx < 0 || idx >= (int64_t)children.size()) {
			result_mask.SetInvalid(i);
		} else {
			result.SetValue(i, children[idx]);
		}
	}
}

static unique_ptr<FunctionData> ListExtractBind(ClientContext &context,
    ScalarFunction &bound_function, vector<unique_ptr<Expression>> &arguments) {
	auto &list_type = arguments[0]->return_type;
	if (list_type.id() == LogicalTypeId::LIST) {
		bound_function.return_type = ListType::GetChildType(list_type);
	} else if (list_type.id() == LogicalTypeId::UNKNOWN || list_type.id() == LogicalTypeId::ANY) {
		bound_function.return_type = LogicalType::ANY;
	} else {
		bound_function.return_type = LogicalType::ANY;
	}
	return nullptr;
}

void ListExtractFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet list_extract("list_extract");
	list_extract.AddFunction(ScalarFunction(
	    {LogicalType::LIST(LogicalType::ANY), LogicalType::BIGINT},
	    LogicalType::ANY, ListExtractFunction, false, false, ListExtractBind));
	list_extract.AddFunction(ScalarFunction(
	    {LogicalType::LIST(LogicalType::ANY), LogicalType::INTEGER},
	    LogicalType::ANY, ListExtractFunction, false, false, ListExtractBind));
	set.AddFunction(list_extract);
}

} // namespace duckdb
