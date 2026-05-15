//===----------------------------------------------------------------------===//
//                         DuckDB
//
// src/function/scalar/struct/struct_extract.cpp
//
//
//===----------------------------------------------------------------------===//

#include "function/scalar/nested_functions.hpp"
#include "common/types/data_chunk.hpp"
#include "common/types/vector.hpp"
#include "execution/expression_executor.hpp"
#include "planner/expression/bound_function_expression.hpp"

namespace duckdb {

struct StructExtractBindData : public FunctionData {
	idx_t field_index;
	explicit StructExtractBindData(idx_t idx) : field_index(idx) {}
	unique_ptr<FunctionData> Copy() const { return make_unique<StructExtractBindData>(field_index); }
	bool Equals(const FunctionData &o) const { return field_index == ((const StructExtractBindData &)o).field_index; }
};

static void StructExtractFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &bind_data = (StructExtractBindData &)*((BoundFunctionExpression &)state.expr).bind_info;
	auto &struct_vec = args.data[0];
	auto &children = StructVector::GetEntries(struct_vec);
	D_ASSERT(bind_data.field_index < children.size());
	// StructVector::GetEntries drills through DICTIONARY_VECTOR to return the
	// underlying flat children, so a Reference alone drops the parent's
	// selection. Propagate the slice so callers that compose this with
	// SelectionVector-driven evaluators (e.g. CASE/coalesce) read the
	// per-row value, not row 0 of the unsliced child (#90).
	if (struct_vec.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &sel = DictionaryVector::SelVector(struct_vec);
		result.Slice(*children[bind_data.field_index], sel, args.size());
	} else {
		result.Reference(*children[bind_data.field_index]);
	}
}

static unique_ptr<FunctionData> StructExtractBind(ClientContext &,
    ScalarFunction &bound_function, vector<unique_ptr<Expression>> &arguments) {
	auto &struct_type = arguments[0]->return_type;
	if (struct_type.id() != LogicalTypeId::STRUCT) {
		bound_function.return_type = LogicalType::ANY;
		return make_unique<StructExtractBindData>(0);
	}
	string field_name;
	if (arguments[1]->IsFoldable()) {
		auto val = ExpressionExecutor::EvaluateScalar(*arguments[1]);
		field_name = val.ToString();
	}
	auto &struct_children = StructType::GetChildTypes(struct_type);
	for (idx_t i = 0; i < struct_children.size(); i++) {
		if (struct_children[i].first == field_name) {
			bound_function.return_type = struct_children[i].second;
			return make_unique<StructExtractBindData>(i);
		}
	}
	bound_function.return_type = LogicalType::ANY;
	return make_unique<StructExtractBindData>(0);
}

void StructExtractFun::RegisterFunction(BuiltinFunctions &set) {
	vector<LogicalType> args = {LogicalType::STRUCT({}), LogicalType::VARCHAR};
	ScalarFunction fun("struct_extract", args, LogicalType::ANY,
	    StructExtractFunction, false, StructExtractBind);
	set.AddFunction(fun);
}

} // namespace duckdb
