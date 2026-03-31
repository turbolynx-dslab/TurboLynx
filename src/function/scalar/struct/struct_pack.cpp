#include "function/scalar/nested_functions.hpp"
#include "common/types/data_chunk.hpp"
#include "common/types/vector.hpp"
#include "planner/expression/bound_function_expression.hpp"

namespace duckdb {

static void StructPackFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &ret_type = func_expr.return_type;

	// Build STRUCT type from actual arg types so child_types match child vectors.
	child_list_t<LogicalType> actual_children;
	if (ret_type.id() == LogicalTypeId::STRUCT) {
		auto &declared = StructType::GetChildTypes(ret_type);
		for (idx_t i = 0; i < args.ColumnCount(); i++) {
			string name = (i < declared.size()) ? declared[i].first : "v" + to_string(i + 1);
			actual_children.push_back({name, args.data[i].GetType()});
		}
	}
	auto actual_type = actual_children.empty() ? ret_type : LogicalType::STRUCT(std::move(actual_children));

	result.SetType(actual_type);
	auto struct_buffer = make_unique<VectorStructBuffer>(actual_type);
	result.SetAuxiliary(move(struct_buffer));
	auto &children = StructVector::GetEntries(result);
	for (idx_t i = 0; i < args.ColumnCount() && i < children.size(); i++) {
		children[i]->Reference(args.data[i]);
	}
	result.SetVectorType(VectorType::FLAT_VECTOR);
}

static unique_ptr<FunctionData> StructPackBind(ClientContext &,
    ScalarFunction &bound_function, vector<unique_ptr<Expression>> &arguments) {
	child_list_t<LogicalType> struct_children;
	for (idx_t i = 0; i < arguments.size(); i++) {
		string name = arguments[i]->alias.empty()
		    ? "v" + to_string(i + 1)
		    : arguments[i]->alias;
		struct_children.push_back(make_pair(name, arguments[i]->return_type));
	}
	bound_function.return_type = LogicalType::STRUCT(std::move(struct_children));
	return make_unique<VariableReturnBindData>(bound_function.return_type);
}

void StructPackFun::RegisterFunction(BuiltinFunctions &set) {
	vector<LogicalType> args;
	ScalarFunction fun("struct_pack", args, LogicalType::STRUCT({}),
	    StructPackFunction, false, StructPackBind);
	fun.varargs = LogicalType::ANY;
	set.AddFunction(fun);
}

} // namespace duckdb
