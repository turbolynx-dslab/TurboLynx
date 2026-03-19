#include "function/scalar/nested_functions.hpp"
#include "common/types/data_chunk.hpp"
#include "common/types/vector.hpp"
#include "planner/expression/bound_function_expression.hpp"

namespace duckdb {

static void StructPackFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &ret_type = func_expr.return_type;

	auto &children = StructVector::GetEntries(result);
	if (children.size() == args.ColumnCount()) {
		// Fast path: result vector has correct number of children
		for (idx_t i = 0; i < args.ColumnCount(); i++) {
			children[i]->Reference(args.data[i]);
		}
	} else {
		// Result vector has bare STRUCT() — reinitialize with correct type.
		// Must update both the type AND auxiliary buffer.
		result.SetType(ret_type);
		auto struct_buffer = make_unique<VectorStructBuffer>(ret_type);
		result.SetAuxiliary(move(struct_buffer));
		auto &new_children = StructVector::GetEntries(result);
		for (idx_t i = 0; i < args.ColumnCount() && i < new_children.size(); i++) {
			new_children[i]->Reference(args.data[i]);
		}
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
