#include "function/scalar/nested_functions.hpp"
#include "common/types/data_chunk.hpp"
#include "common/types/vector.hpp"
#include "planner/expression/bound_function_expression.hpp"

namespace duckdb {

static void StructPackFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &ret_type = func_expr.return_type;
	auto &fields = StructType::GetChildTypes(ret_type);

	// Build a properly-typed STRUCT vector and Reference it into result
	fprintf(stderr, "[STRUCT-PACK] ret_type=%s, fields=%zu, args.size=%lu, args.cols=%lu\n",
	    ret_type.ToString().c_str(), fields.size(), args.size(), args.ColumnCount());
	Vector struct_vec(ret_type, args.size());
	fprintf(stderr, "[STRUCT-PACK] struct_vec children=%zu\n",
	    StructVector::GetEntries(struct_vec).size());
	for (idx_t row = 0; row < args.size(); row++) {
		child_list_t<Value> struct_vals;
		for (idx_t col = 0; col < args.ColumnCount(); col++) {
			string name = col < fields.size() ? fields[col].first
			            : "v" + to_string(col + 1);
			struct_vals.push_back({name, args.GetValue(col, row)});
		}
		struct_vec.SetValue(row, Value::STRUCT(std::move(struct_vals)));
	}
	result.Reference(struct_vec);
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
