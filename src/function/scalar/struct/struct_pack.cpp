#include "function/scalar/nested_functions.hpp"
#include "common/types/data_chunk.hpp"
#include "common/types/vector.hpp"

namespace duckdb {

static void StructPackFunction(DataChunk &args, ExpressionState &, Vector &result) {
	auto &children = StructVector::GetEntries(result);
	D_ASSERT(children.size() == args.ColumnCount());
	for (idx_t i = 0; i < args.ColumnCount(); i++) {
		children[i]->Reference(args.data[i]);
	}
	result.SetVectorType(VectorType::FLAT_VECTOR);
	result.Verify(args.size());
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
	vector<LogicalType> args;  // varargs — no fixed args
	ScalarFunction fun("struct_pack", args, LogicalType::STRUCT({}),
	    StructPackFunction, false, StructPackBind);
	fun.varargs = LogicalType::ANY;
	set.AddFunction(fun);
}

} // namespace duckdb
