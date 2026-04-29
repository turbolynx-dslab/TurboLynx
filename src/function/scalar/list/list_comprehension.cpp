//===----------------------------------------------------------------------===//
//                         DuckDB
//
// src/function/scalar/list/list_comprehension.cpp
//
//===----------------------------------------------------------------------===//

#include "function/scalar/nested_functions.hpp"

#include "common/types/data_chunk.hpp"
#include "execution/expression_executor.hpp"
#include "planner/expression/bound_function_expression.hpp"

namespace duckdb {

struct ListComprehensionLocalState : public FunctionData {
	explicit ListComprehensionLocalState(ListComprehensionBindData &bind)
	    : filter_result(LogicalType::BOOLEAN), map_result(ListType::GetChildType(bind.return_type)) {
		local_chunk.Initialize(bind.input_types, 1);
		if (bind.filter_expr) {
			filter_executor.AddExpression(*bind.filter_expr);
		}
		if (bind.mapping_expr) {
			map_executor.AddExpression(*bind.mapping_expr);
		}
	}

	unique_ptr<FunctionData> Copy() override {
		return nullptr;
	}

	DataChunk local_chunk;
	ExpressionExecutor filter_executor;
	ExpressionExecutor map_executor;
	Vector filter_result;
	Vector map_result;
};

static unique_ptr<FunctionData> InitListComprehensionLocalState(
    const BoundFunctionExpression &expr, FunctionData *bind_data) {
	auto &bind = (ListComprehensionBindData &)*bind_data;
	return make_unique<ListComprehensionLocalState>(bind);
}

static bool ListComprehensionPassesFilter(ListComprehensionBindData &bind,
                                          ListComprehensionLocalState &local) {
	if (!bind.filter_expr) {
		return true;
	}
	local.filter_executor.ExecuteExpression(local.local_chunk, local.filter_result);
	auto filter_value = local.filter_result.GetValue(0);
	return !filter_value.IsNull() && filter_value.GetValue<bool>();
}

static Value ListComprehensionMapValue(ListComprehensionBindData &bind,
                                       ListComprehensionLocalState &local,
                                       const Value &element,
                                       const LogicalType &result_child_type) {
	if (!bind.mapping_expr) {
		return element.CastAs(result_child_type);
	}
	local.map_executor.ExecuteExpression(local.local_chunk, local.map_result);
	return local.map_result.GetValue(0).CastAs(result_child_type);
}

static void ListComprehensionFunction(DataChunk &args, ExpressionState &state,
                                      Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &bind = (ListComprehensionBindData &)*func_expr.bind_info;
	auto &local = (ListComprehensionLocalState &)*ExecuteFunctionState::GetFunctionState(state);
	auto &result_child_type = ListType::GetChildType(result.GetType());

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto entries = FlatVector::GetData<list_entry_t>(result);
	auto &mask = FlatVector::Validity(result);

	for (idx_t row = 0; row < args.size(); row++) {
		entries[row].offset = ListVector::GetListSize(result);
		entries[row].length = 0;

		auto source_value = args.data[0].GetValue(row);
		if (source_value.IsNull()) {
			mask.SetInvalid(row);
			continue;
		}
		if (source_value.type().id() != LogicalTypeId::LIST) {
			throw InvalidInputException("List comprehension source must be a LIST");
		}

		auto &children = ListValue::GetChildren(source_value);
		for (auto &element : children) {
			local.local_chunk.Reset();
			local.local_chunk.SetValue(0, 0, element.CastAs(bind.input_types[0]));
			for (idx_t col = 1; col < bind.input_types.size(); col++) {
				local.local_chunk.SetValue(col, 0, args.data[col].GetValue(row).CastAs(bind.input_types[col]));
			}
			local.local_chunk.SetCardinality(1);

			if (!ListComprehensionPassesFilter(bind, local)) {
				continue;
			}
			auto mapped = ListComprehensionMapValue(bind, local, element, result_child_type);
			ListVector::PushBack(result, mapped);
			entries[row].length++;
		}
	}
	result.Verify(args.size());
}

void ListComprehensionFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunction fun("__list_comprehension", {LogicalType::ANY, LogicalType::BIGINT},
	                   LogicalType::LIST(LogicalType::ANY), ListComprehensionFunction,
	                   false, nullptr, nullptr, nullptr, InitListComprehensionLocalState);
	fun.varargs = LogicalType::ANY;
	set.AddFunction(fun);
}

} // namespace duckdb
