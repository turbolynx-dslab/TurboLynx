//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar/nested_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "function/function_set.hpp"
#include "function/scalar_function.hpp"
#include "planner/expression.hpp"

namespace duckdb {

struct VariableReturnBindData : public FunctionData {
	LogicalType stype;

	explicit VariableReturnBindData(const LogicalType &stype_p) : stype(stype_p) {
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<VariableReturnBindData>(stype);
	}
};

struct ArraySliceFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct StructPackFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct StructExtractFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ListExtractFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ListValueFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ListSizeFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct CheckEdgeExistsFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct PathWeightFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct NodeLabelsFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct EntityKeysFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

// __tl_node_label_at(n, idx) and __tl_entity_key_at(n_or_r, idx) — return the
// idx-th label/key as VARCHAR. Used by the binder to rewrite
// `labels(n)[i]` / `keys(n)[i]` patterns since `labels`/`keys` themselves
// expose a formatted-string return rather than a real LIST(VARCHAR).
struct NodeLabelAtFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct EntityKeyAtFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ListRangeFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

// struct MapFun {
// 	static void RegisterFunction(BuiltinFunctions &set);
// };

// struct MapExtractFun {
// 	static void RegisterFunction(BuiltinFunctions &set);
// };

// struct ListExtractFun {
// 	static void RegisterFunction(BuiltinFunctions &set);
// };

// struct ListConcatFun {
// 	static ScalarFunction GetFunction();
// 	static void RegisterFunction(BuiltinFunctions &set);
// };

struct ListContainsFun {
	static ScalarFunction GetFunction();
	static void RegisterFunction(BuiltinFunctions &set);
};

// struct ListFlattenFun {
// 	static ScalarFunction GetFunction();
// 	static void RegisterFunction(BuiltinFunctions &set);
// };

struct ListPositionFun {
	static ScalarFunction GetFunction();
	static void RegisterFunction(BuiltinFunctions &set);
};

// struct ListAggregateFun {
// 	static ScalarFunction GetFunction();
// 	static void RegisterFunction(BuiltinFunctions &set);
// };

struct ListApplyFun {
	// static ScalarFunction GetFunction();
	// static void RegisterFunction(BuiltinFunctions &set);
};

struct ListFilterFun {
	// static ScalarFunction GetFunction();
	// static void RegisterFunction(BuiltinFunctions &set);
};

struct ListComprehensionBindData : public FunctionData {
	LogicalType return_type;
	vector<LogicalType> input_types;
	unique_ptr<Expression> filter_expr;
	unique_ptr<Expression> mapping_expr;

	ListComprehensionBindData() {
	}

	ListComprehensionBindData(LogicalType return_type_p, vector<LogicalType> input_types_p,
	                          unique_ptr<Expression> filter_expr_p, unique_ptr<Expression> mapping_expr_p)
	    : return_type(std::move(return_type_p)), input_types(std::move(input_types_p)),
	      filter_expr(std::move(filter_expr_p)), mapping_expr(std::move(mapping_expr_p)) {
	}

	unique_ptr<FunctionData> Copy() override {
		auto copy = make_unique<ListComprehensionBindData>();
		copy->return_type = return_type;
		copy->input_types = input_types;
		if (filter_expr) {
			copy->filter_expr = filter_expr->Copy();
		}
		if (mapping_expr) {
			copy->mapping_expr = mapping_expr->Copy();
		}
		return copy;
	}
};

struct ListComprehensionFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

// struct CardinalityFun {
// 	static void RegisterFunction(BuiltinFunctions &set);
// };

// struct StructExtractFun {
// 	static ScalarFunction GetFunction();
// 	static void RegisterFunction(BuiltinFunctions &set);
// };

} // namespace duckdb
