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

// struct CardinalityFun {
// 	static void RegisterFunction(BuiltinFunctions &set);
// };

// struct StructExtractFun {
// 	static ScalarFunction GetFunction();
// 	static void RegisterFunction(BuiltinFunctions &set);
// };

} // namespace duckdb
