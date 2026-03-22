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

	// path_nodes(path) — extract node IDs from path [n,e,n,e,...,n] → [n,n,n,...]
	auto path_nodes_func = [](DataChunk &args, ExpressionState &state, Vector &result) {
		auto &path_vec = args.data[0];
		idx_t count = args.size();
		result.SetVectorType(VectorType::FLAT_VECTOR);
		auto &result_mask = FlatVector::Validity(result);
		for (idx_t i = 0; i < count; i++) {
			auto val = path_vec.GetValue(i);
			if (val.IsNull()) { result_mask.SetInvalid(i); continue; }
			auto &children = ListValue::GetChildren(val);
			vector<Value> nodes;
			for (idx_t j = 0; j < children.size(); j += 2) {
				nodes.push_back(children[j]);
			}
			result.SetValue(i, Value::LIST(nodes));
		}
	};
	set.AddFunction(ScalarFunction("path_nodes",
	    {LogicalType::LIST(LogicalType::UBIGINT)},
	    LogicalType::LIST(LogicalType::UBIGINT), path_nodes_func));

	// path_rels(path) — extract edge IDs from path [n,e,n,e,...,n] → [e,e,...]
	auto path_rels_func = [](DataChunk &args, ExpressionState &state, Vector &result) {
		auto &path_vec = args.data[0];
		idx_t count = args.size();
		result.SetVectorType(VectorType::FLAT_VECTOR);
		auto &result_mask = FlatVector::Validity(result);
		for (idx_t i = 0; i < count; i++) {
			auto val = path_vec.GetValue(i);
			if (val.IsNull()) { result_mask.SetInvalid(i); continue; }
			auto &children = ListValue::GetChildren(val);
			vector<Value> rels;
			for (idx_t j = 1; j < children.size(); j += 2) {
				rels.push_back(children[j]);
			}
			result.SetValue(i, Value::LIST(rels));
		}
	};
	// list_sum(list) — sum all numeric elements in a list
	auto list_sum_func = [](DataChunk &args, ExpressionState &state, Vector &result) {
		auto &list_vec = args.data[0];
		idx_t count = args.size();
		result.SetVectorType(VectorType::FLAT_VECTOR);
		auto result_data = FlatVector::GetData<double>(result);
		auto &result_mask = FlatVector::Validity(result);
		for (idx_t i = 0; i < count; i++) {
			auto val = list_vec.GetValue(i);
			if (val.IsNull()) { result_mask.SetInvalid(i); continue; }
			auto &children = ListValue::GetChildren(val);
			double sum = 0.0;
			for (auto &c : children) {
				if (!c.IsNull()) {
					try { sum += c.GetValue<double>(); }
					catch (...) { /* skip non-numeric */ }
				}
			}
			result_data[i] = sum;
		}
	};
	set.AddFunction(ScalarFunction("list_sum",
	    {LogicalType::ANY}, LogicalType::DOUBLE, list_sum_func));

	set.AddFunction(ScalarFunction("path_rels",
	    {LogicalType::LIST(LogicalType::UBIGINT)},
	    LogicalType::LIST(LogicalType::UBIGINT), path_rels_func));
}

} // namespace duckdb
