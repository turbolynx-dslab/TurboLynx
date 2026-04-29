//===----------------------------------------------------------------------===//
//                         DuckDB
//
// src/function/scalar/nested_functions.cpp
//
//
//===----------------------------------------------------------------------===//

#include "function/scalar/nested_functions.hpp"

#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/graph_catalog_entry.hpp"
#include "catalog/catalog_entry/partition_catalog_entry.hpp"
#include "common/string_util.hpp"
#include "main/client_context.hpp"
#include "main/database.hpp"
#include "planner/expression/bound_function_expression.hpp"

namespace duckdb {

struct GraphMetaBindData : public FunctionData {
	ClientContext *context = nullptr;

	unique_ptr<FunctionData> Copy() override {
		auto copy = make_unique<GraphMetaBindData>();
		copy->context = context;
		return copy;
	}
};

static unique_ptr<FunctionData> BindGraphMetaFunction(
    ClientContext &context, ScalarFunction &bound_function,
    vector<unique_ptr<Expression>> &arguments) {
	auto bind = make_unique<GraphMetaBindData>();
	bind->context = &context;
	return bind;
}

enum class GraphMetaEntityKind : uint8_t { NODE, REL };

static uint64_t GetLogicalIdFromValue(const Value &value) {
	if (value.IsNull()) {
		return 0;
	}
	switch (value.type().id()) {
	case LogicalTypeId::ID:
	case LogicalTypeId::UBIGINT:
		return value.GetValue<uint64_t>();
	case LogicalTypeId::BIGINT:
		return (uint64_t)value.GetValue<int64_t>();
	default:
		return 0;
	}
}

static PartitionCatalogEntry *FindPartitionByLogicalPid(ClientContext &context,
                                                        uint16_t logical_pid) {
	auto &catalog = context.db->GetCatalog();
	auto *gcat = (GraphCatalogEntry *)catalog.GetEntry(
	    context, CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, DEFAULT_GRAPH, true);
	if (!gcat) {
		return nullptr;
	}
	auto find_in = [&](auto *partition_oids) -> PartitionCatalogEntry * {
		if (!partition_oids) {
			return nullptr;
		}
		for (auto part_oid : *partition_oids) {
			auto *part = (PartitionCatalogEntry *)catalog.GetEntry(
			    context, DEFAULT_SCHEMA, part_oid, true);
			if (part && part->GetPartitionID() == logical_pid) {
				return part;
			}
		}
		return nullptr;
	};
	if (auto *part = find_in(gcat->GetVertexPartitionOids())) {
		return part;
	}
	return find_in(gcat->GetEdgePartitionOids());
}

static string FormatStringList(const vector<string> &items) {
	string result = "[";
	for (idx_t i = 0; i < items.size(); i++) {
		if (i > 0) {
			result += ", ";
		}
		result += "\"" + items[i] + "\"";
	}
	result += "]";
	return result;
}

static vector<string> ExtractPartitionLabels(const string &partition_name) {
	if (partition_name.rfind(DEFAULT_VERTEX_PARTITION_PREFIX, 0) != 0) {
		return {};
	}
	auto labelset =
	    partition_name.substr(std::strlen(DEFAULT_VERTEX_PARTITION_PREFIX));
	if (labelset.empty()) {
		return {};
	}
	return StringUtil::Split(labelset, ":");
}

static uint64_t ResolveCurrentEntityPid(GraphMetaBindData &bind,
                                        uint64_t logical_id,
                                        const InsertBuffer **buf = nullptr,
                                        idx_t *row_idx = nullptr) {
	if (!bind.context || !bind.context->db || logical_id == 0) {
		if (buf) {
			*buf = nullptr;
		}
		if (row_idx) {
			*row_idx = 0;
		}
		return 0;
	}
	uint64_t current_pid = 0;
	const InsertBuffer *current_buf = nullptr;
	idx_t current_row = 0;
	if (bind.context->db->delta_store.TryGetCurrentDeltaRowByLogicalId(
	        logical_id, current_pid, current_buf, current_row)) {
		if (buf) {
			*buf = current_buf;
		}
		if (row_idx) {
			*row_idx = current_row;
		}
		return current_pid;
	}
	current_pid = bind.context->db->delta_store.ResolvePid(logical_id);
	if (buf) {
		*buf = nullptr;
	}
	if (row_idx) {
		*row_idx = 0;
	}
	return current_pid;
}

static uint64_t ResolveCurrentEntityPidByUserId(GraphMetaBindData &bind,
                                                uint64_t user_id,
                                                const InsertBuffer **buf = nullptr,
                                                idx_t *row_idx = nullptr) {
	if (!bind.context || !bind.context->db || user_id == 0) {
		if (buf) {
			*buf = nullptr;
		}
		if (row_idx) {
			*row_idx = 0;
		}
		return 0;
	}
	for (auto &[extent_id, candidate] :
	     bind.context->db->delta_store.insert_buffers_exposed()) {
		int id_key_idx = candidate.FindKeyIndex("id");
		if (id_key_idx < 0) {
			continue;
		}
		for (idx_t i = 0; i < candidate.Size(); i++) {
			if (!candidate.IsValid(i)) {
				continue;
			}
			auto &row = candidate.GetRow(i);
			if ((idx_t)id_key_idx >= row.size() || row[id_key_idx].IsNull()) {
				continue;
			}
			auto row_user_id = GetLogicalIdFromValue(row[id_key_idx]);
			if (row_user_id != user_id) {
				continue;
			}
			if (buf) {
				*buf = &candidate;
			}
			if (row_idx) {
				*row_idx = i;
			}
			return MakePhysicalId((uint32_t)extent_id, (uint32_t)i);
		}
	}
	if (buf) {
		*buf = nullptr;
	}
	if (row_idx) {
		*row_idx = 0;
	}
	return 0;
}

static bool IsVertexPartition(PartitionCatalogEntry *part) {
	return part &&
	       part->GetName().rfind(DEFAULT_VERTEX_PARTITION_PREFIX, 0) == 0;
}

static bool IsEdgePartition(PartitionCatalogEntry *part) {
	return part &&
	       part->GetName().rfind(DEFAULT_EDGE_PARTITION_PREFIX, 0) == 0;
}

static bool MatchesEntityKind(PartitionCatalogEntry *part,
                              GraphMetaEntityKind kind) {
	switch (kind) {
	case GraphMetaEntityKind::NODE:
		return IsVertexPartition(part);
	case GraphMetaEntityKind::REL:
		return IsEdgePartition(part);
	default:
		return false;
	}
}

static uint64_t ResolveCurrentEntityPidForMetaInput(
    GraphMetaBindData &bind, uint64_t input_id, GraphMetaEntityKind kind,
    const InsertBuffer **buf = nullptr, idx_t *row_idx = nullptr) {
	auto pid = ResolveCurrentEntityPid(bind, input_id, buf, row_idx);
	// PLAN.md Bug A: pid == 0 is the legitimate (extent 0, row 0) disk slot
	// the first checkpointed row of the first vertex partition lands on. The
	// previous `pid != 0` gate skipped the partition lookup for that row,
	// causing labels()/keys() to fall straight through to the user-id
	// fallback (which fails because freshly bootstrapped nodes have no `id`
	// field) and return an empty list. Gate on the lid's liveness instead so
	// pid == 0 keeps driving the partition_id extraction.
	bool lid_alive = bind.context && bind.context->db && input_id != 0 &&
	                 !bind.context->db->delta_store.IsLogicalIdDeleted(input_id);
	if (lid_alive) {
		auto logical_pid = (uint16_t)(((uint32_t)(pid >> 32)) >> 16);
		auto *part = FindPartitionByLogicalPid(*bind.context, logical_pid);
		if (MatchesEntityKind(part, kind)) {
			return pid;
		}
	}
	if (kind == GraphMetaEntityKind::NODE) {
		auto fallback_pid =
		    ResolveCurrentEntityPidByUserId(bind, input_id, buf, row_idx);
		if (fallback_pid != 0) {
			auto logical_pid = (uint16_t)(((uint32_t)(fallback_pid >> 32)) >> 16);
			auto *part = FindPartitionByLogicalPid(*bind.context, logical_pid);
			if (MatchesEntityKind(part, kind)) {
				return fallback_pid;
			}
		}
	}
	if (buf) {
		*buf = nullptr;
	}
	if (row_idx) {
		*row_idx = 0;
	}
	return 0;
}

static PartitionCatalogEntry *ResolveCurrentPartition(GraphMetaBindData &bind,
                                                      uint64_t logical_id,
                                                      GraphMetaEntityKind kind) {
	auto current_pid = ResolveCurrentEntityPidForMetaInput(bind, logical_id, kind);
	// PLAN.md Bug A: pid == 0 is the legitimate (extent 0, row 0) disk slot.
	// Treat the lookup as failed only when the lid is dead, otherwise extract
	// partition_id from the (possibly zero) pid.
	bool lid_alive = bind.context && bind.context->db && logical_id != 0 &&
	                 !bind.context->db->delta_store.IsLogicalIdDeleted(logical_id);
	if (!lid_alive) {
		return nullptr;
	}
	auto logical_pid = (uint16_t)(((uint32_t)(current_pid >> 32)) >> 16);
	return FindPartitionByLogicalPid(*bind.context, logical_pid);
}

static vector<string> ResolveCurrentEntityKeys(GraphMetaBindData &bind,
                                               uint64_t logical_id,
                                               GraphMetaEntityKind kind) {
	if (!bind.context || !bind.context->db || logical_id == 0) {
		return {};
	}

	const InsertBuffer *buf = nullptr;
	idx_t row_idx = 0;
	auto current_pid = ResolveCurrentEntityPidForMetaInput(bind, logical_id, kind,
	                                                       &buf, &row_idx);
	if (current_pid != 0 && buf && buf->IsValid(row_idx)) {
		auto &schema_keys = buf->GetSchemaKeys();
		if (!schema_keys.empty()) {
			vector<string> live_keys;
			auto &row = buf->GetRow(row_idx);
			live_keys.reserve(schema_keys.size());
			for (idx_t i = 0; i < schema_keys.size(); i++) {
				if (i < row.size() && !row[i].IsNull()) {
					live_keys.push_back(schema_keys[i]);
				}
			}
			if (!live_keys.empty()) {
				return live_keys;
			}
			return schema_keys;
		}
	}

	auto *part = ResolveCurrentPartition(bind, logical_id, kind);
	if (!part) {
		return {};
	}
	auto *names = part->GetUniversalPropertyKeyNames();
	if (!names) {
		return {};
	}
	return vector<string>(names->begin(), names->end());
}

static void NodeLabelsFunction(DataChunk &args, ExpressionState &state,
                               Vector &result) {
	auto &bind =
	    (GraphMetaBindData &)*((BoundFunctionExpression &)state.expr).bind_info;
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto count = args.size();
	auto *result_data = FlatVector::GetData<string_t>(result);
	auto &mask = FlatVector::Validity(result);

	for (idx_t row = 0; row < count; row++) {
		auto logical_id = GetLogicalIdFromValue(args.data[0].GetValue(row));
		auto *part =
		    ResolveCurrentPartition(bind, logical_id, GraphMetaEntityKind::NODE);
		auto labels =
		    part ? ExtractPartitionLabels(part->GetName()) : vector<string>{};
		result_data[row] =
		    StringVector::AddString(result, FormatStringList(labels));
		if (logical_id == 0) {
			mask.SetInvalid(row);
		}
	}
}

static void EntityKeysNodeFunction(DataChunk &args, ExpressionState &state,
                                   Vector &result) {
	auto &bind =
	    (GraphMetaBindData &)*((BoundFunctionExpression &)state.expr).bind_info;
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto count = args.size();
	auto *result_data = FlatVector::GetData<string_t>(result);
	auto &mask = FlatVector::Validity(result);

	for (idx_t row = 0; row < count; row++) {
		auto logical_id = GetLogicalIdFromValue(args.data[0].GetValue(row));
		auto keys =
		    ResolveCurrentEntityKeys(bind, logical_id, GraphMetaEntityKind::NODE);
		result_data[row] = StringVector::AddString(result, FormatStringList(keys));
		if (logical_id == 0) {
			mask.SetInvalid(row);
		}
	}
}

static void EntityKeysRelFunction(DataChunk &args, ExpressionState &state,
                                  Vector &result) {
	auto &bind =
	    (GraphMetaBindData &)*((BoundFunctionExpression &)state.expr).bind_info;
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto count = args.size();
	auto *result_data = FlatVector::GetData<string_t>(result);
	auto &mask = FlatVector::Validity(result);

	for (idx_t row = 0; row < count; row++) {
		auto logical_id = GetLogicalIdFromValue(args.data[0].GetValue(row));
		auto keys =
		    ResolveCurrentEntityKeys(bind, logical_id, GraphMetaEntityKind::REL);
		result_data[row] = StringVector::AddString(result, FormatStringList(keys));
		if (logical_id == 0) {
			mask.SetInvalid(row);
		}
	}
}

void NodeLabelsFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet funcs("__tl_node_labels");
	funcs.AddFunction(ScalarFunction({LogicalType::ID}, LogicalType::VARCHAR,
	                                 NodeLabelsFunction, false, false,
	                                 BindGraphMetaFunction));
	funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT},
	                                 LogicalType::VARCHAR, NodeLabelsFunction,
	                                 false, false, BindGraphMetaFunction));
	funcs.AddFunction(ScalarFunction({LogicalType::BIGINT},
	                                 LogicalType::VARCHAR, NodeLabelsFunction,
	                                 false, false, BindGraphMetaFunction));
	set.AddFunction(funcs);
}

void EntityKeysFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet node_funcs("__tl_node_keys");
	node_funcs.AddFunction(ScalarFunction({LogicalType::ID}, LogicalType::VARCHAR,
	                                 EntityKeysNodeFunction, false, false,
	                                 BindGraphMetaFunction));
	node_funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT},
	                                 LogicalType::VARCHAR, EntityKeysNodeFunction,
	                                 false, false, BindGraphMetaFunction));
	node_funcs.AddFunction(ScalarFunction({LogicalType::BIGINT},
	                                 LogicalType::VARCHAR, EntityKeysNodeFunction,
	                                 false, false, BindGraphMetaFunction));
	set.AddFunction(node_funcs);

	ScalarFunctionSet rel_funcs("__tl_rel_keys");
	rel_funcs.AddFunction(ScalarFunction({LogicalType::ID}, LogicalType::VARCHAR,
	                                 EntityKeysRelFunction, false, false,
	                                 BindGraphMetaFunction));
	rel_funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT},
	                                 LogicalType::VARCHAR, EntityKeysRelFunction,
	                                 false, false, BindGraphMetaFunction));
	rel_funcs.AddFunction(ScalarFunction({LogicalType::BIGINT},
	                                 LogicalType::VARCHAR, EntityKeysRelFunction,
	                                 false, false, BindGraphMetaFunction));
	set.AddFunction(rel_funcs);
}

// Index-aware variants. Cypher's `labels(n)[i]` / `keys(n)[i]` route through
// the binder to these instead of going through list_extract on the formatted
// string — that path types the result as ANY (PhysicalType::INVALID) and
// crashes downstream. Each function resolves the same partition (or live
// schema for keys()) the parent function does and returns the i-th element
// directly as VARCHAR.

static int64_t AdjustCypherIndex(int64_t idx, idx_t size) {
	if (idx < 0) {
		idx = (int64_t)size + idx;
	}
	return idx;
}

static void NodeLabelAtFunction(DataChunk &args, ExpressionState &state,
                                Vector &result) {
	auto &bind =
	    (GraphMetaBindData &)*((BoundFunctionExpression &)state.expr).bind_info;
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto count = args.size();
	auto *result_data = FlatVector::GetData<string_t>(result);
	auto &mask = FlatVector::Validity(result);

	for (idx_t row = 0; row < count; row++) {
		auto logical_id = GetLogicalIdFromValue(args.data[0].GetValue(row));
		auto *part =
		    ResolveCurrentPartition(bind, logical_id, GraphMetaEntityKind::NODE);
		auto labels =
		    part ? ExtractPartitionLabels(part->GetName()) : vector<string>{};
		auto idx_val = args.data[1].GetValue(row);
		if (logical_id == 0 || idx_val.IsNull() || labels.empty()) {
			mask.SetInvalid(row);
			continue;
		}
		int64_t idx = AdjustCypherIndex(idx_val.GetValue<int64_t>(), labels.size());
		if (idx < 0 || idx >= (int64_t)labels.size()) {
			mask.SetInvalid(row);
			continue;
		}
		result_data[row] = StringVector::AddString(result, labels[(idx_t)idx]);
	}
}

static void EntityKeyAtNodeFunction(DataChunk &args, ExpressionState &state,
                                    Vector &result) {
	auto &bind =
	    (GraphMetaBindData &)*((BoundFunctionExpression &)state.expr).bind_info;
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto count = args.size();
	auto *result_data = FlatVector::GetData<string_t>(result);
	auto &mask = FlatVector::Validity(result);

	for (idx_t row = 0; row < count; row++) {
		auto logical_id = GetLogicalIdFromValue(args.data[0].GetValue(row));
		auto keys =
		    ResolveCurrentEntityKeys(bind, logical_id, GraphMetaEntityKind::NODE);
		auto idx_val = args.data[1].GetValue(row);
		if (logical_id == 0 || idx_val.IsNull() || keys.empty()) {
			mask.SetInvalid(row);
			continue;
		}
		int64_t idx = AdjustCypherIndex(idx_val.GetValue<int64_t>(), keys.size());
		if (idx < 0 || idx >= (int64_t)keys.size()) {
			mask.SetInvalid(row);
			continue;
		}
		result_data[row] = StringVector::AddString(result, keys[(idx_t)idx]);
	}
}

static void EntityKeyAtRelFunction(DataChunk &args, ExpressionState &state,
                                   Vector &result) {
	auto &bind =
	    (GraphMetaBindData &)*((BoundFunctionExpression &)state.expr).bind_info;
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto count = args.size();
	auto *result_data = FlatVector::GetData<string_t>(result);
	auto &mask = FlatVector::Validity(result);

	for (idx_t row = 0; row < count; row++) {
		auto logical_id = GetLogicalIdFromValue(args.data[0].GetValue(row));
		auto keys =
		    ResolveCurrentEntityKeys(bind, logical_id, GraphMetaEntityKind::REL);
		auto idx_val = args.data[1].GetValue(row);
		if (logical_id == 0 || idx_val.IsNull() || keys.empty()) {
			mask.SetInvalid(row);
			continue;
		}
		int64_t idx = AdjustCypherIndex(idx_val.GetValue<int64_t>(), keys.size());
		if (idx < 0 || idx >= (int64_t)keys.size()) {
			mask.SetInvalid(row);
			continue;
		}
		result_data[row] = StringVector::AddString(result, keys[(idx_t)idx]);
	}
}

static void RegisterIdxFn(ScalarFunctionSet &fs, LogicalType id_type,
                          scalar_function_t body) {
	fs.AddFunction(ScalarFunction({id_type, LogicalType::INTEGER},
	                              LogicalType::VARCHAR, body, false, false,
	                              BindGraphMetaFunction));
	fs.AddFunction(ScalarFunction({id_type, LogicalType::BIGINT},
	                              LogicalType::VARCHAR, body, false, false,
	                              BindGraphMetaFunction));
}

void NodeLabelAtFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet funcs("__tl_node_label_at");
	for (auto &id_t : {LogicalType::ID, LogicalType::UBIGINT, LogicalType::BIGINT}) {
		RegisterIdxFn(funcs, id_t, NodeLabelAtFunction);
	}
	set.AddFunction(funcs);
}

void EntityKeyAtFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet node_funcs("__tl_node_key_at");
	for (auto &id_t : {LogicalType::ID, LogicalType::UBIGINT, LogicalType::BIGINT}) {
		RegisterIdxFn(node_funcs, id_t, EntityKeyAtNodeFunction);
	}
	set.AddFunction(node_funcs);

	ScalarFunctionSet rel_funcs("__tl_rel_key_at");
	for (auto &id_t : {LogicalType::ID, LogicalType::UBIGINT, LogicalType::BIGINT}) {
		RegisterIdxFn(rel_funcs, id_t, EntityKeyAtRelFunction);
	}
	set.AddFunction(rel_funcs);
}

void BuiltinFunctions::RegisterNestedFunctions() {
	Register<ArraySliceFun>();
	Register<StructPackFun>();
	Register<StructExtractFun>();
	// Register<ListConcatFun>();
	Register<ListContainsFun>();
	Register<ListPositionFun>();
	// Register<ListAggregateFun>();
	Register<ListValueFun>();
	Register<ListComprehensionFun>();
	// Register<ListApplyFun>();
	// Register<ListFilterFun>();
	Register<ListExtractFun>();
	Register<ListSizeFun>();
	Register<CheckEdgeExistsFun>();
	Register<PathWeightFun>();
	Register<NodeLabelsFun>();
	Register<EntityKeysFun>();
	Register<NodeLabelAtFun>();
	Register<EntityKeyAtFun>();
	Register<ListRangeFun>();
	// Register<ListFlattenFun>();
	// Register<MapFun>();
	// Register<MapExtractFun>();
	// Register<CardinalityFun>();
}

} // namespace duckdb
