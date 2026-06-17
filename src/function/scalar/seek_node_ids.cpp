//===----------------------------------------------------------------------===//
//                         TurboLynx
//
// src/function/scalar/seek_node_ids.cpp
//
// seek_node_ids(list_of_vids) -> LIST(BIGINT)
//
// Maps a list of node VIDs (physical ids, e.g. from `nodes(path)`) to the
// user-facing `id` property of each node. `nodes(path)` lowers to
// `path_nodes(path)` which yields the raw internal VIDs (= identity, fine for
// matching), but when those node ids are OUTPUT (e.g. LDBC IC14/q21's
// `[n in nodes(path) | n.id]`) the user expects the stored `id` property, not
// the internal VID.
//
// The VID encodes its node partition in the high bits, so for each VID we
// decode the partition and seek the stored `id` column. We build the
// per-partition `vid -> id` map lazily (once per query, cached in bind data)
// using the same sequential scan API the engine uses, so all storage tiers
// (base extents + in-memory delta rows + id translation) are handled exactly
// as a normal scan would.
//===----------------------------------------------------------------------===//

#include <queue>
#include <unordered_map>

#include "function/scalar/nested_functions.hpp"
#include "common/types/data_chunk.hpp"
#include "main/client_context.hpp"
#include "main/database.hpp"
#include "storage/graph_storage_wrapper.hpp"
#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/graph_catalog_entry.hpp"
#include "catalog/catalog_entry/partition_catalog_entry.hpp"
#include "catalog/catalog_entry/property_schema_catalog_entry.hpp"

namespace duckdb {
using namespace turbolynx;

namespace {

// Resolved scan plan for a single vertex partition: the PropertySchema oids to
// scan plus, per PS, the physical column index of the `id` property (col 0 is
// always `_id`/VID, so stored properties are at index_in_keys + 1).
struct PartitionIdScanPlan {
    // (ps_oid, id_column_position) pairs. id_column_position == 0 means this PS
    // has no `id` property column.
    vector<std::pair<idx_t, idx_t>> ps_id_cols;
};

struct SeekNodeIdsBindData : public FunctionData {
    ClientContext *context = nullptr;
    iTbgppGraphStorageWrapper *graph_storage = nullptr;
    PropertyKeyID id_key_id = (PropertyKeyID)-1;
    LogicalType id_type = LogicalType::BIGINT;
    // partition logical id (vid >> 48) -> scan plan
    std::unordered_map<uint16_t, PartitionIdScanPlan> plan_by_partition;
    // Lazily-built per-partition maps: partition logical id -> (vid -> id value)
    std::shared_ptr<std::unordered_map<uint16_t,
                                       std::unordered_map<uint64_t, Value>>>
        cache;

    SeekNodeIdsBindData()
        : cache(std::make_shared<
                std::unordered_map<uint16_t,
                                   std::unordered_map<uint64_t, Value>>>()) {}

    unique_ptr<FunctionData> Copy() override {
        auto copy = make_unique<SeekNodeIdsBindData>();
        copy->context = context;
        copy->graph_storage = graph_storage;
        copy->id_key_id = id_key_id;
        copy->id_type = id_type;
        copy->plan_by_partition = plan_by_partition;
        copy->cache = cache;  // share the cache across copies
        return copy;
    }
};

static GraphCatalogEntry *SeekNodeIdsGetGraphCatalog(ClientContext &context) {
    auto &catalog = context.db->GetCatalog();
    return (GraphCatalogEntry *)catalog.GetEntry(
        context, CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, DEFAULT_GRAPH, true);
}

// Build (once) the vid -> id map for one vertex partition by scanning its
// PropertySchemas, reading the {_id, id} columns. Mirrors the engine's
// sequential scan so base + delta rows and id translation are handled.
static const std::unordered_map<uint64_t, Value> &
GetPartitionMap(SeekNodeIdsBindData &bind_data, uint16_t partition_id) {
    auto &cache = *bind_data.cache;
    auto cached = cache.find(partition_id);
    if (cached != cache.end()) {
        return cached->second;
    }

    auto &vid_to_id = cache[partition_id];

    auto plan_it = bind_data.plan_by_partition.find(partition_id);
    if (plan_it == bind_data.plan_by_partition.end()) {
        return vid_to_id;  // unknown partition -> empty map (identity fallback)
    }

    ClientContext &context = *bind_data.context;
    auto *graph_storage = bind_data.graph_storage;

    for (auto &ps_id_col : plan_it->second.ps_id_cols) {
        idx_t ps_oid = ps_id_col.first;
        idx_t id_col_pos = ps_id_col.second;
        if (id_col_pos == 0) {
            continue;  // no id column on this PS
        }

        // Scan {_id (col 0), id (col id_col_pos)} for this PropertySchema.
        vector<idx_t> oids{ps_oid};
        vector<vector<uint64_t>> projection_mapping{{0, (uint64_t)id_col_pos}};
        vector<vector<LogicalType>> scan_schemas{
            {LogicalType::ID, bind_data.id_type}};
        vector<LogicalType> scan_schema{LogicalType::ID, bind_data.id_type};

        std::queue<ExtentIterator *> ext_its;
        graph_storage->InitializeScan(ext_its, oids, projection_mapping,
                                      scan_schemas, /*filter_buffering=*/false);

        while (!ext_its.empty()) {
            DataChunk output;
            output.Initialize(scan_schema);
            auto res = graph_storage->doScan(ext_its, output, scan_schema);
            idx_t count = output.size();
            for (idx_t row = 0; row < count; row++) {
                Value vid_val = output.GetValue(0, row);
                if (vid_val.IsNull()) {
                    continue;
                }
                uint64_t vid = vid_val.GetValue<uint64_t>();
                vid_to_id[vid] = output.GetValue(1, row);
            }
            if (res == StoreAPIResult::DONE && ext_its.empty()) {
                break;
            }
        }
    }

    return vid_to_id;
}

static void SeekNodeIdsFunction(DataChunk &args, ExpressionState &state,
                                Vector &result) {
    auto &bind_data =
        (SeekNodeIdsBindData &)*((BoundFunctionExpression &)state.expr)
            .bind_info;
    auto &list_vec = args.data[0];
    idx_t count = args.size();

    result.SetVectorType(VectorType::FLAT_VECTOR);
    auto &result_mask = FlatVector::Validity(result);

    for (idx_t i = 0; i < count; i++) {
        auto val = list_vec.GetValue(i);
        if (val.IsNull()) {
            result_mask.SetInvalid(i);
            continue;
        }
        auto &children = ListValue::GetChildren(val);
        vector<Value> ids;
        ids.reserve(children.size());
        for (auto &child : children) {
            if (child.IsNull()) {
                ids.push_back(Value(bind_data.id_type));
                continue;
            }
            uint64_t vid = child.GetValue<uint64_t>();
            uint16_t partition_id =
                bind_data.graph_storage->getNodePartitionId(vid);
            const auto &vid_to_id = GetPartitionMap(bind_data, partition_id);
            auto found = vid_to_id.find(vid);
            if (found != vid_to_id.end()) {
                ids.push_back(found->second);
            } else {
                // Fall back to the VID itself (identity) so we never produce a
                // worse answer than before for unmapped nodes.
                ids.push_back(Value::BIGINT((int64_t)vid));
            }
        }
        result.SetValue(i, Value::LIST(bind_data.id_type, ids));
    }
}

static unique_ptr<FunctionData> SeekNodeIdsBind(
    ClientContext &context, ScalarFunction &bound_function,
    vector<unique_ptr<Expression>> &arguments) {
    auto data = make_unique<SeekNodeIdsBindData>();
    data->context = &context;
    data->graph_storage = context.graph_storage_wrapper.get();
    if (!data->graph_storage) {
        throw InvalidInputException("seek_node_ids requires graph storage");
    }

    auto *gcat = SeekNodeIdsGetGraphCatalog(context);
    if (!gcat) {
        throw InvalidInputException("seek_node_ids: graph catalog not found");
    }

    string id_name = "id";
    data->id_key_id = gcat->GetPropertyKeyID(context, id_name);
    if (data->id_key_id != (PropertyKeyID)-1) {
        data->id_type =
            LogicalType(gcat->GetTypeIdFromPropertyKeyID(data->id_key_id));
        if (data->id_type.id() == LogicalTypeId::INVALID ||
            data->id_type.id() == LogicalTypeId::ANY) {
            data->id_type = LogicalType::BIGINT;
        }
    }
    bound_function.return_type = LogicalType::LIST(data->id_type);

    // Pre-resolve, per vertex partition, the PropertySchema oids and the
    // physical column index of the `id` property within each.
    auto &catalog = context.db->GetCatalog();
    for (auto part_oid : gcat->vertex_partitions) {
        auto *part_cat = (PartitionCatalogEntry *)catalog.GetEntry(
            context, DEFAULT_SCHEMA, (idx_t)part_oid, true);
        if (!part_cat) {
            continue;
        }
        uint16_t partition_id = (uint16_t)part_cat->GetPartitionID();
        PropertySchemaID_vector *ps_ids = part_cat->GetPropertySchemaIDs();
        if (!ps_ids) {
            continue;
        }
        PartitionIdScanPlan plan;
        for (auto ps_oid : *ps_ids) {
            auto *ps_cat = (PropertySchemaCatalogEntry *)catalog.GetEntry(
                context, DEFAULT_SCHEMA, (idx_t)ps_oid, true);
            if (!ps_cat) {
                continue;
            }
            idx_t id_col_pos = 0;  // 0 == not present
            if (data->id_key_id != (PropertyKeyID)-1) {
                const PropertyKeyID_vector &keys = ps_cat->property_keys;
                for (size_t k = 0; k < keys.size(); k++) {
                    if ((uint64_t)keys[k] == (uint64_t)data->id_key_id) {
                        id_col_pos = k + 1;  // col 0 is _id
                        break;
                    }
                }
            }
            plan.ps_id_cols.emplace_back((idx_t)ps_oid, id_col_pos);
        }
        data->plan_by_partition[partition_id] = std::move(plan);
    }

    return data;
}

}  // namespace

void SeekNodeIdsFun::RegisterFunction(BuiltinFunctions &set) {
    // Single overload: the argument is always the LIST(UBIGINT) produced by
    // path_nodes(), and a lone LIST(ANY) signature avoids overload-resolution
    // ambiguity in the converter's GetScalarFuncMdId.
    ScalarFunctionSet seek_node_ids("seek_node_ids");
    seek_node_ids.AddFunction(ScalarFunction(
        {LogicalType::LIST(LogicalType::ANY)},
        LogicalType::LIST(LogicalType::BIGINT), SeekNodeIdsFunction, false,
        false, SeekNodeIdsBind));
    set.AddFunction(seek_node_ids);
}

}  // namespace duckdb
