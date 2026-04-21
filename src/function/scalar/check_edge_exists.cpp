#include "function/scalar/nested_functions.hpp"
#include "common/types/data_chunk.hpp"
#include "planner/expression/bound_function_expression.hpp"
#include "planner/expression/bound_constant_expression.hpp"
#include "main/client_context.hpp"
#include "main/database.hpp"
#include "storage/graph_storage_wrapper.hpp"
#include "storage/extent/adjlist_iterator.hpp"
#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/graph_catalog_entry.hpp"
#include "catalog/catalog_entry/partition_catalog_entry.hpp"
#include "common/exception.hpp"

#include <unordered_set>

namespace duckdb {
using namespace turbolynx;

enum class PatternEdgeDirection : uint8_t { OUTGOING, INCOMING, BOTH };

struct AdjScanCache {
    vector<int> adj_col_idxs;
    vector<ExpandDirection> scan_dirs;
    vector<uint16_t> src_partition_ids;
    mutable vector<AdjacencyListIterator *> iters;
    mutable vector<ExtentID> prev_eids;
    mutable bool iters_initialized = false;

    ~AdjScanCache() {
        for (auto *it : iters) {
            delete it;
        }
    }

    void InitIterators() const {
        if (iters_initialized) {
            return;
        }
        for (size_t i = 0; i < adj_col_idxs.size(); i++) {
            iters.push_back(new AdjacencyListIterator());
            prev_eids.push_back((ExtentID)-1);
        }
        iters_initialized = true;
    }
};

PatternEdgeDirection ParsePatternDirection(const Value &value) {
    string direction = value.GetValue<string>();
    if (direction == "OUT") {
        return PatternEdgeDirection::OUTGOING;
    }
    if (direction == "IN") {
        return PatternEdgeDirection::INCOMING;
    }
    if (direction == "BOTH") {
        return PatternEdgeDirection::BOTH;
    }
    throw InternalException("Unknown pattern edge direction: %s", direction.c_str());
}

static bool ShouldScanAdjType(PatternEdgeDirection direction, LogicalType adj_type) {
    if (direction == PatternEdgeDirection::BOTH) {
        return adj_type == LogicalType::FORWARD_ADJLIST ||
               adj_type == LogicalType::BACKWARD_ADJLIST;
    }
    if (direction == PatternEdgeDirection::OUTGOING) {
        return adj_type == LogicalType::FORWARD_ADJLIST;
    }
    return adj_type == LogicalType::BACKWARD_ADJLIST;
}

static ExpandDirection AdjTypeToExpandDirection(LogicalType adj_type) {
    if (adj_type == LogicalType::FORWARD_ADJLIST) {
        return ExpandDirection::OUTGOING;
    }
    if (adj_type == LogicalType::BACKWARD_ADJLIST) {
        return ExpandDirection::INCOMING;
    }
    throw InternalException("Unexpected adjacency logical type");
}

static void ResolveAdjCols(duckdb::ClientContext &context, iTbgppGraphStorageWrapper *gs,
                           const string &label, PatternEdgeDirection direction,
                           AdjScanCache &cache) {
    auto &catalog = context.db->GetCatalog();
    auto *gcat = (GraphCatalogEntry *)catalog.GetEntry(
        context, CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, DEFAULT_GRAPH);
    if (!gcat) return;
    for (auto ep_oid : gcat->edge_partitions) {
        auto *epart = static_cast<PartitionCatalogEntry *>(
            catalog.GetEntry(context, DEFAULT_SCHEMA, (idx_t)ep_oid, true));
        if (epart && epart->name.find(label) != string::npos) {
            auto *adj_idx_oids = epart->GetAdjIndexOidVec();
            if (adj_idx_oids) {
                for (auto idx_oid : *adj_idx_oids) {
                    vector<int> cols; vector<LogicalType> types;
                    gs->getAdjColIdxs((idx_t)idx_oid, cols, types);
                    auto src_partition_id = gs->getAdjListSrcPartitionId((idx_t)idx_oid);
                    for (idx_t i = 0; i < cols.size(); i++) {
                        if (!ShouldScanAdjType(direction, types[i])) {
                            continue;
                        }
                        cache.adj_col_idxs.push_back(cols[i]);
                        cache.scan_dirs.push_back(AdjTypeToExpandDirection(types[i]));
                        cache.src_partition_ids.push_back(src_partition_id);
                    }
                }
            }
        }
    }
}

static bool AdjListContains(iTbgppGraphStorageWrapper *graph_storage, AdjScanCache &cache,
                            size_t cache_idx, uint64_t src_vid, uint64_t tgt_vid) {
    if (graph_storage->getNodePartitionId(src_vid) != cache.src_partition_ids[cache_idx]) {
        return false;
    }
    uint64_t *start_ptr = nullptr;
    uint64_t *end_ptr = nullptr;
    auto expand_dir = cache.scan_dirs[cache_idx];
    auto &iter = *cache.iters[cache_idx];
    auto &prev_eid = cache.prev_eids[cache_idx];

    graph_storage->getAdjListFromVid(iter, cache.adj_col_idxs[cache_idx], prev_eid, src_vid,
                                     start_ptr, end_ptr, expand_dir);
    if (!start_ptr || !end_ptr) {
        return false;
    }
    for (uint64_t *p = start_ptr; p < end_ptr; p += 2) {
        if (*p == tgt_vid) {
            return true;
        }
    }
    return false;
}

static bool CheckAdjacency(iTbgppGraphStorageWrapper *graph_storage, AdjScanCache &cache,
                           uint64_t src_vid, uint64_t tgt_vid) {
    cache.InitIterators();
    for (size_t ai = 0; ai < cache.adj_col_idxs.size(); ai++) {
        if (AdjListContains(graph_storage, cache, ai, src_vid, tgt_vid)) {
            return true;
        }
    }
    return false;
}

static void CollectAdjNeighbors(iTbgppGraphStorageWrapper *graph_storage, AdjScanCache &cache,
                                size_t cache_idx, uint64_t vid, unordered_set<uint64_t> &neighbors) {
    if (graph_storage->getNodePartitionId(vid) != cache.src_partition_ids[cache_idx]) {
        return;
    }
    uint64_t *start_ptr = nullptr;
    uint64_t *end_ptr = nullptr;
    auto expand_dir = cache.scan_dirs[cache_idx];
    auto &iter = *cache.iters[cache_idx];
    auto &prev_eid = cache.prev_eids[cache_idx];

    graph_storage->getAdjListFromVid(iter, cache.adj_col_idxs[cache_idx], prev_eid, vid,
                                     start_ptr, end_ptr, expand_dir);
    if (!start_ptr || !end_ptr) {
        return;
    }
    for (uint64_t *p = start_ptr; p < end_ptr; p += 2) {
        neighbors.insert(*p);
    }
}

static void CollectNeighbors(iTbgppGraphStorageWrapper *graph_storage, AdjScanCache &cache,
                             uint64_t vid, unordered_set<uint64_t> &neighbors) {
    cache.InitIterators();
    for (size_t ai = 0; ai < cache.adj_col_idxs.size(); ai++) {
        CollectAdjNeighbors(graph_storage, cache, ai, vid, neighbors);
    }
}

struct CheckEdgeExistsBindData : public FunctionData {
    iTbgppGraphStorageWrapper *graph_storage = nullptr;
    PatternEdgeDirection direction = PatternEdgeDirection::OUTGOING;
    AdjScanCache adj_cache;

    unique_ptr<FunctionData> Copy() override {
        auto copy = make_unique<CheckEdgeExistsBindData>();
        copy->graph_storage = graph_storage;
        copy->direction = direction;
        copy->adj_cache.adj_col_idxs = adj_cache.adj_col_idxs;
        copy->adj_cache.scan_dirs = adj_cache.scan_dirs;
        copy->adj_cache.src_partition_ids = adj_cache.src_partition_ids;
        return copy;
    }
};

static void CheckEdgeExistsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &func_expr = (BoundFunctionExpression &)state.expr;
    auto &bind_data = (CheckEdgeExistsBindData &)*func_expr.bind_info;

    auto &src_vec = args.data[2];
    auto &tgt_vec = args.data[3];
    idx_t count = args.size();

    result.SetVectorType(VectorType::FLAT_VECTOR);
    auto result_data = FlatVector::GetData<bool>(result);

    for (idx_t i = 0; i < count; i++) {
        auto src_val = src_vec.GetValue(i);
        auto tgt_val = tgt_vec.GetValue(i);

        if (src_val.IsNull() || tgt_val.IsNull()) {
            result_data[i] = false;
            continue;
        }

        uint64_t src_vid = src_val.GetValue<uint64_t>();
        uint64_t tgt_vid = tgt_val.GetValue<uint64_t>();
        switch (bind_data.direction) {
        case PatternEdgeDirection::OUTGOING:
            result_data[i] = CheckAdjacency(bind_data.graph_storage, bind_data.adj_cache,
                                            src_vid, tgt_vid);
            break;
        case PatternEdgeDirection::INCOMING:
            result_data[i] = CheckAdjacency(bind_data.graph_storage, bind_data.adj_cache,
                                            tgt_vid, src_vid);
            break;
        case PatternEdgeDirection::BOTH:
            result_data[i] =
                CheckAdjacency(bind_data.graph_storage, bind_data.adj_cache, src_vid, tgt_vid) ||
                CheckAdjacency(bind_data.graph_storage, bind_data.adj_cache, tgt_vid, src_vid);
            break;
        default:
            throw InternalException("Unexpected 1-hop pattern direction");
        }
    }
}

static unique_ptr<FunctionData> CheckEdgeExistsBind(duckdb::ClientContext &context,
    ScalarFunction &bound_function, vector<unique_ptr<Expression>> &arguments) {
    auto data = make_unique<CheckEdgeExistsBindData>();
    data->graph_storage = context.graph_storage_wrapper.get();
    if (arguments[1]->type == ExpressionType::VALUE_CONSTANT) {
        auto &const_expr = (BoundConstantExpression &)*arguments[1];
        data->direction = ParsePatternDirection(const_expr.value);
    } else {
        throw InternalException("__check_edge_exists direction must be a constant");
    }
    if (arguments[0]->type == ExpressionType::VALUE_CONSTANT) {
        data->adj_cache.adj_col_idxs.clear();
        data->adj_cache.scan_dirs.clear();
        data->adj_cache.src_partition_ids.clear();
        auto &const_expr = (BoundConstantExpression &)*arguments[0];
        string edge_label = const_expr.value.GetValue<string>();
        ResolveAdjCols(context, data->graph_storage, edge_label,
                       PatternEdgeDirection::OUTGOING, data->adj_cache);
    }
    return data;
}

struct Check2HopBindData : public FunctionData {
    iTbgppGraphStorageWrapper *graph_storage = nullptr;
    PatternEdgeDirection direction_1 = PatternEdgeDirection::OUTGOING;
    PatternEdgeDirection direction_2 = PatternEdgeDirection::OUTGOING;
    AdjScanCache adj_cache_1;
    AdjScanCache adj_cache_2;

    unique_ptr<FunctionData> Copy() override {
        auto copy = make_unique<Check2HopBindData>();
        copy->graph_storage = graph_storage;
        copy->direction_1 = direction_1;
        copy->direction_2 = direction_2;
        copy->adj_cache_1.adj_col_idxs = adj_cache_1.adj_col_idxs;
        copy->adj_cache_1.scan_dirs = adj_cache_1.scan_dirs;
        copy->adj_cache_1.src_partition_ids = adj_cache_1.src_partition_ids;
        copy->adj_cache_2.adj_col_idxs = adj_cache_2.adj_col_idxs;
        copy->adj_cache_2.scan_dirs = adj_cache_2.scan_dirs;
        copy->adj_cache_2.src_partition_ids = adj_cache_2.src_partition_ids;
        return copy;
    }
};

static void Check2HopExistsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &bind_data = (Check2HopBindData &)*((BoundFunctionExpression &)state.expr).bind_info;
    auto &src_vec = args.data[4];
    auto &tgt_vec = args.data[5];
    idx_t count = args.size();
    result.SetVectorType(VectorType::FLAT_VECTOR);
    auto result_data = FlatVector::GetData<bool>(result);

    for (idx_t i = 0; i < count; i++) {
        auto src_val = src_vec.GetValue(i);
        auto tgt_val = tgt_vec.GetValue(i);
        if (src_val.IsNull() || tgt_val.IsNull()) {
            result_data[i] = false;
            continue;
        }

        uint64_t src_vid = src_val.GetValue<uint64_t>();
        uint64_t tgt_vid = tgt_val.GetValue<uint64_t>();
        unordered_set<uint64_t> src_neighbors;
        CollectNeighbors(bind_data.graph_storage, bind_data.adj_cache_1,
                         src_vid, src_neighbors);
        if (src_neighbors.empty()) {
            result_data[i] = false;
            continue;
        }

        unordered_set<uint64_t> tgt_neighbors;
        CollectNeighbors(bind_data.graph_storage, bind_data.adj_cache_2,
                         tgt_vid, tgt_neighbors);
        bool found = false;
        for (auto neighbor : tgt_neighbors) {
            if (src_neighbors.count(neighbor)) {
                found = true;
                break;
            }
        }
        result_data[i] = found;
    }
}

static unique_ptr<FunctionData> Check2HopBind(duckdb::ClientContext &context,
    ScalarFunction &bound_function, vector<unique_ptr<Expression>> &arguments) {
    auto data = make_unique<Check2HopBindData>();
    data->graph_storage = context.graph_storage_wrapper.get();
    if (arguments[1]->type == ExpressionType::VALUE_CONSTANT) {
        data->direction_1 = ParsePatternDirection(
            ((BoundConstantExpression &)*arguments[1]).value);
    } else {
        throw InternalException("__check_2hop_exists first direction must be a constant");
    }
    if (arguments[3]->type == ExpressionType::VALUE_CONSTANT) {
        data->direction_2 = ParsePatternDirection(
            ((BoundConstantExpression &)*arguments[3]).value);
    } else {
        throw InternalException("__check_2hop_exists second direction must be a constant");
    }
    if (arguments[0]->type == ExpressionType::VALUE_CONSTANT) {
        string label1 = ((BoundConstantExpression &)*arguments[0]).value.GetValue<string>();
        ResolveAdjCols(context, data->graph_storage, label1, data->direction_1, data->adj_cache_1);
    }
    if (arguments[2]->type == ExpressionType::VALUE_CONSTANT) {
        string label2 = ((BoundConstantExpression &)*arguments[2]).value.GetValue<string>();
        ResolveAdjCols(context, data->graph_storage, label2, data->direction_2, data->adj_cache_2);
    }
    return data;
}

void CheckEdgeExistsFun::RegisterFunction(BuiltinFunctions &set) {
    ScalarFunctionSet check_edge("__check_edge_exists");
    check_edge.AddFunction(ScalarFunction(
        {LogicalType::VARCHAR, LogicalType::VARCHAR,
         LogicalType::UBIGINT, LogicalType::UBIGINT},
        LogicalType::BOOLEAN, CheckEdgeExistsFunction, false, false,
        CheckEdgeExistsBind));
    set.AddFunction(check_edge);

    ScalarFunctionSet check_2hop("__check_2hop_exists");
    check_2hop.AddFunction(ScalarFunction(
        {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
         LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::UBIGINT},
        LogicalType::BOOLEAN, Check2HopExistsFunction, false, false,
        Check2HopBind));
    set.AddFunction(check_2hop);
}

} // namespace duckdb
