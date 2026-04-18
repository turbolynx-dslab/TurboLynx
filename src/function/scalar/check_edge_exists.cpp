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

namespace duckdb {
using namespace turbolynx;

struct CheckEdgeExistsBindData : public FunctionData {
    iTbgppGraphStorageWrapper *graph_storage = nullptr;
    // Adjacency column indices for each edge partition matching the label
    vector<int> adj_col_idxs;
    // Per-adj-col iterators and prev extent IDs (mutable for execution)
    mutable vector<AdjacencyListIterator *> adj_iters;
    mutable vector<ExtentID> prev_eids;
    mutable bool iters_initialized = false;

    ~CheckEdgeExistsBindData() {
        for (auto *it : adj_iters) delete it;
    }

    void InitIterators() const {
        if (iters_initialized) return;
        for (size_t i = 0; i < adj_col_idxs.size(); i++) {
            adj_iters.push_back(new AdjacencyListIterator());
            prev_eids.push_back((ExtentID)-1);  // sentinel — forces initialization on first use
        }
        iters_initialized = true;
    }

    unique_ptr<FunctionData> Copy() override {
        auto copy = make_unique<CheckEdgeExistsBindData>();
        copy->graph_storage = graph_storage;
        copy->adj_col_idxs = adj_col_idxs;
        // Don't copy iterators — they'll be lazily initialized
        return copy;
    }
};

static void CheckEdgeExistsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &func_expr = (BoundFunctionExpression &)state.expr;
    auto &bind_data = (CheckEdgeExistsBindData &)*func_expr.bind_info;

    auto &src_vec = args.data[1];
    auto &tgt_vec = args.data[2];
    idx_t count = args.size();

    result.SetVectorType(VectorType::FLAT_VECTOR);
    auto result_data = FlatVector::GetData<bool>(result);
    auto &result_mask = FlatVector::Validity(result);

    for (idx_t i = 0; i < count; i++) {
        auto src_val = src_vec.GetValue(i);
        auto tgt_val = tgt_vec.GetValue(i);

        if (src_val.IsNull() || tgt_val.IsNull()) {
            result_data[i] = false;
            continue;
        }

        uint64_t src_vid = src_val.GetValue<uint64_t>();
        uint64_t tgt_vid = tgt_val.GetValue<uint64_t>();

        bool found = false;
        bind_data.InitIterators();

        // Check each adjacency column (edge partition) for this label
        for (size_t ai = 0; ai < bind_data.adj_col_idxs.size() && !found; ai++) {
            int adj_col_idx = bind_data.adj_col_idxs[ai];
            uint64_t *start_ptr = nullptr, *end_ptr = nullptr;

            bind_data.graph_storage->getAdjListFromVid(
                *bind_data.adj_iters[ai], adj_col_idx, bind_data.prev_eids[ai],
                src_vid, start_ptr, end_ptr, ExpandDirection::OUTGOING);

            if (start_ptr && end_ptr) {
                for (uint64_t *p = start_ptr; p < end_ptr; p += 2) {
                    if (*p == tgt_vid) {
                        found = true;
                        break;
                    }
                }
            }
        }

        result_data[i] = found;
    }
}

static unique_ptr<FunctionData> CheckEdgeExistsBind(duckdb::ClientContext &context,
    ScalarFunction &bound_function, vector<unique_ptr<Expression>> &arguments) {

    auto data = make_unique<CheckEdgeExistsBindData>();
    data->graph_storage = context.graph_storage_wrapper.get();

    // Extract edge label from constant argument
    if (arguments[0]->type == ExpressionType::VALUE_CONSTANT) {
        auto &const_expr = (BoundConstantExpression &)*arguments[0];
        string edge_label = const_expr.value.GetValue<string>();

        // Look up adjacency column indices for this edge label
        auto &catalog = context.db->GetCatalog();
        auto *gcat = (GraphCatalogEntry *)catalog.GetEntry(
            context, CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, DEFAULT_GRAPH);
        if (gcat) {
            // Find edge partitions matching the label
            for (auto ep_oid : gcat->edge_partitions) {
                auto *epart = static_cast<PartitionCatalogEntry *>(
                    catalog.GetEntry(context, DEFAULT_SCHEMA, (idx_t)ep_oid, true));
                if (epart && epart->name.find(edge_label) != string::npos) {
                    // Get adjacency index OIDs and resolve column indices
                    auto *adj_idx_oids = epart->GetAdjIndexOidVec();
                    if (adj_idx_oids) {
                        for (auto idx_oid : *adj_idx_oids) {
                            vector<int> adjColIdxs;
                            vector<LogicalType> adjColTypes;
                            data->graph_storage->getAdjColIdxs(
                                (idx_t)idx_oid, adjColIdxs, adjColTypes);
                            for (int col_idx : adjColIdxs) {
                                data->adj_col_idxs.push_back(col_idx);
                            }
                        }
                    }
                }
            }
        }
    }

    return data;
}

// ============================================================
// 2-hop pattern: __check_2hop_exists(label1, label2, src_vid, tgt_vid)
// Checks: exists node X such that (src)-[:label1]->(X) AND (tgt)-[:label2]->(X)
// ============================================================

struct Check2HopBindData : public FunctionData {
    iTbgppGraphStorageWrapper *graph_storage = nullptr;
    vector<int> adj_col_idxs_1;  // adjacency cols for label1
    vector<int> adj_col_idxs_2;  // adjacency cols for label2
    mutable vector<AdjacencyListIterator *> adj_iters_1;
    mutable vector<AdjacencyListIterator *> adj_iters_2;
    mutable vector<ExtentID> prev_eids_1;
    mutable vector<ExtentID> prev_eids_2;
    mutable bool iters_initialized = false;

    ~Check2HopBindData() {
        for (auto *it : adj_iters_1) delete it;
        for (auto *it : adj_iters_2) delete it;
    }
    void InitIterators() const {
        if (iters_initialized) return;
        for (size_t i = 0; i < adj_col_idxs_1.size(); i++) {
            adj_iters_1.push_back(new AdjacencyListIterator());
            prev_eids_1.push_back((ExtentID)-1);
        }
        for (size_t i = 0; i < adj_col_idxs_2.size(); i++) {
            adj_iters_2.push_back(new AdjacencyListIterator());
            prev_eids_2.push_back((ExtentID)-1);
        }
        iters_initialized = true;
    }
    unique_ptr<FunctionData> Copy() override {
        auto copy = make_unique<Check2HopBindData>();
        copy->graph_storage = graph_storage;
        copy->adj_col_idxs_1 = adj_col_idxs_1;
        copy->adj_col_idxs_2 = adj_col_idxs_2;
        return copy;
    }
};

static void ResolveAdjCols(duckdb::ClientContext &context, iTbgppGraphStorageWrapper *gs,
                           const string &label, vector<int> &out_cols) {
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
                    for (int c : cols) out_cols.push_back(c);
                }
            }
        }
    }
}

static void Check2HopExistsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &bind_data = (Check2HopBindData &)*((BoundFunctionExpression &)state.expr).bind_info;
    auto &src_vec = args.data[2];
    auto &tgt_vec = args.data[3];
    idx_t count = args.size();
    result.SetVectorType(VectorType::FLAT_VECTOR);
    auto result_data = FlatVector::GetData<bool>(result);

    bind_data.InitIterators();

    for (idx_t i = 0; i < count; i++) {
        auto src_val = src_vec.GetValue(i);
        auto tgt_val = tgt_vec.GetValue(i);
        if (src_val.IsNull() || tgt_val.IsNull()) { result_data[i] = false; continue; }

        uint64_t src_vid = src_val.GetValue<uint64_t>();
        uint64_t tgt_vid = tgt_val.GetValue<uint64_t>();
        bool found = false;

        // Get neighbors of src via label1
        for (size_t ai = 0; ai < bind_data.adj_col_idxs_1.size() && !found; ai++) {
            uint64_t *s1 = nullptr, *e1 = nullptr;
            bind_data.graph_storage->getAdjListFromVid(
                *bind_data.adj_iters_1[ai], bind_data.adj_col_idxs_1[ai],
                bind_data.prev_eids_1[ai], src_vid, s1, e1, ExpandDirection::OUTGOING);
            if (!s1 || !e1) continue;

            // Collect src neighbors into a set
            unordered_set<uint64_t> src_neighbors;
            for (uint64_t *p = s1; p < e1; p += 2) src_neighbors.insert(*p);

            // Check if any tgt neighbor via label2 is in src_neighbors
            for (size_t aj = 0; aj < bind_data.adj_col_idxs_2.size() && !found; aj++) {
                uint64_t *s2 = nullptr, *e2 = nullptr;
                bind_data.graph_storage->getAdjListFromVid(
                    *bind_data.adj_iters_2[aj], bind_data.adj_col_idxs_2[aj],
                    bind_data.prev_eids_2[aj], tgt_vid, s2, e2, ExpandDirection::OUTGOING);
                if (!s2 || !e2) continue;
                for (uint64_t *p = s2; p < e2; p += 2) {
                    if (src_neighbors.count(*p)) { found = true; break; }
                }
            }
        }
        result_data[i] = found;
    }
}

static unique_ptr<FunctionData> Check2HopBind(duckdb::ClientContext &context,
    ScalarFunction &bound_function, vector<unique_ptr<Expression>> &arguments) {
    auto data = make_unique<Check2HopBindData>();
    data->graph_storage = context.graph_storage_wrapper.get();
    if (arguments[0]->type == ExpressionType::VALUE_CONSTANT) {
        string label1 = ((BoundConstantExpression &)*arguments[0]).value.GetValue<string>();
        ResolveAdjCols(context, data->graph_storage, label1, data->adj_col_idxs_1);
    }
    if (arguments[1]->type == ExpressionType::VALUE_CONSTANT) {
        string label2 = ((BoundConstantExpression &)*arguments[1]).value.GetValue<string>();
        ResolveAdjCols(context, data->graph_storage, label2, data->adj_col_idxs_2);
    }
    return data;
}

void CheckEdgeExistsFun::RegisterFunction(BuiltinFunctions &set) {
    ScalarFunctionSet check_edge("__check_edge_exists");
    check_edge.AddFunction(ScalarFunction(
        {LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::UBIGINT},
        LogicalType::BOOLEAN, CheckEdgeExistsFunction, false, false,
        CheckEdgeExistsBind));
    set.AddFunction(check_edge);

    ScalarFunctionSet check_2hop("__check_2hop_exists");
    check_2hop.AddFunction(ScalarFunction(
        {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::UBIGINT},
        LogicalType::BOOLEAN, Check2HopExistsFunction, false, false,
        Check2HopBind));
    set.AddFunction(check_2hop);
}

} // namespace duckdb
