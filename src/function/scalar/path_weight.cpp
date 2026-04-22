#include <algorithm>
#include <array>
#include <unordered_map>
#include <unordered_set>

#include "function/scalar/nested_functions.hpp"
#include "common/types/data_chunk.hpp"
#include "main/client_context.hpp"
#include "main/database.hpp"
#include "planner/expression/bound_constant_expression.hpp"
#include "planner/expression/bound_function_expression.hpp"
#include "storage/extent/adjlist_iterator.hpp"
#include "storage/graph_storage_wrapper.hpp"
#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/graph_catalog_entry.hpp"
#include "catalog/catalog_entry/partition_catalog_entry.hpp"

namespace duckdb {
using namespace turbolynx;

namespace {

struct PairHash {
    size_t operator()(const std::pair<uint64_t, uint64_t> &value) const {
        auto lhs = std::hash<uint64_t> {}(value.first);
        auto rhs = std::hash<uint64_t> {}(value.second);
        return lhs ^ (rhs + 0x9e3779b97f4a7c15ULL + (lhs << 6) + (lhs >> 2));
    }
};

struct PathWeightHopBindData {
    std::unordered_map<uint16_t, vector<int>> adj_cols_by_src_pid;
};

struct PathWeightBindData : public FunctionData {
    iTbgppGraphStorageWrapper *graph_storage = nullptr;
    std::array<PathWeightHopBindData, 3> hops;
    double per_match = 0.0;

    unique_ptr<FunctionData> Copy() override {
        auto copy = make_unique<PathWeightBindData>();
        copy->graph_storage = graph_storage;
        copy->hops = hops;
        copy->per_match = per_match;
        return copy;
    }
};

static GraphCatalogEntry *GetGraphCatalog(ClientContext &context) {
    auto &catalog = context.db->GetCatalog();
    return (GraphCatalogEntry *)catalog.GetEntry(
        context, CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, DEFAULT_GRAPH);
}

static bool TryGetStringLiteral(Expression &expr, string &out) {
    if (expr.type != ExpressionType::VALUE_CONSTANT) {
        return false;
    }
    auto &constant = static_cast<BoundConstantExpression &>(expr);
    if (constant.value.IsNull() ||
        constant.value.type().id() != LogicalTypeId::VARCHAR) {
        return false;
    }
    out = constant.value.GetValue<string>();
    return true;
}

static bool TryGetDoubleLiteral(Expression &expr, double &out) {
    if (expr.type != ExpressionType::VALUE_CONSTANT) {
        return false;
    }
    auto &constant = static_cast<BoundConstantExpression &>(expr);
    if (constant.value.IsNull()) {
        return false;
    }
    switch (constant.value.type().id()) {
    case LogicalTypeId::DOUBLE:
        out = constant.value.GetValue<double>();
        return true;
    case LogicalTypeId::FLOAT:
        out = constant.value.GetValue<float>();
        return true;
    case LogicalTypeId::INTEGER:
        out = constant.value.GetValue<int32_t>();
        return true;
    case LogicalTypeId::BIGINT:
        out = constant.value.GetValue<int64_t>();
        return true;
    default:
        return false;
    }
}

static std::unordered_set<idx_t> LookupVertexPartitions(
    ClientContext &context, GraphCatalogEntry &gcat, const string &label) {
    std::unordered_set<idx_t> result;
    if (label.empty()) {
        for (auto oid : gcat.vertex_partitions) {
            result.insert(oid);
        }
        return result;
    }
    auto oids = gcat.LookupPartition(
        context, {label}, GraphComponentType::VERTEX);
    for (auto oid : oids) {
        result.insert(oid);
    }
    return result;
}

static void DeduplicateAdjCols(
    std::unordered_map<uint16_t, vector<int>> &adj_cols_by_src_pid) {
    for (auto &[pid, cols] : adj_cols_by_src_pid) {
        std::sort(cols.begin(), cols.end());
        cols.erase(std::unique(cols.begin(), cols.end()), cols.end());
    }
}

static std::unordered_map<uint16_t, vector<int>> ResolveHopAdjCols(
    ClientContext &context, iTbgppGraphStorageWrapper &graph_storage,
    const string &edge_label, const string &direction,
    const string &src_label, const string &dst_label) {
    auto *gcat = GetGraphCatalog(context);
    if (!gcat) {
        throw InvalidInputException("path_weight: graph catalog not found");
    }
    if (direction != "OUT" && direction != "IN") {
        throw InvalidInputException(
            "path_weight: only directed hop metadata is supported");
    }

    auto allowed_src_partitions = LookupVertexPartitions(context, *gcat, src_label);
    auto allowed_dst_partitions = LookupVertexPartitions(context, *gcat, dst_label);

    vector<idx_t> edge_partition_oids;
    gcat->GetEdgePartitionIndexesInType(context, edge_label, edge_partition_oids);

    auto &catalog = context.db->GetCatalog();
    std::unordered_map<uint16_t, vector<int>> adj_cols_by_src_pid;
    for (auto edge_partition_oid : edge_partition_oids) {
        auto *edge_part = (PartitionCatalogEntry *)catalog.GetEntry(
            context, DEFAULT_SCHEMA, edge_partition_oid, true);
        if (!edge_part) {
            continue;
        }

        idx_t actual_src_part_oid =
            direction == "OUT" ? edge_part->GetSrcPartOid()
                               : edge_part->GetDstPartOid();
        idx_t actual_dst_part_oid =
            direction == "OUT" ? edge_part->GetDstPartOid()
                               : edge_part->GetSrcPartOid();
        if (!allowed_src_partitions.empty() &&
            allowed_src_partitions.count(actual_src_part_oid) == 0) {
            continue;
        }
        if (!allowed_dst_partitions.empty() &&
            allowed_dst_partitions.count(actual_dst_part_oid) == 0) {
            continue;
        }

        auto *adj_index_oids = edge_part->GetAdjIndexOidVec();
        if (!adj_index_oids) {
            continue;
        }
        for (auto index_oid : *adj_index_oids) {
            vector<int> cols;
            vector<LogicalType> types;
            graph_storage.getAdjColIdxs(index_oid, cols, types);
            for (idx_t i = 0; i < cols.size(); i++) {
                bool use_index =
                    (direction == "OUT" &&
                     types[i] == LogicalType::FORWARD_ADJLIST) ||
                    (direction == "IN" &&
                     types[i] == LogicalType::BACKWARD_ADJLIST);
                if (!use_index) {
                    continue;
                }
                uint16_t src_pid =
                    graph_storage.getAdjListSrcPartitionId(index_oid);
                adj_cols_by_src_pid[src_pid].push_back(cols[i]);
            }
        }
    }

    DeduplicateAdjCols(adj_cols_by_src_pid);
    return adj_cols_by_src_pid;
}

static const vector<int> *FindAdjColsForVid(
    const PathWeightHopBindData &hop_data,
    iTbgppGraphStorageWrapper &graph_storage, uint64_t vid) {
    auto src_pid = graph_storage.getNodePartitionId(vid);
    auto it = hop_data.adj_cols_by_src_pid.find(src_pid);
    if (it == hop_data.adj_cols_by_src_pid.end()) {
        return nullptr;
    }
    return &it->second;
}

static void FetchAdjList(iTbgppGraphStorageWrapper &graph_storage,
                         AdjacencyListIterator &adj_iter, ExtentID &prev_eid,
                         int &prev_adj_col, int adj_col_idx, uint64_t vid,
                         uint64_t *&start_ptr, uint64_t *&end_ptr) {
    if (adj_col_idx != prev_adj_col) {
        prev_eid = (ExtentID)-1;
        prev_adj_col = adj_col_idx;
    }
    graph_storage.getAdjListFromVid(adj_iter, adj_col_idx, prev_eid, vid,
                                    start_ptr, end_ptr,
                                    ExpandDirection::OUTGOING);
}

static unique_ptr<FunctionData> UnsupportedPathWeightBind(
    ClientContext &context, ScalarFunction &bound_function,
    vector<unique_ptr<Expression>> &arguments) {
    throw InvalidInputException(
        "path_weight(path, target_label) is no longer supported directly; "
        "use the rewritten weighted path pattern form");
}

static unique_ptr<FunctionData> PathWeightBind(
    ClientContext &context, ScalarFunction &bound_function,
    vector<unique_ptr<Expression>> &arguments) {
    if (arguments.size() != 12) {
        throw InvalidInputException(
            "path_weight expects 12 arguments after rewrite");
    }

    auto data = make_unique<PathWeightBindData>();
    data->graph_storage = context.graph_storage_wrapper.get();
    if (!data->graph_storage) {
        throw InvalidInputException("path_weight requires graph storage");
    }

    string start_label;
    if (!TryGetStringLiteral(*arguments[1], start_label)) {
        throw InvalidInputException("path_weight: start label must be literal");
    }

    string current_src_label = start_label;
    for (idx_t hop = 0; hop < 3; hop++) {
        string edge_label, direction, dst_label;
        idx_t base_idx = 2 + hop * 3;
        if (!TryGetStringLiteral(*arguments[base_idx], edge_label) ||
            !TryGetStringLiteral(*arguments[base_idx + 1], direction) ||
            !TryGetStringLiteral(*arguments[base_idx + 2], dst_label)) {
            throw InvalidInputException(
                "path_weight: hop metadata must be literal");
        }
        data->hops[hop].adj_cols_by_src_pid = ResolveHopAdjCols(
            context, *data->graph_storage, edge_label, direction,
            current_src_label, dst_label);
        current_src_label = dst_label;
    }

    if (!TryGetDoubleLiteral(*arguments[11], data->per_match)) {
        throw InvalidInputException(
            "path_weight: per-match value must be a numeric literal");
    }
    return data;
}

static void PathWeightFunction(DataChunk &args, ExpressionState &state,
                               Vector &result) {
    auto &bind_data =
        (PathWeightBindData &)*((BoundFunctionExpression &)state.expr).bind_info;
    auto &path_vec = args.data[0];
    idx_t count = args.size();

    result.SetVectorType(VectorType::FLAT_VECTOR);
    auto result_data = FlatVector::GetData<double>(result);
    auto &result_validity = FlatVector::Validity(result);

    AdjacencyListIterator hop1_iter, hop2_iter, hop3_iter;
    ExtentID hop1_prev_eid = (ExtentID)-1;
    ExtentID hop2_prev_eid = (ExtentID)-1;
    ExtentID hop3_prev_eid = (ExtentID)-1;
    int hop1_prev_adj_col = -1;
    int hop2_prev_adj_col = -1;
    int hop3_prev_adj_col = -1;

    for (idx_t row = 0; row < count; row++) {
        auto path_value = path_vec.GetValue(row);
        if (path_value.IsNull()) {
            result_validity.SetInvalid(row);
            continue;
        }

        auto &path_children = ListValue::GetChildren(path_value);
        double total_weight = 0.0;
        for (idx_t edge_idx = 1; edge_idx + 1 < path_children.size();
             edge_idx += 2) {
            uint64_t node_a = path_children[edge_idx - 1].GetValue<uint64_t>();
            uint64_t node_b = path_children[edge_idx + 1].GetValue<uint64_t>();

            std::unordered_set<std::pair<uint64_t, uint64_t>, PairHash> counted;
            for (idx_t direction_idx = 0; direction_idx < 2; direction_idx++) {
                uint64_t src = direction_idx == 0 ? node_a : node_b;
                uint64_t dst = direction_idx == 0 ? node_b : node_a;

                auto *hop1_cols = FindAdjColsForVid(
                    bind_data.hops[0], *bind_data.graph_storage, src);
                if (!hop1_cols) {
                    continue;
                }
                for (auto hop1_col : *hop1_cols) {
                    uint64_t *hop1_start = nullptr;
                    uint64_t *hop1_end = nullptr;
                    FetchAdjList(*bind_data.graph_storage, hop1_iter,
                                 hop1_prev_eid, hop1_prev_adj_col, hop1_col,
                                 src, hop1_start, hop1_end);
                    if (!hop1_start) {
                        continue;
                    }

                    for (uint64_t *p = hop1_start; p < hop1_end; p += 2) {
                        uint64_t mid1 = *p;
                        auto *hop2_cols = FindAdjColsForVid(
                            bind_data.hops[1], *bind_data.graph_storage, mid1);
                        if (!hop2_cols) {
                            continue;
                        }
                        for (auto hop2_col : *hop2_cols) {
                            uint64_t *hop2_start = nullptr;
                            uint64_t *hop2_end = nullptr;
                            FetchAdjList(*bind_data.graph_storage, hop2_iter,
                                         hop2_prev_eid, hop2_prev_adj_col,
                                         hop2_col, mid1, hop2_start, hop2_end);
                            if (!hop2_start) {
                                continue;
                            }

                            for (uint64_t *q = hop2_start; q < hop2_end;
                                 q += 2) {
                                uint64_t mid2 = *q;
                                auto key = std::make_pair(mid1, mid2);
                                if (counted.count(key) > 0) {
                                    continue;
                                }

                                auto *hop3_cols = FindAdjColsForVid(
                                    bind_data.hops[2],
                                    *bind_data.graph_storage, mid2);
                                if (!hop3_cols) {
                                    continue;
                                }
                                bool matched = false;
                                for (auto hop3_col : *hop3_cols) {
                                    uint64_t *hop3_start = nullptr;
                                    uint64_t *hop3_end = nullptr;
                                    FetchAdjList(*bind_data.graph_storage,
                                                 hop3_iter, hop3_prev_eid,
                                                 hop3_prev_adj_col, hop3_col,
                                                 mid2, hop3_start, hop3_end);
                                    if (!hop3_start) {
                                        continue;
                                    }

                                    for (uint64_t *r = hop3_start;
                                         r < hop3_end; r += 2) {
                                        if (*r == dst) {
                                            total_weight += bind_data.per_match;
                                            counted.insert(key);
                                            matched = true;
                                            break;
                                        }
                                    }
                                    if (matched) {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        result_data[row] = total_weight;
    }
}

} // namespace

void PathWeightFun::RegisterFunction(BuiltinFunctions &set) {
    ScalarFunctionSet path_weight("path_weight");
    path_weight.AddFunction(ScalarFunction(
        {LogicalType::ANY, LogicalType::VARCHAR},
        LogicalType::DOUBLE, PathWeightFunction, false, false,
        UnsupportedPathWeightBind));
    path_weight.AddFunction(ScalarFunction(
        {LogicalType::ANY,
         LogicalType::VARCHAR,
         LogicalType::VARCHAR,
         LogicalType::VARCHAR,
         LogicalType::VARCHAR,
         LogicalType::VARCHAR,
         LogicalType::VARCHAR,
         LogicalType::VARCHAR,
         LogicalType::VARCHAR,
         LogicalType::VARCHAR,
         LogicalType::VARCHAR,
         LogicalType::DOUBLE},
        LogicalType::DOUBLE, PathWeightFunction, false, false, PathWeightBind));
    set.AddFunction(path_weight);
}

} // namespace duckdb
