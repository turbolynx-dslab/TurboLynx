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

// IC14 weight computation: for each edge in path, count Comment→Reply→Target
// chains between the edge's endpoints.
// path_weight(path, 'Post') → sum of 1.0 per Comment→REPLY_OF→Post chain
// path_weight(path, 'Comment') → sum of 0.5 per Comment→REPLY_OF→Comment chain

struct PathWeightBindData : public FunctionData {
    iTbgppGraphStorageWrapper *graph_storage = nullptr;
    // Adjacency indices for HAS_CREATOR and REPLY_OF
    vector<int> has_creator_adj_cols;   // Comment/Post → Person (backward: Person←HAS_CREATOR←Comment)
    vector<int> reply_of_adj_cols;      // Comment → Post/Comment (forward: Comment→REPLY_OF→Post)
    string target_label; // "Post" or "Comment"

    unique_ptr<FunctionData> Copy() override {
        auto c = make_unique<PathWeightBindData>();
        c->graph_storage = graph_storage;
        c->has_creator_adj_cols = has_creator_adj_cols;
        c->reply_of_adj_cols = reply_of_adj_cols;
        c->target_label = target_label;
        return c;
    }
};

static void ResolveAdjColsForLabel(ClientContext &context, iTbgppGraphStorageWrapper *gs,
                                    const string &edge_label, vector<int> &out_cols) {
    auto &catalog = context.db->GetCatalog();
    auto *gcat = (GraphCatalogEntry *)catalog.GetEntry(
        context, CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, DEFAULT_GRAPH);
    if (!gcat) return;
    for (auto ep_oid : gcat->edge_partitions) {
        auto *epart = static_cast<PartitionCatalogEntry *>(
            catalog.GetEntry(context, DEFAULT_SCHEMA, (idx_t)ep_oid, true));
        if (epart && epart->name.find(edge_label) != string::npos) {
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

static void PathWeightFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &bind_data = (PathWeightBindData &)*((BoundFunctionExpression &)state.expr).bind_info;
    auto &path_vec = args.data[0];  // path LIST
    idx_t count = args.size();

    result.SetVectorType(VectorType::FLAT_VECTOR);
    auto result_data = FlatVector::GetData<double>(result);
    auto &result_mask = FlatVector::Validity(result);

    for (idx_t i = 0; i < count; i++) {
        auto path_val = path_vec.GetValue(i);
        if (path_val.IsNull()) { result_mask.SetInvalid(i); continue; }

        auto &path_children = ListValue::GetChildren(path_val);
        // path = [n0, e0, n1, e1, ..., nk]
        // For each edge (index 1, 3, 5, ...), get endpoints
        double total_weight = 0.0;
        double per_match = (bind_data.target_label == "Post") ? 1.0 : 0.5;

        for (idx_t ei = 1; ei < path_children.size(); ei += 2) {
            uint64_t node_a = path_children[ei - 1].GetValue<uint64_t>(); // left node
            uint64_t node_b = path_children[ei + 1].GetValue<uint64_t>(); // right node

            // Count: how many Comment→REPLY_OF→Target chains exist between node_a and node_b
            // Direction 1: a←HAS_CREATOR←Comment→REPLY_OF→Target→HAS_CREATOR→b
            // Direction 2: b←HAS_CREATOR←Comment→REPLY_OF→Target→HAS_CREATOR→a
            for (int dir = 0; dir < 2; dir++) {
                uint64_t src_person = (dir == 0) ? node_a : node_b;
                uint64_t dst_person = (dir == 0) ? node_b : node_a;

                // Step 1: Get all Comments/Posts created by src_person
                // HAS_CREATOR edge: Comment/Post → Person
                // So we need Person → Comment/Post (backward adj of HAS_CREATOR)
                for (int hc_col : bind_data.has_creator_adj_cols) {
                    AdjacencyListIterator hc_iter;
                    ExtentID hc_eid = (ExtentID)-1;
                    uint64_t *hc_start = nullptr, *hc_end = nullptr;
                    bind_data.graph_storage->getAdjListFromVid(
                        hc_iter, hc_col, hc_eid,
                        src_person, hc_start, hc_end, ExpandDirection::OUTGOING);
                    if (!hc_start) continue;

                    for (uint64_t *p = hc_start; p < hc_end; p += 2) {
                        uint64_t comment_vid = *p;

                        // Step 2: Get REPLY_OF targets of this comment
                        for (int ro_col : bind_data.reply_of_adj_cols) {
                            AdjacencyListIterator ro_iter;
                            ExtentID ro_eid = (ExtentID)-1;
                            uint64_t *ro_start = nullptr, *ro_end = nullptr;
                            bind_data.graph_storage->getAdjListFromVid(
                                ro_iter, ro_col, ro_eid,
                                comment_vid, ro_start, ro_end, ExpandDirection::OUTGOING);
                            if (!ro_start) continue;

                            for (uint64_t *q = ro_start; q < ro_end; q += 2) {
                                uint64_t target_vid = *q;

                                // Step 3: Check if target's creator = dst_person
                                for (int hc_col2 : bind_data.has_creator_adj_cols) {
                                    AdjacencyListIterator hc2_iter;
                                    ExtentID hc2_eid = (ExtentID)-1;
                                    uint64_t *hc2_start = nullptr, *hc2_end = nullptr;
                                    bind_data.graph_storage->getAdjListFromVid(
                                        hc2_iter, hc_col2, hc2_eid,
                                        target_vid, hc2_start, hc2_end, ExpandDirection::OUTGOING);
                                    if (!hc2_start) continue;

                                    for (uint64_t *r = hc2_start; r < hc2_end; r += 2) {
                                        if (*r == dst_person) {
                                            total_weight += per_match;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        result_data[i] = total_weight;
    }
}

static unique_ptr<FunctionData> PathWeightBind(ClientContext &context,
    ScalarFunction &bound_function, vector<unique_ptr<Expression>> &arguments) {
    auto data = make_unique<PathWeightBindData>();
    data->graph_storage = context.graph_storage_wrapper.get();

    // Extract target label from second arg
    if (arguments.size() > 1 && arguments[1]->type == ExpressionType::VALUE_CONSTANT) {
        data->target_label = ((BoundConstantExpression &)*arguments[1]).value.GetValue<string>();
    }

    // Resolve HAS_CREATOR and REPLY_OF adjacency columns
    ResolveAdjColsForLabel(context, data->graph_storage, "HAS_CREATOR", data->has_creator_adj_cols);
    ResolveAdjColsForLabel(context, data->graph_storage, "REPLY_OF", data->reply_of_adj_cols);

    return data;
}

void PathWeightFun::RegisterFunction(BuiltinFunctions &set) {
    ScalarFunctionSet pw("path_weight");
    pw.AddFunction(ScalarFunction(
        {LogicalType::ANY, LogicalType::VARCHAR},
        LogicalType::DOUBLE, PathWeightFunction, false, false, PathWeightBind));
    set.AddFunction(pw);
}

} // namespace duckdb
