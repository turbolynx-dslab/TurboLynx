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
    // Partition-specific adj indices for the 3-hop pattern:
    // Person ←HAS_CREATOR← Comment →REPLY_OF→ Target →HAS_CREATOR→ Person
    int adj_person_to_comments = -1;  // HAS_CREATOR@Comment@Person backward
    int adj_comment_to_target = -1;   // REPLY_OF@Comment@Post or @Comment forward
    int adj_target_to_person = -1;    // HAS_CREATOR@Post@Person or @Comment@Person forward
    double per_match = 1.0;           // 1.0 for Post, 0.5 for Comment

    unique_ptr<FunctionData> Copy() override {
        auto c = make_unique<PathWeightBindData>();
        c->graph_storage = graph_storage;
        c->adj_person_to_comments = adj_person_to_comments;
        c->adj_comment_to_target = adj_comment_to_target;
        c->adj_target_to_person = adj_target_to_person;
        c->per_match = per_match;
        return c;
    }
};

// (Removed generic ResolveAdjColsForLabel — replaced by partition-specific ResolveOneAdjCol)

static void PathWeightFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &bd = (PathWeightBindData &)*((BoundFunctionExpression &)state.expr).bind_info;
    auto &path_vec = args.data[0];
    idx_t count = args.size();

    result.SetVectorType(VectorType::FLAT_VECTOR);
    auto result_data = FlatVector::GetData<double>(result);
    auto &result_mask = FlatVector::Validity(result);

    if (bd.adj_person_to_comments < 0 || bd.adj_comment_to_target < 0 || bd.adj_target_to_person < 0) {
        // Missing adj indices — return 0
        for (idx_t i = 0; i < count; i++) result_data[i] = 0.0;
        return;
    }

    // Reusable iterators (avoid re-initialization overhead)
    AdjacencyListIterator it1, it2, it3;
    ExtentID eid1 = (ExtentID)-1, eid2 = (ExtentID)-1, eid3 = (ExtentID)-1;

    for (idx_t i = 0; i < count; i++) {
        auto path_val = path_vec.GetValue(i);
        if (path_val.IsNull()) { result_mask.SetInvalid(i); continue; }

        auto &path_children = ListValue::GetChildren(path_val);
        double total_weight = 0.0;

        // For each edge in path: [n0, e0, n1, e1, ..., nk]
        for (idx_t ei = 1; ei < path_children.size(); ei += 2) {
            uint64_t node_a = path_children[ei - 1].GetValue<uint64_t>();
            uint64_t node_b = path_children[ei + 1].GetValue<uint64_t>();

            // Check both directions: (a→b) and (b→a)
            for (int dir = 0; dir < 2; dir++) {
                uint64_t src = (dir == 0) ? node_a : node_b;
                uint64_t dst = (dir == 0) ? node_b : node_a;

                // Step 1: Person src → Comments (backward HAS_CREATOR: Person←Comment)
                uint64_t *s1 = nullptr, *e1 = nullptr;
                bd.graph_storage->getAdjListFromVid(
                    it1, bd.adj_person_to_comments, eid1,
                    src, s1, e1, ExpandDirection::INCOMING);
                if (!s1) continue;

                for (uint64_t *p = s1; p < e1; p += 2) {
                    uint64_t comment = *p;

                    // Step 2: Comment → Target (REPLY_OF forward)
                    uint64_t *s2 = nullptr, *e2 = nullptr;
                    bd.graph_storage->getAdjListFromVid(
                        it2, bd.adj_comment_to_target, eid2,
                        comment, s2, e2, ExpandDirection::OUTGOING);
                    if (!s2) continue;

                    for (uint64_t *q = s2; q < e2; q += 2) {
                        uint64_t target = *q;

                        // Step 3: Target → Person dst (HAS_CREATOR forward)
                        uint64_t *s3 = nullptr, *e3 = nullptr;
                        bd.graph_storage->getAdjListFromVid(
                            it3, bd.adj_target_to_person, eid3,
                            target, s3, e3, ExpandDirection::OUTGOING);
                        if (!s3) continue;

                        for (uint64_t *r = s3; r < e3; r += 2) {
                            if (*r == dst) total_weight += bd.per_match;
                        }
                    }
                }
            }
        }
        result_data[i] = total_weight;
    }
}

// Resolve a SINGLE adj col for a specific edge partition (e.g., "HAS_CREATOR@Comment")
static int ResolveOneAdjCol(ClientContext &context, iTbgppGraphStorageWrapper *gs,
                             const string &edge_label, const string &src_label,
                             bool forward) {
    auto &catalog = context.db->GetCatalog();
    auto *gcat = (GraphCatalogEntry *)catalog.GetEntry(
        context, CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, DEFAULT_GRAPH);
    if (!gcat) return -1;
    for (auto ep_oid : gcat->edge_partitions) {
        auto *epart = static_cast<PartitionCatalogEntry *>(
            catalog.GetEntry(context, DEFAULT_SCHEMA, (idx_t)ep_oid, true));
        if (!epart) continue;
        // Match edge label AND source partition label
        if (epart->name.find(edge_label) == string::npos) continue;
        if (!src_label.empty() && epart->name.find(src_label) == string::npos) continue;

        auto *adj_idx_oids = epart->GetAdjIndexOidVec();
        if (!adj_idx_oids || adj_idx_oids->empty()) continue;

        // Each edge partition has [forward_adj, backward_adj] indices
        for (auto idx_oid : *adj_idx_oids) {
            vector<int> cols; vector<LogicalType> types;
            gs->getAdjColIdxs((idx_t)idx_oid, cols, types);
            for (size_t j = 0; j < cols.size(); j++) {
                bool is_fwd = (types[j] == LogicalType::FORWARD_ADJLIST);
                if (is_fwd == forward) return cols[j];
            }
        }
    }
    return -1;
}

static unique_ptr<FunctionData> PathWeightBind(ClientContext &context,
    ScalarFunction &bound_function, vector<unique_ptr<Expression>> &arguments) {
    auto data = make_unique<PathWeightBindData>();
    data->graph_storage = context.graph_storage_wrapper.get();

    // Extract target label from second arg
    string target_label = "Post";
    if (arguments.size() > 1 && arguments[1]->type == ExpressionType::VALUE_CONSTANT) {
        target_label = ((BoundConstantExpression &)*arguments[1]).value.GetValue<string>();
    }
    data->per_match = (target_label == "Post") ? 1.0 : 0.5;

    // Resolve exactly 3 adj indices for the 3-hop pattern:
    // Person ←HAS_CREATOR← Comment →REPLY_OF→ Target →HAS_CREATOR→ Person

    // 1. HAS_CREATOR@Comment backward: Person → Comments created by Person
    data->adj_person_to_comments = ResolveOneAdjCol(
        context, data->graph_storage, "HAS_CREATOR", "Comment", false);

    // 2. REPLY_OF@Comment forward: Comment → Target (Post or Comment)
    data->adj_comment_to_target = ResolveOneAdjCol(
        context, data->graph_storage, "REPLY_OF", target_label, true);

    // 3. HAS_CREATOR@Target forward: Target → Person (creator)
    data->adj_target_to_person = ResolveOneAdjCol(
        context, data->graph_storage, "HAS_CREATOR", target_label, true);

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
