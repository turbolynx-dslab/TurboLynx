#include "binder/graph_pattern/bound_query_graph.hpp"

namespace duckdb {
}
namespace turbolynx {
}
namespace duckdb {
	using namespace turbolynx;
}
namespace turbolynx {
using namespace duckdb;

// ---- BoundQueryGraph ----

bool BoundQueryGraph::IsConnected(const BoundQueryGraph& other) const {
    for (auto& node : other.query_nodes) {
        if (node_name_to_pos.count(node->GetUniqueName()) > 0) {
            return true;
        }
    }
    return false;
}

void BoundQueryGraph::Merge(const BoundQueryGraph& other) {
    for (auto& node : other.query_nodes) {
        if (!ContainsNode(node->GetUniqueName())) {
            AddQueryNode(node);
        }
    }
    for (auto& rel : other.query_rels) {
        if (!ContainsRel(rel->GetUniqueName())) {
            AddQueryRel(rel);
        }
    }
    // Preserve path type and name from the merged graph
    if (other.GetPathType() != PathType::NONE) {
        SetPathType(other.GetPathType());
    }
    if (!other.GetPathName().empty()) {
        SetPathName(other.GetPathName());
    }
}

// ---- BoundQueryGraphCollection ----

void BoundQueryGraphCollection::AddAndMergeIfConnected(unique_ptr<BoundQueryGraph> new_graph) {
    // Find any existing graph connected to the new one and merge.
    for (auto& existing : query_graphs) {
        if (existing->IsConnected(*new_graph)) {
            existing->Merge(*new_graph);
            return;
        }
    }
    query_graphs.push_back(std::move(new_graph));
}

vector<shared_ptr<BoundNodeExpression>> BoundQueryGraphCollection::GetQueryNodes() const {
    vector<shared_ptr<BoundNodeExpression>> result;
    for (auto& g : query_graphs) {
        for (auto& n : g->GetQueryNodes()) {
            result.push_back(n);
        }
    }
    return result;
}

vector<shared_ptr<BoundRelExpression>> BoundQueryGraphCollection::GetQueryRels() const {
    vector<shared_ptr<BoundRelExpression>> result;
    for (auto& g : query_graphs) {
        for (auto& r : g->GetQueryRels()) {
            result.push_back(r);
        }
    }
    return result;
}

} // namespace turbolynx
