#pragma once

#include "binder/query/updating_clause/bound_updating_clause.hpp"
#include "common/types/value.hpp"
#include <string>
#include <vector>
#include <utility>

namespace duckdb {

// Information about a single node to create.
struct BoundCreateNodeInfo {
    string variable_name;  // e.g., "n"
    string label;          // e.g., "Person"
    vector<uint64_t> partition_ids;
    vector<pair<string, Value>> properties;  // {key, constant_value}
};

// Information about a single edge to create.
struct BoundCreateEdgeInfo {
    string variable_name;    // e.g., "r"
    string type;             // e.g., "KNOWS"
    string src_label;        // e.g., "Person" (for partition resolution)
    string dst_label;        // e.g., "Person"
    uint64_t src_vid;        // source vertex VID (resolved from src node's id property)
    uint64_t dst_vid;        // destination vertex VID
    vector<uint64_t> edge_partition_ids;  // edge partition OIDs
    vector<pair<string, Value>> properties;
};

class BoundCreateClause : public BoundUpdatingClause {
public:
    BoundCreateClause() : BoundUpdatingClause(BoundUpdatingClauseType::CREATE) {}

    void AddNode(BoundCreateNodeInfo info) {
        nodes.push_back(std::move(info));
    }
    void AddEdge(BoundCreateEdgeInfo info) {
        edges.push_back(std::move(info));
    }

    const vector<BoundCreateNodeInfo>& GetNodes() const { return nodes; }
    const vector<BoundCreateEdgeInfo>& GetEdges() const { return edges; }

private:
    vector<BoundCreateNodeInfo> nodes;
    vector<BoundCreateEdgeInfo> edges;
};

} // namespace duckdb
