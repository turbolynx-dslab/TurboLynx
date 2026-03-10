#pragma once

#include "binder/graph_pattern/bound_node_expression.hpp"
#include "binder/graph_pattern/bound_rel_expression.hpp"
#include "common/unordered_map.hpp"

namespace duckdb {

// Maximum graph variables per query (same limit as Kuzu's 64).
static constexpr uint8_t MAX_QUERY_VARIABLES = 64;

// BoundQueryGraph represents a single connected pattern from a MATCH clause.
class BoundQueryGraph {
public:
    BoundQueryGraph() = default;

    // ---- Nodes ----
    void AddQueryNode(shared_ptr<BoundNodeExpression> node) {
        node_name_to_pos[node->GetUniqueName()] = query_nodes.size();
        query_nodes.push_back(std::move(node));
    }

    bool ContainsNode(const string& name) const {
        return node_name_to_pos.count(name) > 0;
    }

    uint32_t GetNodePos(const string& name) const {
        return node_name_to_pos.at(name);
    }

    shared_ptr<BoundNodeExpression> GetQueryNode(const string& name) const {
        return query_nodes[node_name_to_pos.at(name)];
    }

    shared_ptr<BoundNodeExpression> GetQueryNode(uint32_t pos) const {
        return query_nodes[pos];
    }

    uint32_t GetNumQueryNodes() const { return query_nodes.size(); }

    const vector<shared_ptr<BoundNodeExpression>>& GetQueryNodes() const { return query_nodes; }

    // ---- Rels ----
    void AddQueryRel(shared_ptr<BoundRelExpression> rel) {
        rel_name_to_pos[rel->GetUniqueName()] = query_rels.size();
        query_rels.push_back(std::move(rel));
    }

    bool ContainsRel(const string& name) const {
        return rel_name_to_pos.count(name) > 0;
    }

    uint32_t GetRelPos(const string& name) const {
        return rel_name_to_pos.at(name);
    }

    shared_ptr<BoundRelExpression> GetQueryRel(const string& name) const {
        return query_rels[rel_name_to_pos.at(name)];
    }

    shared_ptr<BoundRelExpression> GetQueryRel(uint32_t pos) const {
        return query_rels[pos];
    }

    uint32_t GetNumQueryRels() const { return query_rels.size(); }

    const vector<shared_ptr<BoundRelExpression>>& GetQueryRels() const { return query_rels; }

    // ---- Path type (SHORTEST / ALL_SHORTEST) ----
    enum class PathType : uint8_t { NONE = 0, SHORTEST = 1, ALL_SHORTEST = 2 };
    PathType GetPathType() const { return path_type; }
    void SetPathType(PathType t) { path_type = t; }

    // ---- Merging connected components ----
    bool IsConnected(const BoundQueryGraph& other) const;
    void Merge(const BoundQueryGraph& other);

private:
    unordered_map<string, uint32_t>            node_name_to_pos;
    vector<shared_ptr<BoundNodeExpression>>    query_nodes;
    unordered_map<string, uint32_t>            rel_name_to_pos;
    vector<shared_ptr<BoundRelExpression>>     query_rels;
    PathType                                   path_type = PathType::NONE;
};

// Collection of connected query graphs from a single MATCH clause.
class BoundQueryGraphCollection {
public:
    BoundQueryGraphCollection() = default;

    void AddAndMergeIfConnected(unique_ptr<BoundQueryGraph> graph);

    uint32_t             GetNumQueryGraphs() const { return query_graphs.size(); }
    BoundQueryGraph*     GetQueryGraph(uint32_t idx) const { return query_graphs[idx].get(); }

    vector<shared_ptr<BoundNodeExpression>> GetQueryNodes() const;
    vector<shared_ptr<BoundRelExpression>>  GetQueryRels()  const;

private:
    vector<unique_ptr<BoundQueryGraph>> query_graphs;
};

} // namespace duckdb
