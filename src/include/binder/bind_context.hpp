#pragma once

#include "binder/graph_pattern/bound_node_expression.hpp"
#include "binder/graph_pattern/bound_rel_expression.hpp"
#include "common/unordered_map.hpp"
#include "common/unordered_set.hpp"

namespace duckdb {

// Tracks variable bindings within a single query scope.
// Each WITH clause starts a fresh inner scope; the outer scope is referenced for lookup.
class BindContext {
public:
    BindContext() = default;
    explicit BindContext(const BindContext* outer) : outer_(outer) {}

    // ---- Node bindings ----
    void AddNode(const string& name, shared_ptr<BoundNodeExpression> node) {
        node_bindings_[name] = std::move(node);
    }

    bool HasNode(const string& name) const {
        if (node_bindings_.count(name)) return true;
        return outer_ ? outer_->HasNode(name) : false;
    }

    shared_ptr<BoundNodeExpression> GetNode(const string& name) const {
        auto it = node_bindings_.find(name);
        if (it != node_bindings_.end()) return it->second;
        return outer_ ? outer_->GetNode(name) : nullptr;
    }

    // ---- Rel bindings ----
    void AddRel(const string& name, shared_ptr<BoundRelExpression> rel) {
        rel_bindings_[name] = std::move(rel);
    }

    bool HasRel(const string& name) const {
        if (rel_bindings_.count(name)) return true;
        return outer_ ? outer_->HasRel(name) : false;
    }

    shared_ptr<BoundRelExpression> GetRel(const string& name) const {
        auto it = rel_bindings_.find(name);
        if (it != rel_bindings_.end()) return it->second;
        return outer_ ? outer_->GetRel(name) : nullptr;
    }

    // ---- Path bindings ----
    void AddPath(const string& name) {
        path_bindings_.insert(name);
    }

    bool HasPath(const string& name) const {
        if (path_bindings_.count(name)) return true;
        return outer_ ? outer_->HasPath(name) : false;
    }

    void AddPathRelsAlias(const string& alias, const string& path_name) {
        path_rels_aliases_[alias] = path_name;
    }

    bool HasPathRelsAlias(const string& alias) const {
        if (path_rels_aliases_.count(alias)) return true;
        return outer_ ? outer_->HasPathRelsAlias(alias) : false;
    }

    string GetPathRelsAlias(const string& alias) const {
        auto it = path_rels_aliases_.find(alias);
        if (it != path_rels_aliases_.end()) return it->second;
        return outer_ ? outer_->GetPathRelsAlias(alias) : string();
    }

    // ---- Alias type tracking (for STRUCT/complex types) ----
    void AddAliasType(const string& name, LogicalType type) {
        alias_types_[name] = std::move(type);
    }
    bool HasAliasType(const string& name) const {
        if (alias_types_.count(name)) return true;
        return outer_ ? outer_->HasAliasType(name) : false;
    }
    LogicalType GetAliasType(const string& name) const {
        auto it = alias_types_.find(name);
        if (it != alias_types_.end()) return it->second;
        return outer_ ? outer_->GetAliasType(name) : LogicalType::ANY;
    }

    bool HasVar(const string& name) const { return HasNode(name) || HasRel(name) || HasPath(name); }

    // ---- All visible node/rel names (for RETURN *) ----
    vector<string> GetAllNodeNames() const {
        vector<string> result;
        for (auto& kv : node_bindings_) result.push_back(kv.first);
        if (outer_) {
            auto outer_names = outer_->GetAllNodeNames();
            for (auto& n : outer_names) {
                if (!node_bindings_.count(n)) result.push_back(n);
            }
        }
        return result;
    }

    vector<string> GetAllRelNames() const {
        vector<string> result;
        for (auto& kv : rel_bindings_) result.push_back(kv.first);
        if (outer_) {
            auto outer_names = outer_->GetAllRelNames();
            for (auto& n : outer_names) {
                if (!rel_bindings_.count(n)) result.push_back(n);
            }
        }
        return result;
    }

private:
    unordered_map<string, shared_ptr<BoundNodeExpression>> node_bindings_;
    unordered_map<string, shared_ptr<BoundRelExpression>>  rel_bindings_;
    unordered_set<string> path_bindings_;
    unordered_map<string, string> path_rels_aliases_;
    unordered_map<string, LogicalType> alias_types_;
    const BindContext* outer_ = nullptr;
};

} // namespace duckdb
