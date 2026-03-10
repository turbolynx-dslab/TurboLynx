#pragma once

#include "binder/graph_pattern/bound_node_expression.hpp"
#include "binder/graph_pattern/bound_rel_expression.hpp"
#include "common/unordered_map.hpp"

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

    bool HasVar(const string& name) const { return HasNode(name) || HasRel(name); }

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
    const BindContext* outer_ = nullptr;
};

} // namespace duckdb
