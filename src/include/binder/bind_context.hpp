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
    struct PathMeta {
        idx_t num_chains = 0;       // total relationship hops in the pattern
        bool  is_fixed_length = true;  // all rels have lower==upper==1
    };
    void AddPath(const string& name) {
        path_bindings_.insert(name);
    }
    void AddPath(const string& name, const PathMeta& meta) {
        path_bindings_.insert(name);
        path_meta_[name] = meta;
    }

    bool HasPath(const string& name) const {
        if (path_bindings_.count(name)) return true;
        return outer_ ? outer_->HasPath(name) : false;
    }

    PathMeta GetPathMeta(const string& name) const {
        auto it = path_meta_.find(name);
        if (it != path_meta_.end()) return it->second;
        return outer_ ? outer_->GetPathMeta(name) : PathMeta{};
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
    bool HasLocalAliasType(const string& name) const {
        return alias_types_.count(name);
    }
    LogicalType GetLocalAliasType(const string& name) const {
        auto it = alias_types_.find(name);
        return it != alias_types_.end() ? it->second : LogicalType::ANY;
    }
    void RemoveAliasType(const string& name) {
        alias_types_.erase(name);
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

    // Drop every local entry whose name is not in `keep`. Used after a WITH
    // clause so the next query part only sees projected aliases for
    // expression-scope lookups — #19.
    //
    // Cypher 5 entity-introduction semantics (verified against Neo4j 5):
    // when a subsequent MATCH/OPTIONAL MATCH introduces a node/rel with
    // the same name as one visible before the WITH, the previous binding
    // is reused (no fresh, anchor-less scan). Model that by moving the
    // dropped node/rel/path bindings into a `shadowed_` map: invisible to
    // `HasNode`/`HasRel`/`HasPath` (so RETURN/WHERE reject the reference,
    // matching Neo4j's "Variable ... not defined") but recoverable via
    // `RecallShadowedNode`/`Rel`/`Path` which the pattern binder calls
    // when the same name reappears in a MATCH.
    void ResetToProjectedScope(const unordered_set<string>& keep) {
        for (auto it = node_bindings_.begin(); it != node_bindings_.end();) {
            if (keep.count(it->first)) {
                ++it;
            } else {
                shadowed_node_bindings_[it->first] = std::move(it->second);
                it = node_bindings_.erase(it);
            }
        }
        for (auto it = rel_bindings_.begin(); it != rel_bindings_.end();) {
            if (keep.count(it->first)) {
                ++it;
            } else {
                shadowed_rel_bindings_[it->first] = std::move(it->second);
                it = rel_bindings_.erase(it);
            }
        }
        for (auto it = path_bindings_.begin(); it != path_bindings_.end();) {
            if (keep.count(*it)) {
                ++it;
            } else {
                shadowed_path_bindings_.insert(*it);
                it = path_bindings_.erase(it);
            }
        }
        for (auto it = path_meta_.begin(); it != path_meta_.end();) {
            if (keep.count(it->first)) {
                ++it;
            } else {
                shadowed_path_meta_[it->first] = it->second;
                it = path_meta_.erase(it);
            }
        }
        for (auto it = path_rels_aliases_.begin(); it != path_rels_aliases_.end();) {
            if (keep.count(it->first)) ++it; else it = path_rels_aliases_.erase(it);
        }
        for (auto it = alias_types_.begin(); it != alias_types_.end();) {
            if (keep.count(it->first)) ++it; else it = alias_types_.erase(it);
        }
        for (auto it = property_aliased_names_.begin(); it != property_aliased_names_.end();) {
            if (keep.count(*it)) ++it; else it = property_aliased_names_.erase(it);
        }
    }

    // Pattern-binder hooks: when MATCH/OPTIONAL MATCH reuses a name that an
    // earlier WITH dropped, restore the binding so the planner re-uses the
    // colrefs from the prior MATCH. Returns nullptr / false if no shadowed
    // binding exists. #19.
    shared_ptr<BoundNodeExpression> RecallShadowedNode(const string& name) {
        auto it = shadowed_node_bindings_.find(name);
        if (it != shadowed_node_bindings_.end()) {
            auto node = it->second;
            node_bindings_[name] = node;
            shadowed_node_bindings_.erase(it);
            return node;
        }
        return outer_ ? const_cast<BindContext*>(outer_)->RecallShadowedNode(name)
                      : nullptr;
    }
    shared_ptr<BoundRelExpression> RecallShadowedRel(const string& name) {
        auto it = shadowed_rel_bindings_.find(name);
        if (it != shadowed_rel_bindings_.end()) {
            auto rel = it->second;
            rel_bindings_[name] = rel;
            shadowed_rel_bindings_.erase(it);
            return rel;
        }
        return outer_ ? const_cast<BindContext*>(outer_)->RecallShadowedRel(name)
                      : nullptr;
    }
    bool RecallShadowedPath(const string& name) {
        auto it = shadowed_path_bindings_.find(name);
        if (it != shadowed_path_bindings_.end()) {
            path_bindings_.insert(name);
            shadowed_path_bindings_.erase(it);
            auto meta_it = shadowed_path_meta_.find(name);
            if (meta_it != shadowed_path_meta_.end()) {
                path_meta_[name] = meta_it->second;
                shadowed_path_meta_.erase(meta_it);
            }
            return true;
        }
        return outer_ ? const_cast<BindContext*>(outer_)->RecallShadowedPath(name)
                      : false;
    }

    // ---- Property-origin tracking for alias chains ----
    // Marks aliases whose ultimate source is a node/rel property reference
    // (e.g. `WITH p.speaks AS xs`). Lets the UNWIND binder reject aliased
    // property refs the same way it rejects direct ones — the alias would
    // carry the property's value (or a SQLNULL fallback for unknown keys)
    // and silently produce zero rows (#80 follow-up).
    void MarkAliasAsPropertyRef(const string& name) {
        property_aliased_names_.insert(name);
    }
    bool IsAliasPropertyRef(const string& name) const {
        if (property_aliased_names_.count(name)) return true;
        return outer_ ? outer_->IsAliasPropertyRef(name) : false;
    }

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
    unordered_map<string, PathMeta> path_meta_;
    unordered_map<string, string> path_rels_aliases_;
    unordered_map<string, LogicalType> alias_types_;
    unordered_set<string> property_aliased_names_;
    // #19 — bindings dropped by an earlier WITH but recoverable when a
    // subsequent MATCH/OPTIONAL MATCH reuses the same name.
    unordered_map<string, shared_ptr<BoundNodeExpression>> shadowed_node_bindings_;
    unordered_map<string, shared_ptr<BoundRelExpression>>  shadowed_rel_bindings_;
    unordered_set<string> shadowed_path_bindings_;
    unordered_map<string, PathMeta> shadowed_path_meta_;
    const BindContext* outer_ = nullptr;
};

} // namespace duckdb
