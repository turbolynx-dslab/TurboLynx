#pragma once

#include "binder/expression/bound_expression.hpp"
#include "common/unordered_map.hpp"

namespace duckdb {
}
namespace turbolynx {
}
namespace duckdb {
	using namespace turbolynx;
}
namespace turbolynx {
using namespace duckdb;

// A fully-bound graph node variable.
// Carries all catalog OIDs needed by the Cypher2OrcaConverter.
class BoundNodeExpression {
public:
    BoundNodeExpression(string unique_name, vector<string> labels,
                         vector<uint64_t> partition_ids, vector<uint64_t> graphlet_ids)
        : unique_name(std::move(unique_name)), labels(std::move(labels)),
          partition_ids(std::move(partition_ids)), graphlet_ids(std::move(graphlet_ids)) {}

    const string&         GetUniqueName()   const { return unique_name; }
    const vector<string>& GetLabels()       const { return labels; }
    const vector<uint64_t>& GetPartitionIDs() const { return partition_ids; }
    const vector<uint64_t>& GetGraphletIDs()  const { return graphlet_ids; }
    bool                  IsMultiLabeled()  const { return graphlet_ids.size() > 1; }

    // ---- Property access tracking ----
    // Add a property by key ID. The Binder populates these after schema lookup.
    void AddPropertyExpression(uint64_t key_id, shared_ptr<BoundExpression> prop_expr) {
        key_id_to_idx[key_id] = properties.size();
        properties.push_back(std::move(prop_expr));
        used_flags.push_back(false);
    }

    bool HasProperty(uint64_t key_id) const {
        return key_id_to_idx.find(key_id) != key_id_to_idx.end();
    }

    shared_ptr<BoundExpression> GetPropertyExpression(uint64_t key_id) {
        auto it = key_id_to_idx.find(key_id);
        D_ASSERT(it != key_id_to_idx.end());
        used_flags[it->second] = true;
        return properties[it->second];
    }

    const vector<shared_ptr<BoundExpression>>& GetPropertyExpressions() const { return properties; }
    bool IsPropertyUsed(idx_t idx) const { return used_flags[idx]; }
    void MarkAllPropertiesUsed() {
        for (idx_t i = 0; i < used_flags.size(); i++) used_flags[i] = true;
        whole_node_required = true;
    }
    bool IsWholeNodeRequired() const { return whole_node_required; }
    const unordered_map<uint64_t, size_t>& GetKeyIdToIdx() const { return key_id_to_idx; }

private:
    string          unique_name;
    vector<string>  labels;
    vector<uint64_t> partition_ids;
    vector<uint64_t> graphlet_ids;

    unordered_map<uint64_t, size_t>         key_id_to_idx;
    vector<shared_ptr<BoundExpression>>     properties;
    vector<bool>                            used_flags;
    bool                                    whole_node_required = false;
};

} // namespace turbolynx
