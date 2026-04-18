#pragma once

#include "binder/expression/bound_expression.hpp"
#include "common/unordered_map.hpp"
#include "parser/query/graph_pattern/rel_pattern.hpp"

namespace duckdb {
}
namespace turbolynx {
}
namespace duckdb {
	using namespace turbolynx;
}
namespace turbolynx {
using namespace duckdb;

// A fully-bound graph relationship variable.
class BoundRelExpression {
public:
    BoundRelExpression(string unique_name, vector<string> types, RelDirection direction,
                        vector<uint64_t> partition_ids, vector<uint64_t> graphlet_ids,
                        string src_node_name, string dst_node_name,
                        uint64_t lower_bound, uint64_t upper_bound)
        : unique_name(std::move(unique_name)), types(std::move(types)), direction(direction),
          partition_ids(std::move(partition_ids)), graphlet_ids(std::move(graphlet_ids)),
          src_node_name(std::move(src_node_name)), dst_node_name(std::move(dst_node_name)),
          lower_bound(lower_bound), upper_bound(upper_bound) {}

    const string&          GetUniqueName()   const { return unique_name; }
    const vector<string>&  GetTypes()        const { return types; }
    RelDirection           GetDirection()    const { return direction; }
    const vector<uint64_t>& GetPartitionIDs() const { return partition_ids; }
    const vector<uint64_t>& GetGraphletIDs()  const { return graphlet_ids; }
    const string&          GetSrcNodeName() const { return src_node_name; }
    const string&          GetDstNodeName() const { return dst_node_name; }
    uint64_t               GetLowerBound()  const { return lower_bound; }
    uint64_t               GetUpperBound()  const { return upper_bound; }
    bool                   IsVariableLength() const {
        return !(lower_bound == 1 && upper_bound == 1);
    }
    bool                   IsMultiType()    const { return graphlet_ids.size() > 1; }

    // ---- Property access tracking ----
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
    const unordered_map<uint64_t, size_t>& GetKeyIdToIdx() const { return key_id_to_idx; }

private:
    string          unique_name;
    vector<string>  types;
    RelDirection    direction;
    vector<uint64_t> partition_ids;
    vector<uint64_t> graphlet_ids;
    string          src_node_name;
    string          dst_node_name;
    uint64_t        lower_bound;
    uint64_t        upper_bound;

    unordered_map<uint64_t, size_t>         key_id_to_idx;
    vector<shared_ptr<BoundExpression>>     properties;
    vector<bool>                            used_flags;
};

} // namespace turbolynx
