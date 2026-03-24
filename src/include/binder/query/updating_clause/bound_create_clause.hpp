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

class BoundCreateClause : public BoundUpdatingClause {
public:
    BoundCreateClause() : BoundUpdatingClause(BoundUpdatingClauseType::CREATE) {}

    void AddNode(BoundCreateNodeInfo info) {
        nodes.push_back(std::move(info));
    }

    const vector<BoundCreateNodeInfo>& GetNodes() const { return nodes; }

private:
    vector<BoundCreateNodeInfo> nodes;
};

} // namespace duckdb
