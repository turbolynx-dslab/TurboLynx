#pragma once

#include "binder/query/updating_clause/bound_updating_clause.hpp"
#include "common/types/value.hpp"
#include <string>
#include <vector>

namespace duckdb {
}
namespace turbolynx {
}
namespace duckdb {
	using namespace turbolynx;
}
namespace turbolynx {
using namespace duckdb;

// One bound SET item: n.firstName = 'Updated'
struct BoundSetItem {
    string variable_name;  // e.g., "n"
    string property_key;   // e.g., "firstName"
    Value value;           // constant value
    // Optional metadata captured during binding so the mutation post-processor
    // can locate the target row inside a multi-variable result chunk.
    // Empty when the variable wasn't matched in this scope (e.g. RETURN-less
    // SET in a freshly-bootstrapped workspace).
    vector<string> target_ps_keys;       // PS schema keys for this variable's first partition
    idx_t          target_partition_oid = 0;
};

class BoundSetClause : public BoundUpdatingClause {
public:
    BoundSetClause() : BoundUpdatingClause(BoundUpdatingClauseType::SET) {}

    void AddItem(BoundSetItem item) {
        items.push_back(std::move(item));
    }

    const vector<BoundSetItem>& GetItems() const { return items; }

private:
    vector<BoundSetItem> items;
};

} // namespace turbolynx
