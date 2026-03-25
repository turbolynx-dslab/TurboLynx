#pragma once

#include "binder/query/updating_clause/bound_updating_clause.hpp"
#include "common/types/value.hpp"
#include <string>
#include <vector>

namespace duckdb {

// One bound SET item: n.firstName = 'Updated'
struct BoundSetItem {
    string variable_name;  // e.g., "n"
    string property_key;   // e.g., "firstName"
    Value value;           // constant value
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

} // namespace duckdb
