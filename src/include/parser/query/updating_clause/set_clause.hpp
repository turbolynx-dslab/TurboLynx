//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/query/updating_clause/set_clause.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/query/updating_clause/updating_clause.hpp"
#include "parser/parsed_expression.hpp"
#include <vector>
#include <string>
#include <memory>

namespace duckdb {
}
namespace turbolynx {
}
namespace duckdb {
	using namespace turbolynx;
}
namespace turbolynx {
using namespace duckdb;

// One SET item: variable.property = expression
struct SetItem {
    string variable_name;  // e.g., "n"
    string property_key;   // e.g., "firstName"
    unique_ptr<ParsedExpression> value;

    SetItem(string var, string key, unique_ptr<ParsedExpression> val)
        : variable_name(std::move(var)), property_key(std::move(key)), value(std::move(val)) {}
};

// Parsed SET clause: holds a list of SET items.
// SET n.firstName = 'Updated', n.lastName = 'New'
class SetClause : public UpdatingClause {
public:
    SetClause() : UpdatingClause(UpdatingClauseType::SET) {}

    void AddItem(SetItem item) {
        items.push_back(std::move(item));
    }

    const vector<SetItem>& GetItems() const { return items; }

private:
    vector<SetItem> items;
};

} // namespace turbolynx
