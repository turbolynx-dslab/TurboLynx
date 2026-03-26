#pragma once

#include "parser/query/updating_clause/updating_clause.hpp"
#include <vector>
#include <string>

namespace duckdb {

// Parsed DELETE clause: DELETE n, m, ...
// Stores the variable names of nodes/edges to delete.
class DeleteClause : public UpdatingClause {
public:
    DeleteClause() : UpdatingClause(UpdatingClauseType::DELETE_CLAUSE) {}

    void AddVariable(string var) {
        variables.push_back(std::move(var));
    }

    const vector<string>& GetVariables() const { return variables; }

private:
    vector<string> variables;
};

} // namespace duckdb
