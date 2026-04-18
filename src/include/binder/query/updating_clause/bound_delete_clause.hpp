#pragma once

#include "binder/query/updating_clause/bound_updating_clause.hpp"
#include <vector>
#include <string>

namespace duckdb {
}
namespace turbolynx {
}
namespace duckdb {
	using namespace turbolynx;
}
namespace turbolynx {
using namespace duckdb;

class BoundDeleteClause : public BoundUpdatingClause {
public:
    BoundDeleteClause() : BoundUpdatingClause(BoundUpdatingClauseType::DELETE_CLAUSE) {}

    void AddVariable(string var) {
        variables.push_back(std::move(var));
    }

    const vector<string>& GetVariables() const { return variables; }

private:
    vector<string> variables;
};

} // namespace turbolynx
