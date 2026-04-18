//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/query/updating_clause/create_clause.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/query/updating_clause/updating_clause.hpp"
#include "parser/query/graph_pattern/pattern_element.hpp"
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

// Parsed CREATE clause: holds the pattern elements from CREATE (n:Label {props}).
// Each PatternElement's first NodePattern contains variable, labels, and properties.
class CreateClause : public UpdatingClause {
public:
    CreateClause() : UpdatingClause(UpdatingClauseType::INSERT) {}

    void AddPattern(unique_ptr<PatternElement> pattern) {
        patterns.push_back(std::move(pattern));
    }

    const vector<unique_ptr<PatternElement>>& GetPatterns() const { return patterns; }

private:
    vector<unique_ptr<PatternElement>> patterns;
};

} // namespace turbolynx
