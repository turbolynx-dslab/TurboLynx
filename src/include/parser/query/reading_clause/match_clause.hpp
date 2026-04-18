//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/query/reading_clause/match_clause.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/query/reading_clause/reading_clause.hpp"
#include "parser/query/graph_pattern/pattern_element.hpp"
#include "parser/parsed_expression.hpp"
#include <vector>
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

class MatchClause : public ReadingClause {
public:
    explicit MatchClause(vector<unique_ptr<PatternElement>> patterns, bool is_optional = false)
        : ReadingClause(CypherClauseType::MATCH),
          patterns(std::move(patterns)),
          is_optional(is_optional) {}

    const vector<unique_ptr<PatternElement>>& GetPatterns() const { return patterns; }
    bool IsOptional() const { return is_optional; }

    void SetWhere(unique_ptr<ParsedExpression> expr) { where_expr = std::move(expr); }
    bool HasWhere() const { return where_expr != nullptr; }
    ParsedExpression* GetWhere() const { return where_expr.get(); }

private:
    vector<unique_ptr<PatternElement>> patterns;
    unique_ptr<ParsedExpression> where_expr;
    bool is_optional;
};

} // namespace turbolynx
