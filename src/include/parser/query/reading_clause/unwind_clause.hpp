//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/query/reading_clause/unwind_clause.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/query/reading_clause/reading_clause.hpp"
#include "parser/parsed_expression.hpp"
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

// UNWIND expr AS alias
class UnwindClause : public ReadingClause {
public:
    UnwindClause(unique_ptr<ParsedExpression> expr, string alias)
        : ReadingClause(CypherClauseType::UNWIND),
          expr(std::move(expr)),
          alias(std::move(alias)) {}

    ParsedExpression* GetExpression() const { return expr.get(); }
    const string&     GetAlias()      const { return alias; }

private:
    unique_ptr<ParsedExpression> expr;
    string alias;
};

} // namespace turbolynx
