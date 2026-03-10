#pragma once

#include "parser/query/reading_clause/reading_clause.hpp"
#include "parser/parsed_expression.hpp"
#include <string>
#include <memory>

namespace duckdb {

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

} // namespace duckdb
