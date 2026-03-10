#pragma once

#include "parser/query/return_with_clause/return_clause.hpp"
#include "parser/parsed_expression.hpp"

namespace duckdb {

// WITH clause is a RETURN that pipes into the next query part.
// It may optionally have a WHERE predicate.
class WithClause : public ReturnClause {
public:
    explicit WithClause(unique_ptr<ProjectionBody> body) : ReturnClause(std::move(body)) {}

    void SetWhere(unique_ptr<ParsedExpression> expr) { where_expr = std::move(expr); }
    bool HasWhere() const { return where_expr != nullptr; }
    ParsedExpression* GetWhere() const { return where_expr.get(); }

private:
    unique_ptr<ParsedExpression> where_expr;
};

} // namespace duckdb
