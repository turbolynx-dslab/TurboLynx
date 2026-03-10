#pragma once

#include "binder/query/reading_clause/bound_reading_clause.hpp"
#include "binder/expression/bound_expression.hpp"

namespace duckdb {

class BoundUnwindClause : public BoundReadingClause {
public:
    BoundUnwindClause(shared_ptr<BoundExpression> expr, string alias)
        : BoundReadingClause(BoundClauseType::UNWIND),
          expr(std::move(expr)), alias(std::move(alias)) {}

    BoundExpression* GetExpression() const { return expr.get(); }
    const string&    GetAlias()      const { return alias; }

private:
    shared_ptr<BoundExpression> expr;
    string                      alias;
};

} // namespace duckdb
