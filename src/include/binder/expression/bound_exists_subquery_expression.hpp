#pragma once

#include "binder/expression/bound_expression.hpp"
#include "binder/query/reading_clause/bound_match_clause.hpp"
#include <memory>

namespace duckdb {

// Bound representation of EXISTS { MATCH pattern [WHERE expr] }.
// Contains the bound match clause (query graph + predicates) for the subquery.
class BoundExistsSubqueryExpression : public BoundExpression {
public:
    BoundExistsSubqueryExpression(unique_ptr<BoundMatchClause> match, const string& unique_name)
        : BoundExpression(BoundExpressionType::EXISTENTIAL, LogicalType::BOOLEAN, unique_name),
          bound_match(std::move(match)) {}

    BoundMatchClause& GetBoundMatch() { return *bound_match; }
    const BoundMatchClause& GetBoundMatch() const { return *bound_match; }

    shared_ptr<BoundExpression> Copy() const override {
        // EXISTS subquery is consumed during planning — shallow copy is sufficient
        return make_shared<BoundExistsSubqueryExpression>(nullptr, unique_name);
    }

private:
    unique_ptr<BoundMatchClause> bound_match;
};

} // namespace duckdb
