#pragma once

#include "binder/expression/bound_expression.hpp"

namespace duckdb {

struct CypherBoundCaseCheck {
    shared_ptr<BoundExpression> when_expr;
    shared_ptr<BoundExpression> then_expr;
};

class CypherBoundCaseExpression : public BoundExpression {
public:
    CypherBoundCaseExpression(LogicalType result_type,
                         vector<CypherBoundCaseCheck> checks,
                         shared_ptr<BoundExpression> else_expr,
                         string unique_name)
        : BoundExpression(BoundExpressionType::CASE, std::move(result_type), std::move(unique_name)),
          checks(std::move(checks)), else_expr(std::move(else_expr)) {}

    const vector<CypherBoundCaseCheck>& GetChecks()     const { return checks; }
    bool                                 HasElse()       const { return else_expr != nullptr; }
    BoundExpression*                     GetElse()       const { return else_expr.get(); }

    shared_ptr<BoundExpression> Copy() const override {
        vector<CypherBoundCaseCheck> copied_checks;
        for (auto& c : checks) {
            copied_checks.push_back({c.when_expr->Copy(), c.then_expr->Copy()});
        }
        auto copied_else = else_expr ? else_expr->Copy() : nullptr;
        auto copy = make_shared<CypherBoundCaseExpression>(
            data_type, std::move(copied_checks), std::move(copied_else), unique_name);
        copy->SetAlias(alias);
        return copy;
    }

private:
    vector<CypherBoundCaseCheck> checks;
    shared_ptr<BoundExpression>  else_expr;
};

} // namespace duckdb
