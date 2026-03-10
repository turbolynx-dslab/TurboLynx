#pragma once

#include "binder/expression/bound_expression.hpp"
#include "common/enums/expression_type.hpp"

namespace duckdb {

class CypherBoundComparisonExpression : public BoundExpression {
public:
    CypherBoundComparisonExpression(ExpressionType cmp_type,
                               shared_ptr<BoundExpression> left,
                               shared_ptr<BoundExpression> right,
                               string unique_name)
        : BoundExpression(BoundExpressionType::COMPARISON, LogicalType::BOOLEAN, std::move(unique_name)),
          cmp_type(cmp_type), left(std::move(left)), right(std::move(right)) {}

    ExpressionType    GetCmpType()  const { return cmp_type; }
    BoundExpression*  GetLeft()     const { return left.get(); }
    BoundExpression*  GetRight()    const { return right.get(); }

    shared_ptr<BoundExpression> Copy() const override {
        auto copy = make_shared<CypherBoundComparisonExpression>(cmp_type, left->Copy(), right->Copy(), unique_name);
        copy->SetAlias(alias);
        return copy;
    }

private:
    ExpressionType              cmp_type;
    shared_ptr<BoundExpression> left;
    shared_ptr<BoundExpression> right;
};

} // namespace duckdb
