#pragma once

#include "binder/expression/bound_expression.hpp"

namespace duckdb {

// IS NULL / IS NOT NULL
class BoundNullExpression : public BoundExpression {
public:
    BoundNullExpression(bool is_not_null, shared_ptr<BoundExpression> child, string unique_name)
        : BoundExpression(BoundExpressionType::NULL_OP, LogicalType::BOOLEAN, std::move(unique_name)),
          is_not_null(is_not_null), child(std::move(child)) {}

    bool             IsNotNull() const { return is_not_null; }
    BoundExpression* GetChild()  const { return child.get(); }

    shared_ptr<BoundExpression> Copy() const override {
        auto copy = make_shared<BoundNullExpression>(is_not_null, child->Copy(), unique_name);
        copy->SetAlias(alias);
        return copy;
    }

private:
    bool                        is_not_null;
    shared_ptr<BoundExpression> child;
};

} // namespace duckdb
