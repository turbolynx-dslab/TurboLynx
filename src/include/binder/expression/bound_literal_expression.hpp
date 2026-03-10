#pragma once

#include "binder/expression/bound_expression.hpp"
#include "common/types/value.hpp"

namespace duckdb {

class BoundLiteralExpression : public BoundExpression {
public:
    BoundLiteralExpression(Value val, string unique_name)
        : BoundExpression(BoundExpressionType::LITERAL, val.type(), std::move(unique_name)),
          value(std::move(val)) {}

    const Value& GetValue() const { return value; }

    shared_ptr<BoundExpression> Copy() const override {
        auto copy = make_shared<BoundLiteralExpression>(value, unique_name);
        copy->SetAlias(alias);
        return copy;
    }

private:
    Value value;
};

} // namespace duckdb
