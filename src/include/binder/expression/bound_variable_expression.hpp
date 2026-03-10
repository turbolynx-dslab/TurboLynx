#pragma once

#include "binder/expression/bound_expression.hpp"

namespace duckdb {

// Represents a whole-node or whole-rel variable reference (e.g. 'n' in RETURN n).
class BoundVariableExpression : public BoundExpression {
public:
    BoundVariableExpression(string var_name, LogicalType data_type, string unique_name)
        : BoundExpression(BoundExpressionType::VARIABLE, std::move(data_type), std::move(unique_name)),
          var_name(std::move(var_name)) {}

    const string& GetVarName() const { return var_name; }

    shared_ptr<BoundExpression> Copy() const override {
        auto copy = make_shared<BoundVariableExpression>(var_name, data_type, unique_name);
        copy->SetAlias(alias);
        return copy;
    }

private:
    string var_name;
};

} // namespace duckdb
