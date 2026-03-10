#pragma once

#include "binder/expression/bound_expression.hpp"

namespace duckdb {

// Represents n.prop — fully resolved: the variable maps to a node/rel, the
// property maps to a specific property key ID in the catalog.
class BoundPropertyExpression : public BoundExpression {
public:
    BoundPropertyExpression(string var_name, uint64_t property_key_id,
                             LogicalType data_type, string unique_name)
        : BoundExpression(BoundExpressionType::PROPERTY, std::move(data_type), std::move(unique_name)),
          var_name(std::move(var_name)), property_key_id(property_key_id) {}

    const string& GetVarName()       const { return var_name; }
    uint64_t      GetPropertyKeyID() const { return property_key_id; }

    shared_ptr<BoundExpression> Copy() const override {
        auto copy = make_shared<BoundPropertyExpression>(var_name, property_key_id, data_type, unique_name);
        copy->SetAlias(alias);
        return copy;
    }

private:
    string   var_name;
    uint64_t property_key_id;
};

} // namespace duckdb
