//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/property_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_expression.hpp"

namespace duckdb {

// Represents "variable.propertyName" in a Cypher query.
// e.g., "n.name" → variable="n", property="name"
class ParsedPropertyExpression : public ParsedExpression {
public:
    ParsedPropertyExpression(string variable, string property)
        : ParsedExpression(ExpressionType::COLUMN_REF, ExpressionClass::COLUMN_REF),
          variable_name(std::move(variable)), property_name(std::move(property)) {}

    const string& GetVariableName() const { return variable_name; }
    const string& GetPropertyName() const { return property_name; }

    string ToString() const override {
        return variable_name + "." + property_name;
    }
    unique_ptr<ParsedExpression> Copy() const override {
        return make_unique<ParsedPropertyExpression>(variable_name, property_name);
    }
    void Serialize(FieldWriter &) const override {}

private:
    string variable_name;
    string property_name;
};

} // namespace duckdb
