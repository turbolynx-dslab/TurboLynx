//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/variable_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_expression.hpp"

namespace duckdb {

// Represents a bare variable reference in a Cypher query.
// e.g., "n" in "RETURN n"
class ParsedVariableExpression : public ParsedExpression {
public:
    explicit ParsedVariableExpression(string variable)
        : ParsedExpression(ExpressionType::COLUMN_REF, ExpressionClass::COLUMN_REF),
          variable_name(std::move(variable)) {}

    const string& GetVariableName() const { return variable_name; }

    string ToString() const override { return variable_name; }
    unique_ptr<ParsedExpression> Copy() const override {
        return make_unique<ParsedVariableExpression>(variable_name);
    }
    void Serialize(FieldWriter &) const override {}

private:
    string variable_name;
};

} // namespace duckdb
