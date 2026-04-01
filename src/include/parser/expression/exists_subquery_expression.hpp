#pragma once

#include "parser/parsed_expression.hpp"
#include "parser/query/graph_pattern/pattern_element.hpp"
#include <vector>
#include <memory>

namespace duckdb {

// Represents EXISTS { MATCH pattern [WHERE expr] } in Cypher.
// Stores the pattern elements and optional WHERE predicate.
class ExistsSubqueryExpression : public ParsedExpression {
public:
    ExistsSubqueryExpression(bool negated = false)
        : ParsedExpression(ExpressionType::SUBQUERY, ExpressionClass::SUBQUERY),
          negated_(negated) {}

    vector<unique_ptr<PatternElement>> patterns;
    unique_ptr<ParsedExpression> where_expr; // optional WHERE clause
    bool negated_; // NOT EXISTS

    bool HasSubquery() const override { return true; }
    bool IsScalar() const override { return false; }
    string ToString() const override { return negated_ ? "NOT EXISTS {...}" : "EXISTS {...}"; }

    static bool Equals(const ExistsSubqueryExpression *a, const ExistsSubqueryExpression *b) {
        return false; // not used
    }

    unique_ptr<ParsedExpression> Copy() const override {
        auto copy = make_unique<ExistsSubqueryExpression>(negated_);
        // Shallow copy — patterns are consumed during binding
        return copy;
    }

    void Serialize(FieldWriter &writer) const override {}
    static unique_ptr<ParsedExpression> Deserialize(ExpressionType type, FieldReader &source) {
        return make_unique<ExistsSubqueryExpression>();
    }
};

} // namespace duckdb
