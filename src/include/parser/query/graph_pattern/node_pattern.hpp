#pragma once

#include "common/pair.hpp"
#include "parser/parsed_expression.hpp"

namespace duckdb {

// Represents "(varName:Label1:Label2 {k1: v1, k2: v2})" in MATCH patterns.
class NodePattern {
public:
    NodePattern(string var_name, vector<string> labels,
                vector<pair<string, unique_ptr<ParsedExpression>>> properties)
        : var_name(std::move(var_name)),
          labels(std::move(labels)),
          properties(std::move(properties)) {}

    const string&              GetVarName()    const { return var_name; }
    const vector<string>&      GetLabels()     const { return labels; }
    bool                       HasVarName()    const { return !var_name.empty(); }

    idx_t GetNumProperties() const { return properties.size(); }
    const string& GetPropertyKey(idx_t i) const { return properties[i].first; }
    ParsedExpression* GetPropertyValue(idx_t i) const { return properties[i].second.get(); }

private:
    string var_name;
    vector<string> labels;
    vector<pair<string, unique_ptr<ParsedExpression>>> properties;
};

} // namespace duckdb
