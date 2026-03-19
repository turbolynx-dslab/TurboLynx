#pragma once

#include "binder/expression/bound_expression.hpp"

namespace duckdb {

class CypherBoundFunctionExpression : public BoundExpression {
public:
    CypherBoundFunctionExpression(string func_name, LogicalType return_type,
                             bound_expression_vector children, string unique_name,
                             bool is_distinct = false)
        : BoundExpression(BoundExpressionType::FUNCTION, std::move(return_type), std::move(unique_name)),
          func_name(std::move(func_name)), children(std::move(children)),
          is_distinct(is_distinct) {}

    const string&                  GetFuncName()  const { return func_name; }
    const bound_expression_vector& GetChildren()  const { return children; }
    bool                           IsDistinct()   const { return is_distinct; }
    idx_t                          GetNumChildren() const { return children.size(); }
    BoundExpression*               GetChild(idx_t i) const { return children[i].get(); }
    shared_ptr<BoundExpression>    GetChildShared(idx_t i) const { return children[i]; }

    shared_ptr<BoundExpression> Copy() const override {
        bound_expression_vector copied;
        for (auto& c : children) copied.push_back(c->Copy());
        auto copy = make_shared<CypherBoundFunctionExpression>(func_name, data_type, std::move(copied), unique_name, is_distinct);
        copy->SetAlias(alias);
        return copy;
    }

private:
    string                  func_name;
    bound_expression_vector children;
    bool                    is_distinct;
};

} // namespace duckdb
