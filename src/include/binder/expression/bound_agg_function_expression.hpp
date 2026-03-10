#pragma once

#include "binder/expression/bound_expression.hpp"

namespace duckdb {

class BoundAggFunctionExpression : public BoundExpression {
public:
    BoundAggFunctionExpression(string func_name, LogicalType return_type,
                                shared_ptr<BoundExpression> child,
                                bool is_distinct, string unique_name)
        : BoundExpression(BoundExpressionType::AGG_FUNCTION, std::move(return_type), std::move(unique_name)),
          func_name(std::move(func_name)), child(std::move(child)),
          is_distinct(is_distinct) {}

    const string&  GetFuncName()  const { return func_name; }
    bool           IsDistinct()   const { return is_distinct; }
    bool           HasChild()     const { return child != nullptr; }
    BoundExpression* GetChild()   const { return child.get(); }

    shared_ptr<BoundExpression> Copy() const override {
        auto copied_child = child ? child->Copy() : nullptr;
        auto copy = make_shared<BoundAggFunctionExpression>(
            func_name, data_type, std::move(copied_child), is_distinct, unique_name);
        copy->SetAlias(alias);
        return copy;
    }

private:
    string                      func_name;
    shared_ptr<BoundExpression> child;   // nullptr for COUNT(*)
    bool                        is_distinct;
};

} // namespace duckdb
