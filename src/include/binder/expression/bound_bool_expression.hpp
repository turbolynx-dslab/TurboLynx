#pragma once

#include "binder/expression/bound_expression.hpp"

namespace duckdb {

enum class BoundBoolOpType : uint8_t { AND = 0, OR = 1, NOT = 2 };

class BoundBoolExpression : public BoundExpression {
public:
    BoundBoolExpression(BoundBoolOpType op_type, bound_expression_vector children, string unique_name)
        : BoundExpression(BoundExpressionType::BOOL_OP, LogicalType::BOOLEAN, std::move(unique_name)),
          op_type(op_type), children(std::move(children)) {}

    BoundBoolOpType                GetOpType()     const { return op_type; }
    const bound_expression_vector& GetChildren()   const { return children; }
    idx_t                          GetNumChildren() const { return children.size(); }
    BoundExpression*               GetChild(idx_t i) const { return children[i].get(); }

    shared_ptr<BoundExpression> Copy() const override {
        bound_expression_vector copied;
        for (auto& c : children) copied.push_back(c->Copy());
        auto copy = make_shared<BoundBoolExpression>(op_type, std::move(copied), unique_name);
        copy->SetAlias(alias);
        return copy;
    }

private:
    BoundBoolOpType         op_type;
    bound_expression_vector children;
};

} // namespace duckdb
