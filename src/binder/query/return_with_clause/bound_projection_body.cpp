#include "binder/query/return_with_clause/bound_projection_body.hpp"
#include "binder/expression/bound_agg_function_expression.hpp"
#include "binder/expression/bound_function_expression.hpp"
#include "binder/expression/bound_bool_expression.hpp"
#include "binder/expression/bound_comparison_expression.hpp"
#include "binder/expression/bound_case_expression.hpp"

namespace duckdb {
}
namespace turbolynx {
}
namespace duckdb {
	using namespace turbolynx;
}
namespace turbolynx {
using namespace duckdb;

static bool HasAggExpr(const BoundExpression& expr) {
    if (expr.GetExprType() == BoundExpressionType::AGG_FUNCTION) {
        return true;
    }
    if (expr.GetExprType() == BoundExpressionType::FUNCTION) {
        auto& fn = static_cast<const CypherBoundFunctionExpression&>(expr);
        for (idx_t i = 0; i < fn.GetNumChildren(); i++) {
            if (HasAggExpr(*fn.GetChild(i))) return true;
        }
    }
    if (expr.GetExprType() == BoundExpressionType::BOOL_OP) {
        auto& b = static_cast<const BoundBoolExpression&>(expr);
        for (idx_t i = 0; i < b.GetNumChildren(); i++) {
            if (HasAggExpr(*b.GetChild(i))) return true;
        }
    }
    if (expr.GetExprType() == BoundExpressionType::COMPARISON) {
        auto& cmp = static_cast<const CypherBoundComparisonExpression&>(expr);
        return HasAggExpr(*cmp.GetLeft()) || HasAggExpr(*cmp.GetRight());
    }
    if (expr.GetExprType() == BoundExpressionType::CASE) {
        auto& c = static_cast<const CypherBoundCaseExpression&>(expr);
        for (auto& check : c.GetChecks()) {
            if (HasAggExpr(*check.when_expr) || HasAggExpr(*check.then_expr)) return true;
        }
        if (c.HasElse() && HasAggExpr(*c.GetElse())) return true;
    }
    return false;
}

bool BoundProjectionBody::HasAggregation() const {
    for (auto& proj : projections) {
        if (HasAggExpr(*proj)) return true;
    }
    return false;
}

} // namespace turbolynx
