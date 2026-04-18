//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/query/return_with_clause/projection_body.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_expression.hpp"
#include <vector>
#include <memory>

namespace duckdb {
}
namespace turbolynx {
}
namespace duckdb {
	using namespace turbolynx;
}
namespace turbolynx {
using namespace duckdb;

struct OrderByItem {
    unique_ptr<ParsedExpression> expr;
    bool ascending = true;

    OrderByItem(unique_ptr<ParsedExpression> expr, bool ascending)
        : expr(std::move(expr)), ascending(ascending) {}
};

// RETURN / WITH projection body:
//   [DISTINCT] expr [AS alias], ... [ORDER BY ...] [SKIP n] [LIMIT n]
class ProjectionBody {
public:
    ProjectionBody(bool is_distinct, bool contains_star,
                   vector<unique_ptr<ParsedExpression>> projections)
        : is_distinct(is_distinct),
          contains_star(contains_star),
          projections(std::move(projections)) {}

    bool IsDistinct()    const { return is_distinct; }
    bool ContainsStar()  const { return contains_star; }

    const vector<unique_ptr<ParsedExpression>>& GetProjections() const { return projections; }

    void SetOrderBy(vector<OrderByItem> items) { order_by = std::move(items); }
    bool HasOrderBy() const { return !order_by.empty(); }
    const vector<OrderByItem>& GetOrderBy() const { return order_by; }

    void SetSkip(unique_ptr<ParsedExpression> expr)  { skip_expr  = std::move(expr); }
    void SetLimit(unique_ptr<ParsedExpression> expr) { limit_expr = std::move(expr); }
    bool HasSkip()  const { return skip_expr  != nullptr; }
    bool HasLimit() const { return limit_expr != nullptr; }
    ParsedExpression* GetSkip()  const { return skip_expr.get(); }
    ParsedExpression* GetLimit() const { return limit_expr.get(); }

private:
    bool is_distinct;
    bool contains_star;
    vector<unique_ptr<ParsedExpression>> projections;
    vector<OrderByItem> order_by;
    unique_ptr<ParsedExpression> skip_expr;
    unique_ptr<ParsedExpression> limit_expr;
};

} // namespace turbolynx
