//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/query/return_with_clause/with_clause.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/query/return_with_clause/return_clause.hpp"
#include "parser/parsed_expression.hpp"

namespace duckdb {
}
namespace turbolynx {
}
namespace duckdb {
	using namespace turbolynx;
}
namespace turbolynx {
using namespace duckdb;

// WITH clause is a RETURN that pipes into the next query part.
// It may optionally have a WHERE predicate.
class WithClause : public ReturnClause {
public:
    explicit WithClause(unique_ptr<ProjectionBody> body) : ReturnClause(std::move(body)) {}

    void SetWhere(unique_ptr<ParsedExpression> expr) { where_expr = std::move(expr); }
    bool HasWhere() const { return where_expr != nullptr; }
    ParsedExpression* GetWhere() const { return where_expr.get(); }

private:
    unique_ptr<ParsedExpression> where_expr;
};

} // namespace turbolynx
