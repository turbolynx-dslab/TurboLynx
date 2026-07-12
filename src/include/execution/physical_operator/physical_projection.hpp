//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/physical_operator/physical_projection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "common/typedef.hpp"

#include "execution/execution_context.hpp"
#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "planner/expression.hpp"
#include "planner/expression/bound_conjunction_expression.hpp"

#include "common/types/value.hpp"

namespace duckdb {

class PhysicalProjection : public CypherPhysicalOperator {

   public:
    PhysicalProjection(Schema &sch, vector<unique_ptr<Expression>> expressions)
        : CypherPhysicalOperator(PhysicalOperatorType::PROJECTION, sch),
          expressions(move(expressions))
    {
        ExtractCommonSubexpressions();
    }
    ~PhysicalProjection() {}

   public:
    unique_ptr<OperatorState> GetOperatorState(
        ExecutionContext &context) const override;
    OperatorResultType Execute(ExecutionContext &context, DataChunk &input,
                               DataChunk &chunk,
                               OperatorState &state) const override;

    std::string ParamsToString() const override;
    std::string ToString() const override;

    vector<unique_ptr<Expression>>
        expressions;  // projection expression per each column

    //! Common subexpressions shared by multiple projection expressions.
    //! Evaluated once per chunk against the raw input; `expressions` then run
    //! against a combined chunk laid out as [cse results..., input columns...].
    vector<unique_ptr<Expression>> cse_expressions;
    vector<LogicalType> cse_types;

   private:
    void ExtractCommonSubexpressions();
};

}  // namespace duckdb
