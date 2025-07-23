#include "catch.hpp"
#include "test_config.hpp"

#include "execution/physical_operator/physical_projection.hpp"
#include "common/types/data_chunk.hpp"
#include "common/types/vector.hpp"
#include "planner/expression/bound_reference_expression.hpp"
#include "planner/expression/bound_operator_expression.hpp"
#include "planner/expression/bound_function_expression.hpp"
#include "execution/execution_context.hpp"
#include "function/function.hpp"
#include "function/scalar_function.hpp"
#include "function/scalar/operators.hpp"

using namespace duckdb;

TEST_CASE("Test projection with BoundReferenceExpression (col0 + col1)") {
    // 1. Set up input types: 2 integers
    vector<LogicalType> input_types = {LogicalType::INTEGER, LogicalType::INTEGER};

    // 2. Create input DataChunk
    DataChunk input;
    input.Initialize(input_types);
    input.SetCardinality(1);
    input.SetValue(0, 0, Value::INTEGER(10));  // col0 = 10
    input.SetValue(1, 0, Value::INTEGER(20));  // col1 = 20

    // 3. Create projection expression: col0 + col1
    vector<unique_ptr<Expression>> children;
    children.push_back(std::make_unique<BoundReferenceExpression>(LogicalType::INTEGER, 0));  // ref col 0
    children.push_back(std::make_unique<BoundReferenceExpression>(LogicalType::INTEGER, 1));  // ref col 1
    ScalarFunction add_func = AddFun::GetFunction(LogicalType::INTEGER, LogicalType::INTEGER);

    vector<unique_ptr<Expression>> expressions;
    expressions.push_back(
        std::make_unique<BoundFunctionExpression>(
            LogicalType::INTEGER,
            std::move(add_func),
            std::move(children),
            nullptr,
            false
        )
    );

    // 4. Set up schema
    Schema schema;
    schema.setStoredTypes({LogicalType::INTEGER});

    // 5. Create operator
    PhysicalProjection projection(schema, std::move(expressions));

    // 6. Output chunk
    DataChunk output;
    output.Initialize(schema.getStoredTypes(), 1);

    // 7. Execution context/state
    ExecutionContext exec_context(nullptr);
    auto state = projection.GetOperatorState(exec_context);

    // 8. Run projection
    auto result = projection.Execute(exec_context, input, output, *state);

    // 9. Check result
    REQUIRE(result == OperatorResultType::NEED_MORE_INPUT);
    REQUIRE(output.size() == 1);
    REQUIRE(output.GetValue(0, 0) == Value::INTEGER(30));
}
