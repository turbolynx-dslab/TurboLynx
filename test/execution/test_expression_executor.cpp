#include "catch.hpp"

#include "common/types/data_chunk.hpp"
#include "execution/expression_executor.hpp"
#include "planner/expression/bound_comparison_expression.hpp"
#include "planner/expression/bound_conjunction_expression.hpp"
#include "planner/expression/bound_constant_expression.hpp"
#include "planner/expression/bound_reference_expression.hpp"

using namespace duckdb;

namespace {

class TestableExpressionExecutor : public ExpressionExecutor {
public:
	using ExpressionExecutor::ExpressionExecutor;

	idx_t SelectWithVectors(DataChunk &input, SelectionVector *true_sel,
	                        SelectionVector *false_sel) {
		SetChunk(&input);
		auto &states = GetStates();
		return Select(*expressions[0], states[0]->root_state.get(), nullptr,
		              input.size(), true_sel, false_sel);
	}
};

unique_ptr<Expression> EqRefConst(idx_t col_idx, int32_t constant) {
	return make_unique<BoundComparisonExpression>(
	    ExpressionType::COMPARE_EQUAL,
	    make_unique<BoundReferenceExpression>(LogicalType::INTEGER, col_idx),
	    make_unique<BoundConstantExpression>(Value::INTEGER(constant)));
}

void FillSingleIntChunk(DataChunk &input, std::initializer_list<int32_t> values) {
	input.Initialize({LogicalType::INTEGER});
	input.SetCardinality(values.size());
	idx_t row_idx = 0;
	for (auto value : values) {
		input.SetValue(0, row_idx++, Value::INTEGER(value));
	}
}

} // namespace

TEST_CASE("ExpressionExecutor OR false-only select returns true count",
          "[execution]") {
	auto expr = make_unique<BoundConjunctionExpression>(
	    ExpressionType::CONJUNCTION_OR, EqRefConst(0, 1), EqRefConst(0, 2));
	TestableExpressionExecutor executor(*expr);
	DataChunk input;
	FillSingleIntChunk(input, {1, 2, 3, 4});
	SelectionVector false_sel(STANDARD_VECTOR_SIZE);

	auto true_count = executor.SelectWithVectors(input, nullptr, &false_sel);

	REQUIRE(true_count == 2);
	REQUIRE(false_sel.get_index(0) == 2);
	REQUIRE(false_sel.get_index(1) == 3);
}

TEST_CASE("ExpressionExecutor nested OR false-only select keeps child true count",
          "[execution]") {
	auto left_or = make_unique<BoundConjunctionExpression>(
	    ExpressionType::CONJUNCTION_OR, EqRefConst(0, 1), EqRefConst(0, 2));
	auto expr = make_unique<BoundConjunctionExpression>(
	    ExpressionType::CONJUNCTION_OR, std::move(left_or), EqRefConst(0, 3));
	TestableExpressionExecutor executor(*expr);
	DataChunk input;
	FillSingleIntChunk(input, {1, 2, 3, 4});
	SelectionVector false_sel(STANDARD_VECTOR_SIZE);

	auto true_count = executor.SelectWithVectors(input, nullptr, &false_sel);

	REQUIRE(true_count == 3);
	REQUIRE(false_sel.get_index(0) == 3);
}
