//===----------------------------------------------------------------------===//
//                         DuckDB
//
// src/execution/execution/physical_operator/physical_projection.cpp
//
//
//===----------------------------------------------------------------------===//


#include "common/typedef.hpp"
#include "execution/physical_operator/physical_projection.hpp"
#include "execution/expression_executor.hpp"
#include "planner/expression/bound_between_expression.hpp"
#include "planner/expression/bound_case_expression.hpp"
#include "planner/expression/bound_cast_expression.hpp"
#include "planner/expression/bound_comparison_expression.hpp"
#include "planner/expression/bound_conjunction_expression.hpp"
#include "planner/expression/bound_function_expression.hpp"
#include "planner/expression/bound_operator_expression.hpp"
#include "planner/expression/bound_reference_expression.hpp"

#include <unordered_set>

#include "icecream.hpp"

namespace duckdb {

namespace {

bool IsCSECandidate(Expression &expr);

//! Child traversal for the expression classes IsCSECandidate admits, plus refs
//! and constants (ExpressionIterator::EnumerateChildren does not support
//! BOUND_FUNCTION in this codebase)
void EnumerateCSEChildren(Expression &expr, const std::function<void(unique_ptr<Expression> &child)> &callback) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_FUNCTION: {
		for (auto &child : ((BoundFunctionExpression &)expr).children) {
			callback(child);
		}
		break;
	}
	case ExpressionClass::BOUND_OPERATOR: {
		for (auto &child : ((BoundOperatorExpression &)expr).children) {
			callback(child);
		}
		break;
	}
	case ExpressionClass::BOUND_CAST:
		callback(((BoundCastExpression &)expr).child);
		break;
	case ExpressionClass::BOUND_COMPARISON: {
		auto &comp_expr = (BoundComparisonExpression &)expr;
		callback(comp_expr.left);
		callback(comp_expr.right);
		break;
	}
	case ExpressionClass::BOUND_CONJUNCTION: {
		for (auto &child : ((BoundConjunctionExpression &)expr).children) {
			callback(child);
		}
		break;
	}
	case ExpressionClass::BOUND_CASE: {
		auto &case_expr = (BoundCaseExpression &)expr;
		for (auto &case_check : case_expr.case_checks) {
			callback(case_check.when_expr);
			callback(case_check.then_expr);
		}
		callback(case_expr.else_expr);
		break;
	}
	case ExpressionClass::BOUND_BETWEEN: {
		auto &between_expr = (BoundBetweenExpression &)expr;
		callback(between_expr.input);
		callback(between_expr.lower);
		callback(between_expr.upper);
		break;
	}
	default:
		break;
	}
}

//! self-contained property checks (Expression::IsScalar/HasSideEffects rely on
//! ExpressionIterator, which does not support BOUND_FUNCTION here)
bool CSEHasSideEffects(Expression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
	    ((BoundFunctionExpression &)expr).function.has_side_effects) {
		return true;
	}
	bool result = false;
	EnumerateCSEChildren(expr, [&](unique_ptr<Expression> &child) {
		if (CSEHasSideEffects(*child)) {
			result = true;
		}
	});
	return result;
}

bool CSEReferencesColumns(Expression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_REF) {
		return true;
	}
	bool result = false;
	EnumerateCSEChildren(expr, [&](unique_ptr<Expression> &child) {
		if (CSEReferencesColumns(*child)) {
			result = true;
		}
	});
	return result;
}

//! FunctionData::Equals defaults to true, so two functions that differ only in
//! their bind data (e.g. nodes(path) vs relationships(path)) can compare
//! equal; a subtree containing any bind data is unsafe to match structurally
bool CSEContainsBindData(Expression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
	    ((BoundFunctionExpression &)expr).bind_info) {
		return true;
	}
	bool result = false;
	EnumerateCSEChildren(expr, [&](unique_ptr<Expression> &child) {
		if (CSEContainsBindData(*child)) {
			result = true;
		}
	});
	return result;
}

//! Subtrees eligible for common-subexpression extraction: non-trivial,
//! deterministic, column-dependent scalar computations
bool IsCSECandidate(Expression &expr) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_FUNCTION:
	case ExpressionClass::BOUND_OPERATOR:
	case ExpressionClass::BOUND_CAST:
	case ExpressionClass::BOUND_COMPARISON:
	case ExpressionClass::BOUND_CONJUNCTION:
	case ExpressionClass::BOUND_CASE:
	case ExpressionClass::BOUND_BETWEEN:
		break;
	default:
		return false;
	}
	if (CSEHasSideEffects(expr)) {
		return false;
	}
	// constant-only subtrees are evaluated on constant vectors and are cheap
	if (!CSEReferencesColumns(expr)) {
		return false;
	}
	if (CSEContainsBindData(expr)) {
		return false;
	}
	return true;
}

//! CSE analysis must understand every node in the tree (the rewrite pass
//! adjusts column references); bail out on any unknown expression class
bool CSETraversalSupported(Expression &expr) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_REF:
	case ExpressionClass::BOUND_CONSTANT:
	case ExpressionClass::BOUND_PARAMETER:
	case ExpressionClass::BOUND_FUNCTION:
	case ExpressionClass::BOUND_OPERATOR:
	case ExpressionClass::BOUND_CAST:
	case ExpressionClass::BOUND_COMPARISON:
	case ExpressionClass::BOUND_CONJUNCTION:
	case ExpressionClass::BOUND_CASE:
	case ExpressionClass::BOUND_BETWEEN:
		break;
	default:
		return false;
	}
	bool supported = true;
	EnumerateCSEChildren(expr, [&](unique_ptr<Expression> &child) {
		if (!CSETraversalSupported(*child)) {
			supported = false;
		}
	});
	return supported;
}

//! ScalarFunction::operator== cannot distinguish two lambda-registered
//! functions (CompareScalarFunctionT returns true when both targets are
//! lambdas), so Expression::Equals alone can conflate different functions
//! (e.g. nodes(path) vs relationships(path)); additionally require function
//! names to match through the whole tree
bool CSEFunctionNamesMatch(Expression &a, Expression &b) {
	if (a.GetExpressionClass() != b.GetExpressionClass()) {
		return false;
	}
	if (a.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
	    ((BoundFunctionExpression &)a).function.name != ((BoundFunctionExpression &)b).function.name) {
		return false;
	}
	vector<Expression *> a_children;
	vector<Expression *> b_children;
	EnumerateCSEChildren(a, [&](unique_ptr<Expression> &child) { a_children.push_back(child.get()); });
	EnumerateCSEChildren(b, [&](unique_ptr<Expression> &child) { b_children.push_back(child.get()); });
	if (a_children.size() != b_children.size()) {
		return false;
	}
	for (idx_t i = 0; i < a_children.size(); i++) {
		if (!CSEFunctionNamesMatch(*a_children[i], *b_children[i])) {
			return false;
		}
	}
	return true;
}

bool CSEExpressionsEqual(Expression &a, Expression &b) {
	return Expression::Equals(&a, &b) && CSEFunctionNamesMatch(a, b);
}

struct CSEBucket {
	unique_ptr<Expression> repr;
	idx_t count = 0;
	idx_t cse_index = DConstants::INVALID_INDEX;
};

//! CASE branches and conjunctions may be evaluated lazily on a subset of rows;
//! hoisting a subtree out of them would evaluate it eagerly on all rows and
//! can change semantics (e.g. guarded division). Extracting the whole node is
//! fine, descending into it is not.
bool IsLazilyEvaluated(const Expression &expr) {
	return expr.GetExpressionClass() == ExpressionClass::BOUND_CASE ||
	       expr.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION;
}

void CountCSESubtrees(Expression &expr, vector<CSEBucket> &buckets) {
	if (IsCSECandidate(expr)) {
		bool found = false;
		for (auto &bucket : buckets) {
			if (CSEExpressionsEqual(*bucket.repr, expr)) {
				bucket.count++;
				found = true;
				break;
			}
		}
		if (!found) {
			CSEBucket bucket;
			bucket.repr = expr.Copy();
			bucket.count = 1;
			buckets.push_back(move(bucket));
		}
	}
	if (IsLazilyEvaluated(expr)) {
		return;
	}
	EnumerateCSEChildren(expr, [&](unique_ptr<Expression> &child) { CountCSESubtrees(*child, buckets); });
}

//! Top-down: replace maximal duplicated subtrees with references to the CSE
//! output columns (indexes 0..n_cse-1 of the combined chunk)
void RewriteCSESubtrees(unique_ptr<Expression> &expr, vector<CSEBucket> &buckets,
                        vector<unique_ptr<Expression>> &cse_exprs,
                        std::unordered_set<Expression *> &cse_refs) {
	if (IsCSECandidate(*expr)) {
		for (auto &bucket : buckets) {
			if (bucket.count >= 2 && CSEExpressionsEqual(*bucket.repr, *expr)) {
				if (bucket.cse_index == DConstants::INVALID_INDEX) {
					bucket.cse_index = cse_exprs.size();
					cse_exprs.push_back(expr->Copy());
				}
				expr = make_unique<BoundReferenceExpression>(expr->return_type, (storage_t)bucket.cse_index);
				cse_refs.insert(expr.get());
				return;
			}
		}
	}
	if (IsLazilyEvaluated(*expr)) {
		return;
	}
	EnumerateCSEChildren(
	    *expr, [&](unique_ptr<Expression> &child) { RewriteCSESubtrees(child, buckets, cse_exprs, cse_refs); });
}

//! Shift input-column references by n_cse to match the combined chunk layout
void ShiftInputReferences(Expression &expr, idx_t n_cse, const std::unordered_set<Expression *> &cse_refs) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_REF && cse_refs.find(&expr) == cse_refs.end()) {
		((BoundReferenceExpression &)expr).index += n_cse;
	}
	EnumerateCSEChildren(expr,
	                     [&](unique_ptr<Expression> &child) { ShiftInputReferences(*child, n_cse, cse_refs); });
}

} // namespace

void PhysicalProjection::ExtractCommonSubexpressions() {
	for (auto &expr : expressions) {
		if (!CSETraversalSupported(*expr)) {
			return;
		}
	}
	vector<CSEBucket> buckets;
	try {
		// Copy() can throw for expressions carrying non-copyable bind data;
		// no rewrite has happened yet, so simply skip CSE for this projection.
		// The rewrite phase only copies subtrees that were copied successfully
		// here, so it cannot throw mid-rewrite.
		for (auto &expr : expressions) {
			CountCSESubtrees(*expr, buckets);
		}
	} catch (...) {
		return;
	}
	bool has_duplicates = false;
	for (auto &bucket : buckets) {
		if (bucket.count >= 2) {
			has_duplicates = true;
			break;
		}
	}
	if (!has_duplicates) {
		return;
	}
	std::unordered_set<Expression *> cse_refs;
	for (auto &expr : expressions) {
		RewriteCSESubtrees(expr, buckets, cse_expressions, cse_refs);
	}
	if (cse_expressions.empty()) {
		return;
	}
	for (auto &expr : expressions) {
		ShiftInputReferences(*expr, cse_expressions.size(), cse_refs);
	}
	for (auto &cse : cse_expressions) {
		cse_types.push_back(cse->return_type);
	}
}

class ProjectionState : public OperatorState {
public:
	ProjectionState(const vector<unique_ptr<Expression>> &expressions,
	                const vector<unique_ptr<Expression>> &cse_expressions)
	    : executor(expressions) {
		for (auto &expr : cse_expressions) {
			cse_executor.AddExpression(*expr);
		}
	}
public:
	ExpressionExecutor executor;
	//! Executor for common subexpressions (against the raw input chunk)
	ExpressionExecutor cse_executor;
	DataChunk cse_chunk;
	//! [cse results..., input columns...]; lazily initialized on first chunk
	DataChunk combined_chunk;
};

unique_ptr<OperatorState> PhysicalProjection::GetOperatorState(ExecutionContext &context) const {
	return make_unique<ProjectionState>(expressions, cse_expressions);
}

OperatorResultType PhysicalProjection::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {
	auto &state = (ProjectionState &)lstate;
	if (!cse_expressions.empty()) {
		// evaluate shared subexpressions once, then run the projection over
		// the combined [cse..., input...] chunk
		if (state.combined_chunk.ColumnCount() == 0) {
			vector<LogicalType> combined_types = cse_types;
			for (auto &type : input.GetTypes()) {
				combined_types.push_back(type);
			}
			state.combined_chunk.InitializeEmpty(combined_types);
			state.cse_chunk.Initialize(cse_types);
		}
		state.cse_chunk.Reset();
		state.cse_executor.Execute(input, state.cse_chunk);
		for (idx_t i = 0; i < cse_types.size(); i++) {
			state.combined_chunk.data[i].Reference(state.cse_chunk.data[i]);
		}
		for (idx_t i = 0; i < input.ColumnCount(); i++) {
			state.combined_chunk.data[cse_types.size() + i].Reference(input.data[i]);
		}
		state.combined_chunk.SetCardinality(input.size());
		state.executor.Execute(state.combined_chunk, chunk);
	} else {
		state.executor.Execute(input, chunk);
	}
	return OperatorResultType::NEED_MORE_INPUT;
}

std::string PhysicalProjection::ParamsToString() const {
	string result = "projection-param: ";
	for (auto &expr : expressions) {
		result += expr->ToString() + ", ";
	}
	return result;
}

std::string PhysicalProjection::ToString() const {
	return "Projection";
}








}
