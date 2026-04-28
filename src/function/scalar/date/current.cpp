//===----------------------------------------------------------------------===//
//                         DuckDB
//
// src/function/scalar/date/current.cpp
//
//
//===----------------------------------------------------------------------===//

#include "function/scalar/date_functions.hpp"

#include "common/exception.hpp"
#include "common/types/timestamp.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "planner/expression/bound_function_expression.hpp"
#include "main/client_context.hpp"

#include <chrono>
//#include "transaction/transaction.hpp"

namespace duckdb {

struct CurrentBindData : public FunctionData {
	ClientContext &context;

	explicit CurrentBindData(ClientContext &context) : context(context) {
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<CurrentBindData>(context);
	}
};

static timestamp_t GetTransactionTimestamp(ExpressionState &state) {
	// auto &func_expr = (BoundFunctionExpression &)state.expr;
	// auto &info = (CurrentBindData &)*func_expr.bind_info;
	// return info.context.ActiveTransaction().start_timestamp;
	timestamp_t tmp;
	return tmp;
}

static void CurrentTimeFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	// D_ASSERT(input.ColumnCount() == 0);

	// auto val = Value::TIME(Timestamp::GetTime(GetTransactionTimestamp(state)));
	// result.Reference(val);
}

static void CurrentDateFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	// D_ASSERT(input.ColumnCount() == 0);

	// auto val = Value::DATE(Timestamp::GetDate(GetTransactionTimestamp(state)));
	// result.Reference(val);
}

static void CurrentTimestampFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	// D_ASSERT(input.ColumnCount() == 0);

	// auto val = Value::TIMESTAMP(GetTransactionTimestamp(state));
	// result.Reference(val);
}

unique_ptr<FunctionData> BindCurrentTime(ClientContext &context, ScalarFunction &bound_function,
                                         vector<unique_ptr<Expression>> &arguments) {
	return make_unique<CurrentBindData>(context);
}

void CurrentTimeFun::RegisterFunction(BuiltinFunctions &set) {
	// set.AddFunction(ScalarFunction("current_time", {}, LogicalType::TIME, CurrentTimeFunction, false, BindCurrentTime));
}

void CurrentDateFun::RegisterFunction(BuiltinFunctions &set) {
	// set.AddFunction(ScalarFunction("current_date", {}, LogicalType::DATE, CurrentDateFunction, false, BindCurrentTime));
}

void CurrentTimestampFun::RegisterFunction(BuiltinFunctions &set) {
	// set.AddFunction({"now", "current_timestamp"}, ScalarFunction({}, LogicalType::TIMESTAMP, CurrentTimestampFunction,
	//                                                              false, false, BindCurrentTime));

	// Cypher's `timestamp()` returns milliseconds since the Unix epoch as
	// BIGINT — not transaction-scoped, just wall-clock. Implement directly
	// against std::chrono so we don't need the ActiveTransaction plumbing
	// the stubbed CurrentTimestampFunction above is waiting on.
	struct CypherTimestampFunctor {
		static void Function(DataChunk &args, ExpressionState &state,
		                     Vector &result) {
			D_ASSERT(args.ColumnCount() == 0);
			auto now = std::chrono::system_clock::now();
			int64_t ms = std::chrono::duration_cast<std::chrono::milliseconds>(
			                 now.time_since_epoch())
			                 .count();
			Value v = Value::BIGINT(ms);
			result.Reference(v);
		}
	};
	set.AddFunction(ScalarFunction("timestamp", {}, LogicalType::BIGINT,
	                               CypherTimestampFunctor::Function));
}

} // namespace duckdb
