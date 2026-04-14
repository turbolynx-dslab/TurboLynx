#include "function/scalar/string_functions.hpp"

#include "common/exception.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "common/vector_operations/unary_executor.hpp"
#include "common/operator/convert_to_string.hpp"

namespace duckdb {
struct ToStringOperator {
	template <class TA, class TR>
	static inline TR Operation(const TA &input) {
		return ConvertToString::Operation<TA>(input);
	}
};

// VARCHAR passthrough
struct ToStringVarcharOp {
	template <class TA, class TR>
	static inline TR Operation(const TA &input) {
		return input;
	}
};

void ToStringFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet func_set("tostring");
	// INTEGER → VARCHAR
	func_set.AddFunction(ScalarFunction(
	    {LogicalType::INTEGER}, LogicalType::VARCHAR,
	    ScalarFunction::UnaryFunction<int32_t, string_t, ToStringOperator>));
	// BIGINT → VARCHAR
	func_set.AddFunction(ScalarFunction(
	    {LogicalType::BIGINT}, LogicalType::VARCHAR,
	    ScalarFunction::UnaryFunction<int64_t, string_t, ToStringOperator>));
	// UBIGINT → VARCHAR
	func_set.AddFunction(ScalarFunction(
	    {LogicalType::UBIGINT}, LogicalType::VARCHAR,
	    ScalarFunction::UnaryFunction<uint64_t, string_t, ToStringOperator>));
	// DOUBLE → VARCHAR
	func_set.AddFunction(ScalarFunction(
	    {LogicalType::DOUBLE}, LogicalType::VARCHAR,
	    ScalarFunction::UnaryFunction<double, string_t, ToStringOperator>));
	// FLOAT → VARCHAR
	func_set.AddFunction(ScalarFunction(
	    {LogicalType::FLOAT}, LogicalType::VARCHAR,
	    ScalarFunction::UnaryFunction<float, string_t, ToStringOperator>));
	// BOOLEAN → VARCHAR
	func_set.AddFunction(ScalarFunction(
	    {LogicalType::BOOLEAN}, LogicalType::VARCHAR,
	    ScalarFunction::UnaryFunction<bool, string_t, ToStringOperator>));
	// VARCHAR → VARCHAR (passthrough)
	func_set.AddFunction(ScalarFunction(
	    {LogicalType::VARCHAR}, LogicalType::VARCHAR,
	    ScalarFunction::UnaryFunction<string_t, string_t, ToStringVarcharOp>));
	set.AddFunction(func_set);

	// Keep legacy "string" name for backward compat
	ScalarFunction legacy("string",
	                       {LogicalType::INTEGER},
	                       LogicalType::VARCHAR,
	                       ScalarFunction::UnaryFunction<int32_t, string_t, ToStringOperator>);
	set.AddFunction(legacy);
}

} // namespace duckdb
