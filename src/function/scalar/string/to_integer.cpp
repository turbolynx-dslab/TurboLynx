#include "function/scalar/string_functions.hpp"

#include "common/exception.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "common/vector_operations/unary_executor.hpp"

namespace duckdb {

struct ToIntegerFromStringOperator {
	template <class TA, class TR>
	static inline TR Operation(const TA &input) {
		string s = input.GetString();
		return std::stoll(s);
	}
};

struct ToIntegerIdentityOperator {
	template <class TA, class TR>
	static inline TR Operation(const TA &input) {
		return static_cast<TR>(input);
	}
};

void ToIntegerFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet to_integer("toInteger");
	to_integer.AddFunction(ScalarFunction(
		{LogicalType::VARCHAR}, LogicalType::BIGINT,
		ScalarFunction::UnaryFunction<string_t, int64_t, ToIntegerFromStringOperator>));
	to_integer.AddFunction(ScalarFunction(
		{LogicalType::BIGINT}, LogicalType::BIGINT,
		ScalarFunction::UnaryFunction<int64_t, int64_t, ToIntegerIdentityOperator>));
	to_integer.AddFunction(ScalarFunction(
		{LogicalType::INTEGER}, LogicalType::BIGINT,
		ScalarFunction::UnaryFunction<int32_t, int64_t, ToIntegerIdentityOperator>));
	set.AddFunction(to_integer);
}

} // namespace duckdb
