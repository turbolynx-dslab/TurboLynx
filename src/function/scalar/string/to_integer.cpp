//===----------------------------------------------------------------------===//
//                         DuckDB
//
// src/function/scalar/string/to_integer.cpp
//
//
//===----------------------------------------------------------------------===//

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
	ScalarFunctionSet to_integer("tointeger");
	to_integer.AddFunction(ScalarFunction(
		{LogicalType::VARCHAR}, LogicalType::BIGINT,
		ScalarFunction::UnaryFunction<string_t, int64_t, ToIntegerFromStringOperator>));
	to_integer.AddFunction(ScalarFunction(
		{LogicalType::BIGINT}, LogicalType::BIGINT,
		ScalarFunction::UnaryFunction<int64_t, int64_t, ToIntegerIdentityOperator>));
	to_integer.AddFunction(ScalarFunction(
		{LogicalType::INTEGER}, LogicalType::BIGINT,
		ScalarFunction::UnaryFunction<int32_t, int64_t, ToIntegerIdentityOperator>));
	to_integer.AddFunction(ScalarFunction(
		{LogicalType::DOUBLE}, LogicalType::BIGINT,
		ScalarFunction::UnaryFunction<double, int64_t, ToIntegerIdentityOperator>));
	to_integer.AddFunction(ScalarFunction(
		{LogicalType::UBIGINT}, LogicalType::BIGINT,
		ScalarFunction::UnaryFunction<uint64_t, int64_t, ToIntegerIdentityOperator>));
	set.AddFunction(to_integer);
}

// Cypher's toBoolean coerces 'true'/'TRUE'/'false'/'FALSE'/booleans/null
// to a BOOLEAN value. Anything else (e.g. random strings) returns NULL.
struct ToBooleanFromStringOperator {
	template <class TA, class TR>
	static inline TR Operation(const TA &input) {
		string s = input.GetString();
		// Lowercase compare without dragging in StringUtil here.
		auto eq_ci = [&](const char *target) {
			size_t n = 0;
			while (target[n] != '\0') n++;
			if (s.size() != n) return false;
			for (size_t i = 0; i < n; i++) {
				char c = s[i];
				if (c >= 'A' && c <= 'Z') c = (char)(c + 32);
				if (c != target[i]) return false;
			}
			return true;
		};
		if (eq_ci("true")) return true;
		if (eq_ci("false")) return false;
		throw InvalidInputException(
		    "toBoolean: cannot coerce string '%s' to BOOLEAN", s);
	}
};

struct ToBooleanIdentityOperator {
	template <class TA, class TR>
	static inline TR Operation(const TA &input) {
		return static_cast<TR>(input);
	}
};

void ToBooleanFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet to_boolean("toboolean");
	to_boolean.AddFunction(ScalarFunction(
	    {LogicalType::VARCHAR}, LogicalType::BOOLEAN,
	    ScalarFunction::UnaryFunction<string_t, bool, ToBooleanFromStringOperator>));
	to_boolean.AddFunction(ScalarFunction(
	    {LogicalType::BOOLEAN}, LogicalType::BOOLEAN,
	    ScalarFunction::UnaryFunction<bool, bool, ToBooleanIdentityOperator>));
	set.AddFunction(to_boolean);
}

void ToFloatFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet to_float("tofloat");
	to_float.AddFunction(ScalarFunction(
		{LogicalType::VARCHAR}, LogicalType::DOUBLE,
		ScalarFunction::UnaryFunction<string_t, double, ToIntegerFromStringOperator>));
	to_float.AddFunction(ScalarFunction(
		{LogicalType::BIGINT}, LogicalType::DOUBLE,
		ScalarFunction::UnaryFunction<int64_t, double, ToIntegerIdentityOperator>));
	to_float.AddFunction(ScalarFunction(
		{LogicalType::INTEGER}, LogicalType::DOUBLE,
		ScalarFunction::UnaryFunction<int32_t, double, ToIntegerIdentityOperator>));
	to_float.AddFunction(ScalarFunction(
		{LogicalType::DOUBLE}, LogicalType::DOUBLE,
		ScalarFunction::UnaryFunction<double, double, ToIntegerIdentityOperator>));
	to_float.AddFunction(ScalarFunction(
		{LogicalType::UBIGINT}, LogicalType::DOUBLE,
		ScalarFunction::UnaryFunction<uint64_t, double, ToIntegerIdentityOperator>));
	set.AddFunction(to_float);
}

} // namespace duckdb
