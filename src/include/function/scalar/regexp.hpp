//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar/regexp.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "function/function_set.hpp"
#include "re2/re2.h"

namespace duckdb {

struct RegexpMatchesBindData : public FunctionData {
	RegexpMatchesBindData(re2::RE2::Options options, string constant_string);
	~RegexpMatchesBindData() override;

	re2::RE2::Options options;
	string constant_string;
	bool constant_pattern;
	string range_min;
	string range_max;
	bool range_success;

	unique_ptr<FunctionData> Copy() override;
};

struct RegexpReplaceBindData : public FunctionData {
	re2::RE2::Options options;
	bool global_replace;

	unique_ptr<FunctionData> Copy() override;
};

struct RegexpExtractBindData : public FunctionData {
	RegexpExtractBindData(bool constant_pattern, const string &pattern, const string &group_string_p);

	const bool constant_pattern;
	const string constant_string;

	const string group_string;
	const re2::StringPiece rewrite;

	unique_ptr<FunctionData> Copy() override;
};

} // namespace duckdb
