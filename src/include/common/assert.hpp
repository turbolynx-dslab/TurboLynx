//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/assert.hpp
//
//
//===----------------------------------------------------------------------===//

#include "common/winapi.hpp"

#pragma once

#if (defined(DUCKDB_USE_STANDARD_ASSERT) || !defined(DEBUG)) && !defined(DUCKDB_FORCE_ASSERT)

#include <assert.h>
#define D_ASSERT assert
#else
namespace duckdb {
DUCKDB_API void DuckDBAssertInternal(bool condition, const char *condition_name, const char *file, int linenr);
}

// Evaluate the condition at the call site so the success path is a single branch
// with no argument marshalling and no function call. Only on failure do we evaluate
// the cold arguments and invoke DuckDBAssertInternal (which throws InternalException).
// At -O0 this is meaningfully cheaper than unconditionally calling a non-inlineable
// function for every assertion, while preserving the throwing behaviour that some
// tests rely on (so we cannot simply fall back to the standard assert() macro).
#define D_ASSERT(condition) \
	(static_cast<bool>(condition) \
	     ? static_cast<void>(0) \
	     : duckdb::DuckDBAssertInternal(false, #condition, __FILE__, __LINE__))

#endif