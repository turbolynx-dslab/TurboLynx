//===----------------------------------------------------------------------===//
//                         DuckDB
//
// src/execution/parallel/thread_context.cpp
//
//
//===----------------------------------------------------------------------===//

#include "parallel/thread_context.hpp"

#include "main/client_context.hpp"

namespace duckdb {

ThreadContext::ThreadContext(ClientContext &context) : profiler(QueryProfiler::Get(context).IsEnabled()) {
}

} // namespace duckdb
