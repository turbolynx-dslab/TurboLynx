#pragma once
//
// Thin wrapper around turbolynx_prepare/execute/fetch that reuses an
// existing connection (so the L2 framework can share a single
// workspace across all rewriter TEST_CASEs).  Mirrors the column
// extraction performed by qtest::QueryRunner, returning the same
// `qtest::QueryResult` so the shared `tl_fuzz::compare()` helper
// can consume the rows directly.

#include "helpers/query_runner.hpp"

#include <string>

namespace tl_fuzz_l2 {

// Execute `query` against `conn_id`, fetching every row.  Throws
// std::runtime_error on prepare or execute failure with the upstream
// error message attached.
qtest::QueryResult run_against(int64_t conn_id, const std::string& query);

}  // namespace tl_fuzz_l2
