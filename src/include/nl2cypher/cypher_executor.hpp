// CypherExecutor — abstract interface that NL2Cypher uses to run a
// Cypher query and capture its result rows programmatically.
//
// The shell provides the concrete implementation (it owns the
// `Planner`); decoupling lets ProfileCollector and the S3 validator
// stay free of any executor / planner dependency, which keeps the
// nl2cypher unit tests fast.
//
// Result rows are returned as `duckdb::Value` so callers can preserve
// type information (BIGINT vs DOUBLE vs VARCHAR) without re-parsing.

#pragma once

#include <stdexcept>
#include <string>
#include <vector>

#include "common/types/value.hpp"

namespace turbolynx {
namespace nl2cypher {

struct CypherResult {
    std::vector<std::string>                  col_names;
    std::vector<std::vector<duckdb::Value>>   rows;
    bool        ok = true;
    std::string error;
};

class CypherExecutor {
public:
    virtual ~CypherExecutor() = default;

    // Execute `cypher` and return the materialised rows. Implementations
    // SHOULD catch parser/planner/exec exceptions and translate them to
    // {ok=false, error=...} so callers can keep going (profiling sweeps
    // many properties and can tolerate per-query failures).
    virtual CypherResult Execute(const std::string& cypher) = 0;
};

}  // namespace nl2cypher
}  // namespace turbolynx
