#pragma once

#include "renderer.hpp"
#include "main/client_context.hpp"

#include <functional>
#include <memory>
#include <string>

namespace turbolynx {

struct ShellState {
    OutputMode  output_mode   = OutputMode::TABLE;
    bool        timer_enabled = true;
    std::string output_file;    // non-empty → redirect results to this file
    std::string workspace;
};

// Returns true  if the input was a dot/colon command (handled here).
// Returns false if it should be treated as a Cypher query.
// executor: callback invoked by .read to execute a query string.
bool HandleDotCommand(const std::string& cmd,
                      ShellState& state,
                      std::shared_ptr<duckdb::ClientContext>& client,
                      const std::function<void(const std::string&)>& executor);

} // namespace turbolynx
