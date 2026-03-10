#pragma once

#include "renderer.hpp"
#include "main/client_context.hpp"

#include <functional>
#include <memory>
#include <string>

namespace turbolynx {

struct ShellState {
    // Output
    OutputMode  output_mode    = OutputMode::TABLE;
    bool        show_headers   = true;
    std::string null_value;           // NULL display string (default: "")
    std::string col_sep        = ","; // column separator for LIST/CSV
    size_t      max_rows       = 0;   // 0 = unlimited
    size_t      min_col_width  = 0;   // minimum column width

    // Redirection
    std::string output_file;          // persistent redirect (.output)
    std::string output_once;          // single-result redirect (.once), cleared after use

    // Logging
    std::string log_file;             // append all queries to this file (.log)

    // Execution
    bool        timer_enabled  = true;
    bool        echo           = false; // print query before executing
    bool        bail           = false; // stop on first error

    // Shell
    std::string prompt         = "TurboLynx";
    std::string workspace;

    // INSERT mode label
    std::string insert_label   = "data";
};

// Returns true  if the input was a dot/colon command (handled here).
// Returns false if it should be treated as a Cypher query.
// executor: callback invoked by .read to execute a query string.
bool HandleDotCommand(const std::string& cmd,
                      ShellState& state,
                      std::shared_ptr<duckdb::ClientContext>& client,
                      const std::function<void(const std::string&)>& executor);

} // namespace turbolynx
