#pragma once

#include "common/typedef.hpp"

#include <memory>
#include <string>
#include <vector>

namespace duckdb { class DataChunk; }

namespace turbolynx {

enum class OutputMode {
    TABLE,     // ASCII box (default)
    CSV,       // comma-separated
    JSON,      // JSON array of objects
    MARKDOWN,  // GFM pipe table
    JSONLINES, // NDJSON — one object per line
    BOX,       // Unicode box-drawing
    LINE,      // col = val, one per line
    COLUMN,    // space-aligned, no border
    LIST,      // separator-delimited, no escaping
    TABS,      // TSV
    HTML,      // <table>
    LATEX,     // LaTeX tabular
    INSERT,    // SQL INSERT statements
    TRASH,     // discard output (benchmark)
};

OutputMode  ParseOutputMode(const std::string& name);
std::string OutputModeName(OutputMode mode);

struct RenderOptions {
    bool        show_headers  = true;
    std::string null_value;           // display string for NULL (default: "")
    std::string col_sep       = ",";  // column separator (LIST, CSV override)
    size_t      max_rows      = 0;    // 0 = unlimited
    size_t      min_col_width = 0;    // minimum column display width
    std::string output_file;          // non-empty → write to file
    std::string insert_label  = "data"; // table name for INSERT mode
};

void RenderResults(OutputMode mode,
                   const duckdb::PropertyKeys& col_names,
                   std::vector<std::shared_ptr<duckdb::DataChunk>>& results,
                   duckdb::Schema& schema,
                   const RenderOptions& opts = {});

} // namespace turbolynx
