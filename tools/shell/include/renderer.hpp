#pragma once

#include "common/typedef.hpp"

#include <memory>
#include <string>
#include <vector>

namespace duckdb { class DataChunk; }

namespace turbolynx {

enum class OutputMode { TABLE, CSV, JSON, MARKDOWN };

OutputMode  ParseOutputMode(const std::string& name);
std::string OutputModeName(OutputMode mode);

void RenderResults(OutputMode mode,
                   const duckdb::PropertyKeys& col_names,
                   std::vector<std::shared_ptr<duckdb::DataChunk>>& results,
                   duckdb::Schema& schema,
                   const std::string& output_file = "");

} // namespace turbolynx
