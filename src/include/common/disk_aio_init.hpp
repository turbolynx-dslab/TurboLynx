#pragma once

#include <string>

class DiskAioFactory;

namespace turbolynx {

// Initializes DiskAioParameters statics, creates the DiskAioFactory singleton,
// and returns the raw pointer. Callers that need RAII should store or wrap it.
DiskAioFactory* InitializeDiskAio(const std::string& workspace);

} // namespace turbolynx

namespace duckdb {
using turbolynx::InitializeDiskAio;
} // namespace duckdb
