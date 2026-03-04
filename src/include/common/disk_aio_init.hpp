#pragma once

#include <string>

class DiskAioFactory;

namespace duckdb {

// Initializes DiskAioParameters statics, creates the DiskAioFactory singleton,
// and returns the raw pointer. Callers that need RAII should store or wrap it.
DiskAioFactory* InitializeDiskAio(const std::string& workspace);

} // namespace duckdb
