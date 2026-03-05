//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/mutex.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <mutex>
#include <shared_mutex>

namespace duckdb {
using std::lock_guard;
using std::mutex;
using std::shared_mutex;
using std::shared_lock;
using std::unique_lock;
} // namespace duckdb
