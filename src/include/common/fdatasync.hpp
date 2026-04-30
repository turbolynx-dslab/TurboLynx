//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/fdatasync.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <unistd.h>

namespace duckdb {

// Cross-platform equivalent of POSIX fdatasync(2). On macOS, where
// fdatasync is not declared, falls back to fsync(2). Note that on macOS
// this only flushes the OS page cache to the drive — it does NOT flush
// the drive's own write cache. Strict "data on media" durability on
// macOS additionally requires fcntl(fd, F_FULLFSYNC); tracked separately.
inline int Fdatasync(int fd) {
#if defined(__APPLE__)
    return ::fsync(fd);
#else
    return ::fdatasync(fd);
#endif
}

} // namespace duckdb
