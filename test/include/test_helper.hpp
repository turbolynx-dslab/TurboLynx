#pragma once
// =============================================================================
// S62GDB Test Helper
// -----------------------------------------------------------------------------
// Provides TestDB — a lightweight in-memory database wrapper for unit tests.
//
// Usage:
//   TEST_CASE("my test", "[catalog]") {
//       TestDB db;
//       auto& ctx = db.ctx();
//       auto& cat = db.catalog();
//       ...
//   }
//
// TestDB is move-only; construct one per TEST_CASE for full isolation.
// =============================================================================

#include "main/database.hpp"
#include "main/client_context.hpp"
#include "catalog/catalog.hpp"
#include "common/logger.hpp"

#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>

namespace s62test {

// ---------------------------------------------------------------------------
// TestDB — wraps an in-memory DuckDB instance + a ClientContext.
// nullptr path → pure in-memory (no disk IO, no AIO, no persistence).
// ---------------------------------------------------------------------------
class TestDB {
public:
    // In-memory database (default, fastest, no cleanup needed)
    TestDB() : db_(nullptr), ctx_(std::make_shared<duckdb::ClientContext>(db_.instance)) {
        // Ensure logger is available (idempotent — safe to call multiple times)
        SetupLogger();
    }

    // Disk-backed database at 'dir' (test will write/read files)
    explicit TestDB(const std::string &dir)
        : db_(dir.c_str()), ctx_(std::make_shared<duckdb::ClientContext>(db_.instance)) {
        SetupLogger();
    }

    // Non-copyable
    TestDB(const TestDB &) = delete;
    TestDB &operator=(const TestDB &) = delete;

    // ---- Accessors --------------------------------------------------------
    duckdb::ClientContext &ctx()     { return *ctx_; }
    duckdb::Catalog       &catalog() { return db_.instance->GetCatalog(); }
    duckdb::DuckDB        &db()      { return db_; }

    // Convenience: shared_ptr to context (some APIs need this)
    std::shared_ptr<duckdb::ClientContext> ctx_ptr() { return ctx_; }

private:
    duckdb::DuckDB                         db_;
    std::shared_ptr<duckdb::ClientContext> ctx_;
};

// ---------------------------------------------------------------------------
// ScopedTempDir — RAII temp directory for disk-backed tests.
// Creates a unique directory on construction, removes it on destruction.
// ---------------------------------------------------------------------------
class ScopedTempDir {
public:
    ScopedTempDir() {
        auto tid = std::hash<std::thread::id>{}(std::this_thread::get_id());
        auto ts  = (uint64_t)std::chrono::steady_clock::now().time_since_epoch().count();
        base_ = std::filesystem::temp_directory_path() / ("s62test_" + std::to_string(tid ^ ts));
        std::filesystem::create_directories(base_);
    }
    ~ScopedTempDir() {
        std::filesystem::remove_all(base_);
    }
    std::string path() const { return base_.string(); }

private:
    std::filesystem::path base_;
};

} // namespace s62test

// ---------------------------------------------------------------------------
// Convenience macros to reduce boilerplate in test files.
// ---------------------------------------------------------------------------
#define S62_REQUIRE_NOTHROW(expr)  REQUIRE_NOTHROW((expr))
#define S62_SECTION(name)          SECTION(name)
