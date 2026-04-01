#pragma once
// =============================================================================
// WAL (Write-Ahead Log) for DeltaStore mutations
// =============================================================================
// Appends mutation entries to a binary log file. On restart, replays entries
// to reconstruct DeltaStore state. Truncated after Compaction (Phase 6+).

#include "common/types/value.hpp"
#include <string>
#include <vector>
#include <fstream>
#include <cstdint>
#include <mutex>

namespace duckdb {

class DeltaStore;

// WAL entry types
enum class WALEntryType : uint8_t {
    INSERT_NODE      = 1,
    UPDATE_PROP      = 2,
    DELETE_NODE       = 3,
    INSERT_EDGE      = 4,
    DELETE_BY_UID    = 5,
    CHECKPOINT_BEGIN = 10,
    CHECKPOINT_END   = 11,
};

static constexpr uint32_t WAL_MAGIC = 0x4C575454;  // "TLWL"
static constexpr uint8_t  WAL_VERSION = 1;

// ============================================================
// WALWriter — append-only log writer
// ============================================================
class WALWriter {
public:
    explicit WALWriter(const std::string &db_path);
    ~WALWriter();

    // Log a CREATE node operation
    void LogInsertNode(uint16_t partition_id, uint32_t inmem_eid,
                       const std::vector<std::string> &keys,
                       const std::vector<Value> &values);

    // Log a SET property operation (user_id based)
    void LogUpdateProp(uint64_t user_id, const std::string &prop_key, const Value &value);

    // Log a DELETE node operation
    void LogDeleteNode(uint32_t extent_id, uint32_t row_offset, uint64_t user_id);

    // Log an INSERT edge operation
    void LogInsertEdge(uint16_t edge_partition_id, uint64_t src_vid,
                       uint64_t dst_vid, uint64_t edge_id);

    // Log checkpoint markers for crash recovery
    void LogCheckpointBegin();
    void LogCheckpointEnd();

    // Flush to disk
    void Flush();

    // Truncate (after compaction)
    void Truncate();

private:
    void WriteHeader();
    void WriteU8(uint8_t v);
    void WriteU16(uint16_t v);
    void WriteU32(uint32_t v);
    void WriteU64(uint64_t v);
    void WriteString(const std::string &s);
    void WriteValue(const Value &v);

    std::ofstream file_;
    std::string path_;
    std::mutex mutex_;
};

// ============================================================
// WALReader — replay log on startup
// ============================================================
class WALReader {
public:
    // Replay WAL file into DeltaStore. Returns number of entries replayed.
    static idx_t Replay(const std::string &db_path, DeltaStore &delta_store);

    static uint8_t ReadU8(std::ifstream &f);
    static uint16_t ReadU16(std::ifstream &f);
    static uint32_t ReadU32(std::ifstream &f);
    static uint64_t ReadU64(std::ifstream &f);
    static std::string ReadString(std::ifstream &f);
    static Value ReadValue(std::ifstream &f);
};

} // namespace duckdb
