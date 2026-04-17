#include "storage/wal.hpp"
#include "storage/delta_store.hpp"
#include "spdlog/spdlog.h"

#include <filesystem>

namespace duckdb {

// Value serialization type tags
enum class ValTag : uint8_t {
    NULL_VAL = 0, BIGINT = 1, UBIGINT = 2, VARCHAR = 3, DOUBLE = 4, BOOL = 5, INTEGER = 6,
};

static std::string wal_path(const std::string &db_path) {
    return db_path + "/delta.wal";
}

static std::string logical_mapping_path(const std::string &db_path) {
    return db_path + "/logical_mappings.bin";
}

// ============================================================
// WALWriter
// ============================================================

WALWriter::WALWriter(const std::string &db_path) : path_(wal_path(db_path)) {
    bool exists = std::filesystem::exists(path_);
    file_.open(path_, std::ios::binary | std::ios::app);
    if (!file_.is_open()) {
        spdlog::error("[WAL] Cannot open WAL file: {}", path_);
        return;
    }
    if (!exists) WriteHeader();
}

WALWriter::~WALWriter() {
    if (file_.is_open()) file_.close();
}

void WALWriter::WriteHeader() {
    WriteU32(WAL_MAGIC);
    WriteU8(WAL_VERSION);
}

void WALWriter::WriteU8(uint8_t v) { file_.write((char*)&v, 1); }
void WALWriter::WriteU16(uint16_t v) { file_.write((char*)&v, 2); }
void WALWriter::WriteU32(uint32_t v) { file_.write((char*)&v, 4); }
void WALWriter::WriteU64(uint64_t v) { file_.write((char*)&v, 8); }

void WALWriter::WriteString(const std::string &s) {
    uint16_t len = (uint16_t)s.size();
    WriteU16(len);
    file_.write(s.data(), len);
}

void WALWriter::WriteValue(const Value &v) {
    if (v.IsNull()) {
        WriteU8((uint8_t)ValTag::NULL_VAL);
    } else {
        auto tid = v.type().id();
        if (tid == LogicalTypeId::BIGINT) {
            WriteU8((uint8_t)ValTag::BIGINT);
            WriteU64((uint64_t)v.GetValue<int64_t>());
        } else if (tid == LogicalTypeId::UBIGINT) {
            WriteU8((uint8_t)ValTag::UBIGINT);
            WriteU64(v.GetValue<uint64_t>());
        } else if (tid == LogicalTypeId::INTEGER) {
            WriteU8((uint8_t)ValTag::INTEGER);
            WriteU32((uint32_t)v.GetValue<int32_t>());
        } else if (tid == LogicalTypeId::VARCHAR) {
            WriteU8((uint8_t)ValTag::VARCHAR);
            WriteString(StringValue::Get(v));
        } else if (tid == LogicalTypeId::DOUBLE) {
            WriteU8((uint8_t)ValTag::DOUBLE);
            double d = v.GetValue<double>();
            file_.write((char*)&d, 8);
        } else if (tid == LogicalTypeId::BOOLEAN) {
            WriteU8((uint8_t)ValTag::BOOL);
            WriteU8(v.GetValue<bool>() ? 1 : 0);
        } else {
            // Fallback: serialize as string
            WriteU8((uint8_t)ValTag::VARCHAR);
            WriteString(v.ToString());
        }
    }
}

void WALWriter::LogInsertNode(uint16_t partition_id, uint32_t inmem_eid,
                              const std::vector<std::string> &keys,
                              const std::vector<Value> &values) {
    std::lock_guard<std::mutex> lk(mutex_);
    WriteU8((uint8_t)WALEntryType::INSERT_NODE);
    WriteU16(partition_id);
    WriteU32(inmem_eid);
    WriteU16((uint16_t)keys.size());
    for (idx_t i = 0; i < keys.size(); i++) {
        WriteString(keys[i]);
        WriteValue(values[i]);
    }
    file_.flush();
}

void WALWriter::LogInsertNodeV2(uint16_t partition_id, uint64_t logical_id,
                                const std::vector<std::string> &keys,
                                const std::vector<Value> &values) {
    std::lock_guard<std::mutex> lk(mutex_);
    WriteU8((uint8_t)WALEntryType::INSERT_NODE_V2);
    WriteU16(partition_id);
    WriteU64(logical_id);
    WriteU16((uint16_t)keys.size());
    for (idx_t i = 0; i < keys.size(); i++) {
        WriteString(keys[i]);
        WriteValue(values[i]);
    }
    file_.flush();
}

void WALWriter::LogUpdateProp(uint64_t user_id, const std::string &prop_key, const Value &value) {
    std::lock_guard<std::mutex> lk(mutex_);
    WriteU8((uint8_t)WALEntryType::UPDATE_PROP);
    WriteU64(user_id);
    WriteString(prop_key);
    WriteValue(value);
    file_.flush();
}

void WALWriter::LogUpdateNodeV2(uint16_t partition_id, uint64_t logical_id,
                                const std::vector<std::string> &keys,
                                const std::vector<Value> &values) {
    std::lock_guard<std::mutex> lk(mutex_);
    WriteU8((uint8_t)WALEntryType::UPDATE_NODE_V2);
    WriteU16(partition_id);
    WriteU64(logical_id);
    WriteU16((uint16_t)keys.size());
    for (idx_t i = 0; i < keys.size(); i++) {
        WriteString(keys[i]);
        WriteValue(values[i]);
    }
    file_.flush();
}

void WALWriter::LogDeleteNode(uint32_t extent_id, uint32_t row_offset, uint64_t user_id) {
    std::lock_guard<std::mutex> lk(mutex_);
    WriteU8((uint8_t)WALEntryType::DELETE_NODE);
    WriteU32(extent_id);
    WriteU32(row_offset);
    WriteU64(user_id);
    file_.flush();
}

void WALWriter::LogDeleteNodeV2(uint64_t logical_id) {
    std::lock_guard<std::mutex> lk(mutex_);
    WriteU8((uint8_t)WALEntryType::DELETE_NODE_V2);
    WriteU64(logical_id);
    file_.flush();
}

void WALWriter::LogInsertEdge(uint16_t edge_partition_id, uint64_t src_vid,
                              uint64_t dst_vid, uint64_t edge_id) {
    std::lock_guard<std::mutex> lk(mutex_);
    WriteU8((uint8_t)WALEntryType::INSERT_EDGE);
    WriteU16(edge_partition_id);
    WriteU64(src_vid);
    WriteU64(dst_vid);
    WriteU64(edge_id);
    file_.flush();
}

void WALWriter::LogDeleteEdge(uint16_t edge_partition_id, uint64_t src_vid,
                              uint64_t edge_id) {
    std::lock_guard<std::mutex> lk(mutex_);
    WriteU8((uint8_t)WALEntryType::DELETE_EDGE);
    WriteU16(edge_partition_id);
    WriteU64(src_vid);
    WriteU64(edge_id);
    file_.flush();
}

// Atomically persist + apply a bidirectional edge. See wal.hpp for the
// INSERT_EDGE schema contract (one WAL record → two AdjListDelta entries
// on replay). This helper is the ONLY correct way to insert an edge into
// the delta store; do not call LogInsertEdge and AdjListDelta::InsertEdge
// directly at new call sites.
void LogAndApplyInsertEdge(WALWriter* wal, DeltaStore& ds,
                           uint16_t edge_partition_id,
                           uint64_t src_vid, uint64_t dst_vid,
                           uint64_t edge_id) {
    // Write-ahead: durable record first, then in-memory apply.
    if (wal) wal->LogInsertEdge(edge_partition_id, src_vid, dst_vid, edge_id);
    ds.GetAdjListDelta(edge_partition_id).InsertEdge(src_vid, dst_vid, edge_id);
    ds.GetAdjListDelta(edge_partition_id).InsertEdge(dst_vid, src_vid, edge_id);
}

void WALWriter::LogCheckpointBegin() {
    std::lock_guard<std::mutex> lk(mutex_);
    WriteU8((uint8_t)WALEntryType::CHECKPOINT_BEGIN);
    file_.flush();
}

void WALWriter::LogCheckpointEnd() {
    std::lock_guard<std::mutex> lk(mutex_);
    WriteU8((uint8_t)WALEntryType::CHECKPOINT_END);
    file_.flush();
}

void WALWriter::Flush() {
    std::lock_guard<std::mutex> lk(mutex_);
    if (file_.is_open()) file_.flush();
}

void WALWriter::Truncate() {
    std::lock_guard<std::mutex> lk(mutex_);
    if (file_.is_open()) file_.close();
    file_.open(path_, std::ios::binary | std::ios::trunc);
    if (file_.is_open()) WriteHeader();
}

// ============================================================
// WALReader
// ============================================================

uint8_t WALReader::ReadU8(std::ifstream &f) { uint8_t v; f.read((char*)&v, 1); return v; }
uint16_t WALReader::ReadU16(std::ifstream &f) { uint16_t v; f.read((char*)&v, 2); return v; }
uint32_t WALReader::ReadU32(std::ifstream &f) { uint32_t v; f.read((char*)&v, 4); return v; }
uint64_t WALReader::ReadU64(std::ifstream &f) { uint64_t v; f.read((char*)&v, 8); return v; }

std::string WALReader::ReadString(std::ifstream &f) {
    uint16_t len = ReadU16(f);
    std::string s(len, '\0');
    f.read(s.data(), len);
    return s;
}

Value WALReader::ReadValue(std::ifstream &f) {
    auto tag = (ValTag)ReadU8(f);
    switch (tag) {
        case ValTag::NULL_VAL: return Value();
        case ValTag::BIGINT: return Value::BIGINT((int64_t)ReadU64(f));
        case ValTag::UBIGINT: return Value::UBIGINT(ReadU64(f));
        case ValTag::INTEGER: return Value::INTEGER((int32_t)ReadU32(f));
        case ValTag::VARCHAR: return Value(ReadString(f));
        case ValTag::DOUBLE: { double d; f.read((char*)&d, 8); return Value::DOUBLE(d); }
        case ValTag::BOOL: return Value::BOOLEAN(ReadU8(f) != 0);
        default: return Value();
    }
}

// Skip an entry's payload without applying it (used during crash recovery scan).
static bool SkipEntry(std::ifstream &f, WALEntryType type) {
    switch (type) {
        case WALEntryType::INSERT_NODE: {
            WALReader::ReadU16(f); // pid
            WALReader::ReadU32(f); // inmem_eid
            uint16_t num_props = WALReader::ReadU16(f);
            for (uint16_t i = 0; i < num_props; i++) {
                WALReader::ReadString(f);
                WALReader::ReadValue(f);
            }
            return true;
        }
        case WALEntryType::UPDATE_PROP: {
            WALReader::ReadU64(f);
            WALReader::ReadString(f);
            WALReader::ReadValue(f);
            return true;
        }
        case WALEntryType::DELETE_NODE: {
            WALReader::ReadU32(f); WALReader::ReadU32(f); WALReader::ReadU64(f);
            return true;
        }
        case WALEntryType::INSERT_EDGE: {
            WALReader::ReadU16(f); WALReader::ReadU64(f); WALReader::ReadU64(f); WALReader::ReadU64(f);
            return true;
        }
        case WALEntryType::INSERT_NODE_V2:
        case WALEntryType::UPDATE_NODE_V2: {
            WALReader::ReadU16(f);
            WALReader::ReadU64(f);
            uint16_t num_props = WALReader::ReadU16(f);
            for (uint16_t i = 0; i < num_props; i++) {
                WALReader::ReadString(f);
                WALReader::ReadValue(f);
            }
            return true;
        }
        case WALEntryType::DELETE_NODE_V2: {
            WALReader::ReadU64(f);
            return true;
        }
        case WALEntryType::DELETE_EDGE: {
            WALReader::ReadU16(f);
            WALReader::ReadU64(f);
            WALReader::ReadU64(f);
            return true;
        }
        case WALEntryType::CHECKPOINT_BEGIN:
        case WALEntryType::CHECKPOINT_END:
            return true; // no payload
        default:
            return false;
    }
}

idx_t WALReader::Replay(const std::string &db_path, DeltaStore &ds) {
    std::string path = wal_path(db_path);
    if (!std::filesystem::exists(path)) return 0;

    std::ifstream f(path, std::ios::binary);
    if (!f.is_open()) return 0;

    // Verify header
    uint32_t magic = ReadU32(f);
    if (magic != WAL_MAGIC) {
        spdlog::warn("[WAL] Invalid WAL magic: 0x{:08X}", magic);
        return 0;
    }
    uint8_t version = ReadU8(f);
    if (version != WAL_VERSION) {
        spdlog::warn("[WAL] Unsupported WAL version: {}", version);
        return 0;
    }

    // Pass 1: Find the last CHECKPOINT_END position.
    // Entries before CHECKPOINT_END are already on disk — skip them during replay.
    // If CHECKPOINT_BEGIN exists without CHECKPOINT_END → crashed during compaction;
    // INSERTs after BEGIN are NOT on disk (catalog not saved), so replay them.
    std::streampos replay_start = f.tellg();
    bool found_checkpoint_end = false;
    bool in_checkpoint = false;
    while (f.peek() != EOF) {
        std::streampos entry_pos = f.tellg();
        auto type = (WALEntryType)ReadU8(f);
        if (f.eof()) break;

        if (type == WALEntryType::CHECKPOINT_BEGIN) {
            in_checkpoint = true;
        } else if (type == WALEntryType::CHECKPOINT_END) {
            in_checkpoint = false;
            found_checkpoint_end = true;
            replay_start = f.tellg(); // replay from after this marker
        } else {
            if (!SkipEntry(f, type)) {
                spdlog::warn("[WAL] Pass 1: unknown entry type {}", (int)type);
                break;
            }
        }
    }

    if (in_checkpoint) {
        // Crashed during compaction — catalog was NOT saved.
        // All entries (including INSERTs before BEGIN) must be replayed.
        spdlog::warn("[WAL] Detected incomplete checkpoint — replaying all entries");
        replay_start = (std::streampos)5; // after header (4 magic + 1 version)
    }

    if (found_checkpoint_end) {
        spdlog::info("[WAL] Checkpoint found — skipping already-compacted entries");
    }

    // Pass 2: Replay entries from replay_start
    f.clear();
    f.seekg(replay_start);

    idx_t count = 0;
    while (f.peek() != EOF) {
        auto type = (WALEntryType)ReadU8(f);
        if (f.eof()) break;

        switch (type) {
            case WALEntryType::INSERT_NODE: {
                uint16_t pid = ReadU16(f);
                uint32_t inmem_eid = ReadU32(f);
                uint16_t num_props = ReadU16(f);
                std::vector<std::string> keys;
                std::vector<Value> values;
                for (uint16_t i = 0; i < num_props; i++) {
                    keys.push_back(ReadString(f));
                    values.push_back(ReadValue(f));
                }
                auto existing = ds.GetInMemoryExtentIDs(pid);
                if (existing.empty() || existing[0] != inmem_eid) {
                    ds.AllocateInMemoryExtentID(pid);
                }
                ds.GetInsertBuffer(inmem_eid).AppendRow(std::move(keys), std::move(values));
                count++;
                break;
            }
            case WALEntryType::UPDATE_PROP: {
                uint64_t user_id = ReadU64(f);
                std::string key = ReadString(f);
                Value val = ReadValue(f);
                ds.SetPropertyByUserId(user_id, key, std::move(val));
                count++;
                break;
            }
            case WALEntryType::DELETE_NODE: {
                uint32_t eid = ReadU32(f);
                uint32_t off = ReadU32(f);
                uint64_t uid = ReadU64(f);
                spdlog::info("[WAL-REPLAY] DELETE_NODE eid=0x{:08X} off={} uid={}", eid, off, uid);
                if (eid != 0 || off != 0) ds.GetDeleteMask(eid).Delete(off);
                if (uid != 0) ds.DeleteByUserId(uid);
                count++;
                break;
            }
            case WALEntryType::INSERT_EDGE: {
                uint16_t epid = ReadU16(f);
                uint64_t src = ReadU64(f);
                uint64_t dst = ReadU64(f);
                uint64_t eid = ReadU64(f);
                ds.GetAdjListDelta(epid).InsertEdge(src, dst, eid);
                ds.GetAdjListDelta(epid).InsertEdge(dst, src, eid);
                // Restore global edge counter so future allocations don't
                // collide with IDs already on disk (supports both legacy and
                // synthetic edge-ID formats).
                ds.ObserveEdgeCounter(eid);
                count++;
                break;
            }
            case WALEntryType::INSERT_NODE_V2: {
                uint16_t pid = ReadU16(f);
                uint64_t logical_id = ReadU64(f);
                uint16_t num_props = ReadU16(f);
                std::vector<std::string> keys;
                std::vector<Value> values;
                keys.reserve(num_props);
                values.reserve(num_props);
                for (uint16_t i = 0; i < num_props; i++) {
                    keys.push_back(ReadString(f));
                    values.push_back(ReadValue(f));
                }
                auto inmem_eid = ds.GetOrAllocateInMemoryExtentID(pid, keys);
                ds.AppendInsertRow(inmem_eid, std::move(keys), std::move(values),
                                   logical_id);
                count++;
                break;
            }
            case WALEntryType::UPDATE_NODE_V2: {
                uint16_t pid = ReadU16(f);
                uint64_t logical_id = ReadU64(f);
                uint16_t num_props = ReadU16(f);
                std::vector<std::string> keys;
                std::vector<Value> values;
                keys.reserve(num_props);
                values.reserve(num_props);
                for (uint16_t i = 0; i < num_props; i++) {
                    keys.push_back(ReadString(f));
                    values.push_back(ReadValue(f));
                }
                ds.PreserveAdjacencyPidOnUpdate(logical_id);
                ds.InvalidateCurrentVersion(logical_id);
                auto inmem_eid = ds.GetOrAllocateInMemoryExtentID(pid, keys);
                ds.AppendInsertRow(inmem_eid, std::move(keys), std::move(values),
                                   logical_id);
                count++;
                break;
            }
            case WALEntryType::DELETE_NODE_V2: {
                uint64_t logical_id = ReadU64(f);
                ds.InvalidateCurrentVersion(logical_id);
                ds.InvalidateLogicalId(logical_id);
                count++;
                break;
            }
            case WALEntryType::DELETE_EDGE: {
                uint16_t epid = ReadU16(f);
                uint64_t src = ReadU64(f);
                uint64_t eid = ReadU64(f);
                ds.GetAdjListDelta(epid).DeleteEdge(src, eid);
                count++;
                break;
            }
            case WALEntryType::CHECKPOINT_BEGIN:
            case WALEntryType::CHECKPOINT_END:
                break; // skip markers in pass 2
            default:
                spdlog::warn("[WAL] Unknown entry type: {}", (int)type);
                return count;
        }
    }

    spdlog::info("[WAL] Replayed {} entries from {}", count, path);
    return count;
}

void PersistLogicalMappings(const std::string &db_path,
                            const DeltaStore &delta_store) {
    std::filesystem::create_directories(db_path);
    std::ofstream f(logical_mapping_path(db_path),
                    std::ios::binary | std::ios::trunc);
    if (!f.is_open()) {
        spdlog::warn("[WAL] Cannot persist logical mappings for {}", db_path);
        return;
    }

    static constexpr uint32_t MAPPING_MAGIC = 0x50444D4C; // "LMDP"
    static constexpr uint8_t MAPPING_VERSION = 2;
    const auto &mappings = delta_store.GetAllLogicalMappings();
    const auto &adjacency_overrides = delta_store.GetAllAdjacencyPidOverrides();

    f.write((const char *)&MAPPING_MAGIC, sizeof(MAPPING_MAGIC));
    f.write((const char *)&MAPPING_VERSION, sizeof(MAPPING_VERSION));
    uint64_t count = mappings.size();
    f.write((const char *)&count, sizeof(count));
    for (auto &[logical_id, location] : mappings) {
        f.write((const char *)&logical_id, sizeof(logical_id));
        uint8_t valid = location.valid ? 1 : 0;
        uint8_t is_delta = location.is_delta ? 1 : 0;
        f.write((const char *)&valid, sizeof(valid));
        f.write((const char *)&is_delta, sizeof(is_delta));
        f.write((const char *)&location.pid, sizeof(location.pid));
        auto adj_it = adjacency_overrides.find(logical_id);
        uint8_t has_adj_override =
            adj_it != adjacency_overrides.end() ? 1 : 0;
        f.write((const char *)&has_adj_override, sizeof(has_adj_override));
        if (has_adj_override) {
            f.write((const char *)&adj_it->second, sizeof(adj_it->second));
        }
    }
}

void LoadLogicalMappings(const std::string &db_path, DeltaStore &delta_store) {
    auto path = logical_mapping_path(db_path);
    if (!std::filesystem::exists(path)) {
        return;
    }

    std::ifstream f(path, std::ios::binary);
    if (!f.is_open()) {
        spdlog::warn("[WAL] Cannot load logical mappings from {}", path);
        return;
    }

    static constexpr uint32_t MAPPING_MAGIC = 0x50444D4C; // "LMDP"
    uint32_t magic = 0;
    uint8_t version = 0;
    uint64_t count = 0;
    f.read((char *)&magic, sizeof(magic));
    f.read((char *)&version, sizeof(version));
    if (magic != MAPPING_MAGIC || (version != 1 && version != 2)) {
        spdlog::warn("[WAL] Invalid logical mapping file {}", path);
        return;
    }
    f.read((char *)&count, sizeof(count));
    for (uint64_t i = 0; i < count; i++) {
        uint64_t logical_id = 0;
        uint8_t valid = 0;
        uint8_t is_delta = 0;
        uint64_t pid = 0;
        f.read((char *)&logical_id, sizeof(logical_id));
        f.read((char *)&valid, sizeof(valid));
        f.read((char *)&is_delta, sizeof(is_delta));
        f.read((char *)&pid, sizeof(pid));
        uint8_t has_adj_override = 0;
        uint64_t adj_pid = 0;
        if (version >= 2) {
            f.read((char *)&has_adj_override, sizeof(has_adj_override));
            if (has_adj_override) {
                f.read((char *)&adj_pid, sizeof(adj_pid));
            }
        }
        if (valid) {
            delta_store.UpsertLogicalMapping(logical_id, pid, is_delta != 0);
            if (has_adj_override) {
                delta_store.SetAdjacencyPidOverride(logical_id, adj_pid);
            }
        } else {
            delta_store.InvalidateLogicalId(logical_id);
        }
    }
}

} // namespace duckdb
