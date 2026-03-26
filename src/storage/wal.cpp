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

void WALWriter::LogUpdateProp(uint64_t user_id, const std::string &prop_key, const Value &value) {
    std::lock_guard<std::mutex> lk(mutex_);
    WriteU8((uint8_t)WALEntryType::UPDATE_PROP);
    WriteU64(user_id);
    WriteString(prop_key);
    WriteValue(value);
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
                // Replay: ensure in-memory ExtentID is allocated and insert row
                auto existing = ds.GetInMemoryExtentIDs(pid);
                if (existing.empty() || existing[0] != inmem_eid) {
                    // Allocate to match the WAL's extent ID
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
                ds.GetDeleteMask(eid).Delete(off);
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
                ds.GetAdjListDelta(epid).InsertEdge(dst, src, eid); // backward
                count++;
                break;
            }
            default:
                spdlog::warn("[WAL] Unknown entry type: {}", (int)type);
                return count;  // stop on unknown
        }
    }

    spdlog::info("[WAL] Replayed {} entries from {}", count, path);
    return count;
}

} // namespace duckdb
