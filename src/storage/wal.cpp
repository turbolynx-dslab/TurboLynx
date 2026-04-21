//===----------------------------------------------------------------------===//
//                         DuckDB
//
// src/storage/wal.cpp
//
//
//===----------------------------------------------------------------------===//

#include "storage/wal.hpp"
#include "storage/delta_store.hpp"
#include "common/checksum.hpp"
#include "common/exception.hpp"
#include "spdlog/spdlog.h"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

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
    // Detect a stale WAL (wrong magic/version) before we start appending v2
    // frames — writing a v2 frame after a v1 header would produce garbage.
    // If the header doesn't match, reset the file. Any un-checkpointed
    // entries left over from an older format are lost; that's acceptable
    // because checkpointed state is authoritative and WAL is transient.
    bool reset_header = false;
    if (exists) {
        std::ifstream probe(path_, std::ios::binary);
        uint32_t magic = 0;
        uint8_t version = 0;
        if (probe.is_open()) {
            probe.read((char*)&magic, sizeof(magic));
            probe.read((char*)&version, sizeof(version));
        }
        if (magic != WAL_MAGIC || version != WAL_VERSION) {
            spdlog::warn("[WAL] Stale WAL header at {} (magic=0x{:08X} version={}); resetting",
                         path_, magic, (int)version);
            reset_header = true;
        }
    }

    // O_APPEND: every write lands atomically at EOF — no lseek race across
    // threads or processes. 0644 is fine; the directory controls visibility.
    fd_ = ::open(path_.c_str(), O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
    if (fd_ < 0) {
        spdlog::error("[WAL] Cannot open WAL file {}: {}", path_, std::strerror(errno));
        return;
    }
    if (reset_header) {
        if (::ftruncate(fd_, 0) != 0) {
            spdlog::error("[WAL] ftruncate failed on {}: {}", path_, std::strerror(errno));
        }
    }
    if (!exists || reset_header) WriteHeader();
}

WALWriter::~WALWriter() {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

void WALWriter::WriteHeader() {
    // Header is raw (unframed) — framed entries follow after this.
    if (fd_ < 0) return;
    uint8_t hdr[5];
    uint32_t magic = WAL_MAGIC;
    std::memcpy(hdr, &magic, 4);
    hdr[4] = WAL_VERSION;

    const uint8_t *p = hdr;
    size_t remaining = sizeof(hdr);
    while (remaining > 0) {
        ssize_t n = ::write(fd_, p, remaining);
        if (n < 0) {
            if (errno == EINTR) continue;
            int err = errno;
            throw IOException("[WAL] write header to %s failed: %s", path_, std::strerror(err));
        }
        p += n;
        remaining -= (size_t)n;
    }
    if (::fdatasync(fd_) != 0) {
        int err = errno;
        if (err == EIO) {
            throw FatalException("[WAL] fdatasync(%s) returned EIO after header", path_);
        }
        throw IOException("[WAL] fdatasync(%s) header failed: %s", path_, std::strerror(err));
    }
}

void WALWriter::AppendU8(uint8_t v) { entry_buf_.push_back(v); }
void WALWriter::AppendU16(uint16_t v) { AppendBytes(&v, sizeof(v)); }
void WALWriter::AppendU32(uint32_t v) { AppendBytes(&v, sizeof(v)); }
void WALWriter::AppendU64(uint64_t v) { AppendBytes(&v, sizeof(v)); }

void WALWriter::AppendBytes(const void *data, size_t n) {
    auto *p = static_cast<const uint8_t*>(data);
    entry_buf_.insert(entry_buf_.end(), p, p + n);
}

void WALWriter::AppendString(const std::string &s) {
    uint16_t len = (uint16_t)s.size();
    AppendU16(len);
    AppendBytes(s.data(), len);
}

void WALWriter::AppendValue(const Value &v) {
    if (v.IsNull()) {
        AppendU8((uint8_t)ValTag::NULL_VAL);
    } else {
        auto tid = v.type().id();
        if (tid == LogicalTypeId::BIGINT) {
            AppendU8((uint8_t)ValTag::BIGINT);
            AppendU64((uint64_t)v.GetValue<int64_t>());
        } else if (tid == LogicalTypeId::UBIGINT) {
            AppendU8((uint8_t)ValTag::UBIGINT);
            AppendU64(v.GetValue<uint64_t>());
        } else if (tid == LogicalTypeId::INTEGER) {
            AppendU8((uint8_t)ValTag::INTEGER);
            AppendU32((uint32_t)v.GetValue<int32_t>());
        } else if (tid == LogicalTypeId::VARCHAR) {
            AppendU8((uint8_t)ValTag::VARCHAR);
            AppendString(StringValue::Get(v));
        } else if (tid == LogicalTypeId::DOUBLE) {
            AppendU8((uint8_t)ValTag::DOUBLE);
            double d = v.GetValue<double>();
            AppendBytes(&d, sizeof(d));
        } else if (tid == LogicalTypeId::BOOLEAN) {
            AppendU8((uint8_t)ValTag::BOOL);
            AppendU8(v.GetValue<bool>() ? 1 : 0);
        } else {
            // Fallback: serialize as string
            AppendU8((uint8_t)ValTag::VARCHAR);
            AppendString(v.ToString());
        }
    }
}

void WALWriter::CommitEntry() {
    if (fd_ < 0) {
        // Opening failed in ctor — mirror the previous silent-drop behavior
        // so a missing WAL directory doesn't abort the engine. Recovery will
        // simply have nothing to replay.
        entry_buf_.clear();
        return;
    }
    // Frame layout: [size:u32][checksum:u64][entry_buf_]
    // * size   — number of bytes in entry_buf_ (type byte + payload).
    // * checksum — DuckDB's Checksum() over entry_buf_. Detects torn writes
    //              and bit-rot at replay. Computed on the exact bytes the
    //              reader will hash, with no endian conversion.
    // We assemble [size][checksum][entry_buf_] into a contiguous temp and
    // issue one ::write. O_APPEND keeps the whole frame contiguous at EOF
    // even under concurrent writers; a short write (rare at this size) is
    // still handled by the loop below.
    uint32_t size = (uint32_t)entry_buf_.size();
    uint64_t checksum = duckdb::Checksum(entry_buf_.data(), entry_buf_.size());

    std::vector<uint8_t> frame;
    frame.reserve(sizeof(size) + sizeof(checksum) + entry_buf_.size());
    auto append_raw = [&](const void *p, size_t n) {
        auto *bp = static_cast<const uint8_t*>(p);
        frame.insert(frame.end(), bp, bp + n);
    };
    append_raw(&size, sizeof(size));
    append_raw(&checksum, sizeof(checksum));
    append_raw(entry_buf_.data(), entry_buf_.size());

    // 1) Atomically append the whole frame. O_APPEND guarantees the offset
    //    seek+write is race-free; loop only because write() may be short on
    //    large payloads or signal interruption.
    const uint8_t *p = frame.data();
    size_t remaining = frame.size();
    while (remaining > 0) {
        ssize_t n = ::write(fd_, p, remaining);
        if (n < 0) {
            if (errno == EINTR) continue;
            int err = errno;
            throw IOException("[WAL] write to %s failed: %s", path_, std::strerror(err));
        }
        p += n;
        remaining -= (size_t)n;
    }
    // 2) fdatasync: kernel page cache → stable storage. fdatasync skips
    //    metadata we don't need (mtime), so it's cheaper than fsync. EIO
    //    from fsync is unrecoverable — per the fsyncgate discussion the
    //    dirty pages are gone and retrying won't help.
    if (::fdatasync(fd_) != 0) {
        int err = errno;
        if (err == EIO) {
            throw FatalException("[WAL] fdatasync(%s) returned EIO — data may be lost", path_);
        }
        throw IOException("[WAL] fdatasync(%s) failed: %s", path_, std::strerror(err));
    }
    entry_buf_.clear();
}

void WALWriter::LogInsertNode(uint16_t partition_id, uint32_t inmem_eid,
                              const std::vector<std::string> &keys,
                              const std::vector<Value> &values) {
    std::lock_guard<std::mutex> lk(mutex_);
    AppendU8((uint8_t)WALEntryType::INSERT_NODE);
    AppendU16(partition_id);
    AppendU32(inmem_eid);
    AppendU16((uint16_t)keys.size());
    for (idx_t i = 0; i < keys.size(); i++) {
        AppendString(keys[i]);
        AppendValue(values[i]);
    }
    CommitEntry();
}

void WALWriter::LogInsertNodeV2(uint16_t partition_id, uint64_t logical_id,
                                const std::vector<std::string> &keys,
                                const std::vector<Value> &values) {
    std::lock_guard<std::mutex> lk(mutex_);
    AppendU8((uint8_t)WALEntryType::INSERT_NODE_V2);
    AppendU16(partition_id);
    AppendU64(logical_id);
    AppendU16((uint16_t)keys.size());
    for (idx_t i = 0; i < keys.size(); i++) {
        AppendString(keys[i]);
        AppendValue(values[i]);
    }
    CommitEntry();
}

void WALWriter::LogUpdateProp(uint64_t user_id, const std::string &prop_key, const Value &value) {
    std::lock_guard<std::mutex> lk(mutex_);
    AppendU8((uint8_t)WALEntryType::UPDATE_PROP);
    AppendU64(user_id);
    AppendString(prop_key);
    AppendValue(value);
    CommitEntry();
}

void WALWriter::LogUpdateNodeV2(uint16_t partition_id, uint64_t logical_id,
                                const std::vector<std::string> &keys,
                                const std::vector<Value> &values) {
    std::lock_guard<std::mutex> lk(mutex_);
    AppendU8((uint8_t)WALEntryType::UPDATE_NODE_V2);
    AppendU16(partition_id);
    AppendU64(logical_id);
    AppendU16((uint16_t)keys.size());
    for (idx_t i = 0; i < keys.size(); i++) {
        AppendString(keys[i]);
        AppendValue(values[i]);
    }
    CommitEntry();
}

void WALWriter::LogDeleteNode(uint32_t extent_id, uint32_t row_offset, uint64_t user_id) {
    std::lock_guard<std::mutex> lk(mutex_);
    AppendU8((uint8_t)WALEntryType::DELETE_NODE);
    AppendU32(extent_id);
    AppendU32(row_offset);
    AppendU64(user_id);
    CommitEntry();
}

void WALWriter::LogDeleteNodeV2(uint64_t logical_id) {
    std::lock_guard<std::mutex> lk(mutex_);
    AppendU8((uint8_t)WALEntryType::DELETE_NODE_V2);
    AppendU64(logical_id);
    CommitEntry();
}

void WALWriter::LogInsertEdge(uint16_t edge_partition_id, uint64_t src_vid,
                              uint64_t dst_vid, uint64_t edge_id) {
    std::lock_guard<std::mutex> lk(mutex_);
    AppendU8((uint8_t)WALEntryType::INSERT_EDGE);
    AppendU16(edge_partition_id);
    AppendU64(src_vid);
    AppendU64(dst_vid);
    AppendU64(edge_id);
    CommitEntry();
}

void WALWriter::LogDeleteEdge(uint16_t edge_partition_id, uint64_t src_vid,
                              uint64_t edge_id) {
    std::lock_guard<std::mutex> lk(mutex_);
    AppendU8((uint8_t)WALEntryType::DELETE_EDGE);
    AppendU16(edge_partition_id);
    AppendU64(src_vid);
    AppendU64(edge_id);
    CommitEntry();
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
    AppendU8((uint8_t)WALEntryType::CHECKPOINT_BEGIN);
    CommitEntry();
}

void WALWriter::LogCheckpointEnd() {
    std::lock_guard<std::mutex> lk(mutex_);
    AppendU8((uint8_t)WALEntryType::CHECKPOINT_END);
    CommitEntry();
}

void WALWriter::Flush() {
    std::lock_guard<std::mutex> lk(mutex_);
    // Every Log*() already fdatasync's on return, so Flush is a no-op in the
    // common case. Keep it as an explicit sync for defensive callers and in
    // case future code adds unsynced writes.
    if (fd_ < 0) return;
    if (::fdatasync(fd_) != 0) {
        int err = errno;
        if (err == EIO) {
            throw FatalException("[WAL] fdatasync(%s) returned EIO", path_);
        }
        throw IOException("[WAL] fdatasync(%s) failed: %s", path_, std::strerror(err));
    }
}

void WALWriter::Truncate() {
    std::lock_guard<std::mutex> lk(mutex_);
    if (fd_ < 0) return;
    // Drop existing contents in-place — avoids a close/open race where a
    // concurrent opener could observe a missing file between the two.
    if (::ftruncate(fd_, 0) != 0) {
        int err = errno;
        throw IOException("[WAL] ftruncate(%s) failed: %s", path_, std::strerror(err));
    }
    // O_APPEND makes the kernel ignore this, but setting it keeps file
    // position consistent if anything does a non-append write.
    ::lseek(fd_, 0, SEEK_SET);
    WriteHeader();
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

// Read one framed entry: [size:u32][checksum:u64][size bytes].
// Returns true on success and fills `frame_bytes`. Returns false at EOF
// or on any corruption/short-read/checksum mismatch — the caller treats
// that as the end of the WAL (torn-write boundary) and does not replay
// anything beyond it.
static bool ReadFrame(std::ifstream &f, std::vector<uint8_t> &frame_bytes) {
    uint32_t size = 0;
    uint64_t checksum = 0;
    f.read((char*)&size, sizeof(size));
    if (f.gcount() == 0 && f.eof()) return false; // clean EOF
    if ((size_t)f.gcount() != sizeof(size)) {
        spdlog::warn("[WAL] Truncated frame header at offset {}", (int64_t)f.tellg());
        return false;
    }
    f.read((char*)&checksum, sizeof(checksum));
    if ((size_t)f.gcount() != sizeof(checksum)) {
        spdlog::warn("[WAL] Truncated frame checksum at offset {}", (int64_t)f.tellg());
        return false;
    }
    if (size == 0 || size > (1u << 24)) {
        // Entries are small (props + ids). A giant size indicates corruption.
        spdlog::warn("[WAL] Invalid frame size {} — stopping replay", size);
        return false;
    }
    frame_bytes.resize(size);
    f.read((char*)frame_bytes.data(), size);
    if ((size_t)f.gcount() != size) {
        spdlog::warn("[WAL] Truncated frame payload (wanted {}, got {})", size, (idx_t)f.gcount());
        return false;
    }
    uint64_t actual = duckdb::Checksum(frame_bytes.data(), frame_bytes.size());
    if (actual != checksum) {
        spdlog::warn("[WAL] Checksum mismatch at frame size={} (expected 0x{:016X} got 0x{:016X}) — stopping replay",
                     size, checksum, actual);
        return false;
    }
    return true;
}

// A lightweight std::ifstream-like reader over an in-memory buffer, so the
// existing ReadU8/ReadString/ReadValue machinery can decode a frame payload
// without allocating a second parser.
namespace {
class MemStream {
public:
    MemStream(const uint8_t *data, size_t size) : data_(data), size_(size) {}
    void read(char *out, std::streamsize n) {
        size_t avail = size_ - pos_;
        size_t take = (size_t)n <= avail ? (size_t)n : avail;
        std::memcpy(out, data_ + pos_, take);
        last_gcount_ = take;
        pos_ += take;
    }
    std::streamsize gcount() const { return (std::streamsize)last_gcount_; }
private:
    const uint8_t *data_;
    size_t size_;
    size_t pos_ = 0;
    size_t last_gcount_ = 0;
};
} // namespace

// Decoders that operate over either ifstream or MemStream.
template <typename S>
static uint8_t MsReadU8(S &s) { uint8_t v; s.read((char*)&v, 1); return v; }
template <typename S>
static uint16_t MsReadU16(S &s) { uint16_t v; s.read((char*)&v, 2); return v; }
template <typename S>
static uint32_t MsReadU32(S &s) { uint32_t v; s.read((char*)&v, 4); return v; }
template <typename S>
static uint64_t MsReadU64(S &s) { uint64_t v; s.read((char*)&v, 8); return v; }
template <typename S>
static std::string MsReadString(S &s) {
    uint16_t len = MsReadU16(s);
    std::string out(len, '\0');
    s.read(out.data(), len);
    return out;
}
template <typename S>
static Value MsReadValue(S &s) {
    auto tag = (ValTag)MsReadU8(s);
    switch (tag) {
        case ValTag::NULL_VAL: return Value();
        case ValTag::BIGINT:   return Value::BIGINT((int64_t)MsReadU64(s));
        case ValTag::UBIGINT:  return Value::UBIGINT(MsReadU64(s));
        case ValTag::INTEGER:  return Value::INTEGER((int32_t)MsReadU32(s));
        case ValTag::VARCHAR:  return Value(MsReadString(s));
        case ValTag::DOUBLE:   { double d; s.read((char*)&d, 8); return Value::DOUBLE(d); }
        case ValTag::BOOL:     return Value::BOOLEAN(MsReadU8(s) != 0);
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

    // Slurp all intact frames upfront. Any torn/corrupt frame terminates
    // the scan — whatever came after is treated as not-yet-durable. We
    // collect frames into memory because each frame is small and we need
    // to traverse them twice (find last CHECKPOINT_END, then replay).
    struct Frame {
        std::vector<uint8_t> bytes; // [type:u8][payload]
    };
    std::vector<Frame> frames;
    while (true) {
        Frame fr;
        if (!ReadFrame(f, fr.bytes)) break;
        frames.push_back(std::move(fr));
    }

    // Pass 1: find the last CHECKPOINT_END. Entries before that marker are
    // already in the compacted main store; only entries after it still need
    // to be re-applied. If there's a CHECKPOINT_BEGIN without matching END,
    // checkpoint crashed mid-flight and the catalog was NOT saved — replay
    // everything from the start.
    size_t replay_from = 0;
    bool found_checkpoint_end = false;
    bool in_checkpoint = false;
    for (size_t i = 0; i < frames.size(); i++) {
        if (frames[i].bytes.empty()) continue;
        auto type = (WALEntryType)frames[i].bytes[0];
        if (type == WALEntryType::CHECKPOINT_BEGIN) {
            in_checkpoint = true;
        } else if (type == WALEntryType::CHECKPOINT_END) {
            in_checkpoint = false;
            found_checkpoint_end = true;
            replay_from = i + 1;
        }
    }
    if (in_checkpoint) {
        spdlog::warn("[WAL] Detected incomplete checkpoint — replaying all entries");
        replay_from = 0;
    }
    if (found_checkpoint_end) {
        spdlog::info("[WAL] Checkpoint found — skipping already-compacted entries");
    }

    // Pass 2: replay frames from replay_from. Each frame is already
    // checksum-verified; decode from its in-memory byte buffer.
    idx_t count = 0;
    for (size_t i = replay_from; i < frames.size(); i++) {
        if (frames[i].bytes.empty()) continue;
        MemStream ms(frames[i].bytes.data() + 1, frames[i].bytes.size() - 1);
        auto type = (WALEntryType)frames[i].bytes[0];

        switch (type) {
            case WALEntryType::INSERT_NODE: {
                uint16_t pid = MsReadU16(ms);
                uint32_t inmem_eid = MsReadU32(ms);
                uint16_t num_props = MsReadU16(ms);
                std::vector<std::string> keys;
                std::vector<Value> values;
                for (uint16_t j = 0; j < num_props; j++) {
                    keys.push_back(MsReadString(ms));
                    values.push_back(MsReadValue(ms));
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
                uint64_t user_id = MsReadU64(ms);
                std::string key = MsReadString(ms);
                Value val = MsReadValue(ms);
                ds.SetPropertyByUserId(user_id, key, std::move(val));
                count++;
                break;
            }
            case WALEntryType::DELETE_NODE: {
                uint32_t eid = MsReadU32(ms);
                uint32_t off = MsReadU32(ms);
                uint64_t uid = MsReadU64(ms);
                spdlog::info("[WAL-REPLAY] DELETE_NODE eid=0x{:08X} off={} uid={}", eid, off, uid);
                if (eid != 0 || off != 0) ds.GetDeleteMask(eid).Delete(off);
                if (uid != 0) ds.DeleteByUserId(uid);
                count++;
                break;
            }
            case WALEntryType::INSERT_EDGE: {
                uint16_t epid = MsReadU16(ms);
                uint64_t src = MsReadU64(ms);
                uint64_t dst = MsReadU64(ms);
                uint64_t eid = MsReadU64(ms);
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
                uint16_t pid = MsReadU16(ms);
                uint64_t logical_id = MsReadU64(ms);
                uint16_t num_props = MsReadU16(ms);
                std::vector<std::string> keys;
                std::vector<Value> values;
                keys.reserve(num_props);
                values.reserve(num_props);
                for (uint16_t j = 0; j < num_props; j++) {
                    keys.push_back(MsReadString(ms));
                    values.push_back(MsReadValue(ms));
                }
                auto inmem_eid = ds.GetOrAllocateInMemoryExtentID(pid, keys);
                ds.AppendInsertRow(inmem_eid, std::move(keys), std::move(values),
                                   logical_id);
                count++;
                break;
            }
            case WALEntryType::UPDATE_NODE_V2: {
                uint16_t pid = MsReadU16(ms);
                uint64_t logical_id = MsReadU64(ms);
                uint16_t num_props = MsReadU16(ms);
                std::vector<std::string> keys;
                std::vector<Value> values;
                keys.reserve(num_props);
                values.reserve(num_props);
                for (uint16_t j = 0; j < num_props; j++) {
                    keys.push_back(MsReadString(ms));
                    values.push_back(MsReadValue(ms));
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
                uint64_t logical_id = MsReadU64(ms);
                ds.InvalidateCurrentVersion(logical_id);
                ds.InvalidateLogicalId(logical_id);
                count++;
                break;
            }
            case WALEntryType::DELETE_EDGE: {
                uint16_t epid = MsReadU16(ms);
                uint64_t src = MsReadU64(ms);
                uint64_t eid = MsReadU64(ms);
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
