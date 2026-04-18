#pragma once
// =============================================================================
// DeltaStore — In-memory mutation buffer for CRUD operations
// =============================================================================
// Stores mutation deltas (INSERT/UPDATE/DELETE) separately from base extents.
// Read path merges base + delta at scan time (DuckDB UpdateSegment pattern).
// Compaction integrates deltas back into base extents.
//
// Granularity:
//   - UPDATE/DELETE: per-Extent (UpdateSegment, DeleteMask)
//   - INSERT: per-Partition (InsertBuffer)
//   - Edge mutation: per-Partition (AdjListDelta)
// =============================================================================

#include "common/typedef.hpp"
#include "common/types.hpp"
#include "common/types/data_chunk.hpp"
#include "common/types/value.hpp"

#include <atomic>
#include <bitset>
#include <map>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <mutex>

namespace duckdb {
}
namespace turbolynx {
}
namespace duckdb {
    using namespace turbolynx;
}
namespace turbolynx {
using namespace duckdb;

// Max rows per extent for DeleteMask sizing.
// Dynamic bitset used instead of fixed-size std::bitset.
static constexpr idx_t DELTA_STORE_INITIAL_CAPACITY = 8192;

inline uint64_t MakePhysicalId(uint32_t extent_id, uint32_t row_offset) {
    return (uint64_t(extent_id) << 32) | uint64_t(row_offset);
}

// ============================================================
// UpdateSegment — per-Extent property value overrides
// ============================================================
// Maps (row_offset, property_key_id) → new Value.
// Checked during scan: if entry exists, return delta value instead of base.

class UpdateSegment {
public:
    // Record an update by numeric key (legacy / unit tests).
    void Set(idx_t offset, idx_t prop_key, Value value) {
        updates_[offset][prop_key] = std::move(value);
    }

    // Record an update by property name (used by SET executor).
    void SetByName(idx_t offset, const std::string& prop_name, Value value) {
        named_updates_[offset][prop_name] = std::move(value);
    }

    // Record an update keyed by user-visible id property (for queries where VID is not in output).
    void SetByUserId(uint64_t user_id, const std::string& prop_name, Value value) {
        userid_updates_[user_id][prop_name] = std::move(value);
    }

    // Lookup by user id.
    const std::unordered_map<std::string, Value>* GetUserIdUpdates(uint64_t user_id) const {
        auto it = userid_updates_.find(user_id);
        return it != userid_updates_.end() ? &it->second : nullptr;
    }

    bool HasUserIdUpdates() const { return !userid_updates_.empty(); }

    // Lookup by numeric key.
    const Value* Get(idx_t offset, idx_t prop_key) const {
        auto row_it = updates_.find(offset);
        if (row_it == updates_.end()) return nullptr;
        auto prop_it = row_it->second.find(prop_key);
        if (prop_it == row_it->second.end()) return nullptr;
        return &prop_it->second;
    }

    // Lookup by property name.
    const Value* GetByName(idx_t offset, const std::string& prop_name) const {
        auto row_it = named_updates_.find(offset);
        if (row_it == named_updates_.end()) return nullptr;
        auto prop_it = row_it->second.find(prop_name);
        if (prop_it == row_it->second.end()) return nullptr;
        return &prop_it->second;
    }

    // Get all named updates for a row offset (for scan-time merge).
    const std::unordered_map<std::string, Value>* GetNamedUpdates(idx_t offset) const {
        auto it = named_updates_.find(offset);
        return it != named_updates_.end() ? &it->second : nullptr;
    }

    bool HasUpdate(idx_t offset) const {
        return updates_.find(offset) != updates_.end() ||
               named_updates_.find(offset) != named_updates_.end();
    }

    idx_t Size() const { return updates_.size() + named_updates_.size(); }

    bool Empty() const { return updates_.empty() && named_updates_.empty() && userid_updates_.empty(); }

    void Clear() { updates_.clear(); named_updates_.clear(); userid_updates_.clear(); }

    template <typename Fn>
    void ForEach(Fn&& fn) const {
        for (auto& [offset, props] : updates_) {
            for (auto& [prop_key, value] : props) {
                fn(offset, prop_key, value);
            }
        }
    }

private:
    // row_offset → { prop_key_id → value } (numeric keys)
    std::unordered_map<idx_t, std::unordered_map<idx_t, Value>> updates_;
    // row_offset → { prop_name → value } (string keys, used by SET)
    std::unordered_map<idx_t, std::unordered_map<std::string, Value>> named_updates_;
    // user_id → { prop_name → value } (keyed by user-visible id, for merge when VID not in output)
    std::unordered_map<uint64_t, std::unordered_map<std::string, Value>> userid_updates_;
};

// ============================================================
// DeleteMask — per-Extent deletion bitmap
// ============================================================
// Bit = 1 means row is deleted. Checked during scan to skip rows.

class DeleteMask {
public:
    DeleteMask() = default;

    // Mark row at `offset` as deleted.
    void Delete(idx_t offset) {
        deleted_.insert(offset);
    }

    // Check if row is deleted.
    bool IsDeleted(idx_t offset) const {
        return deleted_.find(offset) != deleted_.end();
    }

    // Number of deleted rows.
    idx_t Count() const { return deleted_.size(); }

    bool Empty() const { return deleted_.empty(); }

    void Clear() { deleted_.clear(); }

    // Iterate all deleted offsets.
    const std::unordered_set<idx_t>& GetDeleted() const { return deleted_; }

private:
    std::unordered_set<idx_t> deleted_;
};

// ============================================================
// InsertBuffer — per-Partition new row accumulator
// ============================================================
// Stores newly created nodes/edges as DataChunks.
// Scanned alongside base extents during read.
// On compaction, rows are assigned to extents via CGC.

class InsertBuffer {
public:
    InsertBuffer() : total_rows_(0), live_rows_(0) {}

    // Append a row with property key names (for scan-time projection mapping).
    idx_t AppendRow(vector<string> keys, vector<Value> values, uint64_t logical_id = 0) {
        D_ASSERT(keys.size() == values.size());
        if (rows_.empty()) {
            // First row defines the schema
            schema_keys_ = keys;
        }
        D_ASSERT(schema_keys_.empty() || schema_keys_ == keys);
        rows_.push_back(std::move(values));
        logical_ids_.push_back(logical_id);
        valid_rows_.push_back(true);
        total_rows_++;
        live_rows_++;
        return rows_.size() - 1;
    }

    // Legacy: append without keys (for backward compatibility with tests)
    idx_t AppendRow(vector<Value> values) {
        rows_.push_back(std::move(values));
        logical_ids_.push_back(0);
        valid_rows_.push_back(true);
        total_rows_++;
        live_rows_++;
        return rows_.size() - 1;
    }

    // Get row at index.
    const vector<Value>& GetRow(idx_t idx) const {
        return rows_[idx];
    }

    bool IsValid(idx_t idx) const {
        return idx < valid_rows_.size() && valid_rows_[idx];
    }

    void Invalidate(idx_t idx) {
        if (idx >= valid_rows_.size() || !valid_rows_[idx]) {
            return;
        }
        valid_rows_[idx] = false;
        if (live_rows_ > 0) {
            live_rows_--;
        }
    }

    uint64_t GetLogicalId(idx_t idx) const {
        return idx < logical_ids_.size() ? logical_ids_[idx] : 0;
    }

    void SetLogicalId(idx_t idx, uint64_t logical_id) {
        if (idx >= logical_ids_.size()) {
            logical_ids_.resize(idx + 1, 0);
        }
        logical_ids_[idx] = logical_id;
    }

    // Schema key names (property names in insertion order).
    const vector<string>& GetSchemaKeys() const { return schema_keys_; }

    bool MatchesSchema(const vector<string> &keys) const {
        return schema_keys_ == keys;
    }

    // Find column index by property key name. Returns -1 if not found.
    int FindKeyIndex(const string& key) const {
        for (idx_t i = 0; i < schema_keys_.size(); i++) {
            if (schema_keys_[i] == key) return (int)i;
        }
        return -1;
    }

    // Total number of buffered rows.
    idx_t Size() const { return total_rows_; }

    idx_t LiveSize() const { return live_rows_; }

    bool Empty() const { return live_rows_ == 0; }

    void Clear() {
        rows_.clear();
        schema_keys_.clear();
        logical_ids_.clear();
        valid_rows_.clear();
        total_rows_ = 0;
        live_rows_ = 0;
    }

    // Iterate all rows.
    const vector<vector<Value>>& GetRows() const { return rows_; }

private:
    vector<vector<Value>> rows_;
    vector<string> schema_keys_;  // property key names for column mapping
    vector<uint64_t> logical_ids_;
    vector<bool> valid_rows_;
    idx_t total_rows_;
    idx_t live_rows_;
};

// ============================================================
// AdjListDelta — per-Partition edge mutation buffer
// ============================================================
// Tracks added/deleted edges separately from base CSR.
// Merged into CSR during compaction.

struct EdgeEntry {
    uint64_t dst_vid;
    uint64_t edge_id;

    bool operator==(const EdgeEntry& o) const {
        return dst_vid == o.dst_vid && edge_id == o.edge_id;
    }
};

class AdjListDelta {
public:
    // Add a new edge from src to dst.
    void InsertEdge(uint64_t src_vid, uint64_t dst_vid, uint64_t edge_id) {
        inserted_[src_vid].push_back({dst_vid, edge_id});
    }

    // Mark an edge as deleted.
    void DeleteEdge(uint64_t src_vid, uint64_t edge_id) {
        deleted_[src_vid].insert(edge_id);
    }

    // Get inserted edges for a source vertex.
    const vector<EdgeEntry>* GetInserted(uint64_t src_vid) const {
        auto it = inserted_.find(src_vid);
        return it != inserted_.end() ? &it->second : nullptr;
    }

    // Check if an edge is deleted.
    bool IsEdgeDeleted(uint64_t src_vid, uint64_t edge_id) const {
        auto it = deleted_.find(src_vid);
        if (it == deleted_.end()) return false;
        return it->second.find(edge_id) != it->second.end();
    }

    const std::unordered_set<uint64_t> *GetDeleted(uint64_t src_vid) const {
        auto it = deleted_.find(src_vid);
        return it != deleted_.end() ? &it->second : nullptr;
    }

    idx_t InsertedCount() const {
        idx_t count = 0;
        for (auto& [_, edges] : inserted_) count += edges.size();
        return count;
    }

    idx_t DeletedCount() const {
        idx_t count = 0;
        for (auto& [_, edges] : deleted_) count += edges.size();
        return count;
    }

    bool Empty() const { return inserted_.empty() && deleted_.empty(); }

    void Clear() {
        inserted_.clear();
        deleted_.clear();
    }

    const std::unordered_map<uint64_t, vector<EdgeEntry>>& GetAllInserted() const {
        return inserted_;
    }

    const std::unordered_map<uint64_t, std::unordered_set<uint64_t>>& GetAllDeleted() const {
        return deleted_;
    }

private:
    // src_vid → list of new edges
    std::unordered_map<uint64_t, vector<EdgeEntry>> inserted_;
    // src_vid → set of deleted edge_ids
    std::unordered_map<uint64_t, std::unordered_set<uint64_t>> deleted_;
};

// ============================================================
// DeltaStore — top-level container
// ============================================================
// Owns all delta components. One DeltaStore per database instance.

class DeltaStore {
public:
    static constexpr uint64_t SYNTHETIC_NODE_LID_PREFIX =
        0x7F00000000000000ull;
    static constexpr uint64_t SYNTHETIC_NODE_LID_PREFIX_MASK =
        0xFF00000000000000ull;
    static constexpr uint64_t SYNTHETIC_EDGE_LID_PREFIX =
        0x7E00000000000000ull;
    static constexpr uint64_t SYNTHETIC_EDGE_LID_PREFIX_MASK =
        0xFF00000000000000ull;

    struct LogicalLocation {
        bool valid;
        bool is_delta;
        uint64_t pid;
    };

    // --- Per-Extent deltas (keyed by ExtentID) ---

    UpdateSegment& GetUpdateSegment(idx_t extent_id) {
        return update_segments_[extent_id];
    }

    DeleteMask& GetDeleteMask(idx_t extent_id) {
        return delete_masks_[extent_id];
    }

    // Thread-safe read-only check: returns true if row (extent_id, offset) is deleted.
    // Does NOT insert into the map, safe for concurrent readers.
    bool IsDeletedInMask(idx_t extent_id, idx_t offset) const {
        auto it = delete_masks_.find(extent_id);
        if (it == delete_masks_.end()) return false;
        return it->second.IsDeleted(offset);
    }

    // --- InsertBuffer: keyed by ExtentID (in-memory extent IDs) ---

    // Get or create InsertBuffer for an in-memory ExtentID.
    InsertBuffer& GetInsertBuffer(idx_t extent_id) {
        return insert_buffers_[extent_id];
    }

    // Const lookup — returns nullptr if no InsertBuffer for this extent ID.
    const InsertBuffer* FindInsertBuffer(idx_t extent_id) const {
        auto it = insert_buffers_.find(extent_id);
        return it != insert_buffers_.end() ? &it->second : nullptr;
    }

    // Legacy: find InsertBuffer by partition ID (searches all in-memory extents for that partition).
    const InsertBuffer* FindInsertBufferByPartition(idx_t partition_id) const {
        for (auto& [eid, buf] : insert_buffers_) {
            if ((eid >> 16) == partition_id && !buf.Empty()) return &buf;
        }
        return nullptr;
    }

    // Allocate a new in-memory ExtentID for a partition. Returns the full ExtentID.
    // Format: [PartitionID:16][LocalExtentID:16] where LocalExtentID >= 0xFF00.
    // partition_logical_id is the 16-bit logical PartitionID (from PropertySchemaCatalogEntry::pid).
    uint32_t AllocateInMemoryExtentID(uint16_t partition_logical_id) {
        std::lock_guard<std::mutex> lk(alloc_mutex_);
        auto& counter = partition_inmem_counters_[partition_logical_id];
        uint16_t local_id = 0xFF00 + counter;
        if (counter >= 256) {
            // All 256 in-memory extent slots used; reuse the last one.
            local_id = 0xFFFF;
        } else {
            counter++;
        }
        return ((uint32_t)partition_logical_id << 16) | local_id;
    }

    // Get all in-memory ExtentIDs for a given logical partition ID.
    std::vector<uint32_t> GetInMemoryExtentIDs(uint16_t partition_logical_id) const {
        std::vector<uint32_t> result;
        for (auto& [eid, buf] : insert_buffers_) {
            if (((eid >> 16) & 0xFFFF) == partition_logical_id && !buf.Empty()) {
                result.push_back((uint32_t)eid);
            }
        }
        return result;
    }

    uint32_t GetOrAllocateInMemoryExtentID(uint16_t partition_logical_id,
                                           const vector<string> &schema_keys) {
        for (auto &[eid, buf] : insert_buffers_) {
            if (((eid >> 16) & 0xFFFF) != partition_logical_id) {
                continue;
            }
            if (buf.GetSchemaKeys().empty() || buf.MatchesSchema(schema_keys)) {
                return (uint32_t)eid;
            }
        }
        return AllocateInMemoryExtentID(partition_logical_id);
    }

    idx_t AppendInsertRow(uint32_t extent_id, vector<string> keys,
                          vector<Value> values, uint64_t logical_id) {
        auto &buf = insert_buffers_[extent_id];
        auto row_idx = buf.AppendRow(std::move(keys), std::move(values), logical_id);
        if (logical_id != 0) {
            UpsertLogicalMapping(logical_id,
                                 MakePhysicalId(extent_id, (uint32_t)row_idx),
                                 true);
        }
        return row_idx;
    }

    uint64_t AllocateNodeLogicalId() {
        uint64_t ctr = ++node_lid_counter_;
        return SYNTHETIC_NODE_LID_PREFIX |
               (ctr & ~SYNTHETIC_NODE_LID_PREFIX_MASK);
    }

    void ObserveNodeLogicalId(uint64_t logical_id) {
        if ((logical_id & SYNTHETIC_NODE_LID_PREFIX_MASK) !=
            SYNTHETIC_NODE_LID_PREFIX) {
            return;
        }
        uint64_t observed = logical_id & ~SYNTHETIC_NODE_LID_PREFIX_MASK;
        uint64_t cur = node_lid_counter_.load();
        while (observed > cur &&
               !node_lid_counter_.compare_exchange_weak(cur, observed)) {
        }
    }

    bool TryGetDeltaRow(uint64_t pid, const InsertBuffer *&buf, idx_t &row_idx) const {
        uint32_t extent_id = (uint32_t)(pid >> 32);
        row_idx = (idx_t)(pid & 0xFFFFFFFFull);
        auto it = insert_buffers_.find(extent_id);
        if (it == insert_buffers_.end()) {
            buf = nullptr;
            return false;
        }
        buf = &it->second;
        return row_idx < it->second.Size();
    }

    bool TryGetCurrentDeltaRowByLogicalId(uint64_t logical_id, uint64_t &pid,
                                          const InsertBuffer *&buf,
                                          idx_t &row_idx) const {
        pid = ResolvePid(logical_id);
        if (pid != 0 && TryGetDeltaRow(pid, buf, row_idx) && buf &&
            buf->IsValid(row_idx) && buf->GetLogicalId(row_idx) == logical_id) {
            return true;
        }
        for (auto &[extent_id, candidate] : insert_buffers_) {
            for (idx_t i = 0; i < candidate.Size(); i++) {
                if (!candidate.IsValid(i) ||
                    candidate.GetLogicalId(i) != logical_id) {
                    continue;
                }
                pid = MakePhysicalId((uint32_t)extent_id, (uint32_t)i);
                buf = &candidate;
                row_idx = i;
                return true;
            }
        }
        pid = 0;
        buf = nullptr;
        row_idx = 0;
        return false;
    }

    bool TryGetDeltaRow(uint64_t pid, InsertBuffer *&buf, idx_t &row_idx) {
        uint32_t extent_id = (uint32_t)(pid >> 32);
        row_idx = (idx_t)(pid & 0xFFFFFFFFull);
        auto it = insert_buffers_.find(extent_id);
        if (it == insert_buffers_.end()) {
            buf = nullptr;
            return false;
        }
        buf = &it->second;
        return row_idx < it->second.Size();
    }

    bool InvalidateDeltaRow(uint64_t pid) {
        InsertBuffer *buf = nullptr;
        idx_t row_idx = 0;
        if (!TryGetDeltaRow(pid, buf, row_idx) || !buf) {
            return false;
        }
        buf->Invalidate(row_idx);
        return true;
    }

    bool InvalidateCurrentVersion(uint64_t logical_id) {
        auto current_pid = ResolvePid(logical_id);
        if (current_pid == 0) {
            return false;
        }
        uint32_t extent_id = (uint32_t)(current_pid >> 32);
        uint32_t row_offset = (uint32_t)(current_pid & 0xFFFFFFFFull);
        if (ResolveIsDelta(logical_id) || IsInMemoryExtent(extent_id)) {
            return InvalidateDeltaRow(current_pid);
        }
        GetDeleteMask(extent_id).Delete(row_offset);
        return true;
    }

    void PreserveAdjacencyPidOnUpdate(uint64_t logical_id) {
        if (adjacency_pid_overrides_.find(logical_id) !=
            adjacency_pid_overrides_.end()) {
            return;
        }
        auto current_pid = ResolvePid(logical_id);
        if (current_pid == 0) {
            return;
        }
        adjacency_pid_overrides_[logical_id] = current_pid;
    }

    void SetAdjacencyPidOverride(uint64_t logical_id, uint64_t pid) {
        if (pid == 0) {
            adjacency_pid_overrides_.erase(logical_id);
            return;
        }
        adjacency_pid_overrides_[logical_id] = pid;
    }

    uint64_t GetAdjacencyPidOverride(uint64_t logical_id) const {
        auto it = adjacency_pid_overrides_.find(logical_id);
        return it != adjacency_pid_overrides_.end() ? it->second : 0;
    }

    uint64_t ResolveAdjacencyPid(uint64_t logical_id) const {
        auto override_pid = GetAdjacencyPidOverride(logical_id);
        return override_pid != 0 ? override_pid : ResolvePid(logical_id);
    }

    uint64_t GetDeltaRowLogicalId(uint64_t pid) const {
        const InsertBuffer *buf = nullptr;
        idx_t row_idx = 0;
        if (!TryGetDeltaRow(pid, buf, row_idx) || !buf) {
            return 0;
        }
        return buf->GetLogicalId(row_idx);
    }

    void UpsertLogicalMapping(uint64_t logical_id, uint64_t pid, bool is_delta) {
        ObserveNodeLogicalId(logical_id);
        auto existing = lid_pid_table_.find(logical_id);
        if (existing != lid_pid_table_.end() && existing->second.valid) {
            pid_lid_table_.erase(existing->second.pid);
        }
        lid_pid_table_[logical_id] = {true, is_delta, pid};
        pid_lid_table_[pid] = logical_id;
    }

    void InvalidateLogicalId(uint64_t logical_id) {
        auto existing = lid_pid_table_.find(logical_id);
        if (existing != lid_pid_table_.end() && existing->second.valid) {
            pid_lid_table_.erase(existing->second.pid);
        }
        adjacency_pid_overrides_.erase(logical_id);
        lid_pid_table_[logical_id] = {false, false, 0};
    }

    bool IsLogicalIdDeleted(uint64_t logical_id) const {
        auto it = lid_pid_table_.find(logical_id);
        return it != lid_pid_table_.end() && !it->second.valid;
    }

    uint64_t ResolvePid(uint64_t logical_id) const {
        auto it = lid_pid_table_.find(logical_id);
        if (it == lid_pid_table_.end()) {
            return logical_id;
        }
        if (!it->second.valid) {
            return 0;
        }
        return it->second.pid;
    }

    bool ResolveIsDelta(uint64_t logical_id) const {
        auto it = lid_pid_table_.find(logical_id);
        return it != lid_pid_table_.end() && it->second.valid && it->second.is_delta;
    }

    uint64_t ResolveLogicalId(uint64_t pid) const {
        auto it = pid_lid_table_.find(pid);
        if (it == pid_lid_table_.end()) {
            return pid;
        }
        return it->second;
    }

    const std::unordered_map<uint64_t, LogicalLocation>& GetAllLogicalMappings() const {
        return lid_pid_table_;
    }

    const std::unordered_map<uint64_t, uint64_t>&
    GetAllAdjacencyPidOverrides() const {
        return adjacency_pid_overrides_;
    }

    AdjListDelta& GetAdjListDelta(idx_t partition_id) {
        return adj_deltas_[partition_id];
    }

    // --- Global edge-id allocation ---
    // Edge IDs are stable logical IDs in a synthetic namespace so they never
    // alias node/base physical IDs.
    uint64_t AllocateEdgeId(uint16_t partition_id) {
        (void)partition_id;
        uint64_t ctr = ++edge_counter_;
        return SYNTHETIC_EDGE_LID_PREFIX |
               (ctr & ~SYNTHETIC_EDGE_LID_PREFIX_MASK);
    }

    // During WAL replay, bump the counter so future allocations don't collide
    // with edge IDs already present on disk.
    void ObserveEdgeCounter(uint64_t edge_id) {
        uint64_t observed_counter =
            ((edge_id & SYNTHETIC_EDGE_LID_PREFIX_MASK) ==
             SYNTHETIC_EDGE_LID_PREFIX)
                ? (edge_id & ~SYNTHETIC_EDGE_LID_PREFIX_MASK)
                : (edge_id & 0x0000FFFFFFFFFFFFull);
        uint64_t cur = edge_counter_.load();
        while (observed_counter > cur &&
               !edge_counter_.compare_exchange_weak(cur, observed_counter)) {
            // retry with fresh cur
        }
    }

    uint64_t PeekEdgeCounter() const { return edge_counter_.load(); }

    // Expose adj_deltas for iteration during edge read merge.
    // Expose insert_buffers for compaction
    std::unordered_map<idx_t, InsertBuffer>& insert_buffers_exposed() { return insert_buffers_; }

    const std::unordered_map<idx_t, AdjListDelta>& adj_deltas_exposed() const {
        return adj_deltas_;
    }

    bool HasAdjListData() const {
        for (auto& [_, d] : adj_deltas_) if (!d.Empty()) return true;
        return false;
    }

    // Global user-id based property updates (for SET read merge when VID not in output)
    void SetPropertyByUserId(uint64_t user_id, const std::string& prop_name, Value value) {
        userid_property_updates_[user_id][prop_name] = std::move(value);
    }

    const std::unordered_map<std::string, Value>* GetPropertyByUserId(uint64_t user_id) const {
        auto it = userid_property_updates_.find(user_id);
        return it != userid_property_updates_.end() ? &it->second : nullptr;
    }

    bool HasPropertyUpdates() const { return !userid_property_updates_.empty(); }

    // User-id based delete set (for DELETE when VID may be in-memory)
    void DeleteByUserId(uint64_t user_id) { deleted_user_ids_.insert(user_id); }
    bool IsDeletedByUserId(uint64_t user_id) const { return deleted_user_ids_.count(user_id) > 0; }
    bool HasDeletedUserIds() const { return !deleted_user_ids_.empty(); }

    // --- Global operations ---

    void Clear() {
        update_segments_.clear();
        delete_masks_.clear();
        insert_buffers_.clear();
        adj_deltas_.clear();
        partition_inmem_counters_.clear();
        lid_pid_table_.clear();
        pid_lid_table_.clear();
        adjacency_pid_overrides_.clear();
        userid_property_updates_.clear();
        deleted_user_ids_.clear();
        node_lid_counter_.store(0);
    }

    // Clear only INSERT-related data (after INSERT compaction).
    // Preserves SET/DELETE deltas for WAL re-write.
    void ClearInsertData() {
        insert_buffers_.clear();
        partition_inmem_counters_.clear();
    }

    // Expose userid property updates for WAL re-write during compaction.
    const std::unordered_map<uint64_t, std::unordered_map<std::string, Value>>&
    GetAllPropertyUpdates() const { return userid_property_updates_; }

    // Expose delete masks for WAL re-write during compaction.
    const std::unordered_map<idx_t, DeleteMask>& GetAllDeleteMasks() const { return delete_masks_; }

    // Expose deleted user IDs for WAL re-write during compaction.
    const std::unordered_set<uint64_t>& GetAllDeletedUserIds() const { return deleted_user_ids_; }

    bool Empty() const {
        for (auto& [_, seg] : update_segments_) if (!seg.Empty()) return false;
        for (auto& [_, mask] : delete_masks_) if (!mask.Empty()) return false;
        for (auto& [_, buf] : insert_buffers_) if (!buf.Empty()) return false;
        for (auto& [_, adj] : adj_deltas_) if (!adj.Empty()) return false;
        return true;
    }

    // Check if any InsertBuffer has actual data.
    bool HasInsertData() const {
        for (auto& [_, buf] : insert_buffers_) if (!buf.Empty()) return true;
        return false;
    }

    // Total in-memory rows across all InsertBuffers.
    idx_t GetTotalInMemoryRows() const {
        idx_t total = 0;
        for (auto& [_, buf] : insert_buffers_) total += buf.LiveSize();
        return total;
    }

    // Total in-memory extents count.
    idx_t GetInMemoryExtentCount() const {
        idx_t count = 0;
        for (auto& [_, buf] : insert_buffers_) if (!buf.Empty()) count++;
        return count;
    }

    // Check if any delta (insert/update/delete) exists.
    bool HasAnyDelta() const {
        return HasInsertData() || HasPropertyUpdates() || HasDeletedUserIds()
            || !delete_masks_.empty() || !update_segments_.empty();
    }

private:
    std::unordered_map<idx_t, UpdateSegment> update_segments_;
    std::unordered_map<idx_t, DeleteMask> delete_masks_;
    std::unordered_map<idx_t, InsertBuffer> insert_buffers_;  // keyed by in-memory ExtentID
    std::unordered_map<idx_t, AdjListDelta> adj_deltas_;
    std::unordered_map<uint16_t, uint16_t> partition_inmem_counters_;  // partition_id -> next slot
    std::unordered_map<uint64_t, LogicalLocation> lid_pid_table_;
    std::unordered_map<uint64_t, uint64_t> pid_lid_table_;
    std::unordered_map<uint64_t, uint64_t> adjacency_pid_overrides_;
    std::mutex alloc_mutex_;
    // Global edge-id counter (low 48 bits of an edge_id). Shared across all
    // write paths and restored from WAL on replay.
    std::atomic<uint64_t> edge_counter_{0};
    std::atomic<uint64_t> node_lid_counter_{0};
    // Global user-id → property updates (for SET queries)
    std::unordered_map<uint64_t, std::unordered_map<std::string, Value>> userid_property_updates_;
    // Global user-id delete set (for DELETE queries)
    std::unordered_set<uint64_t> deleted_user_ids_;
};

} // namespace turbolynx

namespace duckdb {
using DeltaStore = turbolynx::DeltaStore;
}
