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

#include <bitset>
#include <map>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <mutex>

namespace duckdb {

// Max rows per extent for DeleteMask sizing.
// Dynamic bitset used instead of fixed-size std::bitset.
static constexpr idx_t DELTA_STORE_INITIAL_CAPACITY = 8192;

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
    InsertBuffer() : total_rows_(0) {}

    // Append a row with property key names (for scan-time projection mapping).
    void AppendRow(vector<string> keys, vector<Value> values) {
        D_ASSERT(keys.size() == values.size());
        if (rows_.empty()) {
            // First row defines the schema
            schema_keys_ = keys;
        }
        // TODO: support multiple schema groups (different key sets)
        rows_.push_back(std::move(values));
        total_rows_++;
    }

    // Legacy: append without keys (for backward compatibility with tests)
    void AppendRow(vector<Value> values) {
        rows_.push_back(std::move(values));
        total_rows_++;
    }

    // Get row at index.
    const vector<Value>& GetRow(idx_t idx) const {
        return rows_[idx];
    }

    // Schema key names (property names in insertion order).
    const vector<string>& GetSchemaKeys() const { return schema_keys_; }

    // Find column index by property key name. Returns -1 if not found.
    int FindKeyIndex(const string& key) const {
        for (idx_t i = 0; i < schema_keys_.size(); i++) {
            if (schema_keys_[i] == key) return (int)i;
        }
        return -1;
    }

    // Total number of buffered rows.
    idx_t Size() const { return total_rows_; }

    bool Empty() const { return total_rows_ == 0; }

    void Clear() {
        rows_.clear();
        schema_keys_.clear();
        total_rows_ = 0;
    }

    // Iterate all rows.
    const vector<vector<Value>>& GetRows() const { return rows_; }

private:
    vector<vector<Value>> rows_;
    vector<string> schema_keys_;  // property key names for column mapping
    idx_t total_rows_;
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
    // --- Per-Extent deltas (keyed by ExtentID) ---

    UpdateSegment& GetUpdateSegment(idx_t extent_id) {
        return update_segments_[extent_id];
    }

    DeleteMask& GetDeleteMask(idx_t extent_id) {
        return delete_masks_[extent_id];
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

    AdjListDelta& GetAdjListDelta(idx_t partition_id) {
        return adj_deltas_[partition_id];
    }

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
        userid_property_updates_.clear();
        deleted_user_ids_.clear();
    }

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

private:
    std::unordered_map<idx_t, UpdateSegment> update_segments_;
    std::unordered_map<idx_t, DeleteMask> delete_masks_;
    std::unordered_map<idx_t, InsertBuffer> insert_buffers_;  // keyed by in-memory ExtentID
    std::unordered_map<idx_t, AdjListDelta> adj_deltas_;
    std::unordered_map<uint16_t, uint16_t> partition_inmem_counters_;  // partition_id -> next slot
    std::mutex alloc_mutex_;
    // Global user-id → property updates (for SET queries)
    std::unordered_map<uint64_t, std::unordered_map<std::string, Value>> userid_property_updates_;
    // Global user-id delete set (for DELETE queries)
    std::unordered_set<uint64_t> deleted_user_ids_;
};

} // namespace duckdb
