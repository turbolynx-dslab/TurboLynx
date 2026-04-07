#include "execution/physical_operator/physical_node_scan.hpp"
#include "storage/extent/extent_iterator.hpp"
#include "icecream.hpp"
#include "planner/expression.hpp"
#include "planner/expression/bound_columnref_expression.hpp"
#include "planner/expression/bound_comparison_expression.hpp"
#include "planner/expression/bound_conjunction_expression.hpp"
#include "planner/expression/bound_reference_expression.hpp"
#include "storage/graph_storage_wrapper.hpp"
#include "main/database.hpp"
#include "catalog/catalog_entry/list.hpp"
#include "spdlog/spdlog.h"

#include <cassert>
#include <queue>
#include <set>
#include <mutex>
#include <atomic>

namespace duckdb {

class NodeScanState : public LocalSourceState {
   public:
    explicit NodeScanState() : iter_inited(false), iter_finished(false),
                               delta_phase(false), delta_cur(0), delta_row(0) {}
    ~NodeScanState() {
        while (!ext_its.empty()) {
            delete ext_its.front();
            ext_its.pop();
        }
    }
   public:
    bool iter_inited;
    bool iter_finished;
    std::queue<ExtentIterator *> ext_its;
    DataChunk extent_cache;
    bool delta_phase;
    vector<uint32_t> delta_eids;
    idx_t delta_cur, delta_row;
    //! Per-thread filter state for parallel scan with filter pushdown
    FilteredChunkBuffer parallel_filter_buffer;
    bool filter_buffer_initialized = false;
    //! Per-thread copies of filter params (non-const refs in GetNextExtent)
    int64_t local_filter_key_idx = -1;
    Value local_eq_filter_value;
    RangeFilterValue local_range_filter_value;
    ExpressionExecutor local_executor;
};

//! Global state for parallel NodeScan: pre-creates per-extent iterators on
//! main thread, then distributes them to worker threads via mutex-protected queue.
class NodeScanGlobalState : public GlobalSourceState {
public:
    NodeScanGlobalState() : initialized(false) {}

    idx_t MaxThreads() override {
        lock_guard<mutex> lock(scan_lock);
        return std::max((idx_t)1, (idx_t)extent_iterators.size());
    }

    //! Get the next pre-created ExtentIterator (thread-safe)
    ExtentIterator *GetNextIterator() {
        lock_guard<mutex> lock(scan_lock);
        if (extent_iterators.empty()) return nullptr;
        auto *it = extent_iterators.front();
        extent_iterators.pop();
        return it;
    }

    ~NodeScanGlobalState() {
        while (!extent_iterators.empty()) {
            delete extent_iterators.front();
            extent_iterators.pop();
        }
    }

    //! Try to claim the right to perform delta-phase scanning. Only one
    //! thread succeeds; other threads see iter_finished after on-disk
    //! extents are exhausted.
    bool TryClaimDeltaPhase() {
        bool expected = false;
        return delta_claimed.compare_exchange_strong(expected, true);
    }

    mutex scan_lock;
    bool initialized;
    //! Per-extent iterators (created on main thread, consumed by workers)
    std::queue<ExtentIterator *> extent_iterators;
    //! Set by the one thread that gets to read in-memory delta extents.
    std::atomic<bool> delta_claimed{false};
};

PhysicalNodeScan::PhysicalNodeScan(
    Schema &sch, vector<idx_t> oids,
    vector<vector<uint64_t>> projection_mapping,
    vector<LogicalType> scan_types_,
    vector<vector<uint64_t>> scan_projection_mapping)
    : CypherPhysicalOperator(PhysicalOperatorType::NODE_SCAN, sch),
      oids(oids),
      projection_mapping(projection_mapping),
      scan_projection_mapping(scan_projection_mapping),
      current_schema_idx(0)
{
    num_schemas = 1;
    scan_types.resize(num_schemas);
    scan_types[0] = std::move(scan_types_);
    filtered_chunk_buffer.Initialize(sch.getStoredTypes());
}

PhysicalNodeScan::PhysicalNodeScan(
    Schema &sch, vector<idx_t> oids,
    vector<vector<uint64_t>> projection_mapping,
    vector<LogicalType> scan_types_,
    vector<vector<uint64_t>> scan_projection_mapping, int64_t filterKeyIndex,
    duckdb::Value filterValue)
    : PhysicalNodeScan(sch, oids, projection_mapping, scan_types_,
                       scan_projection_mapping)
{
    is_filter_pushdowned = true;
    filter_pushdown_type = FilterPushdownType::FP_EQ;
    filter_pushdown_key_idxs.push_back(filterKeyIndex);
    eq_filter_pushdown_values.push_back(filterValue);
}

PhysicalNodeScan::PhysicalNodeScan(
    Schema &sch, vector<idx_t> oids,
    vector<vector<uint64_t>> projection_mapping,
    vector<LogicalType> scan_types_,
    vector<vector<uint64_t>> scan_projection_mapping, int64_t filterKeyIndex,
    duckdb::Value l_filterValue, duckdb::Value r_filterValue, bool l_inclusive,
    bool r_inclusive)
    : PhysicalNodeScan(sch, oids, projection_mapping, scan_types_,
                       scan_projection_mapping)
{
    is_filter_pushdowned = true;
    filter_pushdown_type = FilterPushdownType::FP_RANGE;
    filter_pushdown_key_idxs.push_back(filterKeyIndex);
    range_filter_pushdown_values.push_back(
        {l_filterValue, r_filterValue, l_inclusive, r_inclusive});
}

PhysicalNodeScan::PhysicalNodeScan(
    Schema &sch, vector<idx_t> oids,
    vector<vector<uint64_t>> projection_mapping,
    vector<LogicalType> scan_types_,
    vector<vector<uint64_t>> scan_projection_mapping,
    vector<unique_ptr<Expression>> predicates)
    : PhysicalNodeScan(sch, oids, projection_mapping, scan_types_,
                       scan_projection_mapping)
{
    is_filter_pushdowned = true;
    filter_pushdown_type = FilterPushdownType::FP_COMPLEX;
    D_ASSERT(predicates.size() > 0);
    if (predicates.size() > 1) {
        auto conjunction = make_unique<BoundConjunctionExpression>(
            ExpressionType::CONJUNCTION_AND);
        for (auto &expr : predicates) {
            conjunction->children.push_back(move(expr));
        }
        filter_expression = move(conjunction);
    }
    else {
        filter_expression = move(predicates[0]);
    }
    executor = ExpressionExecutor(*(filter_expression.get()));
}

PhysicalNodeScan::PhysicalNodeScan(
    vector<Schema> &sch, Schema &union_schema, vector<idx_t> oids,
    vector<vector<uint64_t>> projection_mapping,
    vector<vector<uint64_t>> scan_projection_mapping)
    : CypherPhysicalOperator(PhysicalOperatorType::NODE_SCAN, union_schema,
                             sch),
      oids(oids),
      projection_mapping(projection_mapping),
      scan_projection_mapping(scan_projection_mapping),
      current_schema_idx(0)  // without pushdown, two mappings are exactly same
{
    num_schemas = sch.size();
    scan_types.resize(num_schemas);
    for (auto i = 0; i < num_schemas; i++) {
        scan_types[i] = std::move(sch[i].getStoredTypes());
    }
    filtered_chunk_buffer.Initialize(union_schema.getStoredTypes());
}

/* Schemaless Equality Filter Pushdown */
PhysicalNodeScan::PhysicalNodeScan(
    vector<Schema> &sch, Schema &union_schema, vector<idx_t> oids,
    vector<vector<uint64_t>> projection_mapping,
    vector<vector<uint64_t>> scan_projection_mapping,
    vector<int64_t> &filterKeyIndexes, vector<duckdb::Value> &filterValues)
    : PhysicalNodeScan(sch, union_schema, oids, projection_mapping,
                       scan_projection_mapping)
{
    is_filter_pushdowned = true;
    filter_pushdown_type = FilterPushdownType::FP_EQ;
    filter_pushdown_key_idxs = move(filterKeyIndexes);
    eq_filter_pushdown_values = move(filterValues);
}

/* Schemaless Range Filter Pushdown */
PhysicalNodeScan::PhysicalNodeScan(
    vector<Schema> &sch, Schema &union_schema, vector<idx_t> oids,
    vector<vector<uint64_t>> projection_mapping,
    vector<vector<uint64_t>> scan_projection_mapping,
    vector<int64_t> &filterKeyIndexes,
    vector<RangeFilterValue> &rangeFilterValues)
    : PhysicalNodeScan(sch, union_schema, oids, projection_mapping,
                       scan_projection_mapping)
{
    is_filter_pushdowned = true;
    filter_pushdown_type = FilterPushdownType::FP_RANGE;
    filter_pushdown_key_idxs = move(filterKeyIndexes);
    range_filter_pushdown_values = move(rangeFilterValues);
}

/* Schemaless Complex Filter Pushdown */
PhysicalNodeScan::PhysicalNodeScan(
    vector<Schema> &sch, Schema &union_schema, vector<idx_t> oids,
    vector<vector<uint64_t>> projection_mapping,
    vector<vector<uint64_t>> scan_projection_mapping,
    vector<unique_ptr<Expression>> predicates)
    : PhysicalNodeScan(sch, union_schema, oids, projection_mapping,
                       scan_projection_mapping)
{
    is_filter_pushdowned = true;
    filter_pushdown_type = FilterPushdownType::FP_COMPLEX;
    D_ASSERT(predicates.size() > 0);
    if (predicates.size() > 1) {
        auto conjunction = make_unique<BoundConjunctionExpression>(
            ExpressionType::CONJUNCTION_AND);
        for (auto &expr : predicates) {
            conjunction->children.push_back(move(expr));
        }
        filter_expression = move(conjunction);
    }
    else {
        filter_expression = move(predicates[0]);
    }
    executor = ExpressionExecutor(*(filter_expression.get()));
}

PhysicalNodeScan::~PhysicalNodeScan() {}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
unique_ptr<LocalSourceState> PhysicalNodeScan::GetLocalSourceState(
    ExecutionContext &context) const
{
    return make_unique<NodeScanState>();
}

unique_ptr<GlobalSourceState> PhysicalNodeScan::GetGlobalSourceState(
    ClientContext &context) const
{
    auto gstate = make_unique<NodeScanGlobalState>();

    // Pre-create one ExtentIterator per storage extent on the main thread.
    // This avoids thread-safety issues with Catalog::GetEntry and PinSegment
    // during concurrent initialization.
    Catalog &cat = context.db->GetCatalog();
    for (idx_t oi = 0; oi < oids.size(); oi++) {
        auto *ps = (PropertySchemaCatalogEntry *)cat.GetEntry(
            context, DEFAULT_SCHEMA, oids[oi]);
        // Use scan_projection_mapping[oi] when present, else [0] (single-schema fallback).
        idx_t proj_idx = (oi < scan_projection_mapping.size()) ? oi : 0;
        for (auto eid : ps->extent_ids) {
            auto *ext_it = new ExtentIterator();
            ext_it->InitializeSingleExtent(context, scan_types[0],
                                           scan_projection_mapping[proj_idx], eid);
            gstate->extent_iterators.push(ext_it);
        }
    }

    // Set scan metadata without creating iterators (avoids Pin/UnPin side effects)
    context.graph_storage_wrapper->SetScanMetadata(oids, scan_projection_mapping);

    gstate->initialized = true;
    return gstate;
}

bool PhysicalNodeScan::ParallelSource() const
{
    // Parallelize single-schema scans without filter pushdown.
    // Filter pushdown scans typically return few rows — parallelism not beneficial.
    // WHERE conditions are handled by Filter operator in the pipeline instead.
    if (is_filter_pushdowned) {
        return false;
    }
    return (projection_mapping.size() == 1) && (num_schemas == 1);
}

void PhysicalNodeScan::GetData(ExecutionContext &context, DataChunk &chunk,
                               GlobalSourceState &gstate,
                               LocalSourceState &lstate) const
{
    auto &global_state = (NodeScanGlobalState &)gstate;
    auto &state = (NodeScanState &)lstate;

    // Initialize per-thread filter state on first call
    if (is_filter_pushdowned && !state.filter_buffer_initialized) {
        state.parallel_filter_buffer.Initialize(types);
        if (filter_pushdown_type == FilterPushdownType::FP_EQ) {
            state.local_filter_key_idx = filter_pushdown_key_idxs[0];
            state.local_eq_filter_value = eq_filter_pushdown_values[0];
        } else if (filter_pushdown_type == FilterPushdownType::FP_RANGE) {
            state.local_filter_key_idx = filter_pushdown_key_idxs[0];
            state.local_range_filter_value = range_filter_pushdown_values[0];
        } else {
            state.local_filter_key_idx = filter_pushdown_key_idxs[0];
            state.local_executor = ExpressionExecutor(*(filter_expression.get()));
        }
        state.filter_buffer_initialized = true;
    }

    // Delta phase: this thread already claimed delta scanning — keep emitting
    // delta chunks until exhausted.
    if (state.delta_phase) {
        ScanDeltaPhaseChunk(context, chunk, state);
        return;
    }

    while (true) {
        // If we have a current iterator, try scanning
        if (!state.ext_its.empty()) {
            auto *ext_it = state.ext_its.front();
            ExtentID current_eid;
            bool scan_ongoing;

            if (!is_filter_pushdowned) {
                scan_ongoing = ext_it->GetNextExtent(*context.client, chunk, current_eid);
            } else {
                state.parallel_filter_buffer.Reset(scan_types[0]);
                if (filter_pushdown_type == FilterPushdownType::FP_EQ) {
                    scan_ongoing = ext_it->GetNextExtent(
                        *context.client, chunk, state.parallel_filter_buffer, current_eid,
                        state.local_filter_key_idx, state.local_eq_filter_value,
                        scan_projection_mapping[0], scan_types[0], EXEC_ENGINE_VECTOR_SIZE);
                } else if (filter_pushdown_type == FilterPushdownType::FP_RANGE) {
                    scan_ongoing = ext_it->GetNextExtent(
                        *context.client, chunk, state.parallel_filter_buffer, current_eid,
                        state.local_filter_key_idx,
                        state.local_range_filter_value.l_value,
                        state.local_range_filter_value.r_value,
                        state.local_range_filter_value.l_inclusive,
                        state.local_range_filter_value.r_inclusive,
                        scan_projection_mapping[0], scan_types[0], EXEC_ENGINE_VECTOR_SIZE);
                } else {
                    // FP_COMPLEX
                    scan_ongoing = ext_it->GetNextExtent(
                        *context.client, chunk, state.parallel_filter_buffer, current_eid,
                        state.local_executor, scan_projection_mapping[0], scan_types[0],
                        EXEC_ENGINE_VECTOR_SIZE);
                }
            }

            if (scan_ongoing) {
                // Apply delete mask
                if (chunk.size() > 0) {
                    auto &ds = context.client->db->delta_store;
                    idx_t vid_col = DConstants::INVALID_INDEX;
                    for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
                        if (chunk.data[c].GetType().id() == LogicalTypeId::ID) {
                            vid_col = c; break;
                        }
                    }
                    if (vid_col != DConstants::INVALID_INDEX) {
                        auto *vid_data = (uint64_t *)chunk.data[vid_col].GetData();
                        SelectionVector sel(chunk.size());
                        idx_t count = 0;
                        for (idx_t row = 0; row < chunk.size(); row++) {
                            uint64_t vid = vid_data[row];
                            uint32_t eid = (uint32_t)(vid >> 32);
                            uint32_t off = (uint32_t)(vid & 0xFFFFFFFF);
                            if (ds.GetDeleteMask(eid).IsDeleted(off)) continue;
                            sel.set_index(count++, row);
                        }
                        if (count < chunk.size()) {
                            chunk.Slice(sel, count);
                        }
                    }
                }
                chunk.SetSchemaIdx(0);
                return;
            }
            // Extent done — delete iterator (UnPins buffers) and pop
            delete ext_it;
            state.ext_its.pop();
        }

        // Get the next pre-created ExtentIterator from global state
        auto *next_it = global_state.GetNextIterator();
        if (!next_it) {
            // On-disk extents exhausted. Try to claim the right to read
            // in-memory delta extents — at most one thread succeeds.
            if (context.client->db->delta_store.HasInsertData() &&
                global_state.TryClaimDeltaPhase()) {
                state.delta_phase = true;
                ScanDeltaPhaseChunk(context, chunk, state);
                return;
            }
            state.iter_finished = true;
            return;
        }
        state.ext_its.push(next_it);
    }
}

bool PhysicalNodeScan::IsSourceDataRemaining(GlobalSourceState &gstate,
                                              LocalSourceState &lstate) const
{
    auto &state = (NodeScanState &)lstate;
    return !state.iter_finished;
}

void PhysicalNodeScan::GetData(ExecutionContext &context, DataChunk &chunk,
                               LocalSourceState &lstate) const
{
    auto &state = (NodeScanState &)lstate;
    // If first time here, call doScan and get iterator from iTbgppGraphStorageWrapper
    if (!state.iter_inited) {
        state.iter_inited = true;
        bool enable_filter_buffer =
            projection_mapping.size() ==
            1;  // enable buffering only in non-schemaless

        auto initializeAPIResult = context.client->graph_storage_wrapper->InitializeScan(
            state.ext_its, oids, scan_projection_mapping, scan_types,
            enable_filter_buffer);
        D_ASSERT(initializeAPIResult == StoreAPIResult::OK);
    }
    // Delta scan: emit in-memory extent rows after regular scan
    if (state.delta_phase) {
        ScanDeltaPhaseChunk(context, chunk, state);
        return;
    }
    if (state.ext_its.empty()) {
        state.iter_finished = true;
        return;
    }

    StoreAPIResult res;
    if (!is_filter_pushdowned) {
        // no filter pushdown
        if (projection_mapping.size() == 1) {
            res = context.client->graph_storage_wrapper->doScan(state.ext_its, chunk,
                                                      types);
        }
        else {
            res = context.client->graph_storage_wrapper->doScan(state.ext_its, chunk,
                                                      projection_mapping, types,
                                                      current_schema_idx);
        }
    }
    else {
        /* TODO @jhha - Even if the minmax array does not exist,
		 * if the number of predicate application results is 0, 
		 * continue scanning the next area. 
		 */
        // filter pushdown applied
        filtered_chunk_buffer.Reset(scan_types[current_schema_idx]);
        if (filter_pushdown_type == FilterPushdownType::FP_RANGE) {
            res = context.client->graph_storage_wrapper->doScan(
                state.ext_its, chunk, filtered_chunk_buffer, projection_mapping,
                types, current_schema_idx,
                filter_pushdown_key_idxs[current_schema_idx],
                range_filter_pushdown_values[current_schema_idx]);
        }
        else if (filter_pushdown_type == FilterPushdownType::FP_EQ) {
            res = context.client->graph_storage_wrapper->doScan(
                state.ext_its, chunk, filtered_chunk_buffer, projection_mapping,
                types, current_schema_idx,
                filter_pushdown_key_idxs[current_schema_idx],
                eq_filter_pushdown_values[current_schema_idx]);
        }
        else {
            res = context.client->graph_storage_wrapper->doScan(
                state.ext_its, chunk, filtered_chunk_buffer, projection_mapping,
                types, current_schema_idx, executor);
        }
    }

    if (res == StoreAPIResult::DONE) {
        current_schema_idx++;
        if (state.ext_its.empty()) {
            if (context.client->db->delta_store.HasInsertData())
                state.delta_phase = true;
            else
                state.iter_finished = true;
        }
        return;
    }
    else {
        state.iter_finished = false;
    }

    // Filter out deleted rows (Phase 4: DELETE read merge)
    if (chunk.size() > 0) {
        auto &ds = context.client->db->delta_store;
        // Find ID column (VID) or numeric column (user id) for delete check
        idx_t vid_col = DConstants::INVALID_INDEX;
        idx_t uid_col = DConstants::INVALID_INDEX;
        for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
            auto tid = chunk.data[c].GetType().id();
            if (tid == LogicalTypeId::ID) vid_col = c;
            else if (uid_col == DConstants::INVALID_INDEX &&
                     (tid == LogicalTypeId::UBIGINT || tid == LogicalTypeId::BIGINT))
                uid_col = c;
        }
        if (vid_col != DConstants::INVALID_INDEX) {
            auto *vid_data = (uint64_t *)chunk.data[vid_col].GetData();
            SelectionVector sel(chunk.size());
            idx_t count = 0;
            for (idx_t row = 0; row < chunk.size(); row++) {
                uint64_t vid = vid_data[row];
                uint32_t eid = (uint32_t)(vid >> 32);
                uint32_t off = (uint32_t)(vid & 0xFFFFFFFF);
                if (ds.GetDeleteMask(eid).IsDeleted(off)) continue;
                sel.set_index(count++, row);
            }
            if (count < chunk.size()) {
                chunk.Slice(sel, count);
            }
        }
    }

    // Merge SET property updates by user-id lookup
    if (chunk.size() > 0 && context.client->db->delta_store.HasPropertyUpdates()) {
        auto &ds = context.client->db->delta_store;
        // Find a numeric column that could be the user 'id' property
        idx_t id_col = DConstants::INVALID_INDEX;
        for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
            auto tid = chunk.data[c].GetType().id();
            if (tid == LogicalTypeId::ID || tid == LogicalTypeId::UBIGINT || tid == LogicalTypeId::BIGINT) {
                id_col = c; break;
            }
        }
        if (id_col != DConstants::INVALID_INDEX) {
            auto *id_data = (uint64_t *)chunk.data[id_col].GetData();
            Catalog &cat = context.client->db->GetCatalog();
            for (idx_t row = 0; row < chunk.size(); row++) {
                uint64_t user_id = id_data[row];
                // For ID-type column, extract seqno as possible user_id fallback
                if (chunk.data[id_col].GetType().id() == LogicalTypeId::ID) {
                    // Physical VID — not usable as user_id directly.
                    // Try the next numeric column for user id.
                    bool found_uid = false;
                    for (idx_t c = id_col + 1; c < chunk.ColumnCount(); c++) {
                        auto tid = chunk.data[c].GetType().id();
                        if (tid == LogicalTypeId::UBIGINT || tid == LogicalTypeId::BIGINT) {
                            user_id = ((uint64_t *)chunk.data[c].GetData())[row];
                            found_uid = true; break;
                        }
                    }
                    if (!found_uid) continue;
                }
                auto *updates = ds.GetPropertyByUserId(user_id);
                if (!updates) continue;
                // Apply to output columns by property name
                for (auto oid : oids) {
                    auto *ps = (PropertySchemaCatalogEntry *)cat.GetEntry(*context.client, DEFAULT_SCHEMA, oid);
                    if (!ps) continue;
                    auto *keys = ps->GetKeys();
                    if (!keys) break;
                    for (idx_t col = 0; col < chunk.ColumnCount(); col++) {
                        idx_t ps_idx = (col < scan_projection_mapping[0].size())
                                        ? scan_projection_mapping[0][col] : col;
                        // property_key_names excludes _id (col 0), so adjust index.
                        // Also skip non-property columns (ID type, integer types for 'id').
                        if (ps_idx == 0) continue;  // _id column
                        if (chunk.data[col].GetType().id() == LogicalTypeId::ID) continue;
                        idx_t key_idx = ps_idx - 1;
                        if (key_idx < keys->size()) {
                            auto it = updates->find((*keys)[key_idx]);
                            if (it != updates->end()) {
                                try { chunk.SetValue(col, row, it->second); }
                                catch (...) { /* type mismatch */ }
                            }
                        }
                    }
                    break;
                }
            }
        }
    }

    chunk.SetSchemaIdx(current_schema_idx);
}

bool PhysicalNodeScan::IsSourceDataRemaining(LocalSourceState &lstate) const
{
    auto &state = (NodeScanState &)lstate;
    return !state.iter_finished;
}

void PhysicalNodeScan::ScanDeltaPhaseChunk(ExecutionContext &context,
                                           DataChunk &chunk,
                                           NodeScanState &state) const
{
    auto &ds = context.client->db->delta_store;
    if (state.delta_eids.empty() && state.delta_cur == 0) {
        Catalog &cat = context.client->db->GetCatalog();
        std::set<uint16_t> seen;
        for (auto oid : oids) {
            auto *ps = (PropertySchemaCatalogEntry *)cat.GetEntry(*context.client, DEFAULT_SCHEMA, oid);
            if (seen.insert(ps->pid).second)
                for (auto eid : ds.GetInMemoryExtentIDs(ps->pid))
                    if (auto *b = ds.FindInsertBuffer(eid); b && !b->Empty()) state.delta_eids.push_back(eid);
        }
        if (state.delta_eids.empty()) { state.iter_finished = true; return; }
    }
    while (state.delta_cur < state.delta_eids.size()) {
        auto *buf = ds.FindInsertBuffer(state.delta_eids[state.delta_cur]);
        if (!buf || state.delta_row >= buf->Size()) { state.delta_cur++; state.delta_row = 0; continue; }
        chunk.Reset();
        idx_t n = std::min(buf->Size() - state.delta_row, (idx_t)STANDARD_VECTOR_SIZE);
        uint32_t eid = state.delta_eids[state.delta_cur];
        idx_t out_idx = 0;
        for (idx_t i = 0; i < n; i++) {
            auto &row_vals = buf->GetRow(state.delta_row + i);
            int id_ki = buf->FindKeyIndex("id");
            if (id_ki >= 0 && (idx_t)id_ki < row_vals.size()) {
                uint64_t uid = row_vals[id_ki].GetValue<uint64_t>();
                if (ds.IsDeletedByUserId(uid)) continue;  // skip deleted
            }
            // Filter pushdown: check if this row matches the EQ filter
            if (is_filter_pushdowned && filter_pushdown_type == FilterPushdownType::FP_EQ) {
                if (!eq_filter_pushdown_values.empty()) {
                    auto &filter_val = eq_filter_pushdown_values[0];
                    bool match = false;
                    for (idx_t ki = 0; ki < buf->GetSchemaKeys().size(); ki++) {
                        if (ki < row_vals.size()) {
                            try {
                                if (row_vals[ki] == filter_val) { match = true; break; }
                            } catch (...) {}
                        }
                    }
                    if (!match) continue;  // skip non-matching row
                }
            }
            chunk.SetValue(0, out_idx, Value::UBIGINT(((uint64_t)eid << 32) | (state.delta_row + i)));
            for (idx_t c = 1; c < chunk.ColumnCount(); c++) {
                bool filled = false;
                if (c < scan_projection_mapping[0].size()) {
                    idx_t ps_col = scan_projection_mapping[0][c];
                    if (ps_col > 0) {
                        Catalog &cat = context.client->db->GetCatalog();
                        for (auto oid : oids) {
                            auto *ps = (PropertySchemaCatalogEntry *)cat.GetEntry(
                                *context.client, DEFAULT_SCHEMA, oid);
                            if (!ps) continue;
                            auto *keys = ps->GetKeys();
                            if (keys && ps_col - 1 < keys->size()) {
                                int bi = buf->FindKeyIndex((*keys)[ps_col - 1]);
                                if (bi >= 0 && (idx_t)bi < row_vals.size()) {
                                    try { chunk.SetValue(c, out_idx, row_vals[bi]); filled = true; }
                                    catch (...) {}
                                }
                            }
                            break;
                        }
                    }
                }
                if (!filled) chunk.SetValue(c, out_idx, Value());
                if (filled && id_ki >= 0 && (idx_t)id_ki < row_vals.size() && ds.HasPropertyUpdates()) {
                    uint64_t uid = row_vals[id_ki].GetValue<uint64_t>();
                    auto *upd = ds.GetPropertyByUserId(uid);
                    if (upd && c < scan_projection_mapping[0].size()) {
                        idx_t ps_col = scan_projection_mapping[0][c];
                        if (ps_col > 0) {
                            Catalog &cat2 = context.client->db->GetCatalog();
                            for (auto oid : oids) {
                                auto *ps2 = (PropertySchemaCatalogEntry *)cat2.GetEntry(*context.client, DEFAULT_SCHEMA, oid);
                                if (!ps2) continue;
                                auto *k2 = ps2->GetKeys();
                                if (k2 && ps_col - 1 < k2->size()) {
                                    auto it = upd->find((*k2)[ps_col - 1]);
                                    if (it != upd->end()) {
                                        try { chunk.SetValue(c, out_idx, it->second); } catch (...) {}
                                    }
                                }
                                break;
                            }
                        }
                    }
                }
            }
            out_idx++;
        }
        state.delta_row += n;
        if (out_idx > 0) { chunk.SetCardinality(out_idx); return; }
    }
    state.iter_finished = true;
}

std::string PhysicalNodeScan::ParamsToString() const
{
    string params = "nodescan-params: oids {";
    for (auto i = 0; i < oids.size(); i++) {
        params += std::to_string(oids[i]);
        if (i < oids.size() - 1) {
            params += ", ";
        }
    }
    params += "} ";
    if (filter_expression) {
        params += filter_expression->ToString();
    }
    return params;
}

std::string PhysicalNodeScan::ToString() const
{
    return "NodeScan";
}
}  // namespace duckdb