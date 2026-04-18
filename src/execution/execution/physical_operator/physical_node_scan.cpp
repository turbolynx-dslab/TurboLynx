//===----------------------------------------------------------------------===//
//                         DuckDB
//
// src/execution/execution/physical_operator/physical_node_scan.cpp
//
//
//===----------------------------------------------------------------------===//

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
}
namespace turbolynx {
}
namespace duckdb {
    using namespace turbolynx;
}
namespace turbolynx {
using namespace duckdb;

static bool IsInternalIdType(const LogicalType &type) {
    auto tid = type.id();
    return tid == LogicalTypeId::ID || tid == LogicalTypeId::UBIGINT ||
           tid == LogicalTypeId::BIGINT;
}

static bool IsStrictInternalIdType(const LogicalType &type) {
    return type.id() == LogicalTypeId::ID;
}

static idx_t FindIdColumn(const DataChunk &chunk,
                          const vector<uint64_t> *scan_proj = nullptr) {
    if (scan_proj) {
        for (idx_t c = 0; c < chunk.ColumnCount() && c < scan_proj->size(); c++) {
            if ((*scan_proj)[c] == 0 &&
                IsInternalIdType(chunk.data[c].GetType())) {
                return c;
            }
        }
    }
    for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
        if (IsStrictInternalIdType(chunk.data[c].GetType())) {
            return c;
        }
    }
    return DConstants::INVALID_INDEX;
}

static void TranslatePhysicalIdsToLogical(ExecutionContext &context,
                                          DataChunk &chunk,
                                          const vector<uint64_t> *scan_proj = nullptr) {
    auto vid_col = FindIdColumn(chunk, scan_proj);
    if (vid_col == DConstants::INVALID_INDEX) {
        return;
    }
    auto &ds = context.client->db->delta_store;
    auto type_id = chunk.data[vid_col].GetType().id();
    if (type_id == LogicalTypeId::BIGINT) {
        for (idx_t row = 0; row < chunk.size(); row++) {
            auto logical_id =
                ds.ResolveLogicalId((uint64_t)chunk.data[vid_col].GetValue(row)
                                        .GetValue<int64_t>());
            chunk.SetValue(vid_col, row, Value::BIGINT((int64_t)logical_id));
        }
        return;
    }
    for (idx_t row = 0; row < chunk.size(); row++) {
        auto logical_id =
            ds.ResolveLogicalId(chunk.data[vid_col].GetValue(row)
                                    .GetValue<uint64_t>());
        if (type_id == LogicalTypeId::ID) {
            chunk.SetValue(vid_col, row, Value::ID(logical_id));
        } else {
            chunk.SetValue(vid_col, row, Value::UBIGINT(logical_id));
        }
    }
}

static void FilterDeletedRows(DeltaStore &ds, DataChunk &chunk,
                              const vector<uint64_t> *scan_proj = nullptr,
                              const ExtentIterator *ext_it = nullptr) {
    if (chunk.size() == 0) {
        return;
    }

    SelectionVector sel(chunk.size());
    idx_t count = 0;
    auto vid_col = FindIdColumn(chunk, scan_proj);
    if (vid_col != DConstants::INVALID_INDEX) {
        for (idx_t row = 0; row < chunk.size(); row++) {
            auto value = chunk.data[vid_col].GetValue(row);
            if (value.IsNull()) {
                continue;
            }
            uint64_t vid = value.GetValue<uint64_t>();
            uint32_t eid = (uint32_t)(vid >> 32);
            uint32_t off = (uint32_t)(vid & 0xFFFFFFFFull);
            if (ds.IsDeletedInMask(eid, off) || ds.IsLogicalIdDeleted(vid)) {
                continue;
            }
            sel.set_index(count++, row);
        }
    } else if (ext_it) {
        auto &row_offsets = ext_it->GetLastOutputRowOffsets();
        auto extent_id = ext_it->GetLastOutputExtentID();
        if (row_offsets.size() != chunk.size() ||
            extent_id == std::numeric_limits<uint32_t>::max()) {
            return;
        }
        for (idx_t row = 0; row < chunk.size(); row++) {
            auto pid = MakePhysicalId(extent_id, (uint32_t)row_offsets[row]);
            if (ds.IsDeletedInMask(extent_id, row_offsets[row]) ||
                ds.IsLogicalIdDeleted(pid)) {
                continue;
            }
            sel.set_index(count++, row);
        }
    } else {
        return;
    }

    if (count < chunk.size()) {
        chunk.Slice(sel, count);
    }
}

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
            ext_it->disableFilterBuffering();
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
    // Filter-pushdown parallel NodeScan is enabled. Three issues had to be
    // resolved before this gate could be removed:
    //  1. output_column_idxs mapping mismatch and FP_COMPLEX init OOB in the
    //     parallel filter-pushdown GetData path.
    //  2. ChunkCacheManager::PinSegment race that let a second thread observe
    //     a half-loaded segment (TPC-H Q10 SIGSEGV in string_t::VerifyNull).
    //  3. PipelineTask initialised intermediate chunks with raw
    //     DataChunk::Initialize(types), which asserted on operators that emit
    //     empty-type chunks (e.g. PhysicalIdSeek for EXISTS-decorrelated
    //     subqueries). PipelineTask now uses the operator's
    //     InitializeOutputChunks override, mirroring the sequential path.
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
            // FP_COMPLEX: no filter_pushdown_key_idxs is populated; only the
            // ExpressionExecutor is needed.
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
                // Match the sequential wrapper->doScan path: it passes
                // operator-output `projection_mapping` (not storage
                // `scan_projection_mapping`) as the output_column_idxs to
                // ExtentIterator::GetNextExtent's filter overloads.
                state.parallel_filter_buffer.Reset(scan_types[0]);
                auto &output_proj = projection_mapping[0];
                if (filter_pushdown_type == FilterPushdownType::FP_EQ) {
                    scan_ongoing = ext_it->GetNextExtent(
                        *context.client, chunk, state.parallel_filter_buffer, current_eid,
                        state.local_filter_key_idx, state.local_eq_filter_value,
                        output_proj, scan_types[0], EXEC_ENGINE_VECTOR_SIZE);
                } else if (filter_pushdown_type == FilterPushdownType::FP_RANGE) {
                    scan_ongoing = ext_it->GetNextExtent(
                        *context.client, chunk, state.parallel_filter_buffer, current_eid,
                        state.local_filter_key_idx,
                        state.local_range_filter_value.l_value,
                        state.local_range_filter_value.r_value,
                        state.local_range_filter_value.l_inclusive,
                        state.local_range_filter_value.r_inclusive,
                        output_proj, scan_types[0], EXEC_ENGINE_VECTOR_SIZE);
                } else {
                    // FP_COMPLEX
                    scan_ongoing = ext_it->GetNextExtent(
                        *context.client, chunk, state.parallel_filter_buffer, current_eid,
                        state.local_executor, output_proj, scan_types[0],
                        EXEC_ENGINE_VECTOR_SIZE);
                }
            }

            if (scan_ongoing) {
                auto *scan_proj = !scan_projection_mapping.empty()
                                      ? &scan_projection_mapping[0]
                                      : nullptr;
                FilterDeletedRows(context.client->db->delta_store, chunk,
                                  scan_proj, ext_it);
                TranslatePhysicalIdsToLogical(context, chunk, scan_proj);
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
        bool enable_filter_buffer = false;

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

    auto *ext_it = state.ext_its.empty() ? nullptr : state.ext_its.front();
    auto scan_proj_idx =
        (!scan_projection_mapping.empty() &&
         current_schema_idx >= 0 &&
         (idx_t)current_schema_idx < scan_projection_mapping.size())
            ? (idx_t)current_schema_idx
            : 0;
    auto *scan_proj =
        scan_projection_mapping.empty() ? nullptr
                                        : &scan_projection_mapping[scan_proj_idx];
    FilterDeletedRows(context.client->db->delta_store, chunk, scan_proj, ext_it);
    TranslatePhysicalIdsToLogical(context, chunk, scan_proj);

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
    Catalog &cat = context.client->db->GetCatalog();
    auto resolve_delta_mapping =
        [&](const InsertBuffer &buf) -> std::pair<idx_t, PropertySchemaCatalogEntry *> {
        idx_t fallback_idx =
            (!scan_projection_mapping.empty() &&
             current_schema_idx >= 0 &&
             (idx_t)current_schema_idx < scan_projection_mapping.size())
                ? (idx_t)current_schema_idx
                : 0;
        PropertySchemaCatalogEntry *fallback_ps = nullptr;
        if (!oids.empty()) {
            idx_t oid_idx = (fallback_idx < oids.size()) ? fallback_idx : 0;
            fallback_ps = (PropertySchemaCatalogEntry *)cat.GetEntry(
                *context.client, DEFAULT_SCHEMA, oids[oid_idx], true);
        }
        for (idx_t i = 0; i < oids.size(); i++) {
            auto *candidate_ps = (PropertySchemaCatalogEntry *)cat.GetEntry(
                *context.client, DEFAULT_SCHEMA, oids[i], true);
            auto *candidate_keys = candidate_ps ? candidate_ps->GetKeys() : nullptr;
            if (candidate_keys && *candidate_keys == buf.GetSchemaKeys()) {
                return {i, candidate_ps};
            }
        }
        return {fallback_idx, fallback_ps};
    };

    if (state.delta_eids.empty() && state.delta_cur == 0) {
        std::set<uint16_t> seen;
        for (auto oid : oids) {
            auto *ps = (PropertySchemaCatalogEntry *)cat.GetEntry(*context.client, DEFAULT_SCHEMA, oid);
            if (seen.insert(ps->pid).second)
                for (auto eid : ds.GetInMemoryExtentIDs(ps->pid))
                    if (auto *b = ds.FindInsertBuffer(eid); b && !b->Empty()) {
                        auto edge_like =
                            b->FindKeyIndex("_sid") >= 0 && b->FindKeyIndex("_tid") >= 0;
                        if (edge_like) {
                            spdlog::info(
                                "[DeltaEdgeScanInit] oid={} pid={} eid=0x{:08X} keys={} rows={}",
                                oid, ps->pid, (uint32_t)eid,
                                StringUtil::Join(b->GetSchemaKeys(), ","),
                                b->Size());
                        }
                        state.delta_eids.push_back(eid);
                    }
        }
        if (state.delta_eids.empty()) { state.iter_finished = true; return; }
    }
    while (state.delta_cur < state.delta_eids.size()) {
        auto *buf = ds.FindInsertBuffer(state.delta_eids[state.delta_cur]);
        if (!buf || state.delta_row >= buf->Size()) { state.delta_cur++; state.delta_row = 0; continue; }
        auto [mapping_idx, scan_ps] = resolve_delta_mapping(*buf);
        auto edge_like =
            buf->FindKeyIndex("_sid") >= 0 && buf->FindKeyIndex("_tid") >= 0;
        if (edge_like) {
            spdlog::info(
                "[DeltaEdgeScanMap] eid=0x{:08X} mapping_idx={} scan_ps_oid={} scan_ps_name={} keys={} delta_row={} size={}",
                (uint32_t)state.delta_eids[state.delta_cur], mapping_idx,
                scan_ps ? scan_ps->GetOid() : 0,
                scan_ps ? scan_ps->GetName() : "<null>",
                StringUtil::Join(buf->GetSchemaKeys(), ","), state.delta_row,
                buf->Size());
        }
        auto *ps_keys = scan_ps ? scan_ps->GetKeys() : nullptr;
        auto *part_cat = scan_ps
                             ? (PartitionCatalogEntry *)cat.GetEntry(
                                   *context.client, DEFAULT_SCHEMA,
                                   scan_ps->partition_oid, true)
                             : nullptr;
        auto *part_keys =
            part_cat ? part_cat->GetUniversalPropertyKeyNames() : ps_keys;
        auto resolve_scan_key_name = [&](idx_t attr_no) -> string {
            if (attr_no == 0) {
                return "";
            }
            idx_t key_idx = attr_no - 1;
            if (ps_keys && key_idx < ps_keys->size()) {
                return (*ps_keys)[key_idx];
            }
            if (part_keys && key_idx < part_keys->size()) {
                return (*part_keys)[key_idx];
            }
            return "";
        };
        chunk.Reset();
        idx_t n = std::min(buf->Size() - state.delta_row, (idx_t)STANDARD_VECTOR_SIZE);
        auto id_col = FindIdColumn(chunk);
        idx_t out_idx = 0;
        for (idx_t i = 0; i < n; i++) {
            auto row_idx = state.delta_row + i;
            if (!buf->IsValid(row_idx)) {
                continue;
            }
            auto &row_vals = buf->GetRow(row_idx);

            if (is_filter_pushdowned) {
                bool match = true;
                if (filter_pushdown_type == FilterPushdownType::FP_EQ &&
                    !eq_filter_pushdown_values.empty()) {
                    auto &filter_val = eq_filter_pushdown_values[0];
                    match = false;
                    bool compared_specific_key = false;
                    if (ps_keys && !filter_pushdown_key_idxs.empty()) {
                        int64_t filter_key_idx =
                            (mapping_idx < filter_pushdown_key_idxs.size())
                                ? filter_pushdown_key_idxs[mapping_idx]
                                : filter_pushdown_key_idxs[0];
                        auto filter_key_name =
                            resolve_scan_key_name((idx_t)filter_key_idx);
                        if (!filter_key_name.empty()) {
                            int bi = buf->FindKeyIndex(filter_key_name);
                            if (bi >= 0 && (idx_t)bi < row_vals.size()) {
                                compared_specific_key = true;
                                try {
                                    match = (row_vals[bi] == filter_val);
                                } catch (...) {
                                    match = false;
                                }
                            }
                        }
                    }
                    if (!match && !compared_specific_key) {
                        for (idx_t ki = 0; ki < buf->GetSchemaKeys().size(); ki++) {
                            if (ki >= row_vals.size()) {
                                continue;
                            }
                            try {
                                if (row_vals[ki] == filter_val) {
                                    match = true;
                                    break;
                                }
                            } catch (...) {
                            }
                        }
                    }
                } else if (filter_pushdown_type == FilterPushdownType::FP_RANGE &&
                           !range_filter_pushdown_values.empty()) {
                    auto &filter_val =
                        (mapping_idx < range_filter_pushdown_values.size())
                            ? range_filter_pushdown_values[mapping_idx]
                            : range_filter_pushdown_values[0];
                    match = false;
                    if (ps_keys && !filter_pushdown_key_idxs.empty()) {
                        int64_t filter_key_idx =
                            (mapping_idx < filter_pushdown_key_idxs.size())
                                ? filter_pushdown_key_idxs[mapping_idx]
                                : filter_pushdown_key_idxs[0];
                        auto filter_key_name =
                            resolve_scan_key_name((idx_t)filter_key_idx);
                        if (!filter_key_name.empty()) {
                            int bi = buf->FindKeyIndex(filter_key_name);
                            if (bi >= 0 && (idx_t)bi < row_vals.size()) {
                                auto value = row_vals[bi];
                                bool lower_ok = filter_val.l_inclusive
                                                    ? filter_val.l_value <= value
                                                    : filter_val.l_value < value;
                                bool upper_ok = filter_val.r_inclusive
                                                    ? value <= filter_val.r_value
                                                    : value < filter_val.r_value;
                                match = lower_ok && upper_ok;
                            }
                        }
                    }
                }
                if (!match) {
                    continue;
                }
            }

            auto logical_id = buf->GetLogicalId(row_idx);
            auto &scan_proj =
                (mapping_idx < scan_projection_mapping.size())
                    ? scan_projection_mapping[mapping_idx]
                    : scan_projection_mapping[0];
            auto &out_proj =
                (mapping_idx < projection_mapping.size())
                    ? projection_mapping[mapping_idx]
                    : projection_mapping[0];
            std::vector<bool> assigned(chunk.ColumnCount(), false);
            idx_t mapped_cols = std::min(scan_proj.size(), out_proj.size());
            for (idx_t c = 0; c < mapped_cols; c++) {
                auto out_col = out_proj[c];
                if (out_col == std::numeric_limits<uint64_t>::max() ||
                    out_col >= chunk.ColumnCount()) {
                    continue;
                }

                idx_t ps_col = scan_proj[c];
                if (ps_col == 0 ||
                    chunk.data[out_col].GetType().id() == LogicalTypeId::ID) {
                    chunk.data[out_col].SetIsValid(true);
                    chunk.SetValue(out_col, out_idx, Value::ID(logical_id));
                    assigned[out_col] = true;
                    continue;
                }
                if (ps_col == std::numeric_limits<uint64_t>::max() || ps_col == 0) {
                    chunk.SetValue(out_col, out_idx, Value());
                    assigned[out_col] = true;
                    continue;
                }

                auto key_name = resolve_scan_key_name(ps_col);
                int bi = key_name.empty() ? -1 : buf->FindKeyIndex(key_name);
                if (bi >= 0 && (idx_t)bi < row_vals.size()) {
                    try {
                        chunk.data[out_col].SetIsValid(true);
                        if (key_name == "tlSchemaProp") {
                            spdlog::info(
                                "[DeltaScanSetPre] key={} out_col={} row={} value={} source_type={} source_null={} vec_type={} type={}",
                                key_name, out_col, out_idx,
                                row_vals[bi].ToString(),
                                row_vals[bi].type().ToString(),
                                row_vals[bi].IsNull(),
                                (int)chunk.data[out_col].GetVectorType(),
                                chunk.data[out_col].GetType().ToString());
                        }
                        chunk.SetValue(out_col, out_idx, row_vals[bi]);
                        if (key_name == "tlSchemaProp") {
                            auto row_valid =
                                duckdb::FlatVector::Validity(chunk.data[out_col])
                                    .RowIsValid(out_idx);
                            auto raw_value =
                                duckdb::FlatVector::GetData<string_t>(
                                    chunk.data[out_col])[out_idx];
                            auto raw_string = raw_value.GetString();
                            auto vec_cell = chunk.data[out_col].GetValue(out_idx);
                            auto vec_value = vec_cell.ToString();
                            spdlog::info(
                                "[DeltaScanSet] key={} out_col={} row={} value={} row_valid={} raw_size={} raw_string={} vec_is_null={} vec_type_name={} vec_value={} vec_type={}",
                                key_name, out_col, out_idx,
                                row_vals[bi].ToString(),
                                row_valid,
                                raw_value.GetSize(),
                                raw_string,
                                vec_cell.IsNull(),
                                vec_cell.type().ToString(),
                                vec_value,
                                (int)chunk.data[out_col].GetVectorType());
                        }
                        assigned[out_col] = true;
                    } catch (const std::exception &ex) {
                        if (key_name == "tlSchemaProp") {
                            spdlog::info(
                                "[DeltaScanSet] key={} out_col={} row={} value={} status=exception what={}",
                                key_name, out_col, out_idx,
                                row_vals[bi].ToString(), ex.what());
                        }
                    } catch (...) {
                        if (key_name == "tlSchemaProp") {
                            spdlog::info(
                                "[DeltaScanSet] key={} out_col={} row={} value={} status=unknown_exception",
                                key_name, out_col, out_idx,
                                row_vals[bi].ToString());
                        }
                    }
                } else {
                    chunk.SetValue(out_col, out_idx, Value());
                    assigned[out_col] = true;
                }
            }
            for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
                if (!assigned[c]) {
                    chunk.SetValue(c, out_idx, Value());
                }
            }
            if (edge_like) {
                spdlog::info(
                    "[DeltaEdgeScanEmit] logical_id=0x{:016X} row_idx={} out_idx={} chunk_cols={} mapped_cols={}",
                    logical_id, row_idx, out_idx, chunk.ColumnCount(),
                    mapped_cols);
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
} // namespace turbolynx