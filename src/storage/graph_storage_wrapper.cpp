#include <algorithm>
#include <cassert>
#include <limits>
#include <numeric>
#include <set>
#include <vector>

#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/list.hpp"
#include "common/boost_typedefs.hpp"
#include "common/vector_size.hpp"
#include "storage/extent/adjlist_iterator.hpp"
#include "storage/extent/extent_iterator.hpp"
#include "main/client_context.hpp"
#include "main/database.hpp"
#include "storage/graph_storage_wrapper.hpp"
#include "common/typedef.hpp"
#include "common/constants.hpp"

#include "icecream.hpp"

namespace duckdb {
}
namespace turbolynx {
}
namespace duckdb {
    using namespace turbolynx;
}
namespace turbolynx {
using namespace duckdb;

static bool BufferMatchesPropertySchema(const InsertBuffer &buf,
                                        PropertySchemaCatalogEntry &ps) {
    auto *keys = ps.GetKeys();
    return keys && *keys == buf.GetSchemaKeys();
}

static PropertySchemaCatalogEntry *GetSeekPropertySchema(duckdb::ClientContext &client,
                                                         const vector<uint64_t> &oids,
                                                         idx_t mapping_idx) {
    if (mapping_idx >= oids.size()) {
        return nullptr;
    }
    return (PropertySchemaCatalogEntry *)client.db->GetCatalog().GetEntry(
        client, DEFAULT_SCHEMA, oids[mapping_idx], true);
}

static bool FillInMemorySeekOutput(duckdb::ClientContext &client,
                                   const vector<uint64_t> &seek_oids,
                                   const vector<vector<uint64_t>> &seek_scan_projection,
                                   DataChunk &output, DataChunk &input,
                                   idx_t nodeColIdx, ExtentID target_eid,
                                   vector<uint32_t> &target_seqnos,
                                   const vector<uint32_t> &output_col_idx,
                                   vector<idx_t> &cols_to_include,
                                   idx_t mapping_idx) {
    auto &ds = client.db->delta_store;
    auto *buf = ds.FindInsertBuffer(target_eid);
    auto *ps = GetSeekPropertySchema(client, seek_oids, mapping_idx);
    if (!buf || !ps) {
        return false;
    }

    idx_t projection_idx =
        (mapping_idx < seek_scan_projection.size()) ? mapping_idx : 0;
    if (projection_idx >= seek_scan_projection.size()) {
        return false;
    }
    auto *ps_keys = ps->GetKeys();
    if (!ps_keys) {
        return false;
    }

    auto *pid_data = (uint64_t *)input.data[nodeColIdx].GetData();
    for (auto target_seqno : target_seqnos) {
        auto pid = pid_data[target_seqno];
        idx_t row_idx = pid & 0x00000000FFFFFFFFull;
        if (!buf->IsValid(row_idx)) {
            continue;
        }
        const auto &row_vals = buf->GetRow(row_idx);
        for (idx_t src_col = 0; src_col < output_col_idx.size(); src_col++) {
            auto out_col = output_col_idx[src_col];
            if (std::find(cols_to_include.begin(), cols_to_include.end(), out_col) ==
                cols_to_include.end()) {
                continue;
            }
            auto scan_col =
                (src_col < seek_scan_projection[projection_idx].size())
                    ? seek_scan_projection[projection_idx][src_col]
                    : std::numeric_limits<uint64_t>::max();
            if (output.data[out_col].GetType().id() == LogicalTypeId::ID ||
                scan_col == 0) {
                output.SetValue(out_col, target_seqno,
                                Value::ID(buf->GetLogicalId(row_idx)));
                continue;
            }
            if (scan_col == std::numeric_limits<uint64_t>::max() ||
                scan_col == 0 || scan_col - 1 >= ps_keys->size()) {
                output.SetValue(out_col, target_seqno, Value());
                continue;
            }
            int buf_col = buf->FindKeyIndex((*ps_keys)[scan_col - 1]);
            if (buf_col >= 0 && (idx_t)buf_col < row_vals.size()) {
                output.SetValue(out_col, target_seqno, row_vals[buf_col]);
            } else {
                output.SetValue(out_col, target_seqno, Value());
            }
        }
    }
    return true;
}

static void TranslateBaseSeekOutputIds(duckdb::ClientContext &client, DataChunk &output,
                                       const vector<uint32_t> &target_seqnos,
                                       const vector<uint32_t> &output_col_idx) {
    auto &ds = client.db->delta_store;
    for (auto out_col : output_col_idx) {
        if (out_col >= output.ColumnCount() ||
            output.data[out_col].GetType().id() != LogicalTypeId::ID) {
            continue;
        }
        auto *id_data = (uint64_t *)output.data[out_col].GetData();
        for (auto seqno : target_seqnos) {
            id_data[seqno] = ds.ResolveLogicalId(id_data[seqno]);
        }
    }
}

static void TranslateBaseSeekOutputIdColumn(duckdb::ClientContext &client,
                                            DataChunk &output,
                                            const vector<uint32_t> &target_seqnos,
                                            idx_t out_col_idx) {
    if (out_col_idx < 0 || (idx_t)output.ColumnCount() <= out_col_idx ||
        output.data[out_col_idx].GetType().id() != LogicalTypeId::ID) {
        return;
    }

    auto &ds = client.db->delta_store;
    auto *id_data = (uint64_t *)output.data[out_col_idx].GetData();
    for (auto seqno : target_seqnos) {
        id_data[seqno] = ds.ResolveLogicalId(id_data[seqno]);
    }
}

static uint16_t ResolveAdjDeltaPartitionId(duckdb::ClientContext &client, int adjColIdx,
                                           ExpandDirection expand_dir,
                                           uint16_t vertex_part_id) {
    auto &catalog = client.db->GetCatalog();
    auto *gcat = (GraphCatalogEntry *)catalog.GetEntry(
        client, duckdb::CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA,
        DEFAULT_GRAPH, true);
    if (!gcat) {
        return std::numeric_limits<uint16_t>::max();
    }

    for (auto ep_oid : *gcat->GetEdgePartitionOids()) {
        auto *ep = (PartitionCatalogEntry *)catalog.GetEntry(
            client, DEFAULT_SCHEMA, ep_oid, true);
        if (!ep) {
            continue;
        }
        auto *src_part = (PartitionCatalogEntry *)catalog.GetEntry(
            client, DEFAULT_SCHEMA, ep->GetSrcPartOid(), true);
        auto *dst_part = (PartitionCatalogEntry *)catalog.GetEntry(
            client, DEFAULT_SCHEMA, ep->GetDstPartOid(), true);
        auto *idx_ids = ep->GetAdjIndexOidVec();
        if (!idx_ids) {
            continue;
        }

        for (auto idx_oid : *idx_ids) {
            auto *idx_cat = (IndexCatalogEntry *)catalog.GetEntry(
                client, DEFAULT_SCHEMA, idx_oid, true);
            if (!idx_cat || idx_cat->GetAdjColIdx() != (idx_t)adjColIdx) {
                continue;
            }
            bool matches_dir =
                (expand_dir == ExpandDirection::OUTGOING &&
                 idx_cat->GetIndexType() == IndexType::FORWARD_CSR) ||
                (expand_dir == ExpandDirection::INCOMING &&
                 idx_cat->GetIndexType() == IndexType::BACKWARD_CSR);
            if (!matches_dir) {
                continue;
            }
            if (expand_dir == ExpandDirection::OUTGOING &&
                !(src_part && src_part->GetPartitionID() == vertex_part_id)) {
                continue;
            }
            if (expand_dir == ExpandDirection::INCOMING &&
                !(dst_part && dst_part->GetPartitionID() == vertex_part_id)) {
                continue;
            }
            return ep->GetPartitionID();
        }
    }

    return std::numeric_limits<uint16_t>::max();
}

IndexSeekScratch::IndexSeekScratch()
    : target_eid_flags(INITIAL_EXTENT_ID_SPACE),
      target_seqnos_per_extent_map(INITIAL_EXTENT_ID_SPACE,
                                   vector<uint32_t>(STANDARD_VECTOR_SIZE)),
      boundary_position(STANDARD_VECTOR_SIZE),
      tmp_vec(STANDARD_VECTOR_SIZE),
      target_seqnos_per_extent_map_cursors(INITIAL_EXTENT_ID_SPACE, 0)
{}

iTbgppGraphStorageWrapper::iTbgppGraphStorageWrapper(duckdb::ClientContext &client)
    : client(client)
{}

StoreAPIResult iTbgppGraphStorageWrapper::InitializeScan(
    std::queue<ExtentIterator *> &ext_its, vector<idx_t> &oids,
    vector<vector<uint64_t>> &projection_mapping,
    vector<vector<duckdb::LogicalType>> &scanSchemas,
    bool enable_filter_buffering)
{
    Catalog &cat_instance = client.db->GetCatalog();
    D_ASSERT(oids.size() == projection_mapping.size());
    last_scan_oids_ = oids;
    last_scan_projection_ = projection_mapping;

    vector<vector<idx_t>> column_idxs;
    for (idx_t i = 0; i < oids.size(); i++) {
        PropertySchemaCatalogEntry
            *ps_cat_entry =  // TODO get this in compilation process?
            (PropertySchemaCatalogEntry *)cat_instance.GetEntry(
                client, DEFAULT_SCHEMA, oids[i]);

        auto ext_it = new ExtentIterator();
        ext_it->Initialize(client, ps_cat_entry, scanSchemas[i],
                           projection_mapping[i]);
        if (enable_filter_buffering)
            ext_it->enableFilterBuffering();
        else
            ext_it->disableFilterBuffering();
        ext_its.push(ext_it);
    }

    return StoreAPIResult::OK;
}

StoreAPIResult iTbgppGraphStorageWrapper::InitializeScan(
    std::queue<ExtentIterator *> &ext_its, PropertySchemaID_vector *oids,
    vector<vector<uint64_t>> &projection_mapping,
    vector<vector<duckdb::LogicalType>> &scanSchemas,
    bool enable_filter_buffering)
{
    Catalog &cat_instance = client.db->GetCatalog();
    D_ASSERT(oids->size() == projection_mapping.size());

    vector<vector<idx_t>> column_idxs;
    for (idx_t i = 0; i < oids->size(); i++) {
        PropertySchemaCatalogEntry
            *ps_cat_entry =  // TODO get this in compilation process?
            (PropertySchemaCatalogEntry *)cat_instance.GetEntry(
                client, DEFAULT_SCHEMA, oids->at(i));

        auto ext_it = new ExtentIterator();
        ext_it->Initialize(client, ps_cat_entry, scanSchemas[i],
                           projection_mapping[i]);
        if (enable_filter_buffering)
            ext_it->enableFilterBuffering();
        else
            ext_it->disableFilterBuffering();
        ext_its.push(ext_it);
    }

    return StoreAPIResult::OK;
}

/**
 * Scan without filter pushdown
*/
StoreAPIResult iTbgppGraphStorageWrapper::doScan(
    std::queue<ExtentIterator *> &ext_its, duckdb::DataChunk &output,
    std::vector<duckdb::LogicalType> &scanSchema)
{
    ExtentID current_eid;
    auto ext_it = ext_its.front();
    bool scan_ongoing = ext_it->GetNextExtent(client, output, current_eid);

    if (scan_ongoing) {
        return StoreAPIResult::OK;
    }
    else {
        ext_its.pop();
        delete ext_it;
        return StoreAPIResult::DONE;
    }
}

StoreAPIResult iTbgppGraphStorageWrapper::doScan(
    std::queue<ExtentIterator *> &ext_its, duckdb::DataChunk &output,
    vector<vector<uint64_t>> &projection_mapping,
    std::vector<duckdb::LogicalType> &scanSchema, int64_t current_schema_idx,
    bool is_output_initialized)
{
    ExtentID current_eid;
    auto ext_it = ext_its.front();
    bool scan_ongoing = ext_it->GetNextExtent(
        client, output, current_eid, projection_mapping[current_schema_idx],
        EXEC_ENGINE_VECTOR_SIZE, is_output_initialized);
    if (scan_ongoing) {
        return StoreAPIResult::OK;
    }
    else {
        ext_its.pop();
        delete ext_it;
        return StoreAPIResult::DONE;
    }
}

/**
 * Scan with filter pushdown
*/

// equality filter
StoreAPIResult iTbgppGraphStorageWrapper::doScan(
    std::queue<ExtentIterator *> &ext_its, duckdb::DataChunk &output,
    FilteredChunkBuffer &output_buffer,
    vector<vector<uint64_t>> &projection_mapping,
    std::vector<duckdb::LogicalType> &scanSchema, int64_t current_schema_idx,
    int64_t &filterKeyColIdx, duckdb::Value &filterValue)
{
    ExtentID current_eid;
    auto ext_it = ext_its.front();
    bool scan_ongoing = ext_it->GetNextExtent(
        client, output, output_buffer, current_eid, filterKeyColIdx,
        filterValue, projection_mapping[current_schema_idx], scanSchema,
        EXEC_ENGINE_VECTOR_SIZE);
    if (scan_ongoing) {
        return StoreAPIResult::OK;
    }
    else {
        ext_its.pop();
        delete ext_it;
        return StoreAPIResult::DONE;
    }
}

// range filter
StoreAPIResult iTbgppGraphStorageWrapper::doScan(
    std::queue<ExtentIterator *> &ext_its, duckdb::DataChunk &output,
    FilteredChunkBuffer &output_buffer,
    vector<vector<uint64_t>> &projection_mapping,
    std::vector<duckdb::LogicalType> &scanSchema, int64_t current_schema_idx,
    int64_t &filterKeyColIdx, duckdb::RangeFilterValue &rangeFilterValue)
{
    ExtentID current_eid;
    auto ext_it = ext_its.front();
    bool scan_ongoing = ext_it->GetNextExtent(
        client, output, output_buffer, current_eid, filterKeyColIdx,
        rangeFilterValue.l_value, rangeFilterValue.r_value,
        rangeFilterValue.l_inclusive, rangeFilterValue.r_inclusive,
        projection_mapping[current_schema_idx], scanSchema,
        EXEC_ENGINE_VECTOR_SIZE);
    if (scan_ongoing) {
        return StoreAPIResult::OK;
    }
    else {
        /* Move to next ExtentIterator means the scan for the schema has finished */
        ext_its.pop();
        delete ext_it;
        return StoreAPIResult::DONE;
    }
}

// complex filter
StoreAPIResult iTbgppGraphStorageWrapper::doScan(
    std::queue<ExtentIterator *> &ext_its, duckdb::DataChunk &output,
    FilteredChunkBuffer &output_buffer,
    vector<vector<uint64_t>> &projection_mapping,
    std::vector<duckdb::LogicalType> &scanSchema, int64_t current_schema_idx,
    ExpressionExecutor &executor)
{
    ExtentID current_eid;
    auto ext_it = ext_its.front();
    bool scan_ongoing =
        ext_it->GetNextExtent(client, output, output_buffer, current_eid,
                              executor, projection_mapping[current_schema_idx],
                              scanSchema, EXEC_ENGINE_VECTOR_SIZE);
    if (scan_ongoing) {
        return StoreAPIResult::OK;
    }
    else {
        /* Move to next ExtentIterator means the scan for the schema has finished */
        ext_its.pop();
        delete ext_it;
        return StoreAPIResult::DONE;
    }
}

inline void iTbgppGraphStorageWrapper::_fillTargetSeqnosVecAndBoundaryPosition(
    IndexSeekScratch &scratch, idx_t i, ExtentID prev_eid)
{
    auto prev_eid_seqno = GET_EXTENT_SEQNO_FROM_EID(prev_eid);
    while (prev_eid_seqno >= scratch.target_seqnos_per_extent_map.size()) {
        scratch.target_seqnos_per_extent_map.resize(
            scratch.target_seqnos_per_extent_map.size() * 2,
            vector<uint32_t>(STANDARD_VECTOR_SIZE));
        scratch.target_seqnos_per_extent_map_cursors.resize(
            scratch.target_seqnos_per_extent_map_cursors.size() * 2, 0);
    }
    vector<uint32_t> &vec = scratch.target_seqnos_per_extent_map[prev_eid_seqno];
    idx_t &cursor = scratch.target_seqnos_per_extent_map_cursors[prev_eid_seqno];
    for (auto j = 0; j < scratch.tmp_vec_cursor; j++) {
        vec[cursor++] = scratch.tmp_vec[j];
    }
    scratch.boundary_position[scratch.boundary_position_cursor++] = i - 1;
    scratch.tmp_vec_cursor = 0;
}

StoreAPIResult iTbgppGraphStorageWrapper::InitializeVertexIndexSeek(
    ExtentIterator *&ext_it, DataChunk &input, idx_t nodeColIdx,
    vector<ExtentID> &target_eids, vector<vector<uint32_t>> &target_seqnos_per_extent,
    vector<idx_t> &mapping_idxs, vector<idx_t> &null_tuples_idx,
    vector<idx_t> &eid_to_mapping_idx, IOCache *io_cache,
    IndexSeekScratch &scratch)
{
    ExtentID prev_eid = std::numeric_limits<ExtentID>::max();
    Vector &src_vid_column_vector = input.data[nodeColIdx];
    vector<ExtentID> pruned_eids;
    scratch.target_eid_flags.reset();
    scratch.seen_eids.clear();

    // Cursor initialization
    for (auto i = 0; i < scratch.target_seqnos_per_extent_map_cursors.size(); i++) {
        scratch.target_seqnos_per_extent_map_cursors[i] = 0;
    }
    scratch.boundary_position_cursor = 0;
    scratch.tmp_vec_cursor = 0;
    target_eids.clear();
    mapping_idxs.clear();
    target_seqnos_per_extent.clear();
    scratch.base_target_eids.clear();
    scratch.base_mapping_idxs.clear();
    auto &validity = src_vid_column_vector.GetValidity();
    if (validity.CheckAllInValid()) {
        return StoreAPIResult::OK;
    }
    for (size_t i = 0; i < input.size(); i++) {
        auto vid_val = src_vid_column_vector.GetValue(i);
        if (vid_val.IsNull()) {
            null_tuples_idx.push_back(i);
            continue;
        }
        uint64_t vid = vid_val.GetValue<uint64_t>();
        if (vid == 0) {
            null_tuples_idx.push_back(i);
            continue;
        }
        ExtentID target_eid = GET_EID_FROM_PHYSICAL_ID(vid);
        if (prev_eid == std::numeric_limits<ExtentID>::max()) {
            prev_eid = target_eid;
        }
        if (prev_eid != target_eid) {
            auto ext_seqno = GET_EXTENT_SEQNO_FROM_EID(prev_eid);
            scratch.target_eid_flags.set(ext_seqno, true);
            scratch.seen_eids.insert(prev_eid);
            _fillTargetSeqnosVecAndBoundaryPosition(scratch, i, prev_eid);
        }
        scratch.tmp_vec[scratch.tmp_vec_cursor++] = i;
        prev_eid = target_eid;
    }

    // process remaining
    if (scratch.tmp_vec_cursor > 0) {
        auto ext_seqno = GET_EXTENT_SEQNO_FROM_EID(prev_eid);
        scratch.target_eid_flags.set(ext_seqno, true);
        scratch.seen_eids.insert(prev_eid);
        _fillTargetSeqnosVecAndBoundaryPosition(scratch, input.size(), prev_eid);
    }

    // Filter extents by eid_to_mapping_idx: only keep those the IdSeek can handle.
    // Use seen_eids (full EIDs) to correctly handle multi-partition VIDs
    // where different partitions may share the same extent seqno.
    for (auto eid : scratch.seen_eids) {
        if (eid < eid_to_mapping_idx.size() && eid_to_mapping_idx[eid] != (idx_t)-1) {
            target_eids.push_back(eid);
        }
        else {
            pruned_eids.push_back(eid);
        }
    }

    bool is_multi_schema = false;
    mapping_idxs.reserve(target_eids.size());
    for (auto i = 0; i < target_eids.size(); i++) {
        // M28: Use full EID for mapping lookup
        idx_t mapping_idx = eid_to_mapping_idx[target_eids[i]];
        D_ASSERT(mapping_idx != (idx_t)-1);
        mapping_idxs.push_back(mapping_idx);
        if (mapping_idx != mapping_idxs[0])
            is_multi_schema = true;
        auto eid_seqno = GET_EXTENT_SEQNO_FROM_EID(target_eids[i]);
        auto &vec = scratch.target_seqnos_per_extent_map[eid_seqno];
        auto cursor = scratch.target_seqnos_per_extent_map_cursors[eid_seqno];
        target_seqnos_per_extent.push_back({vec.begin(), vec.begin() + cursor});
    }

    // TODO maybe we don't need this..
    if (is_multi_schema) {
        vector<idx_t> order(mapping_idxs.size());
        std::iota(order.begin(), order.end(), 0);
        std::sort(order.begin(), order.end(),
                  [&](idx_t lhs, idx_t rhs) {
                      return mapping_idxs[lhs] < mapping_idxs[rhs];
                  });

        vector<idx_t> sorted_mapping_idxs;
        vector<ExtentID> sorted_target_eids;
        vector<vector<uint32_t>> sorted_target_seqnos_per_extent;
        sorted_mapping_idxs.reserve(mapping_idxs.size());
        sorted_target_eids.reserve(target_eids.size());
        sorted_target_seqnos_per_extent.reserve(target_seqnos_per_extent.size());

        for (auto idx : order) {
            sorted_mapping_idxs.push_back(mapping_idxs[idx]);
            sorted_target_eids.push_back(target_eids[idx]);
            sorted_target_seqnos_per_extent.push_back(
                std::move(target_seqnos_per_extent[idx]));
        }

        mapping_idxs = std::move(sorted_mapping_idxs);
        target_eids = std::move(sorted_target_eids);
        target_seqnos_per_extent = std::move(sorted_target_seqnos_per_extent);
    }

    // Append vector for the pruned eids (this will used in Seek for nullify)
    for (auto i = 0; i < pruned_eids.size(); i++) {
        auto ext_seqno = GET_EXTENT_SEQNO_FROM_EID(pruned_eids[i]);
        auto &vec = scratch.target_seqnos_per_extent_map[ext_seqno];
        auto cursor = scratch.target_seqnos_per_extent_map_cursors[ext_seqno];
        target_seqnos_per_extent.push_back({vec.begin(), vec.begin() + cursor});
    }

    for (idx_t i = 0; i < target_eids.size(); i++) {
        if (IsInMemoryExtent(target_eids[i])) {
            continue;
        }
        scratch.base_target_eids.push_back(target_eids[i]);
        scratch.base_mapping_idxs.push_back(mapping_idxs[i]);
    }

    if (!scratch.base_target_eids.empty()) {
        ext_it->Initialize(client, &scratch.base_mapping_idxs,
                           scratch.base_target_eids);
    }

    return StoreAPIResult::OK;
}

StoreAPIResult iTbgppGraphStorageWrapper::doVertexIndexSeek(
    ExtentIterator *&ext_it, DataChunk &output, DataChunk &input,
    idx_t nodeColIdx, vector<ExtentID> &target_eids,
    vector<vector<uint32_t>> &target_seqnos_per_extent,
    vector<idx_t> &cols_to_include, idx_t current_pos,
    const vector<uint32_t> &output_col_idx)
{
    ExtentID target_eid = target_eids[current_pos];
    if (IsInMemoryExtent(target_eid)) {
        idx_t mapping_idx = (target_eid < last_seek_eid_to_mapping_idx_.size())
                                ? last_seek_eid_to_mapping_idx_[target_eid]
                                : (idx_t)-1;
        if (mapping_idx == (idx_t)-1) {
            return StoreAPIResult::DONE;
        }
        FillInMemorySeekOutput(client, last_seek_oids_, last_seek_scan_projection_,
                               output, input, nodeColIdx, target_eid,
                               target_seqnos_per_extent[current_pos],
                               output_col_idx, cols_to_include, mapping_idx);
        return StoreAPIResult::OK;
    }
    if (ext_it == nullptr)
        return StoreAPIResult::DONE;
    ExtentID current_eid;
    D_ASSERT(ext_it != nullptr && ext_it->IsInitialized());
    D_ASSERT(current_pos < target_seqnos_per_extent.size());
    ext_it->GetNextExtent(
        client, output, current_eid, target_eid, input, nodeColIdx,
        output_col_idx, target_seqnos_per_extent[current_pos], cols_to_include);
    TranslateBaseSeekOutputIds(client, output,
                               target_seqnos_per_extent[current_pos],
                               output_col_idx);
    return StoreAPIResult::OK;
}

StoreAPIResult iTbgppGraphStorageWrapper::doVertexIndexSeek(
    ExtentIterator *&ext_it, DataChunk &output, DataChunk &input,
    idx_t nodeColIdx, vector<ExtentID> &target_eids,
    vector<vector<uint32_t>> &target_seqnos_per_extent, idx_t current_pos,
    idx_t out_id_col_idx, Vector &rowcol_vec, char *row_major_store,
    const vector<uint32_t> &output_col_idx, idx_t &num_output_tuples)
{
    ExtentID target_eid = target_eids[current_pos];
    ExtentID current_eid;
    D_ASSERT(ext_it != nullptr && ext_it->IsInitialized());
    D_ASSERT(current_pos < target_seqnos_per_extent.size());
    ext_it->GetNextExtentInRowFormat(client, output, current_eid, target_eid,
                                     input, nodeColIdx, output_col_idx,
                                     rowcol_vec, row_major_store,
                                     target_seqnos_per_extent[current_pos],
                                     out_id_col_idx, num_output_tuples);
    TranslateBaseSeekOutputIdColumn(client, output,
                                    target_seqnos_per_extent[current_pos],
                                    out_id_col_idx);
    return StoreAPIResult::OK;
}

StoreAPIResult iTbgppGraphStorageWrapper::doVertexIndexSeek(
    ExtentIterator *&ext_it, DataChunk &output, DataChunk &input,
    idx_t nodeColIdx, vector<ExtentID> &target_eids,
    vector<vector<uint32_t>> &target_seqnos_per_extent,
    vector<idx_t> &cols_to_include, idx_t current_pos,
    const vector<uint32_t> &output_col_idx, idx_t &num_tuples_per_chunk)
{
    ExtentID target_eid = target_eids[current_pos];
    ExtentID current_eid;
    D_ASSERT(ext_it != nullptr && ext_it->IsInitialized());
    D_ASSERT(current_pos < target_seqnos_per_extent.size());
    ext_it->GetNextExtent(client, output, current_eid, target_eid, input,
                          nodeColIdx, output_col_idx,
                          target_seqnos_per_extent[current_pos],
                          cols_to_include, num_tuples_per_chunk);
    TranslateBaseSeekOutputIds(client, output,
                               target_seqnos_per_extent[current_pos],
                               output_col_idx);
    return StoreAPIResult::OK;
}

StoreAPIResult iTbgppGraphStorageWrapper::InitializeEdgeIndexSeek(
    ExtentIterator *&ext_it, DataChunk &output, uint64_t vid, LabelSet labels,
    std::vector<LabelSet> &edgeLabels, LoadAdjListOption loadAdj,
    PropertyKeys properties, std::vector<duckdb::LogicalType> &scanSchema)
{
    D_ASSERT(ext_it == nullptr);
    Catalog &cat_instance = client.db->GetCatalog();
    D_ASSERT(labels.size() == 1);  // XXX Temporary
    string entry_name = "eps_";
    for (auto &it : labels.data)
        entry_name += it;
    PropertySchemaCatalogEntry *ps_cat_entry =
        (PropertySchemaCatalogEntry *)cat_instance.GetEntry(
            client, CatalogType::PROPERTY_SCHEMA_ENTRY, DEFAULT_SCHEMA,
            entry_name);

    D_ASSERT(edgeLabels.size() <= 1);  // XXX Temporary
    vector<string> properties_temp;
    for (size_t i = 0; i < edgeLabels.size(); i++) {
        for (auto &it : edgeLabels[i].data)
            properties_temp.push_back(it);
    }
    for (auto &it : properties) {
        // std::cout << "Property: " << it << std::endl;
        properties_temp.push_back(it);
    }
    vector<idx_t> column_idxs;
    column_idxs = move(ps_cat_entry->GetColumnIdxs(properties_temp));

    ExtentID target_eid = GET_EID_FROM_PHYSICAL_ID(vid);
    idx_t target_seqno = GET_SEQNO_FROM_PHYSICAL_ID(vid);

    ext_it = new ExtentIterator();
    ext_it->Initialize(client, scanSchema, column_idxs, target_eid);
    return StoreAPIResult::OK;
}

StoreAPIResult iTbgppGraphStorageWrapper::InitializeEdgeIndexSeek(
    ExtentIterator *&ext_it, DataChunk &output, DataChunk &input,
    idx_t nodeColIdx, LabelSet labels, std::vector<LabelSet> &edgeLabels,
    LoadAdjListOption loadAdj, PropertyKeys properties,
    std::vector<duckdb::LogicalType> &scanSchema, vector<ExtentID> &target_eids,
    vector<idx_t> &boundary_position)
{
    D_ASSERT(false);  // temporary
    D_ASSERT(ext_it == nullptr);
    Catalog &cat_instance = client.db->GetCatalog();
    D_ASSERT(labels.size() == 1);  // XXX Temporary
    string entry_name = "eps_";
    for (auto &it : labels.data)
        entry_name += it;
    PropertySchemaCatalogEntry *ps_cat_entry =
        (PropertySchemaCatalogEntry *)cat_instance.GetEntry(
            client, CatalogType::PROPERTY_SCHEMA_ENTRY, DEFAULT_SCHEMA,
            entry_name);

    D_ASSERT(edgeLabels.size() <= 1);  // XXX Temporary
    vector<string> properties_temp;
    for (size_t i = 0; i < edgeLabels.size(); i++) {
        for (auto &it : edgeLabels[i].data)
            properties_temp.push_back(it);
    }
    for (auto &it : properties) {
        // std::cout << "Property: " << it << std::endl;
        properties_temp.push_back(it);
    }
    vector<idx_t> column_idxs;
    column_idxs = move(ps_cat_entry->GetColumnIdxs(properties_temp));

    ExtentID prev_eid =
        input.size() == 0
            ? 0
            : (UBigIntValue::Get(input.GetValue(nodeColIdx, 0)) >> 32);
    for (size_t i = 0; i < input.size(); i++) {
        uint64_t vid = UBigIntValue::Get(input.GetValue(nodeColIdx, i));
        ExtentID target_eid =
            vid >>
            32;  // TODO make this functionality as Macro --> GetEIDFromPhysicalID
        target_eids.push_back(target_eid);
        if (prev_eid != target_eid)
            boundary_position.push_back(i - 1);
        prev_eid = target_eid;
    }
    boundary_position.push_back(input.size() - 1);

    target_eids.erase(std::unique(target_eids.begin(), target_eids.end()),
                      target_eids.end());

    if (target_eids.size() == 0)
        return StoreAPIResult::DONE;

    ext_it = new ExtentIterator();
    // ext_it->Initialize(client, scanSchema, column_idxs, target_eids);
    return StoreAPIResult::OK;
}

bool iTbgppGraphStorageWrapper::isNodeInLabelset(u_int64_t id, LabelSet labels)
{
    return true;
}

void iTbgppGraphStorageWrapper::getAdjColIdxs(idx_t index_cat_oid,
                                     vector<int> &adjColIdxs,
                                     vector<LogicalType> &adjColTypes)
{
    Catalog &cat_instance = client.db->GetCatalog();
    IndexCatalogEntry *index_cat = (IndexCatalogEntry *)cat_instance.GetEntry(
        client, DEFAULT_SCHEMA, index_cat_oid, true /* if_exists */);

    if (!index_cat) {
        throw InvalidInputException(
            "getAdjColIdxs: adj list index OID %llu not found in catalog", (unsigned long long)index_cat_oid);
    }

    idx_t aci = index_cat->GetAdjColIdx();
    adjColIdxs.push_back(aci);

    if (index_cat->GetIndexType() == IndexType::FORWARD_CSR) {
        adjColTypes.push_back(LogicalType::FORWARD_ADJLIST);
    }
    else if (index_cat->GetIndexType() == IndexType::BACKWARD_CSR) {
        adjColTypes.push_back(LogicalType::BACKWARD_ADJLIST);
    }
    else {
        throw InvalidInputException(
            "IndexType should be one of FORWARD/BACKWARD CSR");
    }
}

uint16_t iTbgppGraphStorageWrapper::getAdjListSrcPartitionId(idx_t index_cat_oid) {
    Catalog &cat = client.db->GetCatalog();
    IndexCatalogEntry *index_cat = (IndexCatalogEntry *)cat.GetEntry(
        client, DEFAULT_SCHEMA, index_cat_oid, true);
    if (!index_cat)
        throw InvalidInputException("getAdjListSrcPartitionId: OID %llu not found",
                                    (unsigned long long)index_cat_oid);

    // index_cat->pid is the edge partition OID.
    // For FORWARD adj lists, the adj list is stored on the src vertex partition.
    // For BACKWARD adj lists, the adj list is stored on the dst vertex partition.
    PartitionCatalogEntry *edge_part = (PartitionCatalogEntry *)cat.GetEntry(
        client, DEFAULT_SCHEMA, index_cat->pid, true);
    if (!edge_part)
        throw InvalidInputException("getAdjListSrcPartitionId: edge partition oid %llu not found",
                                    (unsigned long long)index_cat->pid);

    bool is_forward = (index_cat->GetIndexType() == IndexType::FORWARD_CSR);
    idx_t vertex_part_oid = is_forward ? edge_part->GetSrcPartOid() : edge_part->GetDstPartOid();

    PartitionCatalogEntry *vertex_part = (PartitionCatalogEntry *)cat.GetEntry(
        client, DEFAULT_SCHEMA, vertex_part_oid, true);
    if (!vertex_part)
        throw InvalidInputException("getAdjListSrcPartitionId: vertex partition oid %llu not found",
                                    (unsigned long long)vertex_part_oid);

    return (uint16_t)vertex_part->GetPartitionID();
}

StoreAPIResult
iTbgppGraphStorageWrapper::getAdjListFromVid(AdjacencyListIterator &adj_iter, int adjColIdx, ExtentID &prev_eid, uint64_t vid, uint64_t *&start_ptr, uint64_t *&end_ptr, ExpandDirection expand_dir) {
	D_ASSERT( expand_dir == ExpandDirection::OUTGOING || expand_dir == ExpandDirection::INCOMING );
	bool is_initialized = true;
	auto &delta_store = client.db->delta_store;
	uint64_t current_pid = delta_store.ResolvePid(vid);
	uint64_t adjacency_pid = delta_store.ResolveAdjacencyPid(vid);
	if (current_pid == 0 || adjacency_pid == 0) {
		start_ptr = nullptr;
		end_ptr = nullptr;
		return StoreAPIResult::OK;
	}
	ExtentID target_eid = adjacency_pid >> 32;

	// In-memory extent nodes have no CSR — return empty base, delta handled below
	if (IsInMemoryExtent(target_eid)) {
		start_ptr = nullptr;
		end_ptr = nullptr;
	} else {
		auto &catalog = client.db->GetCatalog();
		auto *extent_cat = (ExtentCatalogEntry *)catalog.GetEntry(
			client, CatalogType::EXTENT_ENTRY, DEFAULT_SCHEMA,
			DEFAULT_EXTENT_PREFIX + std::to_string(target_eid), true);
		if (!extent_cat || adjColIdx >= (int)extent_cat->adjlist_chunks.size()) {
			start_ptr = nullptr;
			end_ptr = nullptr;
		} else {
		if (target_eid != prev_eid) {
			if (expand_dir == ExpandDirection::OUTGOING) {
				is_initialized = adj_iter.Initialize(client, adjColIdx, target_eid, true);
			} else if (expand_dir == ExpandDirection::INCOMING) {
				is_initialized = adj_iter.Initialize(client, adjColIdx, target_eid, false);
			}
			adj_iter.getAdjListPtr(adjacency_pid, target_eid, &start_ptr, &end_ptr, is_initialized);
		} else {
			adj_iter.getAdjListPtr(adjacency_pid, target_eid, &start_ptr, &end_ptr, is_initialized);
		}
		}
	}
	prev_eid = target_eid;

	uint16_t vertex_part_id = (uint16_t)(target_eid >> 16);
	uint16_t edge_part_id =
		ResolveAdjDeltaPartitionId(client, adjColIdx, expand_dir, vertex_part_id);
	if (edge_part_id == std::numeric_limits<uint16_t>::max()) {
		return StoreAPIResult::OK;
	}

	const auto &adj_deltas = delta_store.adj_deltas_exposed();
	auto adj_it = adj_deltas.find(edge_part_id);
	if (adj_it == adj_deltas.end()) {
		return StoreAPIResult::OK;
	}

	const auto &adj_delta = adj_it->second;
	const auto *inserted = adj_delta.GetInserted(vid);
	const auto *deleted = adj_delta.GetDeleted(vid);
	bool has_inserted = inserted && !inserted->empty();
	bool has_deleted = deleted && !deleted->empty();
	if (!has_inserted && !has_deleted) {
		return StoreAPIResult::OK;
	}

	idx_t total = 0;
	for (uint64_t *p = start_ptr; p && p < end_ptr; p += 2) {
		if (deleted && deleted->count(p[1]) > 0) {
			continue;
		}
		total++;
	}
	if (inserted) {
		for (auto &entry : *inserted) {
			if (deleted && deleted->count(entry.edge_id) > 0) {
				continue;
			}
			total++;
		}
	}

	if (total == 0) {
		start_ptr = nullptr;
		end_ptr = nullptr;
		return StoreAPIResult::OK;
	}

	std::lock_guard<std::mutex> guard(adj_merge_buf_mutex_);
	auto &adj_merge_buf = default_scratch_.adj_merge_buf;
	adj_merge_buf.resize(total * 2);
	idx_t cursor = 0;
	for (uint64_t *p = start_ptr; p && p < end_ptr; p += 2) {
		if (deleted && deleted->count(p[1]) > 0) {
			continue;
		}
		adj_merge_buf[cursor++] = p[0];
		adj_merge_buf[cursor++] = p[1];
	}
	if (inserted) {
		for (auto &entry : *inserted) {
			if (deleted && deleted->count(entry.edge_id) > 0) {
				continue;
			}
			adj_merge_buf[cursor++] = entry.dst_vid;
			adj_merge_buf[cursor++] = entry.edge_id;
		}
	}
	start_ptr = adj_merge_buf.data();
	end_ptr = adj_merge_buf.data() + cursor;

	return StoreAPIResult::OK;
}

void iTbgppGraphStorageWrapper::fillEidToMappingIdx(
    vector<uint64_t> &oids, vector<vector<uint64_t>> &scan_projection_mapping,
    vector<idx_t> &eid_to_mapping_idx, bool union_schema)
{
    Catalog &cat_instance = client.db->GetCatalog();
    last_seek_oids_ = oids;
    last_seek_scan_projection_ = scan_projection_mapping;

    for (auto i = 0; i < oids.size(); i++) {
        auto oid = oids[i];
        PropertySchemaCatalogEntry *ps_cat_entry =
            (PropertySchemaCatalogEntry *)cat_instance.GetEntry(
                client, DEFAULT_SCHEMA, oid);
        auto extent_ids = ps_cat_entry->extent_ids;

        for (auto eid : extent_ids) {
            // M28: Use full EID (not just ext_seqno) to avoid collisions
            // across different partitions for multi-partition vertices.
            if (eid >= eid_to_mapping_idx.size()) {
                eid_to_mapping_idx.resize(std::max(eid_to_mapping_idx.size() * 2, (size_t)eid + 1), (idx_t)-1);
            }
            eid_to_mapping_idx[eid] = union_schema ? 0 : i;
        }

        auto &ds = client.db->delta_store;
        for (auto inmem_eid : ds.GetInMemoryExtentIDs(ps_cat_entry->pid)) {
            auto *buf = ds.FindInsertBuffer(inmem_eid);
            if (!buf || buf->Empty()) {
                continue;
            }
            if (inmem_eid >= eid_to_mapping_idx.size()) {
                eid_to_mapping_idx.resize(std::max(eid_to_mapping_idx.size() * 2,
                                                  (size_t)inmem_eid + 1),
                                          (idx_t)-1);
            }
            bool exact_match = BufferMatchesPropertySchema(*buf, *ps_cat_entry);
            if (exact_match || eid_to_mapping_idx[inmem_eid] == (idx_t)-1) {
                eid_to_mapping_idx[inmem_eid] = union_schema ? 0 : i;
            }
        }
    }
    last_seek_eid_to_mapping_idx_ = eid_to_mapping_idx;
}

} // namespace turbolynx