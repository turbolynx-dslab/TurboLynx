#include <algorithm>
#include <cassert>
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
#include "range/v3/all.hpp"

namespace duckdb {

iTbgppGraphStorageWrapper::iTbgppGraphStorageWrapper(ClientContext &client)
    : client(client),
      boundary_position(STANDARD_VECTOR_SIZE),
      tmp_vec(STANDARD_VECTOR_SIZE),
      boundary_position_cursor(0),
      target_eid_flags(INITIAL_EXTENT_ID_SPACE),
      tmp_vec_cursor(0),
      target_seqnos_per_extent_map(INITIAL_EXTENT_ID_SPACE,
                                   vector<uint32_t>(STANDARD_VECTOR_SIZE)),
      target_seqnos_per_extent_map_cursors(INITIAL_EXTENT_ID_SPACE, 0)
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

// Apply UpdateSegment deltas to scan output.
// For each row, extract VID from col 0, look up UpdateSegment, and replace values.
static void mergeUpdateSegment(ClientContext &client, DataChunk &output,
                               const vector<idx_t> *oids_hint,
                               const vector<vector<uint64_t>> *proj_hint)
{
    auto &delta_store = client.db->delta_store;
    if (output.size() == 0 || output.ColumnCount() == 0) return;
    if (!oids_hint || oids_hint->empty()) return;

    // Check col 0 type — must be ID/UBIGINT for VID extraction
    auto col0type = output.data[0].GetType().id();
    if (col0type != LogicalTypeId::ID && col0type != LogicalTypeId::UBIGINT) {
        spdlog::debug("[MERGE] skip: col0 type={}", output.data[0].GetType().ToString());
        return;
    }

    auto *vid_data = (uint64_t *)output.data[0].GetData();
    Catalog &cat = client.db->GetCatalog();

    for (idx_t row = 0; row < output.size(); row++) {
        uint64_t vid = vid_data[row];
        uint32_t extent_id = (uint32_t)(vid >> 32);
        uint32_t row_offset = (uint32_t)(vid & 0xFFFFFFFF);
        if (IsInMemoryExtent(extent_id)) continue;

        auto &update_seg = delta_store.GetUpdateSegment(extent_id);
        if (update_seg.Empty()) continue;
        auto *named = update_seg.GetNamedUpdates(row_offset);
        if (!named) {
            spdlog::debug("[MERGE] vid=0x{:016X} eid=0x{:08X} off={} — seg not empty but no named for this offset", vid, extent_id, row_offset);
            continue;
        }
        spdlog::info("[MERGE] vid=0x{:016X} eid=0x{:08X} off={} updates={}", vid, extent_id, row_offset, named->size());

        // Find the PropertySchema that owns this extent
        for (idx_t si = 0; si < oids_hint->size(); si++) {
            auto *ps = (PropertySchemaCatalogEntry *)cat.GetEntry(
                client, DEFAULT_SCHEMA, (*oids_hint)[si]);
            if (!ps) continue;
            bool found = false;
            for (auto eid : ps->extent_ids) {
                if (eid == extent_id) { found = true; break; }
            }
            if (!found) continue;

            auto *key_names = ps->GetKeys();
            if (!key_names) break;

            // Use scan_projection to map output col → PropertySchema col → key name
            const vector<uint64_t> *proj = (proj_hint && si < proj_hint->size())
                                            ? &(*proj_hint)[si] : nullptr;
            for (idx_t col = 0; col < output.ColumnCount(); col++) {
                // Determine which PropertySchema column this output column represents
                idx_t ps_col = proj ? (*proj)[col] : col;
                if (ps_col < key_names->size()) {
                    auto it = named->find((*key_names)[ps_col]);
                    if (it != named->end()) {
                        output.SetValue(col, row, it->second);
                    }
                }
            }
            break;
        }
    }
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
        mergeUpdateSegment(client, output, &last_scan_oids_, &last_scan_projection_);
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
        mergeUpdateSegment(client, output, &last_scan_oids_, &last_scan_projection_);
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
        mergeUpdateSegment(client, output, &last_scan_oids_, &last_scan_projection_);
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
        mergeUpdateSegment(client, output, &last_scan_oids_, &last_scan_projection_);
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
        mergeUpdateSegment(client, output, &last_scan_oids_, &last_scan_projection_);
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
    idx_t i, ExtentID prev_eid)
{
    auto prev_eid_seqno = GET_EXTENT_SEQNO_FROM_EID(prev_eid);
    while (prev_eid_seqno >= target_seqnos_per_extent_map.size()) {
        target_seqnos_per_extent_map.resize(
            target_seqnos_per_extent_map.size() * 2,
            vector<uint32_t>(STANDARD_VECTOR_SIZE));
        target_seqnos_per_extent_map_cursors.resize(
            target_seqnos_per_extent_map_cursors.size() * 2, 0);
    }
    vector<uint32_t> &vec = target_seqnos_per_extent_map[prev_eid_seqno];
    idx_t &cursor = target_seqnos_per_extent_map_cursors[prev_eid_seqno];
    for (auto i = 0; i < tmp_vec_cursor; i++) {
        vec[cursor++] = tmp_vec[i];
    }
    boundary_position[boundary_position_cursor++] = i - 1;
    tmp_vec_cursor = 0;
}

StoreAPIResult iTbgppGraphStorageWrapper::InitializeVertexIndexSeek(
    ExtentIterator *&ext_it, DataChunk &input, idx_t nodeColIdx,
    vector<ExtentID> &target_eids, vector<vector<uint32_t>> &target_seqnos_per_extent,
    vector<idx_t> &mapping_idxs, vector<idx_t> &null_tuples_idx,
    vector<idx_t> &eid_to_mapping_idx, IOCache *io_cache)
{
    Catalog &cat_instance = client.db->GetCatalog();
    ExtentID prev_eid = std::numeric_limits<ExtentID>::max();
    Vector &src_vid_column_vector = input.data[nodeColIdx];
    vector<ExtentID> pruned_eids;
    target_eid_flags.reset();
    seen_eids.clear();

    // Cursor initialization
    for (auto i = 0; i < target_seqnos_per_extent_map_cursors.size(); i++) {
        target_seqnos_per_extent_map_cursors[i] = 0;
    }
    boundary_position_cursor = 0;
    tmp_vec_cursor = 0;
    target_eids.clear();

    auto &validity = src_vid_column_vector.GetValidity();
    if (validity.AllValid()) {
        switch (src_vid_column_vector.GetVectorType()) {
            case VectorType::DICTIONARY_VECTOR: {
                for (size_t i = 0; i < input.size(); i++) {
                    uint64_t vid = ((uint64_t *)src_vid_column_vector
                                        .GetData())[DictionaryVector::SelVector(
                                                        src_vid_column_vector)
                                                        .get_index(i)];
                    ExtentID target_eid = GET_EID_FROM_PHYSICAL_ID(vid);
                    if (i == 0)
                        prev_eid = target_eid;
                    if (prev_eid != target_eid) {
                        auto ext_seqno = GET_EXTENT_SEQNO_FROM_EID(prev_eid);
                        target_eid_flags.set(ext_seqno, true);
                        seen_eids.insert(prev_eid);
                        _fillTargetSeqnosVecAndBoundaryPosition(i, prev_eid);
                    }
                    tmp_vec[tmp_vec_cursor++] = i;
                    prev_eid = target_eid;
                }
                break;
            }
            case VectorType::FLAT_VECTOR: {
                for (size_t i = 0; i < input.size(); i++) {
                    uint64_t vid =
                        ((uint64_t *)src_vid_column_vector.GetData())[i];
                    ExtentID target_eid = GET_EID_FROM_PHYSICAL_ID(vid);
                    if (i == 0)
                        prev_eid = target_eid;
                    if (prev_eid != target_eid) {
                        auto ext_seqno = GET_EXTENT_SEQNO_FROM_EID(prev_eid);
                        target_eid_flags.set(ext_seqno, true);
                        seen_eids.insert(prev_eid);
                        _fillTargetSeqnosVecAndBoundaryPosition(i, prev_eid);
                    }
                    tmp_vec[tmp_vec_cursor++] = i;
                    prev_eid = target_eid;
                }
                break;
            }
            case VectorType::CONSTANT_VECTOR: {
                for (size_t i = 0; i < input.size(); i++) {
                    uint64_t vid =
                        ((uint64_t *)ConstantVector::GetData<uintptr_t>(
                            src_vid_column_vector))[0];
                    ExtentID target_eid = GET_EID_FROM_PHYSICAL_ID(vid);
                    if (i == 0)
                        prev_eid = target_eid;
                    if (prev_eid != target_eid) {
                        auto ext_seqno = GET_EXTENT_SEQNO_FROM_EID(prev_eid);
                        target_eid_flags.set(ext_seqno, true);
                        seen_eids.insert(prev_eid);
                        _fillTargetSeqnosVecAndBoundaryPosition(i, prev_eid);
                    }
                    tmp_vec[tmp_vec_cursor++] = i;
                    prev_eid = target_eid;
                }
                break;
            }
            default: {
                D_ASSERT(false);
            }
        }
    }
    else if (validity.CheckAllInValid()) {
        // When LEFT AdjIdxJoin cannot find any matched rows, it can be ALL NULL
        return StoreAPIResult::OK;
    }
    else {
        switch (src_vid_column_vector.GetVectorType()) {
            case VectorType::DICTIONARY_VECTOR: {
                for (size_t i = 0; i < input.size(); i++) {
                    auto vid_val = src_vid_column_vector.GetValue(i);
                    if (vid_val.IsNull()) {
                        null_tuples_idx.push_back(i);
                        continue;
                    }
                    uint64_t vid = vid_val.GetValue<uint64_t>();
                    ExtentID target_eid = GET_EID_FROM_PHYSICAL_ID(vid);
                    if (prev_eid == std::numeric_limits<ExtentID>::max())
                        prev_eid = target_eid;
                    if (prev_eid != target_eid) {
                        auto ext_seqno = GET_EXTENT_SEQNO_FROM_EID(prev_eid);
                        target_eid_flags.set(ext_seqno, true);
                        seen_eids.insert(prev_eid);
                        _fillTargetSeqnosVecAndBoundaryPosition(i, prev_eid);
                    }
                    tmp_vec[tmp_vec_cursor++] = i;
                    prev_eid = target_eid;
                }
            }
            case VectorType::FLAT_VECTOR: {
                for (size_t i = 0; i < input.size(); i++) {
                    if (!validity.RowIsValid(i)) {
                        null_tuples_idx.push_back(i);
                        continue;
                    }
                    uint64_t vid =
                        ((uint64_t *)src_vid_column_vector.GetData())[i];
                    ExtentID target_eid = GET_EID_FROM_PHYSICAL_ID(vid);
                    if (prev_eid == std::numeric_limits<ExtentID>::max())
                        prev_eid = target_eid;
                    if (prev_eid != target_eid) {
                        auto ext_seqno = GET_EXTENT_SEQNO_FROM_EID(prev_eid);
                        target_eid_flags.set(ext_seqno, true);
                        seen_eids.insert(prev_eid);
                        _fillTargetSeqnosVecAndBoundaryPosition(i, prev_eid);
                    }
                    tmp_vec[tmp_vec_cursor++] = i;
                    prev_eid = target_eid;
                }
                break;
            }
            case VectorType::CONSTANT_VECTOR: {
                D_ASSERT(false);
            }
            default: {
                D_ASSERT(false);
            }
        }
    }

    // process remaining
    if (tmp_vec_cursor > 0) {
        auto ext_seqno = GET_EXTENT_SEQNO_FROM_EID(prev_eid);
        target_eid_flags.set(ext_seqno, true);
        seen_eids.insert(prev_eid);
        _fillTargetSeqnosVecAndBoundaryPosition(input.size(), prev_eid);
    }

    // Filter extents by eid_to_mapping_idx: only keep those the IdSeek can handle.
    // Use seen_eids (full EIDs) to correctly handle multi-partition VIDs
    // where different partitions may share the same extent seqno.
    for (auto eid : seen_eids) {
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
        auto &vec = target_seqnos_per_extent_map[eid_seqno];
        auto cursor = target_seqnos_per_extent_map_cursors[eid_seqno];
        target_seqnos_per_extent.push_back({vec.begin(), vec.begin() + cursor});
    }

    // TODO maybe we don't need this..
    if (is_multi_schema) {
        ranges::sort(ranges::views::zip(mapping_idxs, target_eids,
                                        target_seqnos_per_extent),
                     std::less{}, [](auto &&p) { return std::get<0>(p); });
    }

    // Append vector for the pruned eids (this will used in Seek for nullify)
    for (auto i = 0; i < pruned_eids.size(); i++) {
        auto ext_seqno = GET_EXTENT_SEQNO_FROM_EID(pruned_eids[i]);
        auto &vec = target_seqnos_per_extent_map[ext_seqno];
        auto cursor = target_seqnos_per_extent_map_cursors[ext_seqno];
        target_seqnos_per_extent.push_back({vec.begin(), vec.begin() + cursor});
    }

    if (target_eids.size() > 0)
        ext_it->Initialize(client, &mapping_idxs, target_eids);

    return StoreAPIResult::OK;
}

StoreAPIResult iTbgppGraphStorageWrapper::doVertexIndexSeek(
    ExtentIterator *&ext_it, DataChunk &output, DataChunk &input,
    idx_t nodeColIdx, vector<ExtentID> &target_eids,
    vector<vector<uint32_t>> &target_seqnos_per_extent,
    vector<idx_t> &cols_to_include, idx_t current_pos,
    const vector<uint32_t> &output_col_idx)
{
    if (ext_it == nullptr)
        return StoreAPIResult::DONE;
    ExtentID target_eid = target_eids[current_pos];
    ExtentID current_eid;
    D_ASSERT(ext_it != nullptr || ext_it->IsInitialized());
    D_ASSERT(current_pos < target_seqnos_per_extent.size());
    ext_it->GetNextExtent(
        client, output, current_eid, target_eid, input, nodeColIdx,
        output_col_idx, target_seqnos_per_extent[current_pos], cols_to_include);
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
    D_ASSERT(ext_it != nullptr || ext_it->IsInitialized());
    D_ASSERT(current_pos < target_seqnos_per_extent.size());
    ext_it->GetNextExtentInRowFormat(client, output, current_eid, target_eid,
                                     input, nodeColIdx, output_col_idx,
                                     rowcol_vec, row_major_store,
                                     target_seqnos_per_extent[current_pos],
                                     out_id_col_idx, num_output_tuples);
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
    D_ASSERT(ext_it != nullptr || ext_it->IsInitialized());
    D_ASSERT(current_pos < target_seqnos_per_extent.size());
    ext_it->GetNextExtent(client, output, current_eid, target_eid, input,
                          nodeColIdx, output_col_idx,
                          target_seqnos_per_extent[current_pos],
                          cols_to_include, num_tuples_per_chunk);
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
	ExtentID target_eid = vid >> 32;

	// In-memory extent nodes have no CSR — return empty base, delta handled below
	if (IsInMemoryExtent(target_eid)) {
		start_ptr = nullptr;
		end_ptr = nullptr;
	} else {
		if (target_eid != prev_eid) {
			if (expand_dir == ExpandDirection::OUTGOING) {
				is_initialized = adj_iter.Initialize(client, adjColIdx, target_eid, true);
			} else if (expand_dir == ExpandDirection::INCOMING) {
				is_initialized = adj_iter.Initialize(client, adjColIdx, target_eid, false);
			}
			adj_iter.getAdjListPtr(vid, target_eid, &start_ptr, &end_ptr, is_initialized);
		} else {
			adj_iter.getAdjListPtr(vid, target_eid, &start_ptr, &end_ptr, is_initialized);
		}
	}
	prev_eid = target_eid;

	// Merge delta edges from AdjListDelta.
	// Delta edges are stored as [dst_vid, edge_id] pairs, same format as CSR.
	// If delta edges exist, copy base + delta into a merged buffer and update pointers.
	auto &delta_store = client.db->delta_store;
	// Check all edge partitions for delta edges (use adj_col index as partition hint)
	// For simplicity, iterate all AdjListDeltas. TODO: use index_cat to map adjColIdx → partition.
	for (auto &[part_id, adj_delta] : delta_store.adj_deltas_exposed()) {
		auto *inserted = adj_delta.GetInserted(vid);
		if (!inserted || inserted->empty()) continue;

		// Count base edges
		idx_t base_count = 0;
		if (start_ptr && end_ptr && end_ptr > start_ptr) {
			base_count = (end_ptr - start_ptr) / 2;  // each edge = 2 uint64_t
		}
		idx_t delta_count = inserted->size();
		idx_t total = base_count + delta_count;

		// Allocate merged buffer (reuse a per-wrapper thread-local buffer)
		adj_merge_buf_.resize(total * 2);
		// Copy base edges
		if (base_count > 0) {
			memcpy(adj_merge_buf_.data(), start_ptr, base_count * 2 * sizeof(uint64_t));
		}
		// Append delta edges
		for (idx_t i = 0; i < delta_count; i++) {
			adj_merge_buf_[(base_count + i) * 2]     = (*inserted)[i].dst_vid;
			adj_merge_buf_[(base_count + i) * 2 + 1] = (*inserted)[i].edge_id;
		}
		start_ptr = adj_merge_buf_.data();
		end_ptr = adj_merge_buf_.data() + total * 2;
		break;  // Only merge from the first matching partition
	}

	return StoreAPIResult::OK;
}

void iTbgppGraphStorageWrapper::fillEidToMappingIdx(vector<uint64_t> &oids,
                                           vector<idx_t> &eid_to_mapping_idx,
                                           bool union_schema)
{
    Catalog &cat_instance = client.db->GetCatalog();

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
    }
}

}  // namespace duckdb