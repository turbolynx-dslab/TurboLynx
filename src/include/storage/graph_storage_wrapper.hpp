#pragma once

#include "common/typedef.hpp"

#include "common/common.hpp"
#include "common/vector.hpp"
#include "common/unordered_map.hpp"
#include "common/types/data_chunk.hpp"
#include "execution/expression_executor.hpp"
#include "common/boost_typedefs.hpp"
#include "planner/expression.hpp"
#include <chrono>
#include <mutex>
#include <queue>
#include <unordered_map>
#include <bitset>

#define END_OF_QUEUE nullptr

namespace duckdb {

class ExtentIterator;
class AdjacencyListIterator;
class ClientContext;
class IOCache;

class GraphStorageWrapper { 

public:
	// define APIs here
	// TODO further need to be re-defined upon discussion

	// ! Scan used by scan operators
	StoreAPIResult InitializeScan(ExtentIterator *&ext_it, LabelSet labels, std::vector<LabelSet> edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> scanSchema) { return StoreAPIResult::OK; }
	StoreAPIResult doScan(ExtentIterator *&ext_it, duckdb::DataChunk& output, LabelSet labels, std::vector<LabelSet> edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> scanSchema) { return StoreAPIResult::OK; }
	StoreAPIResult doScan(ExtentIterator *&ext_it, duckdb::DataChunk &output, LabelSet labels, std::vector<LabelSet> edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> scanSchema, std::string filterKey, duckdb::Value filterValue) { return StoreAPIResult::OK; }
	StoreAPIResult doIndexSeek(ExtentIterator *&ext_it, duckdb::DataChunk& output, uint64_t vid, LabelSet labels, std::vector<LabelSet> edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> scanSchema) { return StoreAPIResult::OK; }
	StoreAPIResult doEdgeIndexSeek(ExtentIterator *&ext_it, duckdb::DataChunk& output, uint64_t vid, LabelSet labels, std::vector<LabelSet> edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> scanSchema) { return StoreAPIResult::OK; }
	bool isNodeInLabelset(uint64_t id, LabelSet labels) { return true; }
	void getAdjColIdxs(LabelSet labels, vector<int> &adjColIdxs) {}
	StoreAPIResult getAdjListRange(AdjacencyListIterator &adj_iter, int adjColIdx, uint64_t vid, uint64_t* start_idx, uint64_t* end_idx) { return StoreAPIResult::OK; }
	StoreAPIResult getAdjListFromRange(AdjacencyListIterator &adj_iter, int adjColIdx, uint64_t vid, uint64_t start_idx, uint64_t end_idx, duckdb::DataChunk& output, idx_t *&adjListBase) { return StoreAPIResult::OK; }
	StoreAPIResult getAdjListFromVid(AdjacencyListIterator &adj_iter, int adjColIdx, uint64_t vid, uint64_t *&start_ptr, uint64_t *&end_ptr) { return StoreAPIResult::OK; }

};

//! Per-thread scratch buffers for InitializeVertexIndexSeek and friends.
//! Owned by the caller (e.g. IdSeekState) so multiple threads can run
//! these functions concurrently against the same iTbgppGraphStorageWrapper.
struct IndexSeekScratch {
	ResizableBoolVector target_eid_flags;
	vector<vector<uint32_t>> target_seqnos_per_extent_map;
	vector<idx_t> boundary_position;
	vector<idx_t> tmp_vec;
	vector<idx_t> target_seqnos_per_extent_map_cursors;
	idx_t boundary_position_cursor = 0;
	idx_t tmp_vec_cursor = 0;
	std::unordered_set<ExtentID> seen_eids;
	//! Temporary buffer for merging base CSR + delta edges in getAdjListFromVid
	vector<uint64_t> adj_merge_buf;
	vector<ExtentID> base_target_eids;
	vector<idx_t> base_mapping_idxs;

	IndexSeekScratch();
};

class iTbgppGraphStorageWrapper: GraphStorageWrapper {
public:
	iTbgppGraphStorageWrapper(ClientContext &client);

	//! Get this connection's "default" scratch — used by single-thread (sequential) code
	//! paths that haven't been refactored to thread-local scratch yet.
	IndexSeekScratch &GetDefaultScratch() { return default_scratch_; }

public:
 //! Initialize Scan Operation
 StoreAPIResult InitializeScan(
     std::queue<ExtentIterator *> &ext_its, vector<idx_t> &oids,
     vector<vector<uint64_t>> &projection_mapping,
     vector<vector<duckdb::LogicalType>> &scanSchemas,
     bool enable_filter_buffering = true);

 //! Initialize Scan Operation
 StoreAPIResult InitializeScan(
     std::queue<ExtentIterator *> &ext_its, PropertySchemaID_vector *oids,
     vector<vector<uint64_t>> &projection_mapping,
     vector<vector<duckdb::LogicalType>> &scanSchemas,
     bool enable_filter_buffering = true);

 // Non filter
 StoreAPIResult doScan(std::queue<ExtentIterator *> &ext_its,
                       duckdb::DataChunk &output,
                       std::vector<duckdb::LogicalType> &scanSchema);
 StoreAPIResult doScan(std::queue<ExtentIterator *> &ext_its,
                       duckdb::DataChunk &output,
                       vector<vector<uint64_t>> &projection_mapping,
                       std::vector<duckdb::LogicalType> &scanSchema,
                       int64_t current_schema_idx,
                       bool is_output_initialized = true);

 // Filter related
 StoreAPIResult doScan(std::queue<ExtentIterator *> &ext_its,
                       duckdb::DataChunk &output,
                       FilteredChunkBuffer &output_buffer,
                       vector<vector<uint64_t>> &projection_mapping,
                       std::vector<duckdb::LogicalType> &scanSchema,
                       int64_t current_schema_idx, int64_t &filterKeyColIdx,
                       duckdb::Value &filterValue);
 StoreAPIResult doScan(std::queue<ExtentIterator *> &ext_its,
                       duckdb::DataChunk &output,
                       FilteredChunkBuffer &output_buffer,
                       vector<vector<uint64_t>> &projection_mapping,
                       std::vector<duckdb::LogicalType> &scanSchema,
                       int64_t current_schema_idx, int64_t &filterKeyColIdx,
                       duckdb::RangeFilterValue &rangeFilterValue);
 StoreAPIResult doScan(std::queue<ExtentIterator *> &ext_its,
                       duckdb::DataChunk &output,
                       FilteredChunkBuffer &output_buffer,
                       vector<vector<uint64_t>> &projection_mapping,
                       std::vector<duckdb::LogicalType> &scanSchema,
                       int64_t current_schema_idx, ExpressionExecutor &expr);

 StoreAPIResult InitializeVertexIndexSeek(
     ExtentIterator *&ext_it, DataChunk &input,
     idx_t nodeColIdx, vector<ExtentID> &target_eids,
     vector<vector<uint32_t>> &target_seqnos_per_extent,
     vector<idx_t> &mapping_idxs, vector<idx_t> &null_tuples_idx,
     vector<idx_t> &eid_to_mapping_idx, IOCache *io_cache,
     IndexSeekScratch &scratch);
 StoreAPIResult doVertexIndexSeek(
     ExtentIterator *&ext_it, DataChunk &output, DataChunk &input,
     idx_t nodeColIdx, 
     vector<ExtentID> &target_eids,
     vector<vector<uint32_t>> &target_seqnos_per_extent,
     vector<idx_t> &cols_to_include, idx_t current_pos,
     const vector<uint32_t> &output_col_idx);
 StoreAPIResult doVertexIndexSeek(
     ExtentIterator *&ext_it, DataChunk &output, DataChunk &input,
     idx_t nodeColIdx, 
     vector<ExtentID> &target_eids,
     vector<vector<uint32_t>> &target_seqnos_per_extent, idx_t current_pos,
     idx_t out_id_col_idx, Vector &rowcol_vec, char *row_major_store, 
     const vector<uint32_t> &output_col_idx, idx_t &num_output_tuples);
 StoreAPIResult doVertexIndexSeek(
     ExtentIterator *&ext_it, DataChunk &output, DataChunk &input,
     idx_t nodeColIdx, 
     vector<ExtentID> &target_eids,
     vector<vector<uint32_t>> &target_seqnos_per_extent,
     vector<idx_t> &cols_to_include, idx_t current_pos,
     const vector<uint32_t> &output_col_idx, idx_t &num_tuples_per_chunk);
 StoreAPIResult InitializeEdgeIndexSeek(
     ExtentIterator *&ext_it, duckdb::DataChunk &output, uint64_t vid,
     LabelSet labels, std::vector<LabelSet> &edgeLabels,
     LoadAdjListOption loadAdj, PropertyKeys properties,
     std::vector<duckdb::LogicalType> &scanSchema);
 StoreAPIResult InitializeEdgeIndexSeek(
     ExtentIterator *&ext_it, duckdb::DataChunk &output, DataChunk &input,
     idx_t nodeColIdx, LabelSet labels, std::vector<LabelSet> &edgeLabels,
     LoadAdjListOption loadAdj, PropertyKeys properties,
     std::vector<duckdb::LogicalType> &scanSchema,
     vector<ExtentID> &target_eids, vector<idx_t> &boundary_position);
 bool isNodeInLabelset(uint64_t id, LabelSet labels);
 void getAdjColIdxs(idx_t index_cat_oid, vector<int> &adjColIdxs,
                    vector<LogicalType> &adjColTypes);
 uint16_t getAdjListSrcPartitionId(idx_t index_cat_oid);
 StoreAPIResult getAdjListFromVid(AdjacencyListIterator &adj_iter,
                                  int adjColIdx, ExtentID &prev_eid,
                                  uint64_t vid, uint64_t *&start_ptr,
                                  uint64_t *&end_ptr,
                                  ExpandDirection expand_dir);

 void fillEidToMappingIdx(vector<uint64_t> &oids,
                          vector<vector<uint64_t>> &scan_projection_mapping,
                          vector<idx_t> &eid_to_mapping_idx,
                          bool union_schema = false);

private:
	inline void _fillTargetSeqnosVecAndBoundaryPosition(IndexSeekScratch &scratch, idx_t i, ExtentID prev_eid);

private:
	ClientContext &client;
	//! Default scratch — used by sequential paths that pass GetDefaultScratch()
	IndexSeekScratch default_scratch_;
	//! Mutex protecting default_scratch_'s adj_merge_buf during getAdjListFromVid
	std::mutex adj_merge_buf_mutex_;
	//! OIDs + projection from last InitializeScan (for UpdateSegment merge in doScan)
	vector<idx_t> last_scan_oids_;
	vector<vector<uint64_t>> last_scan_projection_;
	//! OIDs from the last IdSeek mapping build (used for in-memory delta seeks)
	vector<uint64_t> last_seek_oids_;
	vector<vector<uint64_t>> last_seek_scan_projection_;
	vector<idx_t> last_seek_eid_to_mapping_idx_;

public:
	//! Set scan metadata without creating ExtentIterators (used by parallel scan)
	void SetScanMetadata(const vector<idx_t> &oids, const vector<vector<uint64_t>> &projection) {
		last_scan_oids_ = oids;
		last_scan_projection_ = projection;
	}
};

}
