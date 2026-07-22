#ifndef EXTENT_ITERATOR_H
#define EXTENT_ITERATOR_H

#include "common/typedef.hpp"
#include "common/common.hpp"
#include "common/vector.hpp"
#include "common/unordered_map.hpp"
#include "common/types.hpp"
#include "common/types/value.hpp"
#include "common/vector_size.hpp"
#include "common/types/data_chunk.hpp"
#include "common/types/selection_vector.hpp"
#include "main/client_context.hpp"
#include "storage/extent/compression/compression_function.hpp"
#include "storage/extent/compression/compression_header.hpp"
#include "planner/expression.hpp"
#include "execution/expression_executor.hpp"
#include <deque>
#include <limits>
#include <tuple>

namespace duckdb {
}
namespace turbolynx {
}
namespace duckdb {
    using namespace turbolynx;
}
namespace turbolynx {
using namespace duckdb;

inline uint64_t& getIdRefFromVectorTemp(Vector& vector, idx_t index) {
	switch (vector.GetVectorType()) {
		case VectorType::DICTIONARY_VECTOR: {
			return ((uint64_t *)vector.GetData())[DictionaryVector::SelVector(vector).get_index(index)];
		}
		case VectorType::FLAT_VECTOR: {
			return ((uint64_t *)vector.GetData())[index];
		}
        case VectorType::CONSTANT_VECTOR: {
            return ((uint64_t *)vector.GetData())[0];
        }
		default: {
			D_ASSERT(false);
		}
	}
}

// Read an ID/uint64 vid by type-dispatched direct data access (no Value boxing)
// while reporting NULL. Returns false (out untouched) when the row is NULL.
// Mirrors getIdRefFromVectorTemp for the value and the codebase's
// DICTIONARY null convention (child validity via the selection vector).
inline bool TryReadVidDirect(Vector &vector, idx_t index, uint64_t &out) {
	switch (vector.GetVectorType()) {
		case VectorType::DICTIONARY_VECTOR: {
			auto child_idx = DictionaryVector::SelVector(vector).get_index(index);
			if (!FlatVector::Validity(DictionaryVector::Child(vector)).RowIsValid(child_idx))
				return false;
			out = ((uint64_t *)vector.GetData())[child_idx];
			return true;
		}
		case VectorType::FLAT_VECTOR: {
			if (!FlatVector::Validity(vector).RowIsValid(index))
				return false;
			out = ((uint64_t *)vector.GetData())[index];
			return true;
		}
		case VectorType::CONSTANT_VECTOR: {
			if (ConstantVector::IsNull(vector))
				return false;
			out = ((uint64_t *)vector.GetData())[0];
			return true;
		}
		default:
			return false;
	}
}

class PropertySchemaCatalogEntry;
class ExtentCatalogEntry;

// TODO currently, only support double buffering
// If possible, change this implementation to support prefetching
#define MAX_NUM_DATA_CHUNKS 2
#define FILTER_BUFFERING_THRESHOLD 0.1

typedef vector<uint8_t*> io_buf_ptrs;
typedef vector<size_t> io_buf_sizes;
typedef vector<ChunkDefinitionID> io_cdf_ids;
typedef size_t num_tuple;

struct IOCacheEntry {
    io_cdf_ids cdf_ids;
    io_buf_ptrs buf_ptrs;
    io_buf_sizes buf_sizes;
    num_tuple num_tuples = 0;
};

typedef struct IOCache {
    static constexpr ExtentID kFreeSlot = std::numeric_limits<ExtentID>::max();
    vector<ExtentID> owner;
    std::deque<IOCacheEntry> entries;
    std::unordered_map<ExtentID, IOCacheEntry> overflow;
    IOCache() : owner(INITIAL_EXTENT_ID_SPACE, kFreeSlot), entries(INITIAL_EXTENT_ID_SPACE) {}
    IOCacheEntry *Find(ExtentID eid) {
        auto seqno = GET_EXTENT_SEQNO_FROM_EID(eid)
        if (seqno < owner.size()) {
            if (owner[seqno] == eid) return &entries[seqno];
            if (owner[seqno] == kFreeSlot) return nullptr;
        }
        auto it = overflow.find(eid);
        return it == overflow.end() ? nullptr : &it->second;
    }
    IOCacheEntry *Claim(ExtentID eid) {
        auto seqno = GET_EXTENT_SEQNO_FROM_EID(eid)
        if (seqno >= owner.size()) {
            auto new_size = owner.size();
            while (seqno >= new_size) new_size *= 2;
            owner.resize(new_size, kFreeSlot);
            entries.resize(new_size);
        }
        if (owner[seqno] == kFreeSlot) owner[seqno] = eid;
        if (owner[seqno] == eid) return &entries[seqno];
        return &overflow[eid];
    }
} IOCache;

class ExtentIterator {
public:
    // General constructor
    ExtentIterator(IOCache *io_cache_ = nullptr) : io_cache(io_cache_), data_chunks{nullptr} {
        src_data_seqnos.reserve(STANDARD_VECTOR_SIZE);
        for (int i = 0; i < MAX_NUM_DATA_CHUNKS; i++)
            cur_io_[i] = &local_io_[i];
    }
    // For seek
    ExtentIterator(vector<vector<LogicalType>>& _ext_property_types, vector<vector<idx_t>>& _target_idxs, IOCache *io_cache_ = nullptr) :
        io_cache(io_cache_), ext_property_types(_ext_property_types), target_idxs(_target_idxs), data_chunks{nullptr} {
        src_data_seqnos.reserve(STANDARD_VECTOR_SIZE);
        for (int i = 0; i < MAX_NUM_DATA_CHUNKS; i++)
            cur_io_[i] = &local_io_[i];
    }
    ~ExtentIterator();

    // Iterate all extents related to the PropertySchemaCatalogEntry
    void Initialize(duckdb::ClientContext &context, PropertySchemaCatalogEntry *property_schema_cat_entry);
    void Initialize(duckdb::ClientContext &context, PropertySchemaCatalogEntry *property_schema_cat_entry, vector<LogicalType> &target_types_,
                    vector<idx_t> &target_idxs_);
    void Initialize(duckdb::ClientContext &context, vector<LogicalType> &target_types_, vector<idx_t> &target_idxs_, ExtentID target_eid,
                    ExtentCatalogEntry *prefetched_entry = nullptr);
    //! Initialize for scanning a single storage extent (parallel NodeScan).
    //! The optional PS pointer lets the iterator consult column kinds for
    //! ENDPOINT_REF stored-value emit; pass nullptr for paths that don't
    //! need the kind-aware ID branch (legacy behavior preserved).
    void InitializeSingleExtent(duckdb::ClientContext &context, vector<LogicalType> &target_types_, vector<idx_t> &target_idxs_, ExtentID target_eid,
                                PropertySchemaCatalogEntry *property_schema_cat_entry = nullptr);
    void Initialize(duckdb::ClientContext &context, vector<idx_t> *target_idx_per_eid_, vector<ExtentID> target_eids);
    int RequestNewIO(duckdb::ClientContext &context, ExtentID target_eid, ExtentID &evicted_eid,
                     ExtentCatalogEntry *prefetched_entry = nullptr);
    bool RequestNextIO(duckdb::ClientContext &context, DataChunk &output, ExtentID &output_eid, bool is_output_chunk_initialized);
    void Rewind();

    /* no filter pushdown */
    bool GetNextExtent(duckdb::ClientContext &context, DataChunk &output, ExtentID &output_eid,
                       size_t scan_size = EXEC_ENGINE_VECTOR_SIZE, bool is_output_chunk_initialized=true);
    bool GetNextExtent(duckdb::ClientContext &context, DataChunk &output, ExtentID &output_eid, vector<idx_t> &output_column_idxs,
                       size_t scan_size = EXEC_ENGINE_VECTOR_SIZE, bool is_output_chunk_initialized=true);

    /* filter pushdown */
    bool GetNextExtent(duckdb::ClientContext &context, DataChunk &output, FilteredChunkBuffer &output_buffer, ExtentID &output_eid,
                       int64_t &filterKeyColIdx, Value &filterValue, vector<idx_t> &output_column_idxs,
                       vector<duckdb::LogicalType> &scanSchema, size_t scan_size = EXEC_ENGINE_VECTOR_SIZE,
                       bool is_output_chunk_initialized=true);
    bool GetNextExtent(duckdb::ClientContext &context, DataChunk &output, FilteredChunkBuffer &output_buffer, ExtentID &output_eid,
                       int64_t &filterKeyColIdx, Value &lfilterValue, Value &rfilterValue, bool l_inclusive, bool r_inclusive,
                       vector<idx_t> &output_column_idxs, vector<duckdb::LogicalType> &scanSchema, 
                       size_t scan_size = EXEC_ENGINE_VECTOR_SIZE, bool is_output_chunk_initialized=true);
    bool GetNextExtent(duckdb::ClientContext &context, DataChunk &output, FilteredChunkBuffer &output_buffer, ExtentID &output_eid,
                       ExpressionExecutor& executor, vector<idx_t> &output_column_idxs, vector<duckdb::LogicalType> &scanSchema, 
                       size_t scan_size = EXEC_ENGINE_VECTOR_SIZE, bool is_output_chunk_initialized=true);

    /* IdSeek */
    // bool GetNextExtent(duckdb::ClientContext &context, DataChunk &output, ExtentID &output_eid,
    //                    ExtentID target_eid, idx_t target_seqno, bool is_output_chunk_initialized=true);
    // bool GetNextExtent(duckdb::ClientContext &context, DataChunk &output, ExtentID &output_eid,
    //                    ExtentID target_eid, DataChunk &input, idx_t nodeColIdx, const vector<idx_t> &output_column_idxs,
    //                    idx_t start_seqno, idx_t end_seqno, bool is_output_chunk_initialized=true);
    bool GetNextExtent(duckdb::ClientContext &context, DataChunk &output, ExtentID &output_eid,
                       ExtentID target_eid, DataChunk &input, idx_t nodeColIdx, const vector<uint32_t> &output_column_idxs,
                       const SeqnoView &target_seqnos, vector<idx_t> &cols_to_include, bool is_output_chunk_initialized=true);
    bool GetNextExtentInRowFormat(duckdb::ClientContext &context, DataChunk &output, ExtentID &output_eid,
                       ExtentID target_eid, DataChunk &input, idx_t nodeColIdx, const vector<uint32_t> &output_column_idxs,
                       Vector &rowcol_vec, char *row_major_store, const SeqnoView &target_seqnos, idx_t out_id_col_idx,
                       idx_t &num_output_tuples, bool is_output_chunk_initialized=true);
    bool GetNextExtent(duckdb::ClientContext &context, DataChunk &output, ExtentID &output_eid,
                       ExtentID target_eid, DataChunk &input, idx_t nodeColIdx, const vector<uint32_t> &output_column_idxs,
                       const SeqnoView &target_seqnos, vector<idx_t> &cols_to_include, idx_t &output_seqno, bool is_output_chunk_initialized=true);
    bool GetExtent(data_ptr_t &chunk_ptr, int target_toggle, bool is_initialized);
    void PrefetchSeek(ExtentID eid, idx_t mapping_idx, DataChunk &input,
                      idx_t nodeColIdx, const SeqnoView &target_seqnos,
                      const vector<uint32_t> *output_column_idxs = nullptr,
                      const vector<idx_t> *cols_to_include = nullptr);
    bool BatchedSeek(duckdb::ClientContext &context, DataChunk &output,
                     DataChunk &input, idx_t nodeColIdx,
                     const vector<ExtentID> &target_eids,
                     const vector<SeqnoView> &target_seqnos_per_extent,
                     idx_t num_groups, idx_t mapping_idx,
                     const vector<uint32_t> &output_column_idxs,
                     const vector<idx_t> &cols_to_include);

    /* Optimization */
    void IncreaseCacheSize();
    bool ObtainFromCache(ExtentID &eid, int buf_idx);
    bool IsRewinded() {
        return is_rewinded;
    }

    bool IsInitialized() {
        return is_initialized;
    }

    /* Filter Buffering */
    inline void enableFilterBuffering() {
        is_filter_buffering_enabled = true;
    }
    inline void disableFilterBuffering() {
        is_filter_buffering_enabled = false;
    }
    inline bool isFilterBufferingEnabled() const {
        return is_filter_buffering_enabled;
    }

    const vector<idx_t> &GetLastOutputRowOffsets() const {
        return last_output_row_offsets_;
    }

    ExtentID GetLastOutputExtentID() const {
        return last_output_extent_id_;
    }

private:
    bool _CheckIsMemoryEnough();

    template <typename T, typename TFilter>
    void evalPredicateSIMD(Vector& column_vec, size_t data_len, std::unique_ptr<TFilter>& filter, 
                            idx_t scan_start_offset, idx_t scan_end_offset, vector<idx_t>& matched_row_idxs);

    idx_t findColumnIdx(ChunkDefinitionID filter_cdf_id);
    ChunkDefinitionID getFilterCDFID(ExtentID output_eid, int64_t filterKeyColIdx);
    void refreshFilterCDFCache(duckdb::ClientContext &context, ChunkDefinitionID filter_cdf_id);
    void requestIOForDoubleBuffering(duckdb::ClientContext &context);
    void requestFinalizeIO();

    bool getScanRange(size_t scan_size, idx_t& scan_start_offset, idx_t& scan_end_offset);
    bool getScanRange(size_t scan_size, idx_t idx_in_extent, idx_t& scan_start_offset, idx_t& scan_end_offset);
    bool getScanRange(duckdb::ClientContext &context, ChunkDefinitionID filter_cdf_id, Value &filterValue,
                    size_t scan_size, idx_t& scan_start_offset, idx_t& scan_end_offset);
    bool getScanRange(duckdb::ClientContext &context, ChunkDefinitionID filter_cdf_id, duckdb::Value &l_filterValue, duckdb::Value &r_filterValue,
                    bool l_inclusive, bool r_inclusive, size_t scan_size, idx_t& scan_start_offset, idx_t& scan_end_offset);
    void selVectorToRowIdxs(SelectionVector& sel, size_t sel_size, vector<idx_t>& row_idxs, idx_t offset);
    void getValidOutputMask(vector<idx_t> &output_column_idxs, vector<bool>& valid_output_mask);
    void findMatchedRowsEQFilter(CompressionHeader& comp_header, idx_t col_idx, idx_t scan_start_offset, idx_t scan_end_offset,
                                Value &filterValue, vector<idx_t>& matched_row_idxs);
    void findMatchedRowsRangeFilter(CompressionHeader& comp_header, idx_t col_idx, idx_t scan_start_offset, idx_t scan_end_offset,
                                Value &l_filterValue, Value &r_filterValue, bool l_inclusive, bool r_inclusive, vector<idx_t>& matched_row_idxs);
    void referenceRows(DataChunk &output, ExtentID output_eid, size_t scan_size, vector<idx_t> &output_column_idxs, idx_t scan_begin_offset, idx_t scan_end_offset);
    bool copyMatchedRowsToBuffer(CompressionHeader& comp_header, vector<idx_t>& matched_row_idxs, vector<idx_t> &output_column_idxs, ExtentID &output_eid, FilteredChunkBuffer &output);
    void copyMatchedRows(CompressionHeader& comp_header, vector<idx_t>& matched_row_idxs, vector<idx_t> &output_column_idxs, ExtentID &output_eid, DataChunk &output);
    bool inclusiveAwareRangePredicateCheck(Value &l_filterValue, Value &r_filterValue, bool l_inclusive, bool r_inclusive, Value &filterValue);

    inline bool doFilterBuffer(size_t scan_size, size_t num_filtered_tuples) {
        if (!is_filter_buffering_enabled) {
            return false;
        }
        else {
            return (double)num_filtered_tuples / scan_size < FILTER_BUFFERING_THRESHOLD;
        }
    }

    inline size_t getNumReferencedRows(size_t scan_size) {
        size_t remain_data_size = num_tuples_in_current_extent(toggle) - (current_idx_in_this_extent * scan_size);
        return std::min((size_t) scan_size, remain_data_size);
    }

    void sliceFilteredRows(DataChunk& input, DataChunk &output, idx_t scan_start_offset, vector<idx_t> matched_row_idxs);
    void sliceFilteredRows(DataChunk& input, DataChunk &output, SelectionVector& sel, size_t sel_size);

private:
    void ClaimBuffer(ExtentID eid, int buf_idx) {
        cur_io_[buf_idx] = io_cache ? io_cache->Claim(eid) : &local_io_[buf_idx];
    }
    inline io_cdf_ids &io_requested_cdf_ids(int t) { return cur_io_[t]->cdf_ids; }
    inline io_buf_ptrs &io_requested_buf_ptrs(int t) { return cur_io_[t]->buf_ptrs; }
    inline io_buf_sizes &io_requested_buf_sizes(int t) { return cur_io_[t]->buf_sizes; }
    inline num_tuple &num_tuples_in_current_extent(int t) { return cur_io_[t]->num_tuples; }

    vector<ExtentID> ext_ids_to_iterate;
    DataChunk* data_chunks[MAX_NUM_DATA_CHUNKS];
    IOCacheEntry local_io_[MAX_NUM_DATA_CHUNKS];
    IOCacheEntry *cur_io_[MAX_NUM_DATA_CHUNKS];
    vector<LogicalType> ext_property_type;
    vector<vector<LogicalType>> ext_property_types;
    vector<vector<idx_t>> target_idxs;
    vector<idx_t> target_idx;
    vector<idx_t>* target_idx_per_eid;
    vector<uint32_t> src_data_seqnos;
    idx_t current_idx_in_this_extent;
    idx_t current_idx;
    idx_t max_idx;
    ExtentID current_eid = (ExtentID)std::numeric_limits<uint32_t>::max();
    int num_data_chunks;
    int toggle;
    int target_idxs_offset = 0;
    bool support_double_buffering;
    bool is_initialized = false;
    bool is_rewinded = false;
    bool is_filter_buffering_enabled = true;
    // PropertySchemaCatalogEntry the iterator is bound to. Set by Initialize
    // overloads that take a PS; nullptr for the parallel/single-extent path
    // unless the caller passes one. ID-reconstruct vs ENDPOINT_REF stored-value
    // emit consults this entry's column kinds — when nullptr, callers fall
    // back to the legacy "first ID-typed column is system" behavior.
    PropertySchemaCatalogEntry *ps_cat_entry = nullptr;
    vector<idx_t> last_output_row_offsets_;
    ExtentID last_output_extent_id_ =
        (ExtentID)std::numeric_limits<uint32_t>::max();

    IOCacheEntry *LoadSeekExtent(duckdb::ClientContext &context, ExtentID eid,
                                 idx_t mapping_idx);

    vector<IOCacheEntry *> batch_entries_;
    vector<uint32_t> batch_group_sizes_;
    vector<uint32_t> batch_tgt_rows_;
    vector<uint32_t> batch_src_rows_;

    // Optimization
    IOCache *io_cache;
    ChunkDefinitionID cached_filter_cdf_id_ =
        std::numeric_limits<ChunkDefinitionID>::max();
    bool cached_minmax_exist_ = false;
    idx_t cached_num_entries_in_column_ = 0;
    std::vector<minmax_t> cached_minmax_;
};

} // namespace duckdb

#endif
