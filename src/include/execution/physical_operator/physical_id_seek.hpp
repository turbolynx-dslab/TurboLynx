#pragma once
#include "common/typedef.hpp"

#include "common/enums/join_type.hpp"
#include "common/types/value.hpp"
#include "execution/expression_executor.hpp"
#include "execution/physical_operator/cypher_physical_operator.hpp"

namespace duckdb {

typedef double NullRatio;
typedef double Skewness;

class PartialSchema;
class IdSeekState;
class PhysicalIdSeek : public CypherPhysicalOperator {
   private:
    enum class OutputFormat { UNIONALL = 0, ROW = 1 };

   public:
    // Constructor without filter pushdown
    PhysicalIdSeek(Schema &sch, uint64_t id_col_idx, vector<uint64_t> oids,
                   vector<vector<uint64_t>> projection_mapping,
                   vector<uint32_t> &outer_col_map,
                   vector<vector<uint32_t>> &inner_col_maps,
                   vector<uint32_t> &union_inner_col_map,
                   vector<vector<uint64_t>> scan_projection_mapping,
                   vector<vector<duckdb::LogicalType>> scan_types,
                   bool force_output_union,
                   JoinType join_type = JoinType::INNER,
                   size_t num_outer_schemas = 1);

    // Constructor with filter pushdown
    PhysicalIdSeek(Schema &sch, uint64_t id_col_idx, vector<uint64_t> oids,
                   vector<vector<uint64_t>> projection_mapping,
                   vector<uint32_t> &outer_col_map,
                   vector<vector<uint32_t>> &inner_col_maps,
                   vector<uint32_t> &union_inner_col_map,
                   vector<vector<uint64_t>> scan_projection_mapping,
                   vector<vector<duckdb::LogicalType>> scan_types,
                   vector<vector<unique_ptr<Expression>>> &predicates,
                   vector<vector<idx_t>> &pred_col_idxs_per_schema,
                   bool force_output_union,
                   JoinType join_type = JoinType::INNER,
                   size_t num_outer_schemas = 1);
    ~PhysicalIdSeek()
    {
        for (auto &chunk : tmp_chunks) {
            chunk.reset();
        }
    }

   public:
    virtual void InitializeOutputChunks(
        std::vector<unique_ptr<DataChunk>> &output_chunks,
        Schema &output_schema, idx_t idx) override;
    unique_ptr<OperatorState> GetOperatorState(
        ExecutionContext &context) const override;
    OperatorResultType Execute(ExecutionContext &context, DataChunk &input,
                               DataChunk &chunk,
                               OperatorState &state) const override;
    OperatorResultType ExecuteInner(ExecutionContext &context, DataChunk &input,
                                    DataChunk &chunk,
                                    OperatorState &state) const;
    OperatorResultType ExecuteLeft(ExecutionContext &context, DataChunk &input,
                                   DataChunk &chunk,
                                   OperatorState &state) const;
    std::string ParamsToString() const override;
    std::string ToString() const override;

    // internal functions
    void initializeSeek(ExecutionContext &context, DataChunk &input,
                        DataChunk &chunk, IdSeekState &state, idx_t nodeColIdx,
                        vector<ExtentID> &target_eids,
                        vector<vector<uint32_t>> &target_seqnos_per_extent,
                        vector<idx_t> &mapping_idxs) const;
    void doSeekColumnar(ExecutionContext &context, DataChunk &input,
                        DataChunk &chunk, OperatorState &lstate,
                        vector<ExtentID> &target_eids,
                        vector<vector<uint32_t>> &target_seqnos_per_extent,
                        vector<idx_t> &mapping_idxs, idx_t &output_size) const;
    void doSeekRowMajor(ExecutionContext &context, DataChunk &input,
                          DataChunk &chunk, OperatorState &lstate,
                          vector<ExtentID> &target_eids,
                          vector<vector<uint32_t>> &target_seqnos_per_extent,
                          vector<idx_t> &mapping_idxs, idx_t &output_idx) const;
    OperatorResultType referInputChunk(DataChunk &input, DataChunk &chunk,
                                       OperatorState &lstate,
                                       idx_t output_size) const;
    OperatorResultType referInputChunkLeft(DataChunk &input, DataChunk &chunk,
                                           OperatorState &lstate,
                                           idx_t output_idx) const;

   private:
    void markInvalidForUnseekedValues(
        DataChunk &chunk, IdSeekState &state, vector<ExtentID> &target_eids,
        vector<vector<uint32_t>> &target_seqnos_per_extent,
        vector<idx_t> &mapping_idxs) const;
    void nullifyValuesForPrunedExtents(
        DataChunk &chunk, IdSeekState &state,
        size_t num_unpruned_extents,
        vector<vector<uint32_t>> &target_seqnos_per_extent) const;
    void generatePartialSchemaInfos();
    void getOutputTypesForFilteredSeek(vector<LogicalType> &lhs_type,
                                       vector<LogicalType> &scan_type,
                                       vector<LogicalType> &out_type) const;
    void getOutputIdxsForFilteredSeek(idx_t chunk_idx,
                                      vector<uint32_t> &output_col_idx) const;
    void getFilteredTargetSeqno(vector<idx_t> &seqno_to_eid_idx,
                                size_t num_extents, const sel_t *sel_idxs,
                                size_t count,
                                vector<vector<uint32_t>> &out_seqnos) const;
    void fillSeqnoToEIDIdx(size_t num_valid_extents,
                            vector<vector<uint32_t>> &target_seqnos_per_extent,
                           vector<idx_t> &seqno_to_eid_idx) const;
    void fillSeqnoToEIDIdx(vector<vector<uint32_t>> &target_seqnos_per_extent,
                           vector<idx_t> &seqno_to_eid_idx) const;
    void genNonPredColIdxs();
    size_t calculateTotalNulls(
        DataChunk &chunk,
        vector<ExtentID> &target_eids,
        vector<vector<uint32_t>> &target_seqnos_per_extent,
        vector<idx_t> &mapping_idxs) const;
    OutputFormat determineFormatByCostModel(
        IdSeekState &state, bool sort_order_enforced, size_t total_nulls) const;
    void generateOutputColIdxsForOuter();
    void generateOutputColIdxsForInner();
    void getOutputColIdxsForInner(idx_t extentIdx, vector<idx_t> &mapping_idxs,
                                  vector<idx_t> &output_col_idx) const;
    void fillOutSizePerSchema(IdSeekState &state, vector<ExtentID> &target_eids,
                             vector<vector<uint32_t>> &target_seqnos_per_extent,
                             vector<idx_t> &mapping_idxs) const;
    void getUnionScanTypes();
    void buildExpressionExecutors(vector<vector<unique_ptr<Expression>>> &predicates);
    void markInvalidForColumnsToUnseek(DataChunk &chunk, vector<ExtentID> &target_eids, 
                                    vector<idx_t> &mapping_idxs) const;
    void setupSchemaValidityMasks();

    public:
    bool HasFilterPushdown() const { return do_filter_pushdown; }

    private:
    inline bool isInnerColIdx(idx_t col_idx) const
    {
        if (std::find(outer_output_col_idxs.begin(),
                      outer_output_col_idxs.end(),
                      col_idx) != outer_output_col_idxs.end()) {
            return true;
        }
        return false;
    }

    // parameters
    uint64_t id_col_idx;
    mutable vector<uint64_t> oids;
    mutable vector<LogicalType> union_scan_type;
    mutable vector<vector<LogicalType>> scan_types;  // types scan
    mutable vector<vector<uint64_t>> projection_mapping;
    mutable vector<vector<uint64_t>> scan_projection_mapping;

    // num_tuples_per_schema and target_eids moved to IdSeekState (per-thread).
    vector<uint32_t> outer_col_map;
    vector<vector<uint32_t>> inner_col_maps;
    vector<uint32_t> union_inner_col_map;
    vector<uint32_t> union_inner_col_map_wo_id;
    vector<idx_t> outer_output_col_idxs;
    vector<vector<uint32_t>> inner_output_col_idxs;
    vector<PartialSchema> partial_schemas;
    vector<ValidityMask> schema_validity_masks;
    // target_eids moved to IdSeekState (per-thread)
    bool force_output_union = true;
    idx_t num_outer_schemas = 1;
    idx_t num_inner_schemas = 1;
    idx_t num_total_schemas = 1;
    idx_t out_id_col_idx;

    // filter processing
    bool do_filter_pushdown;
    vector<unique_ptr<Expression>> expressions;
    mutable vector<ExpressionExecutor> executors;
    vector<unique_ptr<DataChunk>> tmp_chunks;
    mutable vector<bool> is_tmp_chunk_initialized_per_schema;
    mutable vector<vector<idx_t>> pred_col_idxs_per_schema;
    mutable vector<vector<idx_t>> non_pred_col_idxs_per_schema;

    JoinType join_type;
};

}  // namespace duckdb