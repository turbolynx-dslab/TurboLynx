#ifndef PHYSICAL_VARLEN_ADJIDXJOIN_H
#define PHYSICAL_VARLEN_ADJIDXJOIN_H

#include "common/typedef.hpp"

#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "common/enums/join_type.hpp"
#include "planner/joinside.hpp"

#include <unordered_set>

namespace duckdb {
}
namespace turbolynx {
}
namespace duckdb {
    using namespace turbolynx;
}
namespace turbolynx {
using namespace duckdb;

class PhysicalVarlenAdjIdxJoin: public CypherPhysicalOperator {

public:
    // PhysicalVarlenAdjIdxJoin() {}
    ~PhysicalVarlenAdjIdxJoin() {}

	PhysicalVarlenAdjIdxJoin(Schema& sch, vector<uint64_t> adjidx_obj_ids, JoinType join_type, uint64_t sid_col_idx, bool load_eid,
					   uint64_t min_length, uint64_t max_length, vector<uint32_t> &outer_col_map, vector<uint32_t> &inner_col_map,
					   std::unordered_set<uint16_t> dst_partition_ids = {},
					   // When a CPhysicalVarlenPath wrap folded path
					   // materialization into this op, these tell Execute
					   // where to write the per-row path LIST and how
					   // deeply to fill it. (-1 / 0 = no path column.)
					   int64_t path_col_idx = -1,
					   uint8_t path_use_mode = 0)
		: CypherPhysicalOperator(PhysicalOperatorType::VARLEN_ADJ_IDX_JOIN, sch), adjidx_obj_ids(adjidx_obj_ids), join_type(join_type), sid_col_idx(sid_col_idx), load_eid(load_eid), min_length(min_length), max_length(max_length),
			outer_col_map(move(outer_col_map)), inner_col_map(move(inner_col_map)), dst_partition_ids(std::move(dst_partition_ids)),
			path_col_idx(path_col_idx), path_use_mode(path_use_mode)
		{ }

    // common interface
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	OperatorResultType Execute(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &state) const override;
	// locally used functions
	OperatorResultType ExecuteRangedInput(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const;
	OperatorResultType ExecuteNaiveInput(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const;
	void GetJoinMatches(ExecutionContext& context, DataChunk &input, OperatorState &lstate) const;
	void ProcessSemiAntiJoin(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const;
	void ProcessEquiJoin(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const;
	void ProcessLeftJoin(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const;
	void ProcessVarlenEquiJoin(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const;

	std::string ParamsToString() const override;
	std::string ToString() const override;

private:
    uint64_t VarlengthExpand_internal(ExecutionContext& context, uint64_t src_vid, DataChunk &chunk, OperatorState &lstate, int64_t remaining_output) const;
	void addNewPathToOutput(uint64_t *tgt_adj_column, uint64_t *eid_adj_column, uint64_t output_idx, vector<uint64_t> &current_path, uint64_t new_tgt_id, uint64_t new_edge_id) const;
	void writePathColumnForRow(DataChunk &chunk, uint64_t output_idx, uint64_t src_vid, const vector<uint64_t> &current_path, const vector<uint64_t> &current_path_vid) const;

	uint64_t min_length;
	uint64_t max_length;

	// from adjidxjoin
	vector<uint64_t> adjidx_obj_ids;  // one per edge type
	uint64_t sid_col_idx;	// source id column

	vector<uint32_t> outer_col_map;
	vector<uint32_t> inner_col_map;
	
	JoinType join_type;

	bool load_eid;

	// Destination partition filter for VarLen: only output vertices in these partitions.
	// Empty means no filtering (all output passes).
	std::unordered_set<uint16_t> dst_partition_ids;

	// Path column slot in the output chunk + materialization mode.
	int64_t path_col_idx;
	uint8_t path_use_mode;
};

} // namespace turbolynx

#endif
