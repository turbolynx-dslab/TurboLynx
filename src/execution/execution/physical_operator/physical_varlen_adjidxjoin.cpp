
#include "common/typedef.hpp"

#include "execution/physical_operator/physical_varlen_adjidxjoin.hpp"
#include "storage/extent/extent_iterator.hpp"
#include "storage/extent/adjlist_iterator.hpp"
#include "common/types/selection_vector.hpp"
#include "planner/joinside.hpp"

#include <string>
#include <unordered_set>
#include "icecream.hpp"

#include "execution/isomorphism_checker.hpp"

namespace duckdb {
}
namespace turbolynx {
}
namespace duckdb {
    using namespace turbolynx;
}
namespace turbolynx {
using namespace duckdb;

#define CHECK_ISOMORPHISM

// PhysicalVarlenAdjIdxJoin::PhysicalVarlenAdjIdxJoin(Schema& sch,
// 	std::string srcName, LabelSet srcLabelSet, LabelSet edgeLabelSet, ExpandDirection expandDir, LabelSet tgtLabelSet, JoinType join_type, bool load_eid, bool enumerate)
// 	: PhysicalVarlenAdjIdxJoin(sch, srcName, srcLabelSet, edgeLabelSet, expandDir, tgtLabelSet, join_type, move(vector<JoinCondition>()), load_eid, enumerate) { }

// PhysicalVarlenAdjIdxJoin::PhysicalVarlenAdjIdxJoin(Schema& sch,
// 	std::string srcName, LabelSet srcLabelSet, LabelSet edgeLabelSet, ExpandDirection expandDir, LabelSet tgtLabelSet, JoinType join_type, vector<JoinCondition> remaining_conditions_p, bool load_eid, bool enumerate)
// 	: CypherPhysicalOperator(sch), srcName(srcName), srcLabelSet(srcLabelSet), edgeLabelSet(edgeLabelSet), expandDir(expandDir), tgtLabelSet(tgtLabelSet), join_type(join_type), remaining_conditions(move(remaining_conditions_p)), load_eid(load_eid), enumerate(enumerate) {

// 	// operator rules
// 	bool check = (enumerate) ? true : (!load_eid);
// 	D_ASSERT( check && "load_eid should be set to false(=not returning edge ids) when `enumerate` set to `false` (=range)");

// 	D_ASSERT( enumerate == true && "always enumerate for now");
// 	D_ASSERT( srcLabelSet.size() == 1 && "src label shuld be assigned and be only one for now");
// 	D_ASSERT( tgtLabelSet.size() <= 1 && "no multiple targets"); // TODO needs support from the storage
// 	D_ASSERT( edgeLabelSet.size() <= 1 && "no multiple edges Storage API support needed"); // TODO needs support from the storage
// 	D_ASSERT( enumerate && "need careful debugging on range mode"); // TODO needs support from the storage

// 	D_ASSERT( remaining_conditions.size() == 0 && "currently not support additional predicate" );
// }




//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
class VarlenAdjIdxJoinState : public OperatorState {
public:
	explicit VarlenAdjIdxJoinState() {
		resetForNewInput();
		dfs_it = new DFSIterator();
#ifdef CHECK_ISOMORPHISM
		iso_checker = new ExactIsoChecker();
#else
		iso_checker = nullptr;
#endif
		rhs_sel.Initialize(STANDARD_VECTOR_SIZE);
		src_nullity.resize(STANDARD_VECTOR_SIZE);
		join_sizes.resize(STANDARD_VECTOR_SIZE);
		// total_join_size.resize(STANDARD_VECTOR_SIZE);
	}
	//! Called when starting processing for new chunk
	inline void resetForNewInput() {
		resetForMoreOutput();
		first_fetch = false;
		equi_join_finished = false;
		left_join_finished = false;
		lhs_idx = 0;
		adj_idx = 0;
		rhs_idx = 0;
		left_lhs_idx = 0;
		// init vectors
		// adj_col_idxs.clear();
		// adj_col_types.clear();
		// std::fill(total_join_size.begin(), total_join_size.end(), 0);
	}
	inline void resetForMoreOutput() {
		output_idx = 0;
	}
public:
	// operator data
	DFSIterator *dfs_it;
	IsoMorphismChecker *iso_checker;

	// dfs related data
	int cur_lv;
	int start_lv;
	int end_lv;
	vector<uint64_t> current_path;
	// vector<uint64_t> current_path_vid; // temp
	bool first_time_in_this_loop = true;

	// initialize rest of operator members
	idx_t srcColIdx;
	idx_t edgeColIdx;
	idx_t tgtColIdx;

	// input -> output col mapping information
	vector<uint32_t> outer_col_map;
	vector<uint32_t> inner_col_map;
	
	// join state - initialized per output
	idx_t output_idx;

	// join state - initialized per new input, and updated while processing
	bool first_fetch;
	idx_t lhs_idx;
	idx_t adj_idx;
	idx_t rhs_idx;
	bool equi_join_finished;	// equi join match finished
	idx_t left_lhs_idx;
	bool left_join_finished;	// when equi_finished and then left finished
	
	// join metadata - initialized per new input
	vector<int> adj_col_idxs;					// indices
	vector<LogicalType> adj_col_types;			// forward_adj or backward_adj
	unordered_set<uint16_t> src_partition_ids;	// partitions where adj lists are stored; others are terminal

	// join data - initialized per new input
	vector<bool> src_nullity;
	vector<vector<idx_t>> join_sizes;	// can be multiple when there are many adjlists per vertex
	vector<idx_t> total_join_size;		// sum of entries in join_sizes

	SelectionVector rhs_sel;	// used multiple times without initialization
};

unique_ptr<OperatorState> PhysicalVarlenAdjIdxJoin::GetOperatorState(ExecutionContext &context) const {
	return make_unique<VarlenAdjIdxJoinState>( );
}

// helper functions
inline ExpandDirection adjListLogicalTypeToExpandDir(LogicalType adjType) {
	return adjType == LogicalType::FORWARD_ADJLIST ? ExpandDirection::OUTGOING : ExpandDirection::INCOMING;
}

void PhysicalVarlenAdjIdxJoin::ProcessSemiAntiJoin(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {
}

void PhysicalVarlenAdjIdxJoin::GetJoinMatches(ExecutionContext& context, DataChunk &input, OperatorState &lstate) const {
	auto &state = (VarlenAdjIdxJoinState &)lstate; 
	
	// check nullity (debug note : this part has no burden in execution time. verified!)
	switch( input.data[state.srcColIdx].GetVectorType() ) {
		case VectorType::DICTIONARY_VECTOR: {
			const auto &sel_vector = DictionaryVector::SelVector(input.data[state.srcColIdx]);
			const auto &child = DictionaryVector::Child(input.data[state.srcColIdx]);
			for(idx_t i = 0; i < input.size(); i++) {
				state.src_nullity[i] = FlatVector::IsNull( child, sel_vector.get_index(i) );
			}
			break;
		}
		case VectorType::CONSTANT_VECTOR: {
			auto is_null = ConstantVector::IsNull(input.data[state.srcColIdx]);
			for (idx_t i = 0; i < input.size(); i++) {
				state.src_nullity[i] = is_null;
			}
			break;
		}
		default: {
			for (idx_t i = 0; i < input.size(); i++) {
				state.src_nullity[i] = FlatVector::IsNull(input.data[state.srcColIdx], i);
			}
			break;
		}
	}

	// fill in adjacency col info (accumulate across all index OIDs)
	if (state.adj_col_idxs.empty()) {
		for (uint64_t oid : adjidx_obj_ids) {
			context.client->graph_storage_wrapper->getAdjColIdxs(
				(idx_t)oid, state.adj_col_idxs, state.adj_col_types);
			uint16_t src_pid = context.client->graph_storage_wrapper->getAdjListSrcPartitionId((idx_t)oid);
			state.src_partition_ids.insert(src_pid);
		}
	}
	
	// resize join sizes using adj_col_idxs
	auto adjColCnt = state.adj_col_idxs.size();
	for( auto& v: state.join_sizes) {
		v.resize(adjColCnt);
	}
}

// TODO function handlepredicates
	// x has label blabla
	// x == x
	// x != x
	// x in y

void PhysicalVarlenAdjIdxJoin::ProcessEquiJoin(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {
	auto &state = (VarlenAdjIdxJoinState &)lstate;

// icecream::ic.enable();
// IC(input.size());
// if (input.size() > 0) {
// 	IC(input.ToString(std::min((idx_t)10, input.size())));
// }
// icecream::ic.disable();
	
	
}

void PhysicalVarlenAdjIdxJoin::ProcessLeftJoin(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {
	D_ASSERT(false);
}


OperatorResultType PhysicalVarlenAdjIdxJoin::ExecuteNaiveInput(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {
	auto &state = (VarlenAdjIdxJoinState &)lstate; 
 
	if( !state.first_fetch ) {
		// values used while processing
		//state.srcColIdx = schema.getColIdxOfKey(srcName);
		state.srcColIdx = sid_col_idx;
		state.cur_lv = 0;
		state.start_lv = static_cast<int>(std::min(min_length, (uint64_t)INT32_MAX));
		state.end_lv = static_cast<int>(std::min(max_length, (uint64_t)INT32_MAX));

#ifdef CHECK_ISOMORPHISM
		state.iso_checker->initialize(max_length - min_length + 1);
#endif

		if (load_eid) {
			state.tgtColIdx = inner_col_map[0];
			state.edgeColIdx = inner_col_map[1];
		} else {
			state.edgeColIdx = -1;
			state.tgtColIdx = inner_col_map[0];
		}
		
		state.outer_col_map = move(outer_col_map);
		state.inner_col_map = move(inner_col_map);
		// Get join matches (sizes) for the LHS. Initialized one time per LHS
		GetJoinMatches(context, input, lstate);
		state.first_fetch = true;
	}

	if( join_type == JoinType::SEMI || join_type == JoinType::ANTI ) {
		// these joins can be processed in single call. explicitly process them and return
		ProcessSemiAntiJoin(context, input, chunk, lstate);
		return OperatorResultType::NEED_MORE_INPUT;
	}

	const bool isProcessingTerminated =
		state.equi_join_finished && (join_type != JoinType::LEFT || state.left_join_finished);
	if ( isProcessingTerminated ) {	
		// initialize state for next chunk
		state.resetForNewInput();

		return OperatorResultType::NEED_MORE_INPUT;
	} else {
		if( ! state.equi_join_finished ) {
			ProcessVarlenEquiJoin(context, input, chunk, lstate);
			// update states
			state.resetForMoreOutput();
			if( state.lhs_idx >= input.size() ) {
				state.equi_join_finished = true;
			}
			return OperatorResultType::HAVE_MORE_OUTPUT;
		} else {
			D_ASSERT( ! state.left_join_finished );
			ProcessLeftJoin(context, input, chunk, lstate);
			// update states
			state.resetForMoreOutput();
			if( state.left_lhs_idx >= input.size() ) {
				state.left_join_finished = true;
			}
			return OperatorResultType::HAVE_MORE_OUTPUT;
		}
	}
}

OperatorResultType PhysicalVarlenAdjIdxJoin::ExecuteRangedInput(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {
	D_ASSERT(false);
	return OperatorResultType::NEED_MORE_INPUT;
}

OperatorResultType PhysicalVarlenAdjIdxJoin::Execute(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {
	// 230303 
	return ExecuteNaiveInput(context, input, chunk, lstate);
}

void PhysicalVarlenAdjIdxJoin::ProcessVarlenEquiJoin(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {
	auto &state = (VarlenAdjIdxJoinState &)lstate;
	Vector& src_vid_column_vector = input.data[state.srcColIdx];	// can be dictionaryvector
	uint64_t num_found_paths;

	while (state.lhs_idx < input.size()) {
		uint64_t& src_vid = getIdRefFromVector(src_vid_column_vector, state.lhs_idx);
		num_found_paths = VarlengthExpand_internal(context, src_vid, chunk, state, STANDARD_VECTOR_SIZE - state.output_idx);

		// set sel vector on lhs	// TODO apply filter predicates
		for(uint64_t tmp_output_idx = state.output_idx ; tmp_output_idx < state.output_idx + num_found_paths ; tmp_output_idx++ ) {
			state.rhs_sel.set_index(tmp_output_idx, state.lhs_idx);
		}

		state.output_idx += num_found_paths;
		if (state.output_idx >= STANDARD_VECTOR_SIZE) break;
		state.lhs_idx++;
		state.first_time_in_this_loop = true;
	}

	// chunk determined. now fill in lhs using slice operation
	D_ASSERT(input.ColumnCount() == state.outer_col_map.size());
	for (idx_t colId = 0; colId < input.ColumnCount(); colId++) {
		if( state.outer_col_map[colId] != std::numeric_limits<uint32_t>::max() ) {
			// when outer col map marked uint32_max, do not return
			D_ASSERT(state.outer_col_map[colId] < chunk.ColumnCount());
			chunk.data[state.outer_col_map[colId]].Slice(input.data[colId], state.rhs_sel, state.output_idx);
		}
	}

	D_ASSERT (state.output_idx <= STANDARD_VECTOR_SIZE);
	chunk.SetCardinality(state.output_idx);

// icecream::ic.enable();
// IC(chunk.size());
// if (chunk.size() > 0) {
// 	IC(chunk.ToString(std::min((idx_t)10, chunk.size())));
// }
// icecream::ic.disable();
}

uint64_t PhysicalVarlenAdjIdxJoin::VarlengthExpand_internal(ExecutionContext& context, uint64_t src_vid, DataChunk &chunk, OperatorState &lstate, int64_t remaining_output) const {
	auto &state = (VarlenAdjIdxJoinState &)lstate;
	uint64_t new_tgt_id, new_edge_id, num_found_paths = 0;
	uint64_t *tgt_adj_column, *eid_adj_column;
    
	// fprintf(stdout, "Start to iterate vid %ld, remaining = %ld\n", src_vid, remaining_output);
	// state.current_path.clear();
	// state.current_path.push_back(src_vid);

	tgt_adj_column = (uint64_t *)chunk.data[state.tgtColIdx].GetData();	// always flatvector[ID]. so ok to access directly
	// eid_adj_column = (uint64_t *)chunk.data[state.edgeColIdx].GetData();	// always flatvector[ID]. so ok to access directly
	eid_adj_column = nullptr; // jhha: this eid column is not used.

	if (state.first_time_in_this_loop) {
		state.first_time_in_this_loop = false;
		// Build is_fwd flags from adj_col_types
		vector<bool> adj_col_is_fwds;
		for (auto &t : state.adj_col_types) {
			adj_col_is_fwds.push_back(t == LogicalType::FORWARD_ADJLIST);
		}
		state.dfs_it->initialize(*context.client, src_vid, state.adj_col_idxs, adj_col_is_fwds, state.src_partition_ids);
		if (state.cur_lv >= state.start_lv) {
			bool src_is_terminal = state.src_partition_ids.empty() ||
			                       state.src_partition_ids.count((uint16_t)(src_vid >> 48)) == 0;
			bool dst_ok = dst_partition_ids.empty() ||
			              dst_partition_ids.count((uint16_t)(src_vid >> 48)) > 0;
				if (src_is_terminal && dst_ok) {
					addNewPathToOutput(tgt_adj_column, eid_adj_column,
					                   state.output_idx + num_found_paths,
					                   state.current_path, src_vid, 0);
					if (++num_found_paths == remaining_output) return num_found_paths;
				}
			}
	}

	// if (state.start_lv == 0) {
	// 	addNewPathToOutput(tgt_adj_column, eid_adj_column, state.output_idx + num_found_paths, state.current_path, src_vid);
	// 	num_found_paths++;
	// }

    while (true) {
        // check if current lv exceeds end_lv
        if (state.cur_lv >= state.end_lv) {
            // reached at end level; go back
            if (state.current_path.empty()) {
                // No more paths to explore (e.g., *0..0 range)
                state.first_time_in_this_loop = true;
                return num_found_paths;
            }
            uint64_t edgeid_to_remove = state.current_path.back();
#ifdef CHECK_ISOMORPHISM
			state.iso_checker->removeFromSet(edgeid_to_remove);
#endif
            state.current_path.pop_back();
			// state.current_path_vid.pop_back(); // temp
            state.cur_lv--;
			state.dfs_it->reduceLevel();
            continue;
        }
        // check if the traversal exit
        if (state.cur_lv < 0) { break; }
        // check if there is more edge to traverse
        bool has_more_edge = state.dfs_it->getNextEdge(*context.client, state.cur_lv, new_tgt_id, new_edge_id);
        if (!has_more_edge) {
			if (state.cur_lv == 0) break;
            uint64_t edgeid_to_remove = state.current_path.back();
#ifdef CHECK_ISOMORPHISM
            state.iso_checker->removeFromSet(edgeid_to_remove);
#endif
            state.current_path.pop_back();
			// state.current_path_vid.pop_back(); //temp
            state.cur_lv--;
            continue;
        }
#ifdef CHECK_ISOMORPHISM
        // check edge isomorphism
        if (state.iso_checker->checkIsoMorphism(new_edge_id)) {
            state.dfs_it->reduceLevel();
            continue;
        }
		state.iso_checker->addToSet(new_edge_id);
#endif
        // traverse more
        state.cur_lv++;
		state.current_path.push_back(new_edge_id);
		// state.current_path_vid.push_back(new_tgt_id); // temp

		// fprintf(stdout, "src_vid %ld, cur_lv %d, Path: [", src_vid, state.cur_lv);
		// for (int path_idx = 0; path_idx < state.current_path.size(); path_idx++) {
		// 	fprintf(stdout, "%ld, ", state.current_path[path_idx]);
		// }
		// fprintf(stdout, "]\n");

        // Output if within level range [start_lv, end_lv) and matches dst partition filter.
        // new_tgt_id from delta-resident adj lists is a synthetic logical_id
        // whose top 16 bits are the prefix `0x7F00` rather than a real
        // partition id. Resolve to the physical_id first so the partition
        // membership checks below see real partition ids — without this,
        // every delta-resident hop counts as "terminal" / fails dst_ok.
        uint64_t resolved_tgt_id = new_tgt_id;
        if (context.client && context.client->db) {
            auto pid = context.client->db->delta_store.ResolvePid(new_tgt_id);
            if (pid != 0) resolved_tgt_id = pid;
        }
        uint16_t resolved_tgt_partition = (uint16_t)(resolved_tgt_id >> 48);
        if (state.cur_lv >= state.start_lv) {
            bool dst_ok = dst_partition_ids.empty() ||
                          dst_partition_ids.count(resolved_tgt_partition) > 0;
            if (dst_ok) {
                addNewPathToOutput(tgt_adj_column, eid_adj_column,
                                   state.output_idx + num_found_paths,
                                   state.current_path, new_tgt_id,
                                   new_edge_id);
                if (++num_found_paths == remaining_output) break;
            }
        }

        // If target is in a terminal partition (no adj lists to recurse into),
        // undo the level increment and do not recurse further.
        if (!state.src_partition_ids.empty()) {
            bool is_terminal = (state.src_partition_ids.count(resolved_tgt_partition) == 0);
            if (is_terminal) {
#ifdef CHECK_ISOMORPHISM
                state.iso_checker->removeFromSet(new_edge_id);
#endif
                state.current_path.pop_back();
                state.dfs_it->reduceLevel();
                state.cur_lv--;
                continue;
            }
        }
    }

    return num_found_paths;
}

void PhysicalVarlenAdjIdxJoin::addNewPathToOutput(
    uint64_t *tgt_adj_column, uint64_t *eid_adj_column, uint64_t output_idx,
    vector<uint64_t> &current_path, uint64_t new_tgt_id,
    uint64_t new_edge_id) const {
	// Var-length expansion binds the endpoint vertex to the target column.
	tgt_adj_column[output_idx] = new_tgt_id;
	if (load_eid && eid_adj_column) {
		eid_adj_column[output_idx] = new_edge_id;
	}
}

std::string PhysicalVarlenAdjIdxJoin::ParamsToString() const {
	std::string result = "";
	result += "adjidx_obj_ids=[";
	for (size_t i = 0; i < adjidx_obj_ids.size(); i++) {
		if (i > 0) result += ",";
		result += std::to_string(adjidx_obj_ids[i]);
	}
	result += "], sid_col_idx=" + std::to_string(sid_col_idx) + ", ";
	result += "min_hop=" + std::to_string(min_length) + ",";
	result += "max_hop=" + std::to_string(max_length);
	return result;
}

std::string PhysicalVarlenAdjIdxJoin::ToString() const {
	return "VarlenAdjIdxJoin";
}


} // namespace turbolynx
