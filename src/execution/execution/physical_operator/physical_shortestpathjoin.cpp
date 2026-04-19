
#include "execution/physical_operator/physical_shortestpathjoin.hpp"
#include "common/typedef.hpp"
#include "common/output_util.hpp"
#include "spdlog/spdlog.h"

namespace duckdb {
}
namespace turbolynx {
}
namespace duckdb {
    using namespace turbolynx;
}
namespace turbolynx {
using namespace duckdb;

class ShortestPathState : public OperatorState {
public:
	explicit ShortestPathState() {
		srtp_iter = new ShortestPathAdvancedIterator();
		input_idx = 0;
		output_idx = 0;
		is_initialized = false;
	 }

	 ~ShortestPathState()
	 {
		 delete srtp_iter;
	 }

	 void resetForMoreInput() {
		input_idx = 0;
		output_idx = 0;
	 }

	 void resetForMoreOutput() {
		output_idx = 0;
	 }
public:
    ShortestPathAdvancedIterator *srtp_iter;

	idx_t input_idx;
	idx_t output_idx;

	bool is_initialized;

	// These are setted only once.
	// Due to const constraint of Execute function, we cannot set these in Execute function.
	vector<int> adj_col_idxs;
	vector<LogicalType> adj_col_types;	
};

//===--------------------------------------------------------------------===//
// Execute
//===--------------------------------------------------------------------===//

unique_ptr<OperatorState> PhysicalShortestPathJoin::GetOperatorState(
    ExecutionContext &context) const
{
    return make_unique<ShortestPathState>();
}

OperatorResultType PhysicalShortestPathJoin::Execute(ExecutionContext &context,
                                                     DataChunk &input,
                                                     DataChunk &chunk,
                                                     OperatorState &state) const
{
	auto &srtp_state = (ShortestPathState &)state;
	spdlog::info("[ShortestPath] input_cols={} input_size={} chunk_cols={} src_id_idx={} dst_id_idx={} output_idx={} input_col_map.size={}",
	             input.ColumnCount(), input.size(), chunk.ColumnCount(),
	             src_id_idx, dst_id_idx, output_idx, input_col_map.size());
	for (idx_t i = 0; i < input_col_map.size(); i++) {
		spdlog::info("[ShortestPath]   input_col_map[{}]={}", i, (int)input_col_map[i]);
	}
	if(!srtp_state.is_initialized) {
		context.client->graph_storage_wrapper->getAdjColIdxs((idx_t)adjidx_obj_id_fwd, srtp_state.adj_col_idxs, srtp_state.adj_col_types);
		context.client->graph_storage_wrapper->getAdjColIdxs((idx_t)adjidx_obj_id_bwd, srtp_state.adj_col_idxs, srtp_state.adj_col_types);
		srtp_state.is_initialized = true;
		D_ASSERT(srtp_state.adj_col_idxs.size() == 2);
		D_ASSERT(srtp_state.adj_col_types.size() == 2);
		D_ASSERT(srtp_state.adj_col_types[0] == LogicalType::FORWARD_ADJLIST);
		D_ASSERT(srtp_state.adj_col_types[1] == LogicalType::BACKWARD_ADJLIST);
	}

	Vector &src_id_vec = input.data[src_id_idx];
	Vector &dst_id_vec = input.data[dst_id_idx];

	// DEBUG: dump all input columns' first row value and types
	for (idx_t c = 0; c < input.ColumnCount(); c++) {
		Vector &v = input.data[c];
		auto type_str = v.GetType().ToString();
		auto vtype_str = std::to_string((int)v.GetVectorType());
		uint64_t raw = 0;
		if (input.size() > 0) {
			raw = getIdRefFromVector(v, 0);
		}
		spdlog::info("[ShortestPath] input[{}] type={} vtype={} row0_raw={}",
		             c, type_str, vtype_str, raw);
	}

	while (srtp_state.input_idx < input.size()) {
		spdlog::info("[ShortestPath] loop input_idx={}", srtp_state.input_idx);
		uint64_t src_id = getIdRefFromVector(src_id_vec, srtp_state.input_idx);
		uint64_t dst_id = getIdRefFromVector(dst_id_vec, srtp_state.input_idx);
		spdlog::info("[ShortestPath]   src_id={} dst_id={} adj_fwd={} adj_bwd={} lb={} ub={}",
		             src_id, dst_id, srtp_state.adj_col_idxs[0], srtp_state.adj_col_idxs[1],
		             lower_bound, upper_bound);
		std::vector<uint64_t> edges;
		std::vector<uint64_t> nodes;
		srtp_state.srtp_iter->initialize(*context.client, src_id, dst_id, srtp_state.adj_col_idxs[0], srtp_state.adj_col_idxs[1],
										lower_bound, upper_bound);
		spdlog::info("[ShortestPath]   initialize done");
		bool found = srtp_state.srtp_iter->getShortestPath(*context.client, edges, nodes);
		spdlog::info("[ShortestPath]   getShortestPath found={} edges.size={} nodes.size={}",
		             found, edges.size(), nodes.size());
		if(found) {
			D_ASSERT(edges.size() == nodes.size() - 1);
			std::vector<Value> path_vec(edges.size() + nodes.size());
			// node-edge-node-edge-...-node
			for (size_t i = 0; i < nodes.size(); i++) {
				path_vec[i * 2] = Value::UBIGINT(nodes[i]);
			}
			for (size_t i = 0; i < edges.size(); i++) {
				path_vec[i * 2 + 1] = Value::UBIGINT(edges[i]);
			}
			Value path_val = Value::LIST(path_vec);
			spdlog::info("[ShortestPath]   SetValue output_idx={} chunk_cols={}", srtp_state.output_idx, chunk.ColumnCount());
			chunk.data[output_idx].SetValue(srtp_state.output_idx, path_val);
			srtp_state.output_idx++;
		}
		srtp_state.input_idx++;
	}

	D_ASSERT(input.ColumnCount() == input_col_map.size());
	for (idx_t i = 0; i < input.ColumnCount(); i++) {
		if (input_col_map[i] == std::numeric_limits<uint32_t>::max()) continue;
		chunk.data[input_col_map[i]].Reference(input.data[i]);
	}

	chunk.SetCardinality(srtp_state.output_idx);
	srtp_state.resetForMoreInput();
	return OperatorResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// ETC
//===--------------------------------------------------------------------===//

std::string PhysicalShortestPathJoin::ParamsToString() const
{
    std::string result = "";
    result +=
        "input_col_map.size()=" + std::to_string(input_col_map.size()) + ", ";
    result +=
        "output_idx=" + std::to_string(output_idx) + ", ";
    result += "adjidx_obj_id=" + std::to_string(adjidx_obj_id) + ", ";
    result += "src_id_idx=" + std::to_string(src_id_idx) + ", ";
    result += "dst_id_idx=" + std::to_string(dst_id_idx);
    return result;
}

std::string PhysicalShortestPathJoin::ToString() const
{
    return "ShortestPath";
}

} // namespace turbolynx