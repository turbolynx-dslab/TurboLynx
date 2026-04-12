#include "execution/physical_operator/physical_all_shortestpathjoin.hpp"
#include "common/typedef.hpp"
#include "common/output_util.hpp"
#include "storage/extent/adjlist_iterator.hpp"

namespace duckdb {

class AllShortestPathState : public OperatorState {
public:
    explicit AllShortestPathState() {
        all_srtp_iter = new AllShortestPathIterator();
        input_idx = 0;
        output_idx = 0;
        is_initialized = false;
        buffered_path_idx = 0;
    }

    ~AllShortestPathState() {
        delete all_srtp_iter;
    }

    void resetForMoreInput() {
        input_idx = 0;
        output_idx = 0;
        buffered_path_idx = 0;
        buffered_paths.clear();
        buffered_input_rows.clear();
    }

public:
    AllShortestPathIterator *all_srtp_iter;

    idx_t input_idx;
    idx_t output_idx;

    bool is_initialized;

    vector<int> adj_col_idxs;
    vector<LogicalType> adj_col_types;

    // Buffered output for multi-chunk emission
    idx_t buffered_path_idx;
    vector<Value> buffered_paths;
    vector<idx_t> buffered_input_rows;
};

unique_ptr<OperatorState> PhysicalAllShortestPathJoin::GetOperatorState(
    ExecutionContext &context) const {
    return make_unique<AllShortestPathState>();
}

OperatorResultType PhysicalAllShortestPathJoin::Execute(ExecutionContext &context,
                                                        DataChunk &input,
                                                        DataChunk &chunk,
                                                        OperatorState &state) const {
    auto &all_srtp_state = (AllShortestPathState &)state;
    if (!all_srtp_state.is_initialized) {
        context.client->graph_storage_wrapper->getAdjColIdxs((idx_t)adjidx_obj_id_fwd, all_srtp_state.adj_col_idxs, all_srtp_state.adj_col_types);
        context.client->graph_storage_wrapper->getAdjColIdxs((idx_t)adjidx_obj_id_bwd, all_srtp_state.adj_col_idxs, all_srtp_state.adj_col_types);
        all_srtp_state.is_initialized = true;

        D_ASSERT(all_srtp_state.adj_col_idxs.size() == 2);
        D_ASSERT(all_srtp_state.adj_col_types.size() == 2);
        D_ASSERT(all_srtp_state.adj_col_types[0] == LogicalType::FORWARD_ADJLIST);
        D_ASSERT(all_srtp_state.adj_col_types[1] == LogicalType::BACKWARD_ADJLIST);
    }

    const idx_t cap = STANDARD_VECTOR_SIZE;

    // Phase 1: if we have buffered paths from a previous call, emit them first
    if (all_srtp_state.buffered_path_idx < all_srtp_state.buffered_paths.size()) {
        idx_t out = 0;
        while (out < cap && all_srtp_state.buffered_path_idx < all_srtp_state.buffered_paths.size()) {
            chunk.data[output_idx].SetValue(out, all_srtp_state.buffered_paths[all_srtp_state.buffered_path_idx]);
            idx_t in_row = all_srtp_state.buffered_input_rows[all_srtp_state.buffered_path_idx];
            for (idx_t i = 0; i < input.ColumnCount(); i++) {
                if (input_col_map[i] == std::numeric_limits<uint32_t>::max()) continue;
                chunk.data[input_col_map[i]].SetValue(out, input.data[i].GetValue(in_row));
            }
            out++;
            all_srtp_state.buffered_path_idx++;
        }
        chunk.SetCardinality(out);
        if (all_srtp_state.buffered_path_idx < all_srtp_state.buffered_paths.size()) {
            return OperatorResultType::HAVE_MORE_OUTPUT;
        }
        all_srtp_state.buffered_paths.clear();
        all_srtp_state.buffered_input_rows.clear();
        all_srtp_state.buffered_path_idx = 0;
        if (all_srtp_state.input_idx >= input.size()) {
            all_srtp_state.resetForMoreInput();
            return OperatorResultType::NEED_MORE_INPUT;
        }
    }

    // Phase 2: compute paths from remaining input rows
    Vector &src_id_vec = input.data[src_id_idx];
    Vector &dst_id_vec = input.data[dst_id_idx];

    idx_t out = 0;
    while (all_srtp_state.input_idx < input.size()) {
        uint64_t src_id = getIdRefFromVector(src_id_vec, all_srtp_state.input_idx);
        uint64_t dst_id = getIdRefFromVector(dst_id_vec, all_srtp_state.input_idx);

        std::vector<std::vector<uint64_t>> all_edges;
        std::vector<std::vector<uint64_t>> all_nodes;
        all_srtp_state.all_srtp_iter->initialize(*context.client, src_id, dst_id, all_srtp_state.adj_col_idxs[0], all_srtp_state.adj_col_idxs[1],
                                                 lower_bound, upper_bound);

        bool found = all_srtp_state.all_srtp_iter->getAllShortestPaths(*context.client, all_edges, all_nodes);

        if (found) {
            for (size_t path_idx = 0; path_idx < all_nodes.size(); path_idx++) {
                const auto &nodes = all_nodes[path_idx];
                const auto &edges = all_edges[path_idx];
                if (edges.size() != nodes.size() - 1) continue;

                std::vector<Value> path_vec(edges.size() + nodes.size());
                for (size_t i = 0; i < nodes.size(); i++) {
                    path_vec[i * 2] = Value::UBIGINT(nodes[i]);
                }
                for (size_t i = 0; i < edges.size(); i++) {
                    path_vec[i * 2 + 1] = Value::UBIGINT(edges[i]);
                }
                Value path_val = Value::LIST(path_vec);

                if (out < cap) {
                    chunk.data[output_idx].SetValue(out, path_val);
                    for (idx_t i = 0; i < input.ColumnCount(); i++) {
                        if (input_col_map[i] == std::numeric_limits<uint32_t>::max()) continue;
                        chunk.data[input_col_map[i]].SetValue(out, input.data[i].GetValue(all_srtp_state.input_idx));
                    }
                    out++;
                } else {
                    all_srtp_state.buffered_paths.push_back(std::move(path_val));
                    all_srtp_state.buffered_input_rows.push_back(all_srtp_state.input_idx);
                }
            }
        }
        all_srtp_state.input_idx++;
    }

    chunk.SetCardinality(out);

    if (!all_srtp_state.buffered_paths.empty()) {
        all_srtp_state.buffered_path_idx = 0;
        return OperatorResultType::HAVE_MORE_OUTPUT;
    }

    all_srtp_state.resetForMoreInput();
    return OperatorResultType::NEED_MORE_INPUT;
}

std::string PhysicalAllShortestPathJoin::ParamsToString() const {
    std::string result = "";
    result += "input_col_map.size()=" + std::to_string(input_col_map.size()) + ", ";
    result += "output_idx=" + std::to_string(output_idx) + ", ";
    result += "adjidx_obj_id_fwd=" + std::to_string(adjidx_obj_id_fwd) + ", ";
    result += "adjidx_obj_id_bwd=" + std::to_string(adjidx_obj_id_bwd) + ", ";
    result += "src_id_idx=" + std::to_string(src_id_idx) + ", ";
    result += "dst_id_idx=" + std::to_string(dst_id_idx);
    return result;
}

std::string PhysicalAllShortestPathJoin::ToString() const {
    return "AllShortestPath";
}

} // namespace duckdb
