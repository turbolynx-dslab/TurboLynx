#ifndef ADJLIST_ITERATOR_H
#define ADJLIST_ITERATOR_H

#include "common/common.hpp"
#include "common/vector.hpp"
#include "common/unordered_map.hpp"

#include "common/types.hpp"
#include "common/types/value.hpp"
#include "common/vector_size.hpp"

#include "main/client_context.hpp"
#include "storage/extent/extent_iterator.hpp"
#include <queue>
#include <unordered_map>

#include <limits>
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

#define BufPtrAdjIdxPair std::pair<idx_t, data_ptr_t>
const BufPtrAdjIdxPair INVALID_PTR_ADJ_IDX_PAIR = std::make_pair<idx_t, data_ptr_t>(-2, nullptr);

// Indexed by per-partition extent seqno (GET_EXTENT_SEQNO_FROM_EID). A vector
// (direct index) is intentionally used over a hash map: getAdjListPtr runs once
// per edge, so a hash lookup there is a per-edge cost on every traversal.
using EidBufPtrMap = std::vector<BufPtrAdjIdxPair>;

class AdjacencyListIterator {
public:
    AdjacencyListIterator() {
        ext_it = std::make_shared<ExtentIterator>();
        eid_to_bufptr_idx_map = std::make_shared<EidBufPtrMap>();
        eid_to_bufptr_idx_map->resize(INITIAL_EXTENT_ID_SPACE, INVALID_PTR_ADJ_IDX_PAIR);
    }
    ~AdjacencyListIterator() {}
    AdjacencyListIterator(std::shared_ptr<ExtentIterator> share_ext_it, std::shared_ptr<EidBufPtrMap> share_eid_to_bufptr_idx_map) {
        ext_it = share_ext_it;
        eid_to_bufptr_idx_map = share_eid_to_bufptr_idx_map;
    }
    bool Initialize(ClientContext &context, int adjColIdx, ExtentID target_eid, bool is_fwd);
    void getAdjListPtr(uint64_t vid, ExtentID target_eid, uint64_t **start_ptr, uint64_t **end_ptr, bool is_initialized);
    idx_t *GetAdjListBase(ClientContext &context, int adjColIdx, ExtentID target_eid, bool is_fwd, idx_t &num_adj);
    int requestNewAdjList(ClientContext &context, int adjColIdx, ExtentID target_eid, bool is_fwd, ExtentCatalogEntry *prefetched_entry = nullptr);

    //! Whether the extent resolved by the last Initialize() actually carries
    //! the requested adj column. Multi-partition vertices spread edge types
    //! across partition-extents, so a given extent may lack a column. Resolved
    //! once per extent change inside Initialize() — callers consult this
    //! instead of doing a per-edge catalog lookup.
    bool CurrentExtentHasAdjColumn() const { return cur_eid_has_adj_col; }

    inline void PrefetchAdjListPtr(uint64_t vid) const {
        auto eid_seqno = GET_EXTENT_SEQNO_FROM_EID((ExtentID)(vid >> 32))
        if (eid_seqno < eid_to_bufptr_idx_map->size()) {
            auto &pair = (*eid_to_bufptr_idx_map)[eid_seqno];
            if (pair.second) {
                idx_t seqno = GET_SEQNO_FROM_PHYSICAL_ID(vid)
                __builtin_prefetch(((idx_t *)pair.second) + 1 + seqno);
            }
        }
    }

private:
    bool is_initialized = false;
    bool cur_eid_has_adj_col = false;
    std::shared_ptr<ExtentIterator> ext_it;
    ExtentID cur_eid = std::numeric_limits<ExtentID>::max();
    std::shared_ptr<EidBufPtrMap> eid_to_bufptr_idx_map;
    std::vector<int8_t> has_adj_col_by_seqno_;
    data_ptr_t cur_adj_list;
    idx_t *adjListBase;
    vector<LogicalType> fwd_types { LogicalType::FORWARD_ADJLIST };
    vector<LogicalType> bwd_types { LogicalType::BACKWARD_ADJLIST };
};

class DFSIterator {
public:
    DFSIterator() {}
    ~DFSIterator() {
        for (auto *p : adjlist_iters) delete p;
    }

    //! `adj_col_src_pids`, when given, holds for each adj col the partition
    //! IDs whose extents actually carry that CSR; cols not owned by the
    //! current node's partition are skipped (the same slot index can hold a
    //! different edge type's CSR on another partition).
    void initialize(ClientContext &context, uint64_t src_id,
                    vector<int> adj_col_idxs, vector<bool> adj_col_is_fwds,
                    const std::unordered_set<uint16_t> &src_partition_ids = {},
                    const vector<std::unordered_set<uint16_t>> *adj_col_src_pids = nullptr);
    bool getNextEdge(ClientContext &context, int lv, uint64_t &tgt, uint64_t &edge);
    void reduceLevel();

private:
    void setupAdjListsForNode(ClientContext &context, int lv, uint64_t src_id);
    void changeLevel(ClientContext &context, bool traverse_child, uint64_t src_id = 0);
    void initializeDSForNewLv(int new_lv);

    int current_lv = 0;
    int max_lv = 0;
    std::unordered_set<uint16_t> src_partition_ids_;
    const vector<std::unordered_set<uint16_t>> *adj_col_src_pids_ = nullptr;

    // Per adj_col
    vector<int> adjColIdxs;
    vector<bool> adjColIsFwds;
    vector<AdjacencyListIterator *> adjlist_iters;  // one per adj_col, each owns its ext_it

    // Per level, per adj_col
    vector<vector<std::pair<uint64_t *, uint64_t *>>> offsets_per_lv_per_col;
    vector<vector<idx_t>> cursor_per_lv_per_col;
    // pointers in offsets_per_lv_per_col may alias into these buffers,
    // so each (level, adj_col) needs its own.
    vector<vector<std::vector<uint64_t>>> delta_merge_bufs_per_lv_per_col;
    vector<vector<ExtentID>> prev_eid_per_lv_per_col;

    // Per level
    vector<int> adj_col_cursor_per_level;
};

typedef uint64_t NodeID;
typedef uint64_t EdgeID;
typedef uint64_t Level;

class ShortestPathIterator {
public:
    ShortestPathIterator();
    ~ShortestPathIterator();

    void initialize(ClientContext &context, NodeID src_id, NodeID tgt_id, uint64_t adj_col_idx, Level lower_bound, Level upper_bound);
    bool getShortestPath(ClientContext &context, std::vector<EdgeID>& edges, std::vector<NodeID>& nodes);

private:
    uint64_t srcId, tgtId;
    uint64_t adjColIdx;
    Level lowerBound;
    Level upperBound;
    std::unordered_map<NodeID, std::pair<NodeID, EdgeID>> predecessor;  // Node ID mapped to a pair of its predecessor and the connecting edge ID
    std::shared_ptr<AdjacencyListIterator> adjlist_iterator;
    std::shared_ptr<ExtentIterator> ext_it = nullptr;
    std::shared_ptr<EidBufPtrMap> eid_to_bufptr_idx_map;
    std::vector<uint64_t> delta_merge_buf;
    ExtentID prev_eid_ = std::numeric_limits<ExtentID>::max();

    bool enqueueNeighbors(ClientContext &context, NodeID node_id, Level node_level, std::queue<std::pair<NodeID, Level>>& queue);
};

class ShortestPathAdvancedIterator {
public:
    ShortestPathAdvancedIterator();
    ~ShortestPathAdvancedIterator();

    void initialize(ClientContext &context, NodeID src_id, NodeID tgt_id, uint64_t adj_col_idx_forward, uint64_t adj_col_idx_backward, Level lower_bound, Level upper_bound);
    bool getShortestPath(ClientContext &context, std::vector<EdgeID>& edges, std::vector<NodeID>& nodes);

private:
    struct NodeIDHasher {
        std::size_t operator()(const NodeID& k) const {
            return GET_SEQNO_FROM_PHYSICAL_ID(k);
        }
    };

private:
    const NodeID INVALID_NODE_ID = -1;
    NodeID meeting_point;
    NodeID src_id, tgt_id;
    uint64_t adj_col_idx_fwd;
    uint64_t adj_col_idx_bwd;
    Level lower_bound;
    Level upper_bound;
    std::unordered_map<NodeID, std::pair<NodeID, EdgeID>, NodeIDHasher> predecessor_forward;
    std::unordered_map<NodeID, std::pair<NodeID, EdgeID>, NodeIDHasher> predecessor_backward;
    std::shared_ptr<AdjacencyListIterator> adjlist_iterator_forward;
    std::shared_ptr<AdjacencyListIterator> adjlist_iterator_backward;
    std::shared_ptr<ExtentIterator> ext_it_forward = nullptr;
    std::shared_ptr<ExtentIterator> ext_it_backward = nullptr;
    std::shared_ptr<EidBufPtrMap> eid_to_bufptr_idx_map_forward;
    std::shared_ptr<EidBufPtrMap> eid_to_bufptr_idx_map_backward;
    std::vector<uint64_t> delta_merge_buf_fwd;
    std::vector<uint64_t> delta_merge_buf_bwd;
    ExtentID prev_eid_fwd_ = std::numeric_limits<ExtentID>::max();
    ExtentID prev_eid_bwd_ = std::numeric_limits<ExtentID>::max();

    bool biDirectionalSearch(ClientContext &context);
    bool enqueueNeighbors(ClientContext &context, NodeID current_node, Level node_level, std::queue<std::pair<NodeID, Level>>& queue, bool is_forward);
};

class AllShortestPathIterator {
public:
    AllShortestPathIterator();
    ~AllShortestPathIterator();

    void initialize(ClientContext &context, NodeID src_id, NodeID tgt_id, uint64_t adj_col_idx_forward, uint64_t adj_col_idx_backward, Level lower_bound, Level upper_bound);
    bool getAllShortestPaths(ClientContext &context, std::vector<std::vector<EdgeID>> &all_edges, std::vector<std::vector<NodeID>> &all_nodes);

private:
    struct NodeIDHasher {
        std::size_t operator()(const NodeID &k) const {
            return GET_SEQNO_FROM_PHYSICAL_ID(k);
        }
    };

    const NodeID INVALID_NODE_ID = -1;
    const NodeID INVALID_EDGE_ID = -1;
    std::unordered_set<NodeID> meeting_points;
    NodeID src_id, tgt_id;
    uint64_t adj_col_idx_fwd;
    uint64_t adj_col_idx_bwd;
    Level lower_bound;
    Level upper_bound;

    std::unordered_map<NodeID, std::vector<std::pair<NodeID, EdgeID>>, NodeIDHasher> predecessor_forward;
    std::unordered_map<NodeID, std::vector<std::pair<NodeID, EdgeID>>, NodeIDHasher> predecessor_backward;
    // First-discovered BFS level per direction. Used to keep additional
    // predecessors that reach a node at the same shortest level while
    // rejecting longer-path predecessors (#39).
    std::unordered_map<NodeID, Level, NodeIDHasher> bfs_level_forward;
    std::unordered_map<NodeID, Level, NodeIDHasher> bfs_level_backward;
    std::shared_ptr<AdjacencyListIterator> adjlist_iterator_forward;
    std::shared_ptr<AdjacencyListIterator> adjlist_iterator_backward;
    std::shared_ptr<ExtentIterator> ext_it_forward = nullptr;
    std::shared_ptr<ExtentIterator> ext_it_backward = nullptr;
    std::shared_ptr<EidBufPtrMap> eid_to_bufptr_idx_map_forward;
    std::shared_ptr<EidBufPtrMap> eid_to_bufptr_idx_map_backward;
    std::vector<uint64_t> delta_merge_buf_fwd;
    std::vector<uint64_t> delta_merge_buf_bwd;
    ExtentID prev_eid_fwd_ = std::numeric_limits<ExtentID>::max();
    ExtentID prev_eid_bwd_ = std::numeric_limits<ExtentID>::max();

    bool biDirectionalSearch(ClientContext &context);
    bool enqueueNeighbors(ClientContext &context, NodeID current_node, Level node_level, std::queue<std::pair<NodeID, Level>> &queue, bool is_forward);
};

} // namespace duckdb

#endif
