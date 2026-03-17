#include "storage/extent/adjlist_iterator.hpp"
#include "catalog/catalog_entry/list.hpp"
#include "storage/cache/disk_aio/TypeDef.hpp"
#include "storage/cache/chunk_cache_manager.h"
#include "storage/extent/compression/compression_function.hpp"
#include "catalog/catalog.hpp"

#include "main/database.hpp"
#include "main/client_context.hpp"
#include "common/types/data_chunk.hpp"

#include "icecream.hpp" 

namespace duckdb {

bool AdjacencyListIterator::Initialize(ClientContext &context, int adjColIdx, ExtentID target_eid, bool is_fwd) {
    if (is_initialized && target_eid == cur_eid) return true;

    cur_eid = target_eid;
    is_initialized = true;

    if (!ext_it ->IsInitialized()) {
        vector<idx_t> target_idxs { (idx_t)adjColIdx };
        ext_it->Initialize(context, is_fwd ? fwd_types : bwd_types, target_idxs, target_eid);
        (*eid_to_bufptr_idx_map)[target_eid] = std::make_pair<idx_t, data_ptr_t>(0, nullptr);
        return false;
    } else {
        auto it = eid_to_bufptr_idx_map->find(target_eid);
        if (it != eid_to_bufptr_idx_map->end()) {
            auto &pair = it->second;
            if (pair.first != (idx_t)-1) {
                // Find! Nothing to do
                return true;
            } else {
                // Evicted extent
                int bufptr_idx = requestNewAdjList(context, adjColIdx, target_eid, is_fwd);
                pair.first = bufptr_idx;
                pair.second = nullptr;
            }
        }
        else {
            // Fail to find
            int bufptr_idx = requestNewAdjList(context, adjColIdx, target_eid, is_fwd);
            (*eid_to_bufptr_idx_map)[target_eid] = std::make_pair<idx_t, data_ptr_t>(bufptr_idx, nullptr);
            return false;
        }
    }

    return true;
}

void AdjacencyListIterator::getAdjListPtr(uint64_t vid, ExtentID target_eid, uint64_t **start_ptr, uint64_t **end_ptr, bool is_initialized) {
    idx_t target_seqno = GET_SEQNO_FROM_PHYSICAL_ID(vid);
    size_t num_adj_lists = 0;
    size_t slot_for_num_adj = 1;

    auto it = eid_to_bufptr_idx_map->find(target_eid);
    D_ASSERT(it != eid_to_bufptr_idx_map->end() && it->second.first != (idx_t)-1);
    auto &bufptr_adjidx_pair = it->second;
    if (!bufptr_adjidx_pair.second) {
        ext_it->GetExtent(cur_adj_list, bufptr_adjidx_pair.first, is_initialized);
        bufptr_adjidx_pair.second = cur_adj_list;
        num_adj_lists = ((idx_t *)cur_adj_list)[0];
        adjListBase = ((idx_t *)cur_adj_list) + slot_for_num_adj;
    } else {
        num_adj_lists = ((idx_t *)bufptr_adjidx_pair.second)[0];
        adjListBase = ((idx_t *)bufptr_adjidx_pair.second) + slot_for_num_adj;
    }
    D_ASSERT(num_adj_lists <= STORAGE_STANDARD_VECTOR_SIZE);
    if (num_adj_lists <= target_seqno) {
        *start_ptr = nullptr;
        *end_ptr = nullptr;
    }
    else {
        *start_ptr =
            adjListBase + (target_seqno == 0
                            ? num_adj_lists
                            : adjListBase[target_seqno - 1]);
        *end_ptr = adjListBase + adjListBase[target_seqno];
    }
}


int AdjacencyListIterator::requestNewAdjList(ClientContext &context, int adjColIdx, ExtentID target_eid, bool is_fwd) {
    ExtentID evicted_eid;
    vector<idx_t> target_idxs { (idx_t)adjColIdx };
    return ext_it->RequestNewIO(context, target_eid, evicted_eid);
}

void DFSIterator::initialize(ClientContext &context, uint64_t src_id,
                              vector<int> adj_col_idxs, vector<bool> adj_col_is_fwds,
                              const std::unordered_set<uint16_t> &src_partition_ids) {
    current_lv = 0;
    src_partition_ids_ = src_partition_ids;
    adjColIdxs = adj_col_idxs;
    adjColIsFwds = adj_col_is_fwds;
    int n_ac = (int)adj_col_idxs.size();

    // Ensure one AdjacencyListIterator per adj_col (each owns its own ext_it/map)
    while ((int)adjlist_iters.size() < n_ac) {
        adjlist_iters.push_back(new AdjacencyListIterator());
    }

    // Ensure lv=0 structures exist
    if (offsets_per_lv_per_col.empty()) {
        offsets_per_lv_per_col.push_back(
            vector<std::pair<uint64_t *, uint64_t *>>(n_ac, {nullptr, nullptr}));
        cursor_per_lv_per_col.push_back(vector<idx_t>(n_ac, 0));
        adj_col_cursor_per_level.push_back(0);
        max_lv = 0;
    }

    // Reset all level cursors
    for (int lv = 0; lv < (int)adj_col_cursor_per_level.size(); lv++) {
        adj_col_cursor_per_level[lv] = 0;
        for (int ac = 0; ac < (int)cursor_per_lv_per_col[lv].size(); ac++) {
            cursor_per_lv_per_col[lv][ac] = 0;
        }
    }

    setupAdjListsForNode(context, 0, src_id);
}

void DFSIterator::setupAdjListsForNode(ClientContext &context, int lv, uint64_t src_id) {
    ExtentID eid = src_id >> 32;
    int n_ac = (int)adjColIdxs.size();

    offsets_per_lv_per_col[lv].resize(n_ac, {nullptr, nullptr});
    cursor_per_lv_per_col[lv].resize(n_ac, 0);

    for (int ac = 0; ac < n_ac; ac++) {
        bool is_fwd = adjColIsFwds[ac];
        bool initialized = adjlist_iters[ac]->Initialize(context, adjColIdxs[ac], eid, is_fwd);
        adjlist_iters[ac]->getAdjListPtr(src_id, eid,
            &offsets_per_lv_per_col[lv][ac].first,
            &offsets_per_lv_per_col[lv][ac].second,
            initialized);
        cursor_per_lv_per_col[lv][ac] = 0;
    }
    adj_col_cursor_per_level[lv] = 0;
}

bool DFSIterator::getNextEdge(ClientContext &context, int lv, uint64_t &tgt, uint64_t &edge) {
    int n_ac = (int)adjColIdxs.size();

    while (adj_col_cursor_per_level[lv] < n_ac) {
        int ac = adj_col_cursor_per_level[lv];
        uint64_t *start = offsets_per_lv_per_col[lv][ac].first;
        uint64_t *end   = offsets_per_lv_per_col[lv][ac].second;

        int64_t adj_size = (start && end) ? (int64_t)(end - start) / 2 : 0;

        if (start == nullptr) {
            adj_col_cursor_per_level[lv]++;
            continue;
        }

        idx_t &cursor = cursor_per_lv_per_col[lv][ac];

        if (cursor < (idx_t)adj_size) {
            tgt  = start[cursor * 2];
            edge = start[cursor * 2 + 1];
            cursor++;
            changeLevel(context, true, tgt);
            return true;
        }

        // This adj_col exhausted at this level
        adj_col_cursor_per_level[lv]++;
    }

    // All adj_cols exhausted — reset cursors for next visit to this level
    adj_col_cursor_per_level[lv] = 0;
    for (int ac = 0; ac < n_ac; ac++) {
        cursor_per_lv_per_col[lv][ac] = 0;
    }
    changeLevel(context, false);
    return false;
}

void DFSIterator::changeLevel(ClientContext &context, bool traverse_child, uint64_t src_id) {
    if (traverse_child) {
        current_lv++;
        if (current_lv > max_lv) {
            initializeDSForNewLv(current_lv);
        }
        // Skip adj list setup for terminal nodes (not in src_partition_ids_).
        // Their level entry exists but has no valid adj list data; the caller
        // will detect terminal and immediately call reduceLevel().
        if (!src_partition_ids_.empty()) {
            uint16_t part = (uint16_t)(src_id >> 48);
            if (src_partition_ids_.count(part) == 0) {
                return; // terminal — do not call setupAdjListsForNode
            }
        }
        setupAdjListsForNode(context, current_lv, src_id);
    } else {
        current_lv--;
    }
}

void DFSIterator::reduceLevel() {
    current_lv--;
}

void DFSIterator::initializeDSForNewLv(int new_lv) {
    max_lv = new_lv;
    int n_ac = (int)adjColIdxs.size();

    while ((int)offsets_per_lv_per_col.size() < new_lv + 1) {
        offsets_per_lv_per_col.push_back(
            vector<std::pair<uint64_t *, uint64_t *>>(n_ac, {nullptr, nullptr}));
        cursor_per_lv_per_col.push_back(vector<idx_t>(n_ac, 0));
        adj_col_cursor_per_level.push_back(0);
    }
}

ShortestPathIterator::ShortestPathIterator() {
    ext_it = std::make_shared<ExtentIterator>();
    eid_to_bufptr_idx_map = std::make_shared<EidBufPtrMap>();
}

ShortestPathIterator::~ShortestPathIterator() {}

void ShortestPathIterator::initialize(ClientContext &context, NodeID src_id, NodeID tgt_id, uint64_t adj_col_idx, Level lower_bound, Level upper_bound) {
    if (src_id == tgt_id) { D_ASSERT(false); }; // Source and target are the same

    srcId = src_id;
    tgtId = tgt_id;
    adjColIdx = adj_col_idx;
    lowerBound = lower_bound;
    upperBound = upper_bound;
    predecessor.clear();
    predecessor[srcId] = {src_id, 0}; // Source node is its own predecessor
    adjlist_iterator = std::make_shared<AdjacencyListIterator>(this->ext_it, this->eid_to_bufptr_idx_map);

    // Start the BFS from the source node
    std::queue<std::pair<NodeID, Level>> queue;
    queue.push(make_pair(srcId, 0));

    while (!queue.empty()) {
        std::pair<NodeID, Level> current = queue.front();
        queue.pop();
        bool found = enqueueNeighbors(context, current.first, current.second, queue);
        if (found) { break; }
    }
}

bool ShortestPathIterator::enqueueNeighbors(ClientContext &context, NodeID node_id, Level node_level, std::queue<std::pair<NodeID, Level>>& queue) {
    uint64_t *start_ptr, *end_ptr;
    ExtentID target_eid = node_id >> 32;
    bool is_initialized = adjlist_iterator->Initialize(context, adjColIdx, target_eid, true);
    adjlist_iterator->getAdjListPtr(node_id, target_eid, &start_ptr, &end_ptr, is_initialized);

    for (uint64_t *ptr = start_ptr; ptr < end_ptr; ptr += 2) {
        uint64_t neighbor = *ptr;
        uint64_t edge_id = *(ptr + 1);

        // If found
        if (neighbor == tgtId && node_level + 1 >= lowerBound) {
            predecessor[neighbor] = {node_id, edge_id};  // Set the current node and edge as the predecessor of the neighbor
            return true;
        }
        else if (node_level + 1 >= upperBound) { // no need for further search
            continue;
        }

        // If the neighbor has not been visited
        if (predecessor.find(neighbor) == predecessor.end()) {
            queue.push(make_pair(neighbor, node_level + 1));
            predecessor[neighbor] = {node_id, edge_id};  // Set the current node and edge as the predecessor of the neighbor
        }
    }

    return false;
}


bool ShortestPathIterator::getShortestPath(ClientContext &context, std::vector<uint64_t>& edges, std::vector<uint64_t>& nodes) {
    if (predecessor.find(tgtId) == predecessor.end()) {
        return false; // Target not reachable from source
    }

    // Reconstruct the path from target to source
    uint64_t current = tgtId;
    while (current != srcId) {
        nodes.push_back(current);
        edges.push_back(predecessor[current].second);
        current = predecessor[current].first;
    }
    nodes.push_back(srcId);

    // Reverse the path to get the correct order from source to target
    std::reverse(nodes.begin(), nodes.end());
    std::reverse(edges.begin(), edges.end());

    return true;
}

/**
 * New implementation
*/


ShortestPathAdvancedIterator::ShortestPathAdvancedIterator() {
    ext_it_forward = std::make_shared<ExtentIterator>();
    eid_to_bufptr_idx_map_forward = std::make_shared<EidBufPtrMap>();

    ext_it_backward = std::make_shared<ExtentIterator>();
    eid_to_bufptr_idx_map_backward = std::make_shared<EidBufPtrMap>();

    meeting_point = INVALID_NODE_ID;

    // initialize iterator
    adjlist_iterator_forward = std::make_shared<AdjacencyListIterator>(this->ext_it_forward, this->eid_to_bufptr_idx_map_forward);
    adjlist_iterator_backward = std::make_shared<AdjacencyListIterator>(this->ext_it_backward, this->eid_to_bufptr_idx_map_backward);
}

ShortestPathAdvancedIterator::~ShortestPathAdvancedIterator() {}

void ShortestPathAdvancedIterator::initialize(ClientContext &context, NodeID src_id, NodeID tgt_id, uint64_t adj_col_idx_fwd, uint64_t adj_col_idx_bwd, Level lower_bound, Level upper_bound) {
    if (src_id == tgt_id) { D_ASSERT(false); }; // Source and target are the same

    this->src_id = src_id;
    this->tgt_id = tgt_id;
    this->adj_col_idx_fwd = adj_col_idx_fwd;
    this->adj_col_idx_bwd = adj_col_idx_bwd;
    this->lower_bound = lower_bound;
    this->upper_bound = upper_bound;
    this->meeting_point = INVALID_NODE_ID;

    // initialize predecessor
    predecessor_forward.clear();
    predecessor_backward.clear();
    predecessor_forward[src_id] = {src_id, 0};
    predecessor_backward[tgt_id] = {tgt_id, 0};

    biDirectionalSearch(context);
}

bool ShortestPathAdvancedIterator::biDirectionalSearch(ClientContext &context) {
    std::queue<std::pair<NodeID, Level>> queue_forward;
    std::queue<std::pair<NodeID, Level>> queue_backward;

    queue_forward.push(make_pair(src_id, 0));
    queue_backward.push(make_pair(tgt_id, 0));

    Level level_forward = 0;
    Level level_backward = 0;

    while (true) {
        // Process all nodes at the current forward level
        if (!queue_forward.empty()) {
            Level cur_level = queue_forward.front().second;
            bool found = false;
            while (!queue_forward.empty() && queue_forward.front().second == cur_level) {
                auto current = queue_forward.front();
                queue_forward.pop();
                found = enqueueNeighbors(context, current.first, current.second, queue_forward, true);
                if (found) break;
            }
            level_forward = cur_level + 1;
            if (found) break;
        }

        if (level_forward + level_backward >= upper_bound) break;
        if (queue_forward.empty() && queue_backward.empty()) break;

        // Process all nodes at the current backward level
        if (!queue_backward.empty()) {
            Level cur_level = queue_backward.front().second;
            bool found = false;
            while (!queue_backward.empty() && queue_backward.front().second == cur_level) {
                auto current = queue_backward.front();
                queue_backward.pop();
                found = enqueueNeighbors(context, current.first, current.second, queue_backward, false);
                if (found) break;
            }
            level_backward = cur_level + 1;
            if (found) break;
        }

        if (level_forward + level_backward >= upper_bound) break;
        if (queue_forward.empty() && queue_backward.empty()) break;
    }

    return meeting_point != INVALID_NODE_ID;
}

bool ShortestPathAdvancedIterator::enqueueNeighbors(ClientContext &context, NodeID node_id, Level node_level, std::queue<std::pair<NodeID, Level>>& queue, bool is_forward) {
    auto &predecessor_to_insert = is_forward ? predecessor_forward : predecessor_backward;
    auto &predecessor_to_find = is_forward ? predecessor_backward : predecessor_forward;

    // Explore BOTH forward and backward adj lists from this node
    // (handles undirected edges like KNOWS where paths can go either direction)
    struct AdjScan {
        uint64_t col_idx;
        bool is_fwd;
        std::shared_ptr<AdjacencyListIterator> &iter;
    };
    AdjScan scans[] = {
        {adj_col_idx_fwd, true, adjlist_iterator_forward},
        {adj_col_idx_bwd, false, adjlist_iterator_backward},
    };

    for (auto &scan : scans) {
        uint64_t *start_ptr, *end_ptr;
        ExtentID target_eid = node_id >> 32;
        bool is_initialized = scan.iter->Initialize(context, scan.col_idx, target_eid, scan.is_fwd);
        scan.iter->getAdjListPtr(node_id, target_eid, &start_ptr, &end_ptr, is_initialized);

        for (uint64_t *ptr = start_ptr; ptr < end_ptr; ptr += 2) {
            uint64_t neighbor = *ptr;
            uint64_t edge_id = *(ptr + 1);

            if ((predecessor_to_find.find(neighbor) != predecessor_to_find.end())
                && node_level + 1 >= lower_bound) {
                predecessor_to_insert[neighbor] = {node_id, edge_id};
                meeting_point = neighbor;
                return true;
            }
            else if (node_level + 1 >= upper_bound) {
                continue;
            }

            if (predecessor_to_insert.find(neighbor) == predecessor_to_insert.end()) {
                queue.push(make_pair(neighbor, node_level + 1));
                predecessor_to_insert[neighbor] = {node_id, edge_id};
            }
        }
    }

    return false;
}


bool ShortestPathAdvancedIterator::getShortestPath(ClientContext &context, std::vector<uint64_t>& edges, std::vector<uint64_t>& nodes) {
    if (meeting_point == INVALID_NODE_ID) {
        return false;
    }

    // Construct src -> meeting point
    NodeID current = meeting_point;
    while (current != src_id) {
        nodes.push_back(current);
        edges.push_back(predecessor_forward[current].second);
        current = predecessor_forward[current].first;
    }
    nodes.push_back(src_id);
    std::reverse(nodes.begin(), nodes.end());
    std::reverse(edges.begin(), edges.end());

    // Construct meeting point -> tgt
    current = meeting_point;
    while (current != tgt_id) {
        nodes.push_back(predecessor_backward[current].first);
        edges.push_back(predecessor_backward[current].second);
        current = predecessor_backward[current].first;
    }
    return true;
}

AllShortestPathIterator::AllShortestPathIterator()
    : src_id(INVALID_NODE_ID), tgt_id(INVALID_NODE_ID),
      adj_col_idx_fwd(0), adj_col_idx_bwd(0), lower_bound(0), upper_bound(0) {

    ext_it_forward = std::make_shared<ExtentIterator>();
    eid_to_bufptr_idx_map_forward = std::make_shared<EidBufPtrMap>();

    ext_it_backward = std::make_shared<ExtentIterator>();
    eid_to_bufptr_idx_map_backward = std::make_shared<EidBufPtrMap>();

    adjlist_iterator_forward = std::make_shared<AdjacencyListIterator>(this->ext_it_forward, this->eid_to_bufptr_idx_map_forward);
    adjlist_iterator_backward = std::make_shared<AdjacencyListIterator>(this->ext_it_backward, this->eid_to_bufptr_idx_map_backward);
}

AllShortestPathIterator::~AllShortestPathIterator() = default;

void AllShortestPathIterator::initialize(ClientContext &context, NodeID src_id, NodeID tgt_id, uint64_t adj_col_idx_forward, uint64_t adj_col_idx_backward, Level lower_bound, Level upper_bound) {
    this->src_id = src_id;
    this->tgt_id = tgt_id;
    this->adj_col_idx_fwd = adj_col_idx_forward;
    this->adj_col_idx_bwd = adj_col_idx_backward;
    this->lower_bound = lower_bound;
    this->upper_bound = upper_bound;
    this->meeting_points.clear();

    predecessor_forward.clear();
    predecessor_backward.clear();
    predecessor_forward[src_id] = {{src_id, 0}};
    predecessor_backward[tgt_id] = {{tgt_id, 0}};

    biDirectionalSearch(context);
}

bool AllShortestPathIterator::biDirectionalSearch(ClientContext &context) {
    std::queue<std::pair<NodeID, Level>> queue_forward;
    std::queue<std::pair<NodeID, Level>> queue_backward;

    queue_forward.push(make_pair(src_id, 0));
    queue_backward.push(make_pair(tgt_id, 0));

    Level level_forward = 0;
    Level level_backward = 0;

    bool meeting_point_found = false;

    while (true) {
        if (!queue_forward.empty()) {
            auto current_forward = queue_forward.front();
            queue_forward.pop();
            bool found = enqueueNeighbors(context, current_forward.first, current_forward.second, queue_forward, true);
            if (found) {
                meeting_point_found = true;
                break;
            }
            level_forward++;
        }

        if (level_forward + level_backward >= upper_bound) break;

        if (!queue_backward.empty()) {
            auto current_backward = queue_backward.front();
            queue_backward.pop();
            bool found = enqueueNeighbors(context, current_backward.first, current_backward.second, queue_backward, false);
            if (found) {
                meeting_point_found = true;
                break;
            }
            level_backward++;
        }

        if (level_forward + level_backward >= upper_bound) break;

        if (queue_forward.empty() && queue_backward.empty()) break;
    }

    return meeting_point_found;
}

bool AllShortestPathIterator::enqueueNeighbors(ClientContext &context, NodeID current_node, Level node_level, std::queue<std::pair<NodeID, Level>> &queue, bool is_forward) {
    auto &predecessor_to_insert = is_forward ? predecessor_forward : predecessor_backward;
    auto &predecessor_to_find = is_forward ? predecessor_backward : predecessor_forward;
    auto adj_col_idx = is_forward ? adj_col_idx_fwd : adj_col_idx_bwd;
    std::shared_ptr<AdjacencyListIterator> &adjlist_iterator = is_forward ? adjlist_iterator_forward : adjlist_iterator_backward;

    uint64_t *start_ptr = nullptr, *end_ptr = nullptr;
    ExtentID target_eid = current_node >> 32;
    bool is_initialized = adjlist_iterator->Initialize(context, adj_col_idx, target_eid, is_forward);
    adjlist_iterator->getAdjListPtr(current_node, target_eid, &start_ptr, &end_ptr, is_initialized);

    for (uint64_t *ptr = start_ptr; ptr < end_ptr; ptr += 2) {
        uint64_t neighbor = *ptr;
        uint64_t edge_id = *(ptr + 1);

        // If the neighbor is a valid meeting point
        if (predecessor_to_find.find(neighbor) != predecessor_to_find.end() && 
            node_level + 1 >= lower_bound) { 
            predecessor_to_insert[neighbor].emplace_back(current_node, edge_id);
            meeting_points.insert(neighbor);  // Add to the set of meeting points
        }

        // Skip neighbors beyond the upper bound
        if (node_level + 1 >= upper_bound) {
            continue;
        }

        // If the neighbor has not been visited in the current direction
        if (predecessor_to_insert.find(neighbor) == predecessor_to_insert.end()) {
            queue.emplace(neighbor, node_level + 1);
            predecessor_to_insert[neighbor].emplace_back(current_node, edge_id);
        }
    }

    return !meeting_points.empty();  // Return true if at least one meeting point was found
}

bool AllShortestPathIterator::getAllShortestPaths(ClientContext &context, std::vector<std::vector<EdgeID>> &all_edges, std::vector<std::vector<NodeID>> &all_nodes) {
    for (NodeID meeting_point : meeting_points) {
        std::vector<NodeID> nodes;
        std::vector<EdgeID> edges;

        // Construct path from src_id -> meeting_point
        NodeID current = meeting_point;
        while (current != src_id) {
            if (predecessor_forward.find(current) == predecessor_forward.end()) {
                throw std::runtime_error("Incomplete predecessor map for forward traversal");
            }
            nodes.push_back(current);
            edges.push_back(predecessor_forward[current][0].second); // Get edge ID
            current = predecessor_forward[current][0].first;        // Get predecessor node
        }
        nodes.push_back(src_id);  // Add source node
        std::reverse(nodes.begin(), nodes.end());
        std::reverse(edges.begin(), edges.end());

        // Construct path from meeting_point -> tgt_id
        current = meeting_point;
        while (current != tgt_id) {
            if (predecessor_backward.find(current) == predecessor_backward.end()) {
                throw std::runtime_error("Incomplete predecessor map for backward traversal");
            }
            nodes.push_back(predecessor_backward[current][0].first); // Append next node
            edges.push_back(predecessor_backward[current][0].second); // Append edge ID
            current = predecessor_backward[current][0].first;
        }

        // Add the path and edges to the result
        D_ASSERT(edges.size() == nodes.size() - 1); // Ensure edges and nodes match
        all_nodes.push_back(nodes);
        all_edges.push_back(edges);
    }

    return true;
}

}