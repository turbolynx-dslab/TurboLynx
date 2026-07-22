#include "storage/extent/adjlist_iterator.hpp"
#include "catalog/catalog_entry/list.hpp"
#include "storage/cache/disk_aio/TypeDef.hpp"
#include "storage/cache/chunk_cache_manager.h"
#include "storage/extent/compression/compression_function.hpp"
#include "catalog/catalog.hpp"
#include "spdlog/spdlog.h"

#include "main/database.hpp"
#include "main/client_context.hpp"
#include "common/types/data_chunk.hpp"
#include "common/constants.hpp"
#include "common/typedef.hpp"
#include "storage/delta_store.hpp"
#include "storage/graph_storage_wrapper.hpp"
#include "catalog/catalog_entry/graph_catalog_entry.hpp"
#include "catalog/catalog_entry/index_catalog_entry.hpp"

#include "icecream.hpp"

namespace duckdb {
}
namespace turbolynx {
}
namespace duckdb {
    using namespace turbolynx;
}
namespace turbolynx {
using namespace duckdb;

bool AdjacencyListIterator::Initialize(duckdb::ClientContext &context, int adjColIdx, ExtentID target_eid, bool is_fwd) {
    if (is_initialized && target_eid == cur_eid) return true;

    cur_eid = target_eid;
    is_initialized = true;

    // Index the buffer-pointer table by per-partition extent seqno (a vector,
    // not a hash map — getAdjListPtr below is called once per edge).
    auto target_eid_seqno = GET_EXTENT_SEQNO_FROM_EID(target_eid);
    while (target_eid_seqno >= eid_to_bufptr_idx_map->size()) {
        eid_to_bufptr_idx_map->resize(eid_to_bufptr_idx_map->size() * 2, INVALID_PTR_ADJ_IDX_PAIR);
    }
    if (target_eid_seqno >= has_adj_col_by_seqno_.size()) {
        has_adj_col_by_seqno_.resize(eid_to_bufptr_idx_map->size(), -1);
    }

    // Resolve, once per extent (cached across extent switches), whether this
    // extent actually carries the requested adj column. Multi-partition
    // vertices spread their edge types across partition-extents, so a given
    // extent may lack this column. The catalog entry itself is only fetched
    // on first sight of the extent or on the load-miss paths below, which
    // accept prefetched_entry == nullptr and do their own lookup.
    Catalog &catalog = context.db->GetCatalog();
    ExtentCatalogEntry *extent_cat_entry = nullptr;
    int8_t cached_has_col = has_adj_col_by_seqno_[target_eid_seqno];
    if (cached_has_col < 0) {
        extent_cat_entry = (ExtentCatalogEntry *)catalog.GetEntry(
            context, CatalogType::EXTENT_ENTRY, DEFAULT_SCHEMA,
            DEFAULT_EXTENT_PREFIX + std::to_string(target_eid),
            true /* if_exists */);
        cur_eid_has_adj_col =
            extent_cat_entry != nullptr &&
            adjColIdx < (int)extent_cat_entry->adjlist_chunks.size();
        has_adj_col_by_seqno_[target_eid_seqno] = cur_eid_has_adj_col ? 1 : 0;
    }
    else {
        cur_eid_has_adj_col = cached_has_col == 1;
    }
    if (!cur_eid_has_adj_col) {
        // No CSR for this adj column in this extent — caller returns an empty
        // adj list without any I/O.
        return true;
    }

    if (!ext_it ->IsInitialized()) {
        vector<idx_t> target_idxs { (idx_t)adjColIdx };
        ext_it->Initialize(context, is_fwd ? fwd_types : bwd_types, target_idxs, target_eid, extent_cat_entry);
        (*eid_to_bufptr_idx_map)[target_eid_seqno] = std::make_pair<idx_t, data_ptr_t>(0, nullptr);
        return false;
    } else {
        auto &pair = (*eid_to_bufptr_idx_map)[target_eid_seqno];
        if (pair != INVALID_PTR_ADJ_IDX_PAIR) {
            if (pair.first != (idx_t)-1) {
                // Find! Nothing to do
                return true;
            } else {
                // Evicted extent
                int bufptr_idx = requestNewAdjList(context, adjColIdx, target_eid, is_fwd, extent_cat_entry);
                pair.first = bufptr_idx;
                pair.second = nullptr;
            }
        }
        else {
            // Fail to find
            int bufptr_idx = requestNewAdjList(context, adjColIdx, target_eid, is_fwd, extent_cat_entry);
            (*eid_to_bufptr_idx_map)[target_eid_seqno] = std::make_pair<idx_t, data_ptr_t>(bufptr_idx, nullptr);
            return false;
        }
    }

    return true;
}

void AdjacencyListIterator::getAdjListPtr(uint64_t vid, ExtentID target_eid, uint64_t **start_ptr, uint64_t **end_ptr, bool is_initialized) {
    idx_t target_seqno = GET_SEQNO_FROM_PHYSICAL_ID(vid);
    auto target_eid_seqno = GET_EXTENT_SEQNO_FROM_EID(target_eid);
    size_t num_adj_lists = 0;
    size_t slot_for_num_adj = 1;

    // Direct vector index (no per-edge hash lookup).
    auto &bufptr_adjidx_pair = (*eid_to_bufptr_idx_map)[target_eid_seqno];
    D_ASSERT(bufptr_adjidx_pair.first != (idx_t)-1);
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


idx_t *AdjacencyListIterator::GetAdjListBase(duckdb::ClientContext &context,
                                             int adjColIdx, ExtentID target_eid,
                                             bool is_fwd, idx_t &num_adj)
{
    bool inited = Initialize(context, adjColIdx, target_eid, is_fwd);
    if (!cur_eid_has_adj_col) {
        num_adj = 0;
        return nullptr;
    }
    auto target_eid_seqno = GET_EXTENT_SEQNO_FROM_EID(target_eid);
    auto &bufptr_adjidx_pair = (*eid_to_bufptr_idx_map)[target_eid_seqno];
    D_ASSERT(bufptr_adjidx_pair.first != (idx_t)-1);
    if (!bufptr_adjidx_pair.second) {
        ext_it->GetExtent(cur_adj_list, bufptr_adjidx_pair.first, inited);
        bufptr_adjidx_pair.second = cur_adj_list;
    }
    num_adj = ((idx_t *)bufptr_adjidx_pair.second)[0];
    D_ASSERT(num_adj <= STORAGE_STANDARD_VECTOR_SIZE);
    return ((idx_t *)bufptr_adjidx_pair.second) + 1;
}

int AdjacencyListIterator::requestNewAdjList(duckdb::ClientContext &context, int adjColIdx, ExtentID target_eid, bool is_fwd, ExtentCatalogEntry *prefetched_entry) {
    ExtentID evicted_eid;
    vector<idx_t> target_idxs { (idx_t)adjColIdx };
    return ext_it->RequestNewIO(context, target_eid, evicted_eid, prefetched_entry);
}

void DFSIterator::initialize(duckdb::ClientContext &context, uint64_t src_id,
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

    // Reset all level cursors and adj-list offsets so stale pointers
    // from a previous source's traversal can't be reused (#193).
    for (int lv = 0; lv < (int)adj_col_cursor_per_level.size(); lv++) {
        adj_col_cursor_per_level[lv] = 0;
        for (int ac = 0; ac < (int)cursor_per_lv_per_col[lv].size(); ac++) {
            cursor_per_lv_per_col[lv][ac] = 0;
            offsets_per_lv_per_col[lv][ac] = {nullptr, nullptr};
        }
    }

    setupAdjListsForNode(context, 0, src_id);
}

void DFSIterator::setupAdjListsForNode(duckdb::ClientContext &context, int lv, uint64_t src_id) {
    int n_ac = (int)adjColIdxs.size();
    offsets_per_lv_per_col[lv].resize(n_ac, {nullptr, nullptr});
    cursor_per_lv_per_col[lv].resize(n_ac, 0);
    if ((int)delta_merge_bufs_per_lv_per_col.size() <= lv) {
        delta_merge_bufs_per_lv_per_col.resize(lv + 1);
    }
    delta_merge_bufs_per_lv_per_col[lv].resize(n_ac);
    if ((int)prev_eid_per_lv_per_col.size() <= lv) {
        prev_eid_per_lv_per_col.resize(lv + 1);
    }
    prev_eid_per_lv_per_col[lv].resize(n_ac, std::numeric_limits<ExtentID>::max());

    auto &wrapper = *context.graph_storage_wrapper;
    for (int ac = 0; ac < n_ac; ac++) {
        ExpandDirection dir = adjColIsFwds[ac] ? ExpandDirection::OUTGOING
                                               : ExpandDirection::INCOMING;
        uint64_t *start = nullptr;
        uint64_t *end = nullptr;
        wrapper.getAdjListFromVid(*adjlist_iters[ac], adjColIdxs[ac],
            prev_eid_per_lv_per_col[lv][ac], src_id, start, end, dir,
            delta_merge_bufs_per_lv_per_col[lv][ac]);
        offsets_per_lv_per_col[lv][ac] = {start, end};
        cursor_per_lv_per_col[lv][ac] = 0;
    }
    adj_col_cursor_per_level[lv] = 0;
}

bool DFSIterator::getNextEdge(duckdb::ClientContext &context, int lv, uint64_t &tgt, uint64_t &edge) {
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

void DFSIterator::changeLevel(duckdb::ClientContext &context, bool traverse_child, uint64_t src_id) {
    if (traverse_child) {
        current_lv++;
        if (current_lv > max_lv) {
            initializeDSForNewLv(current_lv);
        }
        // Skip adj list setup for terminal nodes (not in src_partition_ids_).
        // Their level entry exists but has no valid adj list data; the caller
        // will detect terminal and immediately call reduceLevel().
        if (!src_partition_ids_.empty()) {
            uint16_t part = context.graph_storage_wrapper->getNodePartitionId(src_id);
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
    eid_to_bufptr_idx_map->resize(INITIAL_EXTENT_ID_SPACE, INVALID_PTR_ADJ_IDX_PAIR);
}

ShortestPathIterator::~ShortestPathIterator() {}

void ShortestPathIterator::initialize(duckdb::ClientContext &context, NodeID src_id, NodeID tgt_id, uint64_t adj_col_idx, Level lower_bound, Level upper_bound) {
    if (src_id == tgt_id) { return; }; // Self-path: no shortest path to self

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

bool ShortestPathIterator::enqueueNeighbors(duckdb::ClientContext &context, NodeID node_id, Level node_level, std::queue<std::pair<NodeID, Level>>& queue) {
    uint64_t *start_ptr = nullptr, *end_ptr = nullptr;
    context.graph_storage_wrapper->getAdjListFromVid(
        *adjlist_iterator, (int)adjColIdx, prev_eid_, node_id,
        start_ptr, end_ptr, ExpandDirection::OUTGOING, delta_merge_buf);
    if (start_ptr == nullptr) return true;

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


bool ShortestPathIterator::getShortestPath(duckdb::ClientContext &context, std::vector<uint64_t>& edges, std::vector<uint64_t>& nodes) {
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
    eid_to_bufptr_idx_map_forward->resize(INITIAL_EXTENT_ID_SPACE, INVALID_PTR_ADJ_IDX_PAIR);

    ext_it_backward = std::make_shared<ExtentIterator>();
    eid_to_bufptr_idx_map_backward = std::make_shared<EidBufPtrMap>();
    eid_to_bufptr_idx_map_backward->resize(INITIAL_EXTENT_ID_SPACE, INVALID_PTR_ADJ_IDX_PAIR);

    meeting_point = INVALID_NODE_ID;

    // initialize iterator
    adjlist_iterator_forward = std::make_shared<AdjacencyListIterator>(this->ext_it_forward, this->eid_to_bufptr_idx_map_forward);
    adjlist_iterator_backward = std::make_shared<AdjacencyListIterator>(this->ext_it_backward, this->eid_to_bufptr_idx_map_backward);
}

ShortestPathAdvancedIterator::~ShortestPathAdvancedIterator() {}

void ShortestPathAdvancedIterator::initialize(duckdb::ClientContext &context, NodeID src_id, NodeID tgt_id, uint64_t adj_col_idx_fwd, uint64_t adj_col_idx_bwd, Level lower_bound, Level upper_bound) {
    this->src_id = src_id;
    this->tgt_id = tgt_id;
    this->adj_col_idx_fwd = adj_col_idx_fwd;
    this->adj_col_idx_bwd = adj_col_idx_bwd;
    this->lower_bound = lower_bound;
    this->upper_bound = upper_bound;
    this->meeting_point = INVALID_NODE_ID;

    predecessor_forward.clear();
    predecessor_backward.clear();

    if (src_id == tgt_id) {
        return; // Self-path: no shortest path to self
    }

    predecessor_forward[src_id] = {src_id, 0};
    predecessor_backward[tgt_id] = {tgt_id, 0};

    biDirectionalSearch(context);
}

bool ShortestPathAdvancedIterator::biDirectionalSearch(duckdb::ClientContext &context) {
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

bool ShortestPathAdvancedIterator::enqueueNeighbors(duckdb::ClientContext &context, NodeID node_id, Level node_level, std::queue<std::pair<NodeID, Level>>& queue, bool is_forward) {
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
        uint64_t *start_ptr = nullptr, *end_ptr = nullptr;
        ExpandDirection dir = scan.is_fwd ? ExpandDirection::OUTGOING
                                          : ExpandDirection::INCOMING;
        std::vector<uint64_t> &merge_buf =
            scan.is_fwd ? delta_merge_buf_fwd : delta_merge_buf_bwd;
        ExtentID &prev = scan.is_fwd ? prev_eid_fwd_ : prev_eid_bwd_;
        context.graph_storage_wrapper->getAdjListFromVid(
            *scan.iter, (int)scan.col_idx, prev, node_id,
            start_ptr, end_ptr, dir, merge_buf);
        if (start_ptr == nullptr) continue;

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


bool ShortestPathAdvancedIterator::getShortestPath(duckdb::ClientContext &context, std::vector<uint64_t>& edges, std::vector<uint64_t>& nodes) {
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
    eid_to_bufptr_idx_map_forward->resize(INITIAL_EXTENT_ID_SPACE, INVALID_PTR_ADJ_IDX_PAIR);

    ext_it_backward = std::make_shared<ExtentIterator>();
    eid_to_bufptr_idx_map_backward = std::make_shared<EidBufPtrMap>();
    eid_to_bufptr_idx_map_backward->resize(INITIAL_EXTENT_ID_SPACE, INVALID_PTR_ADJ_IDX_PAIR);

    adjlist_iterator_forward = std::make_shared<AdjacencyListIterator>(this->ext_it_forward, this->eid_to_bufptr_idx_map_forward);
    adjlist_iterator_backward = std::make_shared<AdjacencyListIterator>(this->ext_it_backward, this->eid_to_bufptr_idx_map_backward);
}

AllShortestPathIterator::~AllShortestPathIterator() = default;

void AllShortestPathIterator::initialize(duckdb::ClientContext &context, NodeID src_id, NodeID tgt_id, uint64_t adj_col_idx_forward, uint64_t adj_col_idx_backward, Level lower_bound, Level upper_bound) {
    this->src_id = src_id;
    this->tgt_id = tgt_id;
    this->adj_col_idx_fwd = adj_col_idx_forward;
    this->adj_col_idx_bwd = adj_col_idx_backward;
    this->lower_bound = lower_bound;
    this->upper_bound = upper_bound;
    this->meeting_points.clear();

    predecessor_forward.clear();
    predecessor_backward.clear();
    bfs_level_forward.clear();
    bfs_level_backward.clear();
    predecessor_forward[src_id] = {{src_id, 0}};
    predecessor_backward[tgt_id] = {{tgt_id, 0}};
    bfs_level_forward[src_id] = 0;
    bfs_level_backward[tgt_id] = 0;

    biDirectionalSearch(context);
}

bool AllShortestPathIterator::biDirectionalSearch(duckdb::ClientContext &context) {
    std::queue<std::pair<NodeID, Level>> queue_forward;
    std::queue<std::pair<NodeID, Level>> queue_backward;

    queue_forward.push(make_pair(src_id, 0));
    queue_backward.push(make_pair(tgt_id, 0));

    Level level_forward = 0;
    Level level_backward = 0;

    bool meeting_point_found = false;

    while (true) {
        // Expand forward: process all nodes at current level_forward
        if (!queue_forward.empty()) {
            // Collect all nodes at this level first
            std::vector<std::pair<NodeID, Level>> current_level_nodes;
            while (!queue_forward.empty() && queue_forward.front().second == level_forward) {
                current_level_nodes.push_back(queue_forward.front());
                queue_forward.pop();
            }
            for (auto &node : current_level_nodes) {
                bool found = enqueueNeighbors(context, node.first, node.second, queue_forward, true);
                if (found) meeting_point_found = true;
            }
            if (meeting_point_found) break;
            level_forward++;
        }

        if (level_forward + level_backward >= upper_bound) break;

        // Expand backward: process all nodes at current level_backward
        if (!queue_backward.empty()) {
            std::vector<std::pair<NodeID, Level>> current_level_nodes;
            while (!queue_backward.empty() && queue_backward.front().second == level_backward) {
                current_level_nodes.push_back(queue_backward.front());
                queue_backward.pop();
            }
            for (auto &node : current_level_nodes) {
                bool found = enqueueNeighbors(context, node.first, node.second, queue_backward, false);
                if (found) meeting_point_found = true;
            }
            if (meeting_point_found) break;
            level_backward++;
        }

        if (level_forward + level_backward >= upper_bound) break;

        if (queue_forward.empty() && queue_backward.empty()) break;
    }

    return meeting_point_found;
}

bool AllShortestPathIterator::enqueueNeighbors(duckdb::ClientContext &context, NodeID current_node, Level node_level, std::queue<std::pair<NodeID, Level>> &queue, bool is_forward) {
    auto &predecessor_to_insert = is_forward ? predecessor_forward : predecessor_backward;
    auto &predecessor_to_find = is_forward ? predecessor_backward : predecessor_forward;
    auto &level_to_insert = is_forward ? bfs_level_forward : bfs_level_backward;

    // Check BOTH directions (undirected graph) — same as ShortestPathAdvancedIterator
    struct AdjDir {
        uint64_t col_idx;
        bool fwd;
        std::shared_ptr<AdjacencyListIterator> &iter;
    };
    AdjDir dirs[] = {
        {adj_col_idx_fwd, true, adjlist_iterator_forward},
        {adj_col_idx_bwd, false, adjlist_iterator_backward},
    };

    for (auto &dir : dirs) {
    uint64_t *start_ptr = nullptr, *end_ptr = nullptr;
    ExpandDirection expand_dir = dir.fwd ? ExpandDirection::OUTGOING
                                         : ExpandDirection::INCOMING;
    std::vector<uint64_t> &merge_buf =
        dir.fwd ? delta_merge_buf_fwd : delta_merge_buf_bwd;
    ExtentID &prev = dir.fwd ? prev_eid_fwd_ : prev_eid_bwd_;
    context.graph_storage_wrapper->getAdjListFromVid(
        *dir.iter, (int)dir.col_idx, prev, current_node,
        start_ptr, end_ptr, expand_dir, merge_buf);
    if (start_ptr == nullptr) continue;

    for (uint64_t *ptr = start_ptr; ptr < end_ptr; ptr += 2) {
        uint64_t neighbor = *ptr;
        uint64_t edge_id = *(ptr + 1);

        bool is_meeting =
            predecessor_to_find.find(neighbor) != predecessor_to_find.end() &&
            node_level + 1 >= lower_bound;
        bool within_upper = node_level + 1 < upper_bound;

        // First visit: record level + enqueue. Same-level revisit:
        // append the additional predecessor without re-enqueueing.
        // Longer-path predecessors are silently dropped (#39).
        auto it_level = level_to_insert.find(neighbor);
        bool first_visit = it_level == level_to_insert.end();
        bool same_level = !first_visit && it_level->second == node_level + 1;

        bool recorded = false;
        if (first_visit && within_upper) {
            level_to_insert[neighbor] = node_level + 1;
            queue.emplace(neighbor, node_level + 1);
            predecessor_to_insert[neighbor].emplace_back(current_node, edge_id);
            recorded = true;
        } else if (same_level) {
            predecessor_to_insert[neighbor].emplace_back(current_node, edge_id);
            recorded = true;
        }

        if (is_meeting) {
            if (!recorded) {
                predecessor_to_insert[neighbor].emplace_back(current_node, edge_id);
            }
            meeting_points.insert(neighbor);
        }
    }
    } // end for (dirs)

    return !meeting_points.empty();  // Return true if at least one meeting point was found
}

bool AllShortestPathIterator::getAllShortestPaths(duckdb::ClientContext &context, std::vector<std::vector<EdgeID>> &all_edges, std::vector<std::vector<NodeID>> &all_nodes) {
    // Hard caps that prevent the path enumeration from blowing up on
    // high-fanout graphs. They are intentionally generous and a single
    // warning is emitted at the end (issue #37) when any of them is hit
    // so callers know the result was truncated rather than complete —
    // the function name promises *all* shortest paths.
    static constexpr size_t MAX_PATHS_PER_DIRECTION = 1000;
    static constexpr size_t MAX_TOTAL_PATHS = 10000;
    bool fwd_truncated = false;
    bool bwd_truncated = false;
    bool total_truncated = false;

    for (NodeID meeting_point : meeting_points) {
        std::vector<std::vector<std::pair<NodeID, EdgeID>>> fwd_paths;
        {
            std::vector<std::pair<NodeID, EdgeID>> current_path;
            bool limit_hit = false;
            std::function<void(NodeID)> enumerate_fwd = [&](NodeID node) {
                if (limit_hit) return;
                if (node == src_id) {
                    fwd_paths.push_back(current_path);
                    if (fwd_paths.size() >= MAX_PATHS_PER_DIRECTION) {
                        limit_hit = true;
                        fwd_truncated = true;
                    }
                    return;
                }
                auto it = predecessor_forward.find(node);
                if (it == predecessor_forward.end()) return;
                for (auto &[pred_node, edge_id] : it->second) {
                    if (limit_hit) return;
                    if (pred_node == node) continue;
                    current_path.push_back({node, edge_id});
                    enumerate_fwd(pred_node);
                    current_path.pop_back();
                }
            };
            enumerate_fwd(meeting_point);
        }

        std::vector<std::vector<std::pair<NodeID, EdgeID>>> bwd_paths;
        {
            std::vector<std::pair<NodeID, EdgeID>> current_path;
            bool limit_hit = false;
            std::function<void(NodeID)> enumerate_bwd = [&](NodeID node) {
                if (limit_hit) return;
                if (node == tgt_id) {
                    bwd_paths.push_back(current_path);
                    if (bwd_paths.size() >= MAX_PATHS_PER_DIRECTION) {
                        limit_hit = true;
                        bwd_truncated = true;
                    }
                    return;
                }
                auto it = predecessor_backward.find(node);
                if (it == predecessor_backward.end()) return;
                for (auto &[pred_node, edge_id] : it->second) {
                    if (limit_hit) return;
                    if (pred_node == node) continue;
                    current_path.push_back({pred_node, edge_id});
                    enumerate_bwd(pred_node);
                    current_path.pop_back();
                }
            };
            enumerate_bwd(meeting_point);
        }

        for (auto &fwd : fwd_paths) {
            for (auto &bwd : bwd_paths) {
                if (all_nodes.size() >= MAX_TOTAL_PATHS) {
                    total_truncated = true;
                    goto done;
                }

                std::vector<NodeID> nodes;
                std::vector<EdgeID> edges;

                nodes.push_back(src_id);
                for (int i = (int)fwd.size() - 1; i >= 0; i--) {
                    nodes.push_back(fwd[i].first);
                    edges.push_back(fwd[i].second);
                }

                for (auto &[node, edge] : bwd) {
                    nodes.push_back(node);
                    edges.push_back(edge);
                }

                if (edges.size() == nodes.size() - 1) {
                    all_nodes.push_back(std::move(nodes));
                    all_edges.push_back(std::move(edges));
                }
            }
        }
    }
done:
    if (fwd_truncated || bwd_truncated || total_truncated) {
        spdlog::warn(
            "[allShortestPaths] result truncated: fwd_cap={} (per direction "
            "limit {}), bwd_cap={}, total_cap={} (total limit {}), returned "
            "{} paths between src=0x{:016X} tgt=0x{:016X}",
            fwd_truncated, MAX_PATHS_PER_DIRECTION, bwd_truncated,
            total_truncated, MAX_TOTAL_PATHS, all_nodes.size(),
            (uint64_t)src_id, (uint64_t)tgt_id);
    }
    return !all_nodes.empty();
}

} // namespace turbolynx
