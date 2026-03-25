#include "execution/physical_operator/physical_node_scan.hpp"
#include "storage/extent/extent_iterator.hpp"
#include "icecream.hpp"
#include "planner/expression.hpp"
#include "planner/expression/bound_columnref_expression.hpp"
#include "planner/expression/bound_comparison_expression.hpp"
#include "planner/expression/bound_conjunction_expression.hpp"
#include "planner/expression/bound_reference_expression.hpp"
#include "storage/graph_storage_wrapper.hpp"
#include "main/database.hpp"
#include "catalog/catalog_entry/list.hpp"

#include <cassert>
#include <queue>
#include <set>

namespace duckdb {

class NodeScanState : public LocalSourceState {
   public:
    explicit NodeScanState() : iter_inited(false), iter_finished(false),
                               delta_phase(false), delta_cur(0), delta_row(0) {}
   public:
    bool iter_inited;
    bool iter_finished;
    std::queue<ExtentIterator *> ext_its;
    DataChunk extent_cache;
    bool delta_phase;
    vector<uint32_t> delta_eids;
    idx_t delta_cur, delta_row;
};

PhysicalNodeScan::PhysicalNodeScan(
    Schema &sch, vector<idx_t> oids,
    vector<vector<uint64_t>> projection_mapping,
    vector<LogicalType> scan_types_,
    vector<vector<uint64_t>> scan_projection_mapping)
    : CypherPhysicalOperator(PhysicalOperatorType::NODE_SCAN, sch),
      oids(oids),
      projection_mapping(projection_mapping),
      scan_projection_mapping(scan_projection_mapping),
      current_schema_idx(0)
{
    num_schemas = 1;
    scan_types.resize(num_schemas);
    scan_types[0] = std::move(scan_types_);
    filtered_chunk_buffer.Initialize(sch.getStoredTypes());
}

PhysicalNodeScan::PhysicalNodeScan(
    Schema &sch, vector<idx_t> oids,
    vector<vector<uint64_t>> projection_mapping,
    vector<LogicalType> scan_types_,
    vector<vector<uint64_t>> scan_projection_mapping, int64_t filterKeyIndex,
    duckdb::Value filterValue)
    : PhysicalNodeScan(sch, oids, projection_mapping, scan_types_,
                       scan_projection_mapping)
{
    is_filter_pushdowned = true;
    filter_pushdown_type = FilterPushdownType::FP_EQ;
    filter_pushdown_key_idxs.push_back(filterKeyIndex);
    eq_filter_pushdown_values.push_back(filterValue);
}

PhysicalNodeScan::PhysicalNodeScan(
    Schema &sch, vector<idx_t> oids,
    vector<vector<uint64_t>> projection_mapping,
    vector<LogicalType> scan_types_,
    vector<vector<uint64_t>> scan_projection_mapping, int64_t filterKeyIndex,
    duckdb::Value l_filterValue, duckdb::Value r_filterValue, bool l_inclusive,
    bool r_inclusive)
    : PhysicalNodeScan(sch, oids, projection_mapping, scan_types_,
                       scan_projection_mapping)
{
    is_filter_pushdowned = true;
    filter_pushdown_type = FilterPushdownType::FP_RANGE;
    filter_pushdown_key_idxs.push_back(filterKeyIndex);
    range_filter_pushdown_values.push_back(
        {l_filterValue, r_filterValue, l_inclusive, r_inclusive});
}

PhysicalNodeScan::PhysicalNodeScan(
    Schema &sch, vector<idx_t> oids,
    vector<vector<uint64_t>> projection_mapping,
    vector<LogicalType> scan_types_,
    vector<vector<uint64_t>> scan_projection_mapping,
    vector<unique_ptr<Expression>> predicates)
    : PhysicalNodeScan(sch, oids, projection_mapping, scan_types_,
                       scan_projection_mapping)
{
    is_filter_pushdowned = true;
    filter_pushdown_type = FilterPushdownType::FP_COMPLEX;
    D_ASSERT(predicates.size() > 0);
    if (predicates.size() > 1) {
        auto conjunction = make_unique<BoundConjunctionExpression>(
            ExpressionType::CONJUNCTION_AND);
        for (auto &expr : predicates) {
            conjunction->children.push_back(move(expr));
        }
        filter_expression = move(conjunction);
    }
    else {
        filter_expression = move(predicates[0]);
    }
    executor = ExpressionExecutor(*(filter_expression.get()));
}

PhysicalNodeScan::PhysicalNodeScan(
    vector<Schema> &sch, Schema &union_schema, vector<idx_t> oids,
    vector<vector<uint64_t>> projection_mapping,
    vector<vector<uint64_t>> scan_projection_mapping)
    : CypherPhysicalOperator(PhysicalOperatorType::NODE_SCAN, union_schema,
                             sch),
      oids(oids),
      projection_mapping(projection_mapping),
      scan_projection_mapping(scan_projection_mapping),
      current_schema_idx(0)  // without pushdown, two mappings are exactly same
{
    num_schemas = sch.size();
    scan_types.resize(num_schemas);
    for (auto i = 0; i < num_schemas; i++) {
        scan_types[i] = std::move(sch[i].getStoredTypes());
    }
    filtered_chunk_buffer.Initialize(union_schema.getStoredTypes());
}

/* Schemaless Equality Filter Pushdown */
PhysicalNodeScan::PhysicalNodeScan(
    vector<Schema> &sch, Schema &union_schema, vector<idx_t> oids,
    vector<vector<uint64_t>> projection_mapping,
    vector<vector<uint64_t>> scan_projection_mapping,
    vector<int64_t> &filterKeyIndexes, vector<duckdb::Value> &filterValues)
    : PhysicalNodeScan(sch, union_schema, oids, projection_mapping,
                       scan_projection_mapping)
{
    is_filter_pushdowned = true;
    filter_pushdown_type = FilterPushdownType::FP_EQ;
    filter_pushdown_key_idxs = move(filterKeyIndexes);
    eq_filter_pushdown_values = move(filterValues);
}

/* Schemaless Range Filter Pushdown */
PhysicalNodeScan::PhysicalNodeScan(
    vector<Schema> &sch, Schema &union_schema, vector<idx_t> oids,
    vector<vector<uint64_t>> projection_mapping,
    vector<vector<uint64_t>> scan_projection_mapping,
    vector<int64_t> &filterKeyIndexes,
    vector<RangeFilterValue> &rangeFilterValues)
    : PhysicalNodeScan(sch, union_schema, oids, projection_mapping,
                       scan_projection_mapping)
{
    is_filter_pushdowned = true;
    filter_pushdown_type = FilterPushdownType::FP_RANGE;
    filter_pushdown_key_idxs = move(filterKeyIndexes);
    range_filter_pushdown_values = move(rangeFilterValues);
}

/* Schemaless Complex Filter Pushdown */
PhysicalNodeScan::PhysicalNodeScan(
    vector<Schema> &sch, Schema &union_schema, vector<idx_t> oids,
    vector<vector<uint64_t>> projection_mapping,
    vector<vector<uint64_t>> scan_projection_mapping,
    vector<unique_ptr<Expression>> predicates)
    : PhysicalNodeScan(sch, union_schema, oids, projection_mapping,
                       scan_projection_mapping)
{
    is_filter_pushdowned = true;
    filter_pushdown_type = FilterPushdownType::FP_COMPLEX;
    D_ASSERT(predicates.size() > 0);
    if (predicates.size() > 1) {
        auto conjunction = make_unique<BoundConjunctionExpression>(
            ExpressionType::CONJUNCTION_AND);
        for (auto &expr : predicates) {
            conjunction->children.push_back(move(expr));
        }
        filter_expression = move(conjunction);
    }
    else {
        filter_expression = move(predicates[0]);
    }
    executor = ExpressionExecutor(*(filter_expression.get()));
}

PhysicalNodeScan::~PhysicalNodeScan() {}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
unique_ptr<LocalSourceState> PhysicalNodeScan::GetLocalSourceState(
    ExecutionContext &context) const
{
    return make_unique<NodeScanState>();
}

void PhysicalNodeScan::GetData(ExecutionContext &context, DataChunk &chunk,
                               LocalSourceState &lstate) const
{
    auto &state = (NodeScanState &)lstate;
    // If first time here, call doScan and get iterator from iTbgppGraphStorageWrapper
    if (!state.iter_inited) {
        state.iter_inited = true;
        bool enable_filter_buffer =
            projection_mapping.size() ==
            1;  // enable buffering only in non-schemaless

        auto initializeAPIResult = context.client->graph_storage_wrapper->InitializeScan(
            state.ext_its, oids, scan_projection_mapping, scan_types,
            enable_filter_buffer);
        D_ASSERT(initializeAPIResult == StoreAPIResult::OK);
    }
    // Delta scan: emit in-memory extent rows after regular scan
    if (state.delta_phase) {
        auto &ds = context.client->db->delta_store;
        if (state.delta_eids.empty() && state.delta_cur == 0) {
            Catalog &cat = context.client->db->GetCatalog();
            std::set<uint16_t> seen;
            for (auto oid : oids) {
                auto *ps = (PropertySchemaCatalogEntry *)cat.GetEntry(*context.client, DEFAULT_SCHEMA, oid);
                if (seen.insert(ps->pid).second)
                    for (auto eid : ds.GetInMemoryExtentIDs(ps->pid))
                        if (auto *b = ds.FindInsertBuffer(eid); b && !b->Empty()) state.delta_eids.push_back(eid);
            }
            if (state.delta_eids.empty()) { state.iter_finished = true; return; }
        }
        while (state.delta_cur < state.delta_eids.size()) {
            auto *buf = ds.FindInsertBuffer(state.delta_eids[state.delta_cur]);
            if (!buf || state.delta_row >= buf->Size()) { state.delta_cur++; state.delta_row = 0; continue; }
            chunk.Reset();
            idx_t n = std::min(buf->Size() - state.delta_row, (idx_t)STANDARD_VECTOR_SIZE);
            uint32_t eid = state.delta_eids[state.delta_cur];
            for (idx_t i = 0; i < n; i++) {
                chunk.SetValue(0, i, Value::UBIGINT(((uint64_t)eid << 32) | (state.delta_row + i)));
                for (idx_t c = 1; c < chunk.ColumnCount(); c++) chunk.SetValue(c, i, Value());
            }
            state.delta_row += n; chunk.SetCardinality(n); return;
        }
        state.iter_finished = true; return;
    }
    if (state.ext_its.empty()) {
        state.iter_finished = true;
        return;
    }

    StoreAPIResult res;
    if (!is_filter_pushdowned) {
        // no filter pushdown
        if (projection_mapping.size() == 1) {
            res = context.client->graph_storage_wrapper->doScan(state.ext_its, chunk,
                                                      types);
        }
        else {
            res = context.client->graph_storage_wrapper->doScan(state.ext_its, chunk,
                                                      projection_mapping, types,
                                                      current_schema_idx);
        }
    }
    else {
        /* TODO @jhha - Even if the minmax array does not exist,
		 * if the number of predicate application results is 0, 
		 * continue scanning the next area. 
		 */
        // filter pushdown applied
        filtered_chunk_buffer.Reset(scan_types[current_schema_idx]);
        if (filter_pushdown_type == FilterPushdownType::FP_RANGE) {
            res = context.client->graph_storage_wrapper->doScan(
                state.ext_its, chunk, filtered_chunk_buffer, projection_mapping,
                types, current_schema_idx,
                filter_pushdown_key_idxs[current_schema_idx],
                range_filter_pushdown_values[current_schema_idx]);
        }
        else if (filter_pushdown_type == FilterPushdownType::FP_EQ) {
            res = context.client->graph_storage_wrapper->doScan(
                state.ext_its, chunk, filtered_chunk_buffer, projection_mapping,
                types, current_schema_idx,
                filter_pushdown_key_idxs[current_schema_idx],
                eq_filter_pushdown_values[current_schema_idx]);
        }
        else {
            res = context.client->graph_storage_wrapper->doScan(
                state.ext_its, chunk, filtered_chunk_buffer, projection_mapping,
                types, current_schema_idx, executor);
        }
    }

    if (res == StoreAPIResult::DONE) {
        current_schema_idx++;
        if (state.ext_its.empty()) {
            if (!is_filter_pushdowned && context.client->db->delta_store.HasInsertData())
                state.delta_phase = true;
            else
                state.iter_finished = true;
        }
        return;
    }
    else {
        state.iter_finished = false;
    }

    chunk.SetSchemaIdx(current_schema_idx);
}

bool PhysicalNodeScan::IsSourceDataRemaining(LocalSourceState &lstate) const
{
    auto &state = (NodeScanState &)lstate;
    return !state.iter_finished;
}

std::string PhysicalNodeScan::ParamsToString() const
{
    string params = "nodescan-params: oids {";
    for (auto i = 0; i < oids.size(); i++) {
        params += std::to_string(oids[i]);
        if (i < oids.size() - 1) {
            params += ", ";
        }
    }
    params += "} ";
    if (filter_expression) {
        params += filter_expression->ToString();
    }
    return params;
}

std::string PhysicalNodeScan::ToString() const
{
    return "NodeScan";
}
}  // namespace duckdb