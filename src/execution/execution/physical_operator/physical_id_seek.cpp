
#include <algorithm>
#include <cmath>
#include <numeric>
#include <queue>
#include "common/typedef.hpp"

// catalog related
#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/partition_catalog_entry.hpp"
#include "catalog/catalog_entry/property_schema_catalog_entry.hpp"
#include "main/client_context.hpp"
#include "main/database.hpp"

#include <string>
#include "common/output_util.hpp"
#include "common/types/rowcol_type.hpp"
#include "common/types/schemaless_data_chunk.hpp"
#include "execution/physical_operator/physical_id_seek.hpp"
#include "storage/extent/extent_iterator.hpp"
#include "icecream.hpp"
#include "planner/expression.hpp"
#include "planner/expression/bound_conjunction_expression.hpp"
#include "planner/expression/bound_constant_expression.hpp"

namespace duckdb {
}
namespace turbolynx {
}
namespace duckdb {
    using namespace turbolynx;
}
namespace turbolynx {
using namespace duckdb;

class IdSeekState : public OperatorState {
   public:
    explicit IdSeekState(ClientContext &client, vector<uint64_t> oids, vector<vector<LogicalType>>& scan_types, vector<vector<uint64_t>>& scan_proj_mapping, idx_t num_total_schemas)
    {
        seqno_to_eid_idx.resize(STANDARD_VECTOR_SIZE, -1);
        eid_to_schema_idx.resize(INITIAL_EXTENT_ID_SPACE, -1);
        ext_it = new ExtentIterator(scan_types, scan_proj_mapping, &io_cache);
        // Per-thread temporary buffers (was PhysicalIdSeek mutable members)
        target_eids.reserve(INITIAL_EXTENT_ID_SPACE);
        num_tuples_per_schema.resize(num_total_schemas, 0);
        materialized_sel.Initialize();
    }

    void InitializeSels(size_t num_schemas)
    {
        if (sels.size() == num_schemas) {
            return;
        }

        sels.resize(num_schemas);
        filter_sels.resize(num_schemas);
        for (auto i = 0; i < num_schemas; i++) {
            sels[i].Initialize();
            filter_sels[i].Initialize();
        }
    }

   public:
    ExtentIterator *ext_it;
    bool need_initialize_extit = true;
    bool has_remaining_output = false;
    idx_t cur_schema_idx;
    vector<idx_t> null_tuples_idx;
    vector<idx_t> eid_to_schema_idx;
    vector<idx_t> seqno_to_eid_idx;

    // Selection vectors (TODO: Optimize this using pools)
    // jhha: we cannot avoid filter_sels.
    // Since columns scanned after filter should have
    // difference sel vector than other columns.
    vector<SelectionVector> sels;
    vector<SelectionVector> filter_sels;

    IOCache io_cache;

    // Per-thread temporary buffers (was mutable members on PhysicalIdSeek)
    vector<ExtentID> target_eids;
    vector<idx_t> num_tuples_per_schema;

    // Per-thread scratch for graph_storage_wrapper::InitializeVertexIndexSeek
    IndexSeekScratch wrapper_scratch;

    // Per-thread filter-pushdown scratch (was on PhysicalIdSeek as mutable
    // members; moved here for thread-safety). Lazily initialized inside
    // Execute() on the first filter-pushdown call.
    vector<unique_ptr<DataChunk>> tmp_chunks;
    vector<bool> is_tmp_chunk_initialized_per_schema;
    //! Per-thread ExpressionExecutors built from the operator's shared
    //! `expressions` AST. ExpressionExecutor holds intermediate state, so
    //! every thread needs its own.
    vector<ExpressionExecutor> executors;
    bool filter_state_initialized = false;
    SelectionVector materialized_sel;
    unique_ptr<DataChunk> materialized_filtered_chunk;
    bool has_materialized_filtered_output = false;
    idx_t seek_node_col_idx = 0;
    vector<ExtentID> seek_target_eids;
    bool seek_target_is_edge = false;
};

static bool IsMappedSeekEid(const vector<idx_t> &eid_to_schema_idx,
                            ExtentID eid) {
    return eid < eid_to_schema_idx.size() &&
           eid_to_schema_idx[eid] != (idx_t)-1;
}

static uint64_t RemapSeekPid(uint64_t pid, bool seek_target_is_edge,
                             const vector<ExtentID> &seek_target_eids,
                             const vector<idx_t> &eid_to_schema_idx) {
    auto raw_eid = GET_EID_FROM_PHYSICAL_ID(pid);
    if (IsMappedSeekEid(eid_to_schema_idx, raw_eid)) {
        return pid;
    }
    // raw_eid is not one of this IdSeek's target extents: the pid belongs to a
    // different partition and cannot alias any row in the target. Drop it.
    return 0;
}

static void BuildSeekInput(ExecutionContext &context, DataChunk &input,
                           idx_t nodeColIdx,
                           const vector<ExtentID> &seek_target_eids,
                           const vector<idx_t> &eid_to_schema_idx,
                           bool seek_target_is_edge,
                           DataChunk &seek_input) {
    seek_input.Initialize(input.GetTypes());
    for (idx_t c = 0; c < input.ColumnCount(); c++) {
        if (c == nodeColIdx) {
            continue;
        }
        seek_input.data[c].Reference(input.data[c]);
    }

    auto &dst_vec = seek_input.data[nodeColIdx];
    auto &dst_validity = FlatVector::Validity(dst_vec);
    auto *dst_data = (uint64_t *)dst_vec.GetData();
    auto &ds = context.client->db->delta_store;

    for (idx_t row = 0; row < input.size(); row++) {
        auto logical_id = input.GetValue(nodeColIdx, row);
        if (logical_id.IsNull()) {
            dst_validity.SetInvalid(row);
            continue;
        }
        auto logical_id_value = logical_id.GetValue<uint64_t>();

        // Skip lids the delta has explicitly invalidated.
        if (ds.IsLogicalIdDeleted(logical_id_value)) {
            dst_validity.SetInvalid(row);
            continue;
        }

        auto current_pid = ds.ResolvePid(logical_id_value);

        // Decide schema membership by extent_id directly rather than using
        // RemapSeekPid's (pid == 0) rejection sentinel. The sentinel collides
        // with the legitimate (extent 0, row 0) disk slot a freshly
        // bootstrapped + checkpointed node lands on, which silently dropped
        // post-checkpoint INCOMING traversals through delta. See PLAN.md
        // Bug B for the failing repro.
        auto raw_eid = GET_EID_FROM_PHYSICAL_ID(current_pid);
        if (!IsMappedSeekEid(eid_to_schema_idx, raw_eid)) {
            dst_validity.SetInvalid(row);
            continue;
        }
        dst_data[row] = current_pid;
    }

    seek_input.SetCardinality(input.size());
    seek_input.SetSchemaIdx(input.GetSchemaIdx());
}

static bool IsSeekFallbackCandidate(const DataChunk &input, idx_t col_idx) {
    switch (input.data[col_idx].GetType().id()) {
    case LogicalTypeId::ID:
    case LogicalTypeId::TINYINT:
    case LogicalTypeId::SMALLINT:
    case LogicalTypeId::INTEGER:
    case LogicalTypeId::BIGINT:
    case LogicalTypeId::UTINYINT:
    case LogicalTypeId::USMALLINT:
    case LogicalTypeId::UINTEGER:
    case LogicalTypeId::UBIGINT:
        return true;
    default:
        return false;
    }
}

static bool HasInMemoryTargets(const vector<ExtentID> &target_eids) {
    for (auto eid : target_eids) {
        if (IsInMemoryExtent(eid)) {
            return true;
        }
    }
    return false;
}

static std::string JoinIdxVec(const vector<uint32_t> &vals) {
    std::string out;
    for (idx_t i = 0; i < vals.size(); i++) {
        if (i) {
            out += ",";
        }
        if (vals[i] == std::numeric_limits<uint32_t>::max()) {
            out += "X";
        } else {
            out += std::to_string(vals[i]);
        }
    }
    return out;
}

static std::string JoinIdxVec64(const vector<uint64_t> &vals) {
    std::string out;
    for (idx_t i = 0; i < vals.size(); i++) {
        if (i) {
            out += ",";
        }
        if (vals[i] == std::numeric_limits<uint64_t>::max()) {
            out += "X";
        } else {
            out += std::to_string(vals[i]);
        }
    }
    return out;
}

static std::string JoinTypeVec(const vector<LogicalType> &vals) {
    std::string out;
    for (idx_t i = 0; i < vals.size(); i++) {
        if (i) {
            out += ",";
        }
        out += vals[i].ToString();
    }
    return out;
}

PhysicalIdSeek::PhysicalIdSeek(Schema &sch, uint64_t id_col_idx,
                               vector<uint64_t> oids,
                               vector<vector<uint64_t>> projection_mapping,
                               vector<uint32_t> &outer_col_map,
                               vector<vector<uint32_t>> &inner_col_maps,
                               vector<uint32_t> &union_inner_col_map,
                               vector<vector<uint64_t>> scan_projection_mapping,
                               vector<vector<duckdb::LogicalType>> scan_types,
                               bool force_output_union, JoinType join_type,
                               size_t num_outer_schemas)
    : CypherPhysicalOperator(PhysicalOperatorType::ID_SEEK, sch),
      id_col_idx(id_col_idx),
      oids(oids),
      projection_mapping(projection_mapping),
      outer_col_map(outer_col_map),
      inner_col_maps(move(inner_col_maps)),
      union_inner_col_map(move(union_inner_col_map)),
      scan_projection_mapping(scan_projection_mapping),
      scan_types(scan_types),
      force_output_union(force_output_union),
      join_type(join_type)
{
    D_ASSERT(oids.size() == projection_mapping.size());
    this->num_outer_schemas = num_outer_schemas;
    num_inner_schemas = this->inner_col_maps.size();
    num_total_schemas = this->num_outer_schemas * num_inner_schemas;
    D_ASSERT(num_total_schemas > 0);

    do_filter_pushdown = false;

    genNonPredColIdxs();
    generatePartialSchemaInfos();
    getUnionScanTypes();
    generateOutputColIdxsForOuter();
    generateOutputColIdxsForInner();
    setupSchemaValidityMasks();
    // Per-thread scratch buffers (target_eids, num_tuples_per_schema) are
    // initialized in IdSeekState constructor.
}

PhysicalIdSeek::PhysicalIdSeek(
    Schema &sch, uint64_t id_col_idx, vector<uint64_t> oids,
    vector<vector<uint64_t>> projection_mapping,
    vector<uint32_t> &outer_col_map, vector<vector<uint32_t>> &inner_col_maps,
    vector<uint32_t> &union_inner_col_map,
    vector<vector<uint64_t>> scan_projection_mapping,
    vector<vector<duckdb::LogicalType>> scan_types,
    vector<vector<unique_ptr<Expression>>> &predicates,
    vector<vector<idx_t>> &pred_col_idxs_per_schema, bool force_output_union,
    JoinType join_type, size_t num_outer_schemas)
    : CypherPhysicalOperator(PhysicalOperatorType::ID_SEEK, sch),
      id_col_idx(id_col_idx),
      oids(oids),
      scan_types(scan_types),
      projection_mapping(projection_mapping),
      outer_col_map(outer_col_map),
      inner_col_maps(move(inner_col_maps)),
      union_inner_col_map(move(union_inner_col_map)),
      scan_projection_mapping(scan_projection_mapping),
      force_output_union(force_output_union),
      pred_col_idxs_per_schema(pred_col_idxs_per_schema),
      join_type(join_type)
{
    D_ASSERT(oids.size() == projection_mapping.size());
    this->num_outer_schemas = num_outer_schemas;
    num_inner_schemas = this->inner_col_maps.size();
    num_total_schemas = this->num_outer_schemas * num_inner_schemas;
    D_ASSERT(num_total_schemas > 0);

    buildExpressionExecutors(predicates);

    // Filter settings
    do_filter_pushdown = true;
    // tmp_chunks / is_tmp_chunk_initialized_per_schema / executors moved
    // to per-thread IdSeekState; built lazily on first Execute() call.

    genNonPredColIdxs();
    generatePartialSchemaInfos();
    getUnionScanTypes();
    generateOutputColIdxsForOuter();
    generateOutputColIdxsForInner();
    setupSchemaValidityMasks();
    // Per-thread scratch buffers (target_eids, num_tuples_per_schema) are
    // initialized in IdSeekState constructor.
}

unique_ptr<OperatorState> PhysicalIdSeek::GetOperatorState(
    ExecutionContext &context) const
{
    auto state = make_unique<IdSeekState>(*(context.client), oids, scan_types, scan_projection_mapping, num_total_schemas);
    std::string oids_str;
    for (idx_t i = 0; i < oids.size(); i++) {
        if (i) {
            oids_str += ",";
        }
        oids_str += std::to_string(oids[i]);
    }
    spdlog::debug("[IDSEEK-STATE] id_col_idx={} oids=[{}] num_inner_schemas={}",
                 id_col_idx, oids_str, num_inner_schemas);
    for (idx_t schema_idx = 0; schema_idx < inner_col_maps.size(); schema_idx++) {
        std::string inner_map =
            (schema_idx < inner_col_maps.size())
                ? JoinIdxVec(inner_col_maps[schema_idx])
                : "";
        std::string scan_proj =
            (schema_idx < scan_projection_mapping.size())
                ? JoinIdxVec64(scan_projection_mapping[schema_idx])
                : "";
        std::string scan_type =
            (schema_idx < scan_types.size())
                ? JoinTypeVec(scan_types[schema_idx])
                : "";
        spdlog::debug(
            "[IDSEEK-SCHEMA] id_col_idx={} schema_idx={} outer_map=[{}] "
            "inner_map=[{}] union_inner=[{}] scan_proj=[{}] scan_types=[{}]",
            id_col_idx, schema_idx, JoinIdxVec(outer_col_map), inner_map,
            JoinIdxVec(union_inner_col_map), scan_proj, scan_type);
    }
    context.client->graph_storage_wrapper->fillEidToMappingIdx(oids,
                                                     scan_projection_mapping,
                                                     state->eid_to_schema_idx);
    auto &catalog = context.client->db->GetCatalog();
    for (auto oid : oids) {
        auto *ps = (PropertySchemaCatalogEntry *)catalog.GetEntry(
            *context.client, DEFAULT_SCHEMA, oid, true);
        if (!ps) {
            continue;
        }
        state->seek_target_eids.insert(state->seek_target_eids.end(),
                                       ps->extent_ids.begin(),
                                       ps->extent_ids.end());
        for (auto inmem_eid :
             context.client->db->delta_store.GetInMemoryExtentIDs(ps->GetPartitionID())) {
            state->seek_target_eids.push_back(inmem_eid);
        }
        if (!state->seek_target_is_edge) {
            auto *part = (PartitionCatalogEntry *)catalog.GetEntry(
                *context.client, DEFAULT_SCHEMA, ps->partition_oid, true);
            if (part && part->name.rfind("epart_", 0) == 0) {
                state->seek_target_is_edge = true;
            }
        }
    }
    std::sort(state->seek_target_eids.begin(), state->seek_target_eids.end());
    state->seek_target_eids.erase(
        std::unique(state->seek_target_eids.begin(), state->seek_target_eids.end()),
        state->seek_target_eids.end());
    return state;
}

OperatorResultType PhysicalIdSeek::Execute(ExecutionContext &context,
                                           DataChunk &input, DataChunk &chunk,
                                           OperatorState &lstate) const
{
    OperatorResultType result;
    // [IDSEEK-PROBE-IN] Log input — when id_col_idx==2 with do_filter, scan for critical msg ids in input.
    if (input.size() > 0 && input.ColumnCount() >= 3 && id_col_idx == 2 && do_filter_pushdown) {
        idx_t eunhye_count = 0;
        idx_t yang_count = 0;
        for (idx_t i = 0; i < input.size(); i++) {
            try {
                auto fid_v = input.data[0].GetValue(i);
                auto mid_v = input.data[1].GetValue(i);
                auto cvid_v = input.data[2].GetValue(i);
                auto fid_s = fid_v.ToString();
                auto mid_s = mid_v.ToString();
                if (fid_s.find("8796093029689") != std::string::npos) eunhye_count++;
                else if (fid_s.find("10995116280436") != std::string::npos) yang_count++;
                // Log critical rows of interest
                if (mid_s == "1236955090897" || mid_s == "1099514180240" ||
                    mid_s == "1924148384434" || mid_s == "687195265362" ||
                    mid_s == "687195574999") {
                    spdlog::debug("[IDSEEK-CRIT] row={} fid={} mid={} cvid={}",
                                 (int)i, fid_s, mid_s, cvid_v.ToString());
                }
            } catch (...) {}
        }
        spdlog::debug("[IDSEEK-PROBE-IN-SCAN] id_col_idx={} in_size={} Yang_count={} EunHye_count={}",
                     id_col_idx, input.size(), yang_count, eunhye_count);
    }
    if (input.size() > 0 && input.ColumnCount() > 0) {
        std::string perm_str;
        idx_t n = std::min<idx_t>(input.size(), 4);
        for (idx_t i = 0; i < n; i++) {
            std::string row = "row" + std::to_string(i) + "=[";
            for (idx_t c = 0; c < input.ColumnCount(); c++) {
                try {
                    auto v = input.data[c].GetValue(i);
                    if (c) row += ",";
                    row += v.ToString();
                } catch (...) { row += ",?"; }
            }
            row += "]";
            perm_str += row + " ";
        }
        auto first_oid = oids.empty() ? 0 : oids[0];
        spdlog::debug("[IDSEEK-PROBE-IN] id_col_idx={} oid={} in_cols={} in_size={} do_filter={} {}",
                     id_col_idx, first_oid, input.ColumnCount(), input.size(),
                     (int)do_filter_pushdown, perm_str);
    }
    if (join_type == JoinType::INNER) {
        result = ExecuteInner(context, input, chunk, lstate);
    } else if (join_type == JoinType::LEFT) {
        result = ExecuteLeft(context, input, chunk, lstate);
    } else {
        throw NotImplementedException("PhysicalIdSeek-Execute");
    }
    // [IDSEEK-PROBE] Log first 4 output rows across all columns + rows 36/329/344.
    if (chunk.size() > 0 && chunk.ColumnCount() > 0) {
        std::string perm_str;
        idx_t n = std::min<idx_t>(chunk.size(), 4);
        for (idx_t i = 0; i < n; i++) {
            std::string row = "row" + std::to_string(i) + "=[";
            for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
                try {
                    auto v = chunk.data[c].GetValue(i);
                    if (c) row += ",";
                    row += v.ToString();
                } catch (...) { row += ",?"; }
            }
            row += "]";
            perm_str += row + " ";
        }
        std::initializer_list<idx_t> extra = {36, 329, 344};
        for (idx_t r : extra) {
            if (r < chunk.size()) {
                std::string row = "row" + std::to_string(r) + "=[";
                for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
                    try { auto v = chunk.data[c].GetValue(r); if (c) row += ","; row += v.ToString(); } catch (...) { row += ",?"; }
                }
                row += "]";
                perm_str += row + " ";
            }
        }
        spdlog::debug("[IDSEEK-PROBE] id_col_idx={} out_cols={} out_size={} {}",
                     id_col_idx, chunk.ColumnCount(), chunk.size(), perm_str);
        // [IDSEEK-SCAN-PHANTOM] locate phantom vid 562949953711426 or id 687195265362 in chunk
        if (chunk.ColumnCount() >= 3 && id_col_idx == 1) {
            for (idx_t r = 0; r < chunk.size(); r++) {
                try {
                    std::string c1 = chunk.data[1].GetValue(r).ToString();
                    std::string c2 = chunk.data[2].GetValue(r).ToString();
                    if (c1 == "562949953711426" || c2 == "687195265362" ||
                        c2 == "1099512954251") {
                        spdlog::debug("[IDSEEK-SCAN-PHANTOM] row={} col1={} col2={}",
                                     (int)r, c1, c2);
                    }
                } catch (...) {}
            }
        }
    }
    return result;
}

OperatorResultType PhysicalIdSeek::ExecuteInner(ExecutionContext &context,
                                                DataChunk &input,
                                                DataChunk &chunk,
                                                OperatorState &lstate) const
{
    if (input.size() == 0) {
        chunk.SetCardinality(0);
        return OperatorResultType::NEED_MORE_INPUT;
    }

    auto &state = (IdSeekState &)lstate;
    idx_t nodeColIdx = id_col_idx;
    D_ASSERT(nodeColIdx < input.ColumnCount());
    DataChunk seek_input;
    BuildSeekInput(context, input, nodeColIdx, state.seek_target_eids,
                   state.eid_to_schema_idx, state.seek_target_is_edge,
                   seek_input);

    // initialize indexseek
    vector<vector<uint32_t>> target_seqnos_per_extent;
    vector<idx_t> mapping_idxs;
    idx_t output_size = 0;

    if (state.need_initialize_extit) {
        initializeSeek(context, seek_input, chunk, state, nodeColIdx, state.target_eids,
                       target_seqnos_per_extent, mapping_idxs);

        if (state.target_eids.empty()) {
            for (idx_t candidate_col = 0; candidate_col < input.ColumnCount();
                 candidate_col++) {
                if (candidate_col == nodeColIdx ||
                    !IsSeekFallbackCandidate(input, candidate_col)) {
                    continue;
                }
                DataChunk candidate_seek_input;
                BuildSeekInput(context, input, candidate_col,
                               state.seek_target_eids, state.eid_to_schema_idx,
                               state.seek_target_is_edge, candidate_seek_input);
                idx_t candidate_node_col_idx = candidate_col;
                initializeSeek(context, candidate_seek_input, chunk, state,
                               candidate_node_col_idx, state.target_eids,
                               target_seqnos_per_extent, mapping_idxs);
                if (!state.target_eids.empty()) {
                    nodeColIdx = candidate_col;
                    seek_input.Destroy();
                    BuildSeekInput(context, input, nodeColIdx,
                                   state.seek_target_eids,
                                   state.eid_to_schema_idx,
                                   state.seek_target_is_edge, seek_input);
                    break;
                }
            }
        }
        state.seek_node_col_idx = nodeColIdx;

        if (state.target_eids.size() == 0) {
            chunk.SetCardinality(0);
            state.has_remaining_output = false;
            state.need_initialize_extit = true;
            return OperatorResultType::OUTPUT_EMPTY;
        }
    }

    // Calculate Format
    auto total_nulls = calculateTotalNulls(
        chunk, state.target_eids, target_seqnos_per_extent, mapping_idxs);
    fillOutSizePerSchema(state, state.target_eids, target_seqnos_per_extent, mapping_idxs);
    auto format = determineFormatByCostModel(state, false, total_nulls);

    if (format == OutputFormat::ROW) {
        doSeekRowMajor(context, seek_input, chunk, state, state.target_eids,
                         target_seqnos_per_extent, mapping_idxs, output_size);
    }
    else if (format == OutputFormat::UNIONALL) {
        state.has_materialized_filtered_output = false;
        doSeekColumnar(context, seek_input, chunk, state, state.target_eids,
                       target_seqnos_per_extent, mapping_idxs, output_size);
        spdlog::debug("[IDSEEK-PATH] id_col_idx={} do_filter={} num_inner_schemas={} has_mat={}",
                     id_col_idx, (int)do_filter_pushdown, num_inner_schemas,
                     (int)state.has_materialized_filtered_output);
        if (state.has_materialized_filtered_output) {
            chunk.Reset();
            if (output_size == 0) {
                chunk.SetCardinality(0);
            }
            else {
                D_ASSERT(state.materialized_filtered_chunk != nullptr);
                auto &filtered_output = *state.materialized_filtered_chunk;
                for (idx_t i = 0;
                     i < outer_col_map.size() && i < input.ColumnCount(); i++) {
                    if (outer_col_map[i] != std::numeric_limits<uint32_t>::max()) {
                        chunk.data[outer_col_map[i]].Slice(
                            input.data[i], state.materialized_sel, output_size);
                    }
                }
                for (idx_t col_idx = 0; col_idx < chunk.ColumnCount();
                     col_idx++) {
                    if (!isInnerColIdx(col_idx)) {
                        continue;
                    }
                    chunk.data[col_idx].Slice(filtered_output.data[col_idx],
                                              state.materialized_sel,
                                              output_size);
                }
                chunk.SetCardinality(output_size);
            }
            state.has_materialized_filtered_output = false;
            state.has_remaining_output = false;
            state.need_initialize_extit = true;
            return OperatorResultType::NEED_MORE_INPUT;
        }
        markInvalidForUnseekedValues(chunk, state, state.target_eids,
                                      target_seqnos_per_extent, mapping_idxs);
    }

    nullifyValuesForPrunedExtents(chunk, state, state.target_eids.size(),
                                  target_seqnos_per_extent);
    return referInputChunk(input, chunk, state, output_size);
}

OperatorResultType PhysicalIdSeek::ExecuteLeft(ExecutionContext &context,
                                               DataChunk &input,
                                               DataChunk &chunk,
                                               OperatorState &lstate) const
{
    if (input.size() == 0) {
        chunk.SetCardinality(0);
        return OperatorResultType::NEED_MORE_INPUT;
    }

    auto &state = (IdSeekState &)lstate;
    idx_t nodeColIdx = id_col_idx;
    D_ASSERT(nodeColIdx < input.ColumnCount());
    DataChunk seek_input;
    BuildSeekInput(context, input, nodeColIdx, state.seek_target_eids,
                   state.eid_to_schema_idx, state.seek_target_is_edge,
                   seek_input);
    idx_t output_idx = 0;

    // initialize indexseek
    vector<vector<uint32_t>> target_seqnos_per_extent;
    vector<idx_t> mapping_idxs;

    context.client->graph_storage_wrapper->InitializeVertexIndexSeek(
        state.ext_it, seek_input, nodeColIdx, state.target_eids, target_seqnos_per_extent, mapping_idxs,
        state.null_tuples_idx, state.eid_to_schema_idx, &state.io_cache,
        state.wrapper_scratch);

    if (state.target_eids.empty()) {
        for (idx_t candidate_col = 0; candidate_col < input.ColumnCount();
             candidate_col++) {
            if (candidate_col == nodeColIdx ||
                !IsSeekFallbackCandidate(input, candidate_col)) {
                continue;
            }
            DataChunk candidate_seek_input;
            BuildSeekInput(context, input, candidate_col,
                           state.seek_target_eids, state.eid_to_schema_idx,
                           state.seek_target_is_edge, candidate_seek_input);
            context.client->graph_storage_wrapper->InitializeVertexIndexSeek(
                state.ext_it, candidate_seek_input, candidate_col,
                state.target_eids, target_seqnos_per_extent, mapping_idxs,
                state.null_tuples_idx, state.eid_to_schema_idx,
                &state.io_cache, state.wrapper_scratch);
            if (!state.target_eids.empty()) {
                nodeColIdx = candidate_col;
                seek_input.Destroy();
                BuildSeekInput(context, input, nodeColIdx,
                               state.seek_target_eids,
                               state.eid_to_schema_idx,
                               state.seek_target_is_edge, seek_input);
                break;
            }
        }
    }
    state.seek_node_col_idx = nodeColIdx;

    fillSeqnoToEIDIdx(target_seqnos_per_extent, state.seqno_to_eid_idx);

    // TODO
    bool do_unionall = true;

    if (do_unionall) {
        doSeekColumnar(context, seek_input, chunk, state, state.target_eids,
                       target_seqnos_per_extent, mapping_idxs, output_idx);
    }
    else {
        doSeekRowMajor(context, seek_input, chunk, state, state.target_eids,
                         target_seqnos_per_extent, mapping_idxs, output_idx);
    }

    return referInputChunkLeft(input, chunk, state, output_idx);
}

void PhysicalIdSeek::initializeSeek(
    ExecutionContext &context, DataChunk &input, DataChunk &chunk,
    IdSeekState &state, idx_t nodeColIdx, vector<ExtentID> &target_eids,
    vector<vector<uint32_t>> &target_seqnos_per_extent,
    vector<idx_t> &mapping_idxs) const
{
    state.null_tuples_idx.clear();
    context.client->graph_storage_wrapper->InitializeVertexIndexSeek(
        state.ext_it, input, nodeColIdx, target_eids, target_seqnos_per_extent, mapping_idxs,
        state.null_tuples_idx, state.eid_to_schema_idx, &state.io_cache,
        state.wrapper_scratch);
    state.need_initialize_extit = false;
    state.has_remaining_output = false;
    state.cur_schema_idx = 0;
    state.InitializeSels(1);
    chunk.SetSchemaIdx(input.GetSchemaIdx());
    chunk.Reset();
    fillSeqnoToEIDIdx(target_eids.size(), target_seqnos_per_extent, state.seqno_to_eid_idx);
    markInvalidForColumnsToUnseek(chunk, target_eids, mapping_idxs);
}

void PhysicalIdSeek::InitializeOutputChunks(
    std::vector<unique_ptr<DataChunk>> &output_chunks, Schema &output_schema,
    idx_t idx)
{
    idx_t inner_idx = idx % inner_col_maps.size();
    D_ASSERT(inner_idx < inner_col_maps.size());

    auto opOutputChunk = std::make_unique<DataChunk>();
    auto stored_types = output_schema.getStoredTypes();
    if (stored_types.empty()) {
        // EXISTS subquery decorrelation: ORCA pruned all output columns.
        // Add a dummy BOOLEAN column so DataChunk is valid.
        stored_types.push_back(LogicalType::BOOLEAN);
    }
    opOutputChunk->Initialize(stored_types);

    if (!force_output_union) {
        for (auto i = 0; i < union_inner_col_map.size(); i++) {
            if (union_inner_col_map[i] < opOutputChunk->ColumnCount()) {
                opOutputChunk->data[union_inner_col_map[i]].SetIsValid(true);
            }
        }
    }
    output_chunks.push_back(std::move(opOutputChunk));
}

void PhysicalIdSeek::doSeekColumnar(
    ExecutionContext &context, DataChunk &input, DataChunk &chunk,
    OperatorState &lstate, vector<ExtentID> &target_eids,
    vector<vector<uint32_t>> &target_seqnos_per_extent,
    vector<idx_t> &mapping_idxs, idx_t &output_size) const
{
    auto &state = (IdSeekState &)lstate;
    idx_t nodeColIdx = state.seek_node_col_idx;
    if (!do_filter_pushdown) {
        // Special handling for OPTIONAL MATCH (ALL NULL Case)
        if (target_eids.size() == 0) {
            for (auto inner_col_idx: union_inner_col_map) {
                chunk.data[inner_col_idx].SetIsValid(false);
            }
            return;
        }
        for (u_int64_t extentIdx = 0; extentIdx < target_eids.size();
             extentIdx++) {
            const vector<uint32_t> &output_col_idx =
                inner_output_col_idxs[mapping_idxs[extentIdx]];
            auto &non_pred_col_idxs =
                non_pred_col_idxs_per_schema[mapping_idxs[extentIdx]];
            context.client->graph_storage_wrapper->doVertexIndexSeek(
                state.ext_it, chunk, input, nodeColIdx, target_eids,
                target_seqnos_per_extent, non_pred_col_idxs, extentIdx,
                output_col_idx);
        }
        output_size = input.size();
    }
    else {
        if (inner_col_maps.size() > 1) {
            // Multi-schema filter pushdown cannot share a single tmp chunk or
            // executor: each inner schema has its own scan layout and predicate
            // bindings. Evaluate predicates schema-by-schema, merge the passing
            // input rows, then rescan the final output columns for only those
            // rows.
            if (!state.filter_state_initialized) {
                state.tmp_chunks.clear();
                for (idx_t s = 0; s < num_inner_schemas; s++) {
                    state.tmp_chunks.push_back(std::make_unique<DataChunk>());
                }
                state.is_tmp_chunk_initialized_per_schema.assign(
                    num_inner_schemas, false);
                state.executors.resize(expressions.size());
                for (idx_t i = 0; i < expressions.size(); i++) {
                    state.executors[i].AddExpression(*(expressions[i]));
                }
                state.filter_state_initialized = true;
            }

            state.InitializeSels(num_inner_schemas);
            vector<bool> row_selected(input.size(), false);
            vector<bool> schema_seen(num_inner_schemas, false);
            vector<bool> schema_tmp_prepared(num_inner_schemas, false);
            vector<SelectionVector> schema_candidate_sels(num_inner_schemas);
            vector<idx_t> schema_candidate_counts(num_inner_schemas, 0);
            for (idx_t schema_idx = 0; schema_idx < num_inner_schemas;
                 schema_idx++) {
                schema_candidate_sels[schema_idx].Initialize();
            }

            for (u_int64_t extentIdx = 0; extentIdx < target_eids.size();
                 extentIdx++) {
                auto schema_idx = mapping_idxs[extentIdx];
                D_ASSERT(schema_idx < num_inner_schemas);
                schema_seen[schema_idx] = true;
                for (auto seqno : target_seqnos_per_extent[extentIdx]) {
                    schema_candidate_sels[schema_idx].set_index(
                        schema_candidate_counts[schema_idx]++, seqno);
                }

                auto &tmp_chunk = *(state.tmp_chunks[schema_idx].get());
                if (!schema_tmp_prepared[schema_idx]) {
                    if (state.is_tmp_chunk_initialized_per_schema[schema_idx]) {
                        tmp_chunk.Reset();
                    }
                    else {
                        vector<LogicalType> tmp_chunk_type;
                        auto lhs_type = input.GetTypes();
                        getOutputTypesForFilteredSeek(lhs_type,
                                                      scan_types[schema_idx],
                                                      tmp_chunk_type);
                        tmp_chunk.Initialize(tmp_chunk_type);
                        state.is_tmp_chunk_initialized_per_schema[schema_idx] =
                            true;
                    }
                    schema_tmp_prepared[schema_idx] = true;
                }

                vector<uint32_t> output_col_idx;
                getOutputIdxsForFilteredSeek(schema_idx, output_col_idx);
                auto &pred_col_idxs = pred_col_idxs_per_schema[schema_idx];
                if (pred_col_idxs.empty()) {
                    continue;
                }
                context.client->graph_storage_wrapper->doVertexIndexSeek(
                    state.ext_it, tmp_chunk, input, nodeColIdx, target_eids,
                    target_seqnos_per_extent, pred_col_idxs, extentIdx,
                    output_col_idx);
            }

            for (idx_t schema_idx = 0; schema_idx < num_inner_schemas;
                 schema_idx++) {
                if (!schema_seen[schema_idx]) {
                    continue;
                }
                auto &pred_col_idxs = pred_col_idxs_per_schema[schema_idx];
                if (pred_col_idxs.empty()) {
                    // No predicate columns for this schema: the planner gave us
                    // a trivial TRUE filter, so every row mapped to the schema
                    // passes.
                    for (u_int64_t extentIdx = 0; extentIdx < target_eids.size();
                         extentIdx++) {
                        if (mapping_idxs[extentIdx] != schema_idx) {
                            continue;
                        }
                        for (auto seqno : target_seqnos_per_extent[extentIdx]) {
                            row_selected[seqno] = true;
                        }
                    }
                    continue;
                }

                auto &tmp_chunk = *(state.tmp_chunks[schema_idx].get());
                for (idx_t i = 0; i < input.ColumnCount(); i++) {
                    tmp_chunk.data[i].Reference(input.data[i]);
                }
                tmp_chunk.SetCardinality(input.size());

                DataChunk schema_input;
                schema_input.Initialize(tmp_chunk.GetTypes());
                schema_input.Slice(tmp_chunk, schema_candidate_sels[schema_idx],
                                   schema_candidate_counts[schema_idx]);
                auto local_count =
                    state.executors[schema_idx].SelectExpression(
                        schema_input, state.filter_sels[schema_idx]);
                for (idx_t i = 0; i < local_count; i++) {
                    auto candidate_idx =
                        state.filter_sels[schema_idx].get_index(i);
                    row_selected[schema_candidate_sels[schema_idx].get_index(
                        candidate_idx)] = true;
                }
            }

            output_size = 0;
            for (idx_t row = 0; row < input.size(); row++) {
                if (!row_selected[row]) {
                    continue;
                }
                state.materialized_sel.set_index(output_size++, row);
            }
            state.has_materialized_filtered_output = true;
            if (output_size == 0) {
                return;
            }

            vector<vector<uint32_t>> target_seqnos_per_extent_after_filter;
            getFilteredTargetSeqno(state.seqno_to_eid_idx,
                                   target_seqnos_per_extent.size(),
                                   state.materialized_sel.data(), output_size,
                                   target_seqnos_per_extent_after_filter);

            if (state.ext_it && state.ext_it->IsInitialized()) {
                state.ext_it->Rewind();
            }

            if (!state.materialized_filtered_chunk) {
                state.materialized_filtered_chunk = std::make_unique<DataChunk>();
                state.materialized_filtered_chunk->Initialize(chunk.GetTypes());
            }
            auto &filtered_output = *state.materialized_filtered_chunk;
            filtered_output.SetSchemaIdx(input.GetSchemaIdx());
            filtered_output.Reset();
            markInvalidForColumnsToUnseek(filtered_output, target_eids,
                                          mapping_idxs);

            for (u_int64_t extentIdx = 0; extentIdx < target_eids.size();
                 extentIdx++) {
                const vector<uint32_t> &output_col_idx =
                    inner_output_col_idxs[mapping_idxs[extentIdx]];
                vector<idx_t> cols_to_include;
                cols_to_include.reserve(output_col_idx.size());
                for (auto col_idx : output_col_idx) {
                    cols_to_include.push_back(col_idx);
                }
                context.client->graph_storage_wrapper->doVertexIndexSeek(
                    state.ext_it, filtered_output, input, nodeColIdx,
                    target_eids, target_seqnos_per_extent_after_filter,
                    cols_to_include, extentIdx, output_col_idx);
            }

            markInvalidForUnseekedValues(filtered_output, state, target_eids,
                                         target_seqnos_per_extent_after_filter,
                                         mapping_idxs);
            filtered_output.SetCardinality(input.size());
            return;
        }

        // Assume single schema
        idx_t chunk_idx = input.GetSchemaIdx();
        // Lazily build per-thread filter scratch on the first call
        if (!state.filter_state_initialized) {
            state.tmp_chunks.clear();
            for (idx_t s = 0; s < num_total_schemas; s++) {
                state.tmp_chunks.push_back(std::make_unique<DataChunk>());
            }
            state.is_tmp_chunk_initialized_per_schema.assign(num_total_schemas, false);
            state.executors.resize(expressions.size());
            for (idx_t i = 0; i < expressions.size(); i++) {
                state.executors[i].AddExpression(*(expressions[i]));
            }
            state.filter_state_initialized = true;
        }
        auto &tmp_chunk = *(state.tmp_chunks[chunk_idx].get());
        vector<vector<uint32_t>> chunk_idx_to_output_cols_idx(1);
        getOutputIdxsForFilteredSeek(chunk_idx,
                                     chunk_idx_to_output_cols_idx[0]);

        if (state.is_tmp_chunk_initialized_per_schema[chunk_idx]) {
            tmp_chunk.Reset();
        }

        for (u_int64_t extentIdx = 0; extentIdx < target_eids.size();
             extentIdx++) {
            // init intermediate chunk
            if (!state.is_tmp_chunk_initialized_per_schema[chunk_idx]) {
                vector<LogicalType> tmp_chunk_type;
                auto lhs_type = input.GetTypes();
                getOutputTypesForFilteredSeek(
                    lhs_type, scan_types[mapping_idxs[extentIdx]],
                    tmp_chunk_type);
                tmp_chunk.Initialize(tmp_chunk_type);
                state.is_tmp_chunk_initialized_per_schema[chunk_idx] = true;
            }

            // Get output col idx
            const auto &output_col_idx = chunk_idx_to_output_cols_idx[0];
            auto &pred_col_idxs =
                pred_col_idxs_per_schema[mapping_idxs[extentIdx]];
            // do VertexIdSeek (but only scan cols used in filter)
            context.client->graph_storage_wrapper->doVertexIndexSeek(
                state.ext_it, tmp_chunk, input, nodeColIdx, target_eids,
                target_seqnos_per_extent, pred_col_idxs, extentIdx,
                output_col_idx);
        }

        // Filter may have column on lhs. Make tmp_chunk reference it
        for (int i = 0; i < input.ColumnCount(); i++) {
            tmp_chunk.data[i].Reference(input.data[i]);
        }
        tmp_chunk.SetCardinality(input.size());

        output_size = state.executors[0].SelectExpression(tmp_chunk, state.sels[0]);

        // Rewind only when we actually initialized a base-extent iterator.
        // In-memory-only seeks populate remaining columns directly from the
        // delta insert buffer and have nothing to rewind here.
        if (chunk_idx_to_output_cols_idx[0].size() > 0 && state.ext_it &&
            state.ext_it->IsInitialized()) {
            state.ext_it->Rewind();
        }
        auto &non_pred_col_idxs = non_pred_col_idxs_per_schema[0];
        if (non_pred_col_idxs.size() > 0 && output_size > 0) {
            vector<vector<uint32_t>> target_seqnos_per_extent_after_filter;
            getFilteredTargetSeqno(state.seqno_to_eid_idx,
                                   target_seqnos_per_extent.size(),
                                   state.sels[0].data(), output_size,
                                   target_seqnos_per_extent_after_filter);
            // Perform actual scan — use the same tmp_chunk as the first scan
            auto &output_col_idx = chunk_idx_to_output_cols_idx[0];
            for (u_int64_t extentIdx = 0; extentIdx < target_eids.size();
                 extentIdx++) {
                context.client->graph_storage_wrapper->doVertexIndexSeek(
                    state.ext_it, tmp_chunk, input, nodeColIdx, target_eids,
                    target_seqnos_per_extent_after_filter, non_pred_col_idxs,
                    extentIdx, output_col_idx);
            }
        }
    }
}

void PhysicalIdSeek::doSeekRowMajor(
    ExecutionContext &context, DataChunk &input, DataChunk &chunk,
    OperatorState &lstate, vector<ExtentID> &target_eids,
    vector<vector<uint32_t>> &target_seqnos_per_extent,
    vector<idx_t> &mapping_idxs, idx_t &output_idx) const
{
    auto &state = (IdSeekState &)lstate;
    idx_t nodeColIdx = state.seek_node_col_idx;

    if (target_eids.size() == 0) throw NotImplementedException("doSeekRowMajor No TargetEIDs");
    if (HasInMemoryTargets(target_eids)) {
        idx_t output_size_tmp = 0;
        doSeekColumnar(context, input, chunk, lstate, target_eids,
                       target_seqnos_per_extent, mapping_idxs, output_size_tmp);
        output_idx = output_size_tmp;
        return;
    }

    if (!do_filter_pushdown) {
        if (union_inner_col_map_wo_id.size() == 0) {
            doSeekColumnar(context, input, chunk, state, target_eids,
                           target_seqnos_per_extent, mapping_idxs, output_idx);
        }
        else {
            chunk.SetHasRowChunk(true);
            // create rowcol_t column for the row chunk

            chunk.InitializeRowColumn(union_inner_col_map_wo_id, input.size());
            Vector &rowcol = chunk.data[union_inner_col_map_wo_id[0]];

            rowcol_t *rowcol_arr = (rowcol_t *)rowcol.GetData();
            for (u_int64_t extentIdx = 0; extentIdx < target_eids.size();
                 extentIdx++) {
                for (idx_t i = 0;
                     i < target_seqnos_per_extent[extentIdx].size(); i++) {
                    idx_t schema_idx = mapping_idxs[extentIdx];
                    idx_t row_data_seqno = target_seqnos_per_extent[extentIdx][i];
                    PartialSchema *schema_ptr =
                        (PartialSchema
                             *)(&partial_schemas[schema_idx]);
                    rowcol_arr[row_data_seqno]
                        .schema_ptr = (char *)schema_ptr;
                    rowcol_arr[row_data_seqno].schema_idx
                        = schema_idx;
                    rowcol_arr[row_data_seqno].offset =
                        schema_ptr->getStoredTypesSize();
                }
            }
            
            // For pruned seqnos
            for (u_int64_t extentIdx = target_eids.size(); extentIdx < target_seqnos_per_extent.size();
                 extentIdx++) {
                for (idx_t i = 0;
                     i < target_seqnos_per_extent[extentIdx].size(); i++) {
                    rowcol_arr[target_seqnos_per_extent[extentIdx][i]]
                        .schema_ptr = (char *)&partial_schemas[partial_schemas.size() - 1];
                    rowcol_arr[target_seqnos_per_extent[extentIdx][i]].schema_idx 
                        = partial_schemas.size() - 1;
                    rowcol_arr[target_seqnos_per_extent[extentIdx][i]].offset = 0;
                }
            }

            uint64_t accm_offset = 0;
            for (idx_t i = 0; i < input.size(); i++) {
                idx_t total_types_size = rowcol_arr[i].offset;
                rowcol_arr[i].offset = accm_offset;
                accm_offset += total_types_size;
            }
            try {
                chunk.CreateRowMajorStore(union_inner_col_map_wo_id, accm_offset, (schema_mask_ptr_t)&schema_validity_masks);
            }
            catch (const std::exception &e) {
                std::cerr << e.what() << '\n';
            }

            for (u_int64_t extentIdx = 0; extentIdx < target_eids.size();
                 extentIdx++) {
                // Note: Row store is shared accross all column
                const vector<uint32_t> &output_col_idx =
                    inner_output_col_idxs[mapping_idxs[extentIdx]];
                context.client->graph_storage_wrapper->doVertexIndexSeek(
                    state.ext_it, chunk, input, nodeColIdx, target_eids,
                    target_seqnos_per_extent, extentIdx, out_id_col_idx, rowcol,
                    chunk.GetRowMajorStore(union_inner_col_map_wo_id[0]),
                    output_col_idx, output_idx);
            }
        }
    }
    else {
        // Filter pushdown in row-major mode: fall back to columnar path
        // which already handles filter pushdown correctly.
        idx_t output_size_tmp = 0;
        doSeekColumnar(context, input, chunk, lstate, target_eids,
                       target_seqnos_per_extent, mapping_idxs, output_size_tmp);
        output_idx = output_size_tmp;
    }
}

OperatorResultType PhysicalIdSeek::referInputChunk(DataChunk &input,
                                                   DataChunk &chunk,
                                                   OperatorState &lstate,
                                                   idx_t output_size) const
{
    auto &state = (IdSeekState &)lstate;
    // for original ones reference existing columns
    if (!do_filter_pushdown) {
        idx_t schema_idx = input.GetSchemaIdx();
        D_ASSERT(schema_idx < num_outer_schemas);
        // Map input columns to output using outer_col_map.
        // input may have more columns than outer_col_map when decorrelated
        // subqueries produce extra key columns not in the original mapping.
        for (idx_t i = 0; i < outer_col_map.size() && i < input.ColumnCount(); i++) {
            if (outer_col_map[i] != std::numeric_limits<uint32_t>::max()) {
                D_ASSERT(outer_col_map[i] < chunk.ColumnCount());
                chunk.data[outer_col_map[i]].Reference(input.data[i]);
            }
        }
        chunk.SetCardinality(input.size());
    }
    else {
        idx_t schema_idx = input.GetSchemaIdx();
        auto &tmp_chunk = *(state.tmp_chunks[schema_idx].get());
        std::string ocm_str;
        for (idx_t i = 0; i < outer_col_map.size(); i++) {
            if (i) ocm_str += ",";
            if (outer_col_map[i] == std::numeric_limits<uint32_t>::max()) ocm_str += "X";
            else ocm_str += std::to_string(outer_col_map[i]);
        }
        std::string icm_str;
        for (idx_t i = 0; i < inner_col_maps[0].size(); i++) {
            if (i) icm_str += ",";
            if (inner_col_maps[0][i] == std::numeric_limits<uint32_t>::max()) icm_str += "X";
            else icm_str += std::to_string(inner_col_maps[0][i]);
        }
        std::string sels_str;
        idx_t ns = std::min<idx_t>(output_size, 4);
        for (idx_t i = 0; i < ns; i++) {
            if (i) sels_str += ",";
            sels_str += std::to_string(state.sels[0].get_index(i));
        }
        spdlog::debug("[IDSEEK-REFER] id_col_idx={} out_size={} input_cols={} chunk_cols={} tmp_cols={} outer_map=[{}] inner_map=[{}] sels[0..3]=[{}]",
                     id_col_idx, output_size, input.ColumnCount(), chunk.ColumnCount(),
                     tmp_chunk.ColumnCount(), ocm_str, icm_str, sels_str);
        for (idx_t i = 0; i < outer_col_map.size() && i < input.ColumnCount(); i++) {
            if (outer_col_map[i] != std::numeric_limits<uint32_t>::max()) {
                D_ASSERT(outer_col_map[i] < chunk.ColumnCount());
                chunk.data[outer_col_map[i]].Slice(input.data[i], state.sels[0],
                                                   output_size);
            }
        }
        // Slice filter columns (note that those columns are appened to the outer cols)
        for (int i = 0; i < inner_col_maps[0].size(); i++) {
            if (inner_col_maps[0][i] != std::numeric_limits<uint32_t>::max()) {
                chunk.data[inner_col_maps[0][i]].Slice(
                    tmp_chunk.data[i + input.ColumnCount()], state.sels[0],
                    output_size);
            }
        }
        chunk.SetCardinality(output_size);
    }

    state.has_remaining_output = false;
    state.need_initialize_extit = true;

    return OperatorResultType::NEED_MORE_INPUT;
}

OperatorResultType PhysicalIdSeek::referInputChunkLeft(DataChunk &input,
                                                       DataChunk &chunk,
                                                       OperatorState &lstate,
                                                       idx_t output_idx) const
{
    auto &state = (IdSeekState &)lstate;
    // for original ones reference existing columns
    if (!do_filter_pushdown) {
        idx_t schema_idx = input.GetSchemaIdx();
        D_ASSERT(schema_idx < num_outer_schemas);
        D_ASSERT(input.ColumnCount() == outer_col_map.size());
        for (int i = 0; i < input.ColumnCount(); i++) {
            if (outer_col_map[i] != std::numeric_limits<uint32_t>::max()) {
                D_ASSERT(outer_col_map[i] < chunk.ColumnCount());
                chunk.data[outer_col_map[i]].Reference(input.data[i]);
            }
        }

        // Nullify inner columns for unmatched tuples across ALL inner schemas
        for (idx_t inner_s = 0; inner_s < inner_col_maps.size(); inner_s++) {
            for (int i = 0; i < inner_col_maps[inner_s].size(); i++) {
                // Else case means  the filter-only column case.
                if (inner_col_maps[inner_s][i] < chunk.ColumnCount()) {
                    D_ASSERT(
                        chunk.data[inner_col_maps[inner_s][i]].GetVectorType() ==
                        VectorType::FLAT_VECTOR);
                    auto &validity = FlatVector::Validity(
                        chunk.data[inner_col_maps[inner_s][i]]);
                    D_ASSERT(inner_col_maps[inner_s][i] < chunk.ColumnCount());
                    for (auto j = 0; j < state.null_tuples_idx.size(); j++) {
                        validity.SetInvalid(state.null_tuples_idx[j]);
                    }
                }
            }
        }
        chunk.SetCardinality(input.size());
    }
    else {
        idx_t schema_idx = input.GetSchemaIdx();
        auto &tmp_chunk = *(state.tmp_chunks[schema_idx].get());
        D_ASSERT(input.ColumnCount() == outer_col_map.size());
        for (int i = 0; i < input.ColumnCount(); i++) {
            if (outer_col_map[i] != std::numeric_limits<uint32_t>::max()) {
                D_ASSERT(outer_col_map[i] < chunk.ColumnCount());
                chunk.data[outer_col_map[i]].Slice(input.data[i], state.sels[0],
                                                   output_idx);
            }
        }
        for (int i = 0; i < inner_col_maps[schema_idx].size();
             i++) {  // TODO inner_col_maps[schema_idx]
            chunk.data[inner_col_maps[schema_idx][i]].Slice(
                tmp_chunk.data[i + input.ColumnCount()], state.sels[0],
                output_idx);
        }
        chunk.SetCardinality(output_idx);
    }
    return OperatorResultType::NEED_MORE_INPUT;
}

void PhysicalIdSeek::setupSchemaValidityMasks() {
    for (size_t i = 0; i < union_inner_col_map_wo_id.size(); i++) {
        schema_validity_masks.push_back(ValidityMask(partial_schemas.size()));
        auto &mask = schema_validity_masks.back();
        for (size_t j = 0; j < partial_schemas.size(); j++) {
            mask.Set(j, partial_schemas[j].hasIthCol(i));
        }
    }
}

void PhysicalIdSeek::generatePartialSchemaInfos()
{
    auto &union_types = this->schema.getStoredTypesRef();
    if (union_inner_col_map.size() == 0) {
        for (auto i = 0; i < inner_col_maps[0].size(); i++) {
            union_inner_col_map.push_back(inner_col_maps[0][i]);
        }
    }
    for (auto i = 0; i < inner_col_maps.size(); i++) {
        // Remove ID column for rowcol_t
        auto &ith_scan_type = scan_types[i];
        auto num_id_columns = 0;
        std::queue<idx_t> id_col_queue;
        for (auto j = 0; j < ith_scan_type.size(); j++) {
            if (ith_scan_type[j].id() == LogicalTypeId::ID) {
                num_id_columns++;
                id_col_queue.push(j);
            }
        }

        idx_t idx_shift_for_non_id_columns = 0;
        uint64_t accumulated_offset = 0;
        partial_schemas.push_back(PartialSchema());
        partial_schemas[i].offset_info.resize(
            union_inner_col_map.size() - num_id_columns, -1);

        for (auto j = 0; j < inner_col_maps[i].size(); j++) {
            // TODO check if inefficient
            if (inner_col_maps[i][j] ==
                std::numeric_limits<uint32_t>::
                    max())  // this case is not handled well, please fix this
                continue;
            if (ith_scan_type[j].id() == LogicalTypeId::ID)
                continue;

            // Check if current index is greater than the front of the queue
            if (!id_col_queue.empty() && j >= id_col_queue.front()) {
                // Increment the shift and pop the queue
                idx_shift_for_non_id_columns++;
                id_col_queue.pop();
            }

            auto it =
                std::find(union_inner_col_map.begin(),
                          union_inner_col_map.end(), inner_col_maps[i][j]);
            auto pos =
                it - union_inner_col_map.begin() - idx_shift_for_non_id_columns;
            partial_schemas[i].offset_info[pos] = accumulated_offset;
            accumulated_offset +=
                GetTypeIdSize(union_types[inner_col_maps[i][j]].InternalType());
        }
        partial_schemas[i].stored_types_size = accumulated_offset;
    }
    partial_schemas.push_back(PartialSchema()); // empty schema for pruned
}

void PhysicalIdSeek::getOutputTypesForFilteredSeek(
    vector<LogicalType> &lhs_type, vector<LogicalType> &scan_type,
    vector<LogicalType> &out_type) const
{
    out_type = lhs_type;
    for (auto i = 0; i < scan_type.size(); i++) {
        out_type.push_back(scan_type[i]);
    }
}

void PhysicalIdSeek::getOutputIdxsForFilteredSeek(
    idx_t chunk_idx, vector<uint32_t> &output_col_idx) const
{
    /**
     * In filtered seek, we do seek on outer cols + inner cols chunk
     * without any projection.
     * However, output_col_idx is based on the output cols, which is
     * the result of the projection.
     * Therefore, we need to postprocess output_col_idx.
     * 
     * Strong assumption: inner cols are appended to the outer cols in the output.
    */
    auto inner_col_maps_idx = chunk_idx % inner_col_maps.size();
    auto outer_size = outer_col_map.size();
    auto inner_size = inner_col_maps[inner_col_maps_idx].size();
    output_col_idx.reserve(inner_size);
    for (idx_t i = 0; i < inner_size; i++) {
        output_col_idx.push_back(i + outer_size);
    }
}

void PhysicalIdSeek::getFilteredTargetSeqno(
    vector<idx_t> &seqno_to_eid_idx, size_t num_extents, const sel_t *sel_idxs,
    size_t count, vector<vector<uint32_t>> &out_seqnos) const
{
    out_seqnos.clear();  // Ensure the output is empty before starting.
    out_seqnos.resize(
        num_extents);  // Prepare the output with the correct number of inner vectors.
    for (auto &out_vec : out_seqnos) {
        out_vec.reserve(count);
    }

    for (auto i = 0; i < count; i++) {
        auto seqno = sel_idxs[i];
        auto eid_idx = seqno_to_eid_idx[seqno];
        if (eid_idx != -1) {
            out_seqnos[eid_idx].push_back(seqno);
        }
    }
}

void PhysicalIdSeek::genNonPredColIdxs()
{
    non_pred_col_idxs_per_schema.resize(inner_col_maps.size());
    for (auto i = 0; i < this->inner_col_maps.size(); i++) {
        auto &inner_col_map = this->inner_col_maps[i];
        if (!do_filter_pushdown) {
            for (auto j = 0; j < inner_col_map.size(); j++) {
                non_pred_col_idxs_per_schema[i].push_back(inner_col_map[j]);
            }
        }
        else {
            // In filter pushdown case, the tmp_chunk layout is:
            //   [outer_col_0, ..., outer_col_n, inner_col_0, ..., inner_col_m]
            // Outer columns are filled from input via Reference(), not scanned
            // from the extent. Only iterate over inner column indices
            // (starting from outer_col_map.size()) to find non-predicate
            // inner columns that need to be scanned in the second pass.
            auto &pred_col_idxs = this->pred_col_idxs_per_schema[i];
            auto outer_size = this->outer_col_map.size();
            for (auto j = outer_size;
                 j < inner_col_map.size() + outer_size; j++) {
                if (std::find(pred_col_idxs.begin(), pred_col_idxs.end(), j) ==
                    pred_col_idxs.end()) {
                    non_pred_col_idxs_per_schema[i].push_back(j);
                }
            }
        }
    }
}

void PhysicalIdSeek::fillSeqnoToEIDIdx(
    size_t num_valid_extents,
    vector<vector<uint32_t>> &target_seqnos_per_extent,
    vector<idx_t> &seqno_to_eid_idx) const
{
    std::fill(seqno_to_eid_idx.begin(), seqno_to_eid_idx.end(), -1);
    for (auto i = 0; i < num_valid_extents; i++) {
        auto &vec = target_seqnos_per_extent[i];
        for (auto &idx : vec) {
            seqno_to_eid_idx[idx] = i;
        }
    }
}

void PhysicalIdSeek::fillSeqnoToEIDIdx(
    vector<vector<uint32_t>> &target_seqnos_per_extent,
    vector<idx_t> &seqno_to_eid_idx) const
{
    std::fill(seqno_to_eid_idx.begin(), seqno_to_eid_idx.end(), -1);
    for (auto i = 0; i < target_seqnos_per_extent.size(); i++) {
        auto &vec = target_seqnos_per_extent[i];
        for (auto &idx : vec) {
            seqno_to_eid_idx[idx] = i;
        }
    }
}

// Since the difference comes from the null vector, we only consider the ratio of null values, not actual bytes.
size_t PhysicalIdSeek::calculateTotalNulls(
    DataChunk &unified_chunk, vector<ExtentID> &target_eids,
    vector<vector<uint32_t>> &target_seqnos_per_extent,
    vector<idx_t> &mapping_idxs) const
{
    vector<size_t> size_per_extent(target_eids.size(), 0);
    vector<size_t> num_nulls_per_extent(target_eids.size(), 0);

    // Get outer cols that are in the output
    size_t num_outer_output_cols = outer_output_col_idxs.size();

    for (u_int64_t extent_idx = 0; extent_idx < target_eids.size();
         extent_idx++) {
        // Fill size_per_extent
        size_t extent_size = target_seqnos_per_extent[extent_idx].size();
        size_per_extent[extent_idx] = extent_size;

        // Fill num_nulls_per_extent
        size_t num_nulls = 0;
        vector<idx_t> inner_output_col_idxs;
        getOutputColIdxsForInner(extent_idx, mapping_idxs,
                                 inner_output_col_idxs);

        for (auto columnIdx = 0; columnIdx < unified_chunk.ColumnCount();
             columnIdx++) {
            // Inner column only
            if (std::find(outer_output_col_idxs.begin(),
                          outer_output_col_idxs.end(),
                          columnIdx) != outer_output_col_idxs.end()) {
                continue;
            }

            // If the column is not in the output, it is a null column
            if (std::find(inner_output_col_idxs.begin(),
                          inner_output_col_idxs.end(),
                          columnIdx) == inner_output_col_idxs.end()) {
                num_nulls += extent_size;
            }
        }

        num_nulls_per_extent[extent_idx] = num_nulls;
    }

    size_t total_nulls = std::accumulate(num_nulls_per_extent.begin(),
                                         num_nulls_per_extent.end(), 0);
    return total_nulls;
}

PhysicalIdSeek::OutputFormat PhysicalIdSeek::determineFormatByCostModel(
    IdSeekState &state, bool sort_order_enforced, size_t total_nulls) const
{
    const double COLUMNAR_PROCESSING_UNIT_COST = 0.8;
    const double ROW_PROCESSING_UNIT_COST = 1.5;
    const double NULL_PROCESSING_UNIT_COST = 0.009;
    if (sort_order_enforced) {
        throw NotImplementedException(
            "PhysicalIdSeek::determineFormatByCostModel - sort_order_enforced");
    }
    else {
        /**
         * The cost is calculated by two terms, 1) per schema processing cost, 2) null processing cost
         * To be detailed, we can use width or so, but this could introduce too much overhead.
         * Per schema processing cost is modeled as C1*log(x+1), where x is number of tuples belong to a schema
         * Null processing cost is modeled as C2*y, where y is the number of null values 
        */
        double union_cost, row_cost;

        // calculate per schema processing cost
        double union_processing_cost, row_processing_cost;
        size_t total_tuples = std::accumulate(state.num_tuples_per_schema.begin(),
                                              state.num_tuples_per_schema.end(), 0);
        union_processing_cost =
            COLUMNAR_PROCESSING_UNIT_COST * log2(total_tuples + 1);
        row_processing_cost = ROW_PROCESSING_UNIT_COST * log2(total_tuples + 1);

        // calculate cost
        union_cost =
            union_processing_cost + NULL_PROCESSING_UNIT_COST * total_nulls;
        row_cost = row_processing_cost;

        if (union_cost < row_cost) {
            return OutputFormat::UNIONALL;
        }
        else {
            return OutputFormat::ROW;
        }
    }
}

void PhysicalIdSeek::fillOutSizePerSchema(
    IdSeekState &state, vector<ExtentID> &target_eids,
    vector<vector<uint32_t>> &target_seqnos_per_extent,
    vector<idx_t> &mapping_idxs) const
{
    D_ASSERT(state.num_tuples_per_schema.size() == num_total_schemas);
    std::fill(state.num_tuples_per_schema.begin(), state.num_tuples_per_schema.end(), 0);
    for (u_int64_t extent_idx = 0; extent_idx < target_eids.size();
         extent_idx++) {
        auto mapping_idx = mapping_idxs[extent_idx];
        state.num_tuples_per_schema[mapping_idx] +=
            target_seqnos_per_extent[extent_idx].size();
    }
}

void PhysicalIdSeek::markInvalidForUnseekedValues(
    DataChunk &chunk, IdSeekState &state, vector<ExtentID> &target_eids,
    vector<vector<uint32_t>> &target_seqnos_per_extent,
    vector<idx_t> &mapping_idxs) const
{
    for (u_int64_t extentIdx = 0; extentIdx < target_eids.size(); extentIdx++) {
        vector<idx_t> inner_output_col_idx;
        getOutputColIdxsForInner(extentIdx, mapping_idxs, inner_output_col_idx);
        auto &target_seqnos = target_seqnos_per_extent[extentIdx];

        for (auto columnIdx = 0; columnIdx < chunk.ColumnCount(); columnIdx++) {
            // Two cases, 1) outer column, 2) inner column, but not in the output
            if (std::find(outer_output_col_idxs.begin(),
                          outer_output_col_idxs.end(),
                          columnIdx) != outer_output_col_idxs.end()) {
                continue;
            }
            if (std::find(inner_output_col_idx.begin(),
                          inner_output_col_idx.end(),
                          columnIdx) == inner_output_col_idx.end()) {
                auto &vec = chunk.data[columnIdx];
                vec.SetIsValid(true);
                auto &validity = FlatVector::Validity(vec);
                if (validity.GetData() == nullptr) {
                    validity.Initialize(STANDARD_VECTOR_SIZE);
                }
                for (auto seqno : target_seqnos) {
                    validity.SetInvalid(seqno);
                }
            }
        }
    }
}

void PhysicalIdSeek::nullifyValuesForPrunedExtents(
    DataChunk &chunk, IdSeekState &state, size_t num_unpruned_extents,
    vector<vector<uint32_t>> &target_seqnos_per_extent) const
{
    // @jhha
    // Some extents can be prunned during planning due to applied filters.
    // In such cases, the seek will not load values for these extents,
    // resulting in dummy values (not null values) in the output chunk.
    // We need to eliminate these dummy values.
    // Currently, we are replacing them with null values.
    // However, we may need another solution (e.g., slice).
    // Note: target_seqnos_per_extent have vector for all extents,
    // including the pruned one. Those are appended in the last.
    for (u_int64_t extentIdx = num_unpruned_extents;
         extentIdx < target_seqnos_per_extent.size(); extentIdx++) {
        auto &target_seqnos = target_seqnos_per_extent[extentIdx];
        for (auto columnIdx = 0; columnIdx < chunk.ColumnCount(); columnIdx++) {
            auto &vec = chunk.data[columnIdx];
            auto &validity = FlatVector::Validity(vec);
            if (validity.GetData() == nullptr) {
                validity.Initialize(STANDARD_VECTOR_SIZE);
            }
            for (auto seqno : target_seqnos) {
                validity.SetInvalid(seqno);
            }
        }
    }
}

// void PhysicalIdSeek::getOutputColIdxsForInner(
//     idx_t extentIdx, vector<idx_t> &mapping_idxs,
//     vector<idx_t> &output_col_idx) const
// {
//     for (idx_t i = 0; i < inner_col_maps[mapping_idxs[extentIdx]].size(); i++) {
//         if (inner_col_maps[mapping_idxs[extentIdx]][i] !=
//             std::numeric_limits<uint32_t>::max()) {
//             output_col_idx.push_back(
//                 inner_col_maps[mapping_idxs[extentIdx]][i]);
//         }
//     }
// }

void PhysicalIdSeek::getOutputColIdxsForInner(
    idx_t extentIdx, std::vector<idx_t> &mapping_idxs,
    std::vector<idx_t> &output_col_idx) const
{
    if (extentIdx >= mapping_idxs.size()) {
        throw std::out_of_range("extentIdx is out of bounds of mapping_idxs");
    }

    idx_t map_idx = mapping_idxs[extentIdx];
    if (map_idx >= inner_col_maps.size()) {
        throw std::out_of_range("mapping_idxs[extentIdx] is out of bounds of inner_col_maps");
    }

    const auto &col_map = inner_col_maps[map_idx];
    for (idx_t i = 0; i < col_map.size(); ++i) {
        if (col_map[i] != std::numeric_limits<uint32_t>::max()) {
            output_col_idx.push_back(col_map[i]);
        }
    }
}

void PhysicalIdSeek::generateOutputColIdxsForOuter()
{
    for (idx_t i = 0; i < outer_col_map.size(); i++) {
        if (outer_col_map[i] != std::numeric_limits<uint32_t>::max()) {
            outer_output_col_idxs.push_back(outer_col_map[i]);
        }
    }
}

void PhysicalIdSeek::generateOutputColIdxsForInner()
{
    inner_output_col_idxs.resize(inner_col_maps.size());
    for (idx_t i = 0; i < inner_col_maps.size(); i++) {
        inner_output_col_idxs[i].reserve(inner_col_maps[i].size());
        for (idx_t j = 0; j < inner_col_maps[i].size(); j++) {
            if (inner_col_maps[i][j] != std::numeric_limits<uint32_t>::max()) {
                inner_output_col_idxs[i].push_back(inner_col_maps[i][j]);
            }
        }
    }
}

/**
 * @brief This code is very error prone
 * Check the algorithm and fix the code @jhha
 */
void PhysicalIdSeek::getUnionScanTypes()
{
    if (num_total_schemas == 1)
        return;

    bool found = false;
    for (auto i = 0; i < inner_col_maps.size(); i++) {
        auto &per_schema_scan_type = scan_types[i];
        auto &per_schema_inner_col_map = inner_col_maps[i];

        for (auto j = 0; j < per_schema_scan_type.size(); j++) {
            if (per_schema_scan_type[j].id() == LogicalTypeId::ID) {
                out_id_col_idx = per_schema_inner_col_map[j];
                found = true;
                break;
            }
        }
        if (found)
            break;
    }

    if (!found) {
        out_id_col_idx = -1;
    }

    union_inner_col_map_wo_id.reserve(union_inner_col_map.size());
    for (auto i = 0; i < union_inner_col_map.size(); i++) {
        if (union_inner_col_map[i] != out_id_col_idx) {
            union_inner_col_map_wo_id.push_back(union_inner_col_map[i]);
        }
    }
}

void PhysicalIdSeek::buildExpressionExecutors(
    vector<vector<unique_ptr<Expression>>> &predicates)
{
    // Store the predicate ASTs on the operator (read-only, shared across
    // threads). Each per-thread IdSeekState builds its own ExpressionExecutor
    // referencing these expressions.
    for (auto i = 0; i < predicates.size(); i++) {
        if (predicates[i].empty()) {
            // No filter for this schema (e.g., MPV sibling with no predicate).
            // Create a trivial TRUE constant so the executor has something valid.
            expressions.push_back(
                make_unique<BoundConstantExpression>(Value::BOOLEAN(true)));
        } else if (predicates[i].size() > 1) {
            auto conjunction = make_unique<BoundConjunctionExpression>(
                ExpressionType::CONJUNCTION_AND);
            for (auto &expr : predicates[i]) {
                conjunction->children.push_back(move(expr));
            }
            expressions.push_back(move(conjunction));
        }
        else {
            expressions.push_back(move(predicates[i][0]));
        }
    }
}

// TODO: Optimzie this function (or, is it needed?)
void PhysicalIdSeek::markInvalidForColumnsToUnseek(DataChunk &chunk, vector<ExtentID> &target_eids, 
                            vector<idx_t> &mapping_idxs) const
{
    // Mark all inner columns invalid
    for (auto columnIdx = 0; columnIdx < chunk.ColumnCount(); columnIdx++) {
        if (isInnerColIdx(columnIdx)) {
            chunk.data[columnIdx].SetIsValid(false);
        }
    }
    // Mark seek columns valid
    for (u_int64_t extentIdx = 0; extentIdx < target_eids.size(); extentIdx++) {
        auto mapping_idx = mapping_idxs[extentIdx];
        vector<idx_t> inner_output_col_idx;
        getOutputColIdxsForInner(extentIdx, mapping_idxs,
                                 inner_output_col_idx);
        for (auto columnIdx : inner_output_col_idx) {
            if (columnIdx < chunk.ColumnCount()) {
                chunk.data[columnIdx].SetIsValid(true);
            }
        }
    }
}

std::string PhysicalIdSeek::ParamsToString() const
{
    std::string result = "";
    result += JoinTypeToString(join_type) + ", ";
    result += "id_col_idx=" + std::to_string(id_col_idx) + ", ";
    result += "projection_mapping.size()=" +
              std::to_string(projection_mapping.size()) + ", ";
    result += "projection_mapping[0].size()=" +
              std::to_string(projection_mapping[0].size()) + ", ";
    result +=
        "outer_col_map.size()=" + std::to_string(outer_col_map.size()) + ", ";
    result += "inner_col_maps.size()=" + std::to_string(inner_col_maps.size());
    if (expressions.size() > 0 && expressions[0] != nullptr) {
        result += ", expression[0]=" + expressions[0]->ToString();
    }
    return result;
}

std::string PhysicalIdSeek::ToString() const
{
    return "IdSeek";
}

} // namespace turbolynx
