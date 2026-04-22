#pragma once

#include <functional>

// ============================================================
// Cypher2OrcaConverter: BoundRegularQuery → ORCA LogicalPlan
// ============================================================
// Replaces planner_logical.cpp + planner_logical_scalar.cpp.
// Operates on TurboLynx-native bound types; produces the same
// ORCA CExpression tree that the rest of the Planner consumes.
// ============================================================

#include "main/client_context.hpp"
#include "catalog/catalog_entry/graph_catalog_entry.hpp"
#include "catalog/catalog_entry/partition_catalog_entry.hpp"
#include "catalog/catalog_entry/property_schema_catalog_entry.hpp"

// ORCA
#include "gpos/base.h"
#include "gpos/memory/CMemoryPool.h"
#include "gpopt/engine/CEngine.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/base/CColRef.h"
#include "gpopt/base/CColRefTable.h"
#include "gpopt/base/CQueryContext.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/mdcache/CMDAccessorUtils.h"
#include "gpopt/operators/CLogicalGet.h"
#include "gpopt/operators/CScalarProjectList.h"
#include "gpopt/operators/CScalarProjectElement.h"
#include "gpopt/operators/CLogicalProject.h"
#include "gpopt/operators/CLogicalProjectColumnar.h"
#include "gpopt/operators/CScalarIdent.h"
#include "gpopt/operators/CScalarBoolOp.h"
#include "gpopt/operators/CScalarCast.h"
#include "gpopt/operators/CLogicalUnionAll.h"
#include "gpopt/operators/CLogicalUnion.h"
#include "gpopt/operators/COperator.h"
#include "gpopt/operators/CLogicalInnerJoin.h"
#include "gpopt/operators/CLogicalLeftOuterJoin.h"
#include "gpopt/operators/CLogicalRightOuterJoin.h"
#include "gpopt/operators/CLogicalLimit.h"
#include "gpopt/operators/CLogicalPathJoin.h"
#include "gpopt/operators/CLogicalPathGet.h"
#include "gpopt/operators/CLogicalGbAgg.h"
#include "gpopt/operators/CLogicalShortestPath.h"
#include "gpopt/operators/CLogicalAllShortestPath.h"
#include "gpopt/operators/CScalarConst.h"
#include "gpopt/operators/CScalarCmp.h"
#include "gpopt/operators/CScalarBoolOp.h"
#include "gpopt/operators/CScalarFunc.h"
#include "gpopt/operators/CScalarAggFunc.h"
#include "gpopt/operators/CScalarValuesList.h"
#include "gpopt/operators/CScalarSwitch.h"
#include "gpopt/operators/CScalarSwitchCase.h"
#include "gpopt/operators/CScalarIf.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "naucrates/md/IMDType.h"
#include "naucrates/md/CMDIdGPDB.h"
#include "naucrates/md/IMDCast.h"
#include "naucrates/md/IMDAggregate.h"
#include "naucrates/base/CDatumGenericGPDB.h"
#include "naucrates/base/CDatumInt8GPDB.h"
#include "naucrates/base/CDatumBoolGPDB.h"
#include "gpdbcost/CCostModelGPDB.h"

// TurboLynx planner layer
#include "planner/logical_plan.hpp"
#include "planner/logical_schema.hpp"
#include "optimizer/mdprovider/MDProviderTBGPP.h"

// TurboLynx bound types
#include "binder/query/bound_regular_query.hpp"
#include "binder/query/normalized_single_query.hpp"
#include "binder/query/normalized_query_part.hpp"
#include "binder/query/reading_clause/bound_match_clause.hpp"
#include "binder/query/reading_clause/bound_unwind_clause.hpp"
#include "binder/query/return_with_clause/bound_projection_body.hpp"
#include "binder/graph_pattern/bound_query_graph.hpp"
#include "binder/graph_pattern/bound_node_expression.hpp"
#include "binder/graph_pattern/bound_rel_expression.hpp"
#include "binder/expression/bound_expression.hpp"
#include "binder/expression/bound_literal_expression.hpp"
#include "binder/expression/bound_property_expression.hpp"
#include "binder/expression/bound_exists_subquery_expression.hpp"
#include "binder/expression/bound_variable_expression.hpp"
#include "binder/expression/bound_function_expression.hpp"
#include "binder/expression/bound_agg_function_expression.hpp"
#include "binder/expression/bound_comparison_expression.hpp"
#include "binder/expression/bound_bool_expression.hpp"
#include "binder/expression/bound_case_expression.hpp"
#include "binder/expression/bound_null_expression.hpp"

#include "common/enums/expression_type.hpp"
#include "function/aggregate_function.hpp"
#include "function/aggregate/distributive_functions.hpp"
#include "function/function.hpp"

#include <map>
#include <limits>
#include <unordered_set>

using namespace gpopt;
using namespace gpmd;
using namespace gpos;

namespace duckdb {
}
namespace turbolynx {
}
namespace duckdb {
    using namespace turbolynx;
}
namespace turbolynx {
using namespace duckdb;

// --------------------------------------------------------
// Cypher2OrcaConverter
// --------------------------------------------------------
// Created by Planner for each query; holds all ORCA state
// passed in from the owning Planner.
// --------------------------------------------------------
class Cypher2OrcaConverter {
public:
    // mp, context, provider — owned by caller (Planner)
    // col_name_map — reference to Planner's mapping
    // Forward declaration for MPV null property info
    struct MpvNullPropInfo {
        uint64_t prop_key_id;
        string prop_name;
        duckdb::LogicalType type;
    };

    Cypher2OrcaConverter(CMemoryPool *mp,
                         duckdb::ClientContext *context,
                         MDProviderTBGPP *provider,
                         std::map<CColRef *, std::string> &col_name_map,
                         std::unordered_set<duckdb::idx_t> &both_edge_partitions,
                         std::unordered_map<duckdb::idx_t, std::vector<duckdb::idx_t>> &multi_edge_partitions,
                         std::unordered_map<duckdb::idx_t, std::vector<duckdb::idx_t>> &multi_vertex_partitions,
                         std::unordered_map<ULONG, MpvNullPropInfo> &mpv_null_colref_props,
                         std::unordered_map<INT, LogicalType> &complex_type_registry,
                         INT &next_complex_type_id);

    // Entry point: convert a fully-bound regular query into a ORCA LogicalPlan.
    turbolynx::LogicalPlan *Convert(const BoundRegularQuery &query);

private:
    // ---- query structure ----
    turbolynx::LogicalPlan *PlanSingleQuery(const NormalizedSingleQuery &sq);
    turbolynx::LogicalPlan *PlanQueryPart(const NormalizedQueryPart &qp,
                                    turbolynx::LogicalPlan *prev_plan);
    turbolynx::LogicalPlan *PlanReadingClause(const BoundReadingClause &rc,
                                        turbolynx::LogicalPlan *prev_plan);
    turbolynx::LogicalPlan *PlanMatchClause(const BoundMatchClause &mc,
                                      turbolynx::LogicalPlan *prev_plan);
    turbolynx::LogicalPlan *PlanUnwindClause(const BoundUnwindClause &uc,
                                       turbolynx::LogicalPlan *prev_plan);
    turbolynx::LogicalPlan *PlanProjectionBody(turbolynx::LogicalPlan *plan,
                                         const BoundProjectionBody &proj);
    // subquery_corr_keys: output map from outer node name → (edge_name, edge_key_id)
    // for EXISTS subquery correlation. Only populated when subquery_outer_nodes is non-empty.
    struct SubqueryCorrelation {
        string edge_name;
        uint64_t edge_key_id;   // SID_KEY_ID or TID_KEY_ID
    };
    turbolynx::LogicalPlan *PlanRegularMatch(const BoundQueryGraphCollection &qgc,
                                       turbolynx::LogicalPlan *prev_plan,
                                       const bound_expression_vector &predicates = {},
                                       const vector<string> &subquery_outer_nodes = {},
                                       map<string, SubqueryCorrelation> *subquery_corr_keys = nullptr);
    turbolynx::LogicalPlan *PlanOptionalMatch(const BoundQueryGraphCollection &qgc,
                                        turbolynx::LogicalPlan *prev_plan,
                                        const bound_expression_vector &predicates = {});

    // ---- operator planners ----
    turbolynx::LogicalPlan *PlanSelection(const bound_expression_vector &preds,
                                    turbolynx::LogicalPlan *prev_plan);
    turbolynx::LogicalPlan *PlanProjection(const bound_expression_vector &exprs,
                                     turbolynx::LogicalPlan *prev_plan);
    turbolynx::LogicalPlan *PlanGroupBy(bound_expression_vector &exprs,
                                  turbolynx::LogicalPlan *prev_plan);
    turbolynx::LogicalPlan *PlanOrderBy(const vector<BoundOrderByItem> &items,
                                  turbolynx::LogicalPlan *prev_plan);
    turbolynx::LogicalPlan *PlanDistinct(const bound_expression_vector &exprs,
                                   CColRefArray *colrefs,
                                   turbolynx::LogicalPlan *prev_plan);
    turbolynx::LogicalPlan *PlanSkipOrLimit(const BoundProjectionBody &proj,
                                      turbolynx::LogicalPlan *prev_plan);

    // ---- graph scan planners ----
    turbolynx::LogicalPlan *PlanNodeScan(const BoundNodeExpression &node);
    turbolynx::LogicalPlan *PlanEdgeScan(const BoundRelExpression &rel);
    // Scan only one partition of a multi-partition edge (by index into GetPartitionIDs).
    // Used with multi_edge_partitions_ for AdjIdxJoin sibling expansion.
    turbolynx::LogicalPlan *PlanEdgeScanSinglePartition(
        const BoundRelExpression &rel, size_t partition_idx);
    turbolynx::LogicalPlan *PlanPathGet(const BoundRelExpression &rel);
    turbolynx::LogicalPlan *PlanShortestPath(
        const BoundQueryGraph &qg, turbolynx::LogicalPlan *prev_plan);

    // ---- schema helpers ----
    // Build col_idx → column_pos mapping per graphlet.
    // col_idx 0 = _id.  col_idx i = i-th property in node.GetPropertyExpressions().
    void BuildSchemaProjectionMapping(
        const vector<uint64_t> &graphlet_oids,
        const vector<shared_ptr<BoundExpression>> &prop_exprs,
        bool all_used,
        map<uint64_t, map<uint64_t, uint64_t>> &out_mapping,
        vector<int> &out_used_col_idx,
        std::vector<std::vector<uint64_t>> *table_oids_in_groups = nullptr,
        const std::function<bool(int)> &is_col_used = {});

    void GenerateNodeSchema(const BoundNodeExpression &node,
                            const vector<int> &used_col_idx,
                            CColRefArray *colrefs,
                            turbolynx::LogicalSchema &schema);

    void GenerateEdgeSchema(const BoundRelExpression &rel,
                            const vector<int> &used_col_idx,
                            CColRefArray *colrefs,
                            turbolynx::LogicalSchema &schema);

    // ---- ORCA expression builders (identical logic to Planner) ----
    CExpression *ExprLogicalGet(uint64_t obj_id, const string &name,
                                 bool whole_node_required = false,
                                 bool is_instance = false,
                                 std::vector<uint64_t> *table_oids_in_group = nullptr);
    CExpression *ExprLogicalJoin(CExpression *lhs, CExpression *rhs,
                                  CColRef *lhs_col, CColRef *rhs_col,
                                  gpopt::COperator::EOperatorId join_op,
                                  CExpression *additional_pred);
    CExpression *ExprLogicalPathJoin(CExpression *lhs, CExpression *rhs,
                                      CColRef *lhs_col, CColRef *rhs_col,
                                      int32_t lb, int32_t ub,
                                      gpopt::COperator::EOperatorId join_op);
    CExpression *ExprLogicalCartProd(CExpression *lhs, CExpression *rhs);

    // Create LogicalGet/UnionAll scan for a multi-graphlet node or edge.
    pair<CExpression *, CColRefArray *> ExprLogicalGetNodeOrEdge(
        const string &name,
        vector<uint64_t> &graphlet_oids,
        const vector<int> &used_col_idx,
        map<uint64_t, map<uint64_t, uint64_t>> *mapping,
        bool whole_node_required,
        std::vector<std::vector<uint64_t>> *table_oids_in_groups = nullptr);

    // Schema-conforming projection (for multi-graphlet UnionAll).
    pair<CExpression *, CColRefArray *> ExprScalarAddSchemaConformProject(
        CExpression *relation,
        vector<uint64_t> &col_ids_to_project,
        vector<pair<gpmd::IMDId *, gpos::INT>> *target_schema_types,
        vector<CColRef *> &union_schema_colrefs);

    CTableDescriptor *CreateTableDescForRel(uint64_t obj_id, const string &rel_name);

    // ---- scalar expression converters ----
    CExpression *ConvertExpression(const BoundExpression &expr,
                                   turbolynx::LogicalPlan *plan);
    CExpression *TryGenScalarIdent(const BoundExpression &expr,
                                    turbolynx::LogicalPlan *plan);
    CExpression *ExprScalarCmpEq(CExpression *left, CExpression *right);
    CExpression *ExprScalarProperty(const string &var_name, uint64_t key_id,
                                     turbolynx::LogicalPlan *plan);

    CExpression *ConvertLiteral(const BoundLiteralExpression &expr);
    CExpression *ConvertProperty(const BoundPropertyExpression &expr,
                                  turbolynx::LogicalPlan *plan);
    CExpression *ConvertVariable(const BoundVariableExpression &expr,
                                  turbolynx::LogicalPlan *plan);
    CExpression *ConvertFunction(const CypherBoundFunctionExpression &expr,
                                  turbolynx::LogicalPlan *plan);
    CExpression *ConvertCastFunction(const CypherBoundFunctionExpression &expr,
                                      turbolynx::LogicalPlan *plan);
    CExpression *ConvertAggFunc(const BoundAggFunctionExpression &expr,
                                 turbolynx::LogicalPlan *plan);
    CExpression *ConvertComparison(const CypherBoundComparisonExpression &expr,
                                    turbolynx::LogicalPlan *plan);
    CExpression *ConvertBoolOp(const BoundBoolExpression &expr,
                                turbolynx::LogicalPlan *plan);
    CExpression *ConvertNullOp(const BoundNullExpression &expr,
                                turbolynx::LogicalPlan *plan);
    CExpression *ConvertCase(const CypherBoundCaseExpression &expr,
                              turbolynx::LogicalPlan *plan);
    CExpression *ConvertExistsSubquery(const BoundExistsSubqueryExpression &expr,
                                        turbolynx::LogicalPlan *plan);

    // DuckDB expression for function type resolution
    unique_ptr<duckdb::Expression> ConvertExpressionDuckDB(const BoundExpression &expr);
    unique_ptr<duckdb::Expression> ConvertLiteralDuckDB(const BoundLiteralExpression &expr);
    unique_ptr<duckdb::Expression> ConvertPropertyDuckDB(const BoundPropertyExpression &expr);
    unique_ptr<duckdb::Expression> ConvertFunctionDuckDB(const CypherBoundFunctionExpression &expr);
    unique_ptr<duckdb::Expression> ConvertAggFuncDuckDB(const BoundAggFunctionExpression &expr);
    unique_ptr<duckdb::Expression> ConvertComparisonDuckDB(const CypherBoundComparisonExpression &expr);
    unique_ptr<duckdb::Expression> ConvertBoolOpDuckDB(const BoundBoolExpression &expr);

    // Type helpers
    INT GetTypeMod(const LogicalType &type);
    OID GetTypeOidFromCExpr(CExpression *expr);
    INT GetTypeModFromCExpr(CExpression *expr);
    IMDType::ECmpType MapCmpType(ExpressionType t, bool swap);

    // ---- catalog helpers ----
    // Lookup property schema by graphlet OID.
    PropertySchemaCatalogEntry *GetPropertySchema(uint64_t graphlet_oid);
    // Find column position (1-based) of key_id in graphlet.
    // Returns UINT64_MAX if the property is not in this graphlet.
    uint64_t FindKeyColumnInGraphlet(uint64_t graphlet_oid, uint64_t key_id);

    GraphCatalogEntry *GetGraphCatalog();

    // ---- inline helpers ----
    inline CMDAccessor *GetMDAccessor() {
        return COptCtxt::PoctxtFromTLS()->Pmda();
    }
    inline CMDIdGPDB *GenRelMdid(uint64_t obj_id) {
        return GPOS_NEW(mp_) CMDIdGPDB(IMDId::EmdidRel, obj_id, 0, 0);
    }
    inline const IMDRelation *GetRelMd(uint64_t obj_id) {
        return GetMDAccessor()->RetrieveRel(GenRelMdid(obj_id));
    }

    bool IsCastingFunction(const string &func_name);

private:
    CMemoryPool *mp_;
    duckdb::ClientContext *context_;
    MDProviderTBGPP *provider_;
    std::map<CColRef *, std::string> &col_name_map_;
    std::unordered_set<duckdb::idx_t> &both_edge_partitions_;
    std::unordered_map<duckdb::idx_t, std::vector<duckdb::idx_t>> &multi_edge_partitions_;
    std::unordered_map<duckdb::idx_t, std::vector<duckdb::idx_t>> &multi_vertex_partitions_;
    std::unordered_map<ULONG, MpvNullPropInfo> &mpv_null_colref_props_;

    // Complex type registry — shared with Planner for STRUCT/ANY type resolution
    std::unordered_map<INT, LogicalType> &complex_type_registry_;
    INT &next_complex_type_id_;

    GraphCatalogEntry *graph_cat_ = nullptr;

    // Track if current node scan is MPV (for PlanProjection to tag NULL properties)
    bool current_node_is_mpv_ = false;

    // System column key IDs (initialized in constructor)
    uint64_t ID_KEY_ID  = 0;   // _id
    uint64_t SID_KEY_ID = 1;   // _sid (updated from catalog)
    uint64_t TID_KEY_ID = 2;   // _tid (updated from catalog)

    // Subquery support
    bool outer_plan_registered_ = false;
    turbolynx::LogicalPlan *outer_plan_ = nullptr;

    // Multi-part query context (set by PlanSingleQuery for PlanGroupBy lookahead)
    const NormalizedSingleQuery *current_sq_ = nullptr;
    idx_t current_part_idx_ = 0;

    // Collect property key IDs of var_name referenced in downstream query parts
    void CollectDownstreamPropertyRefs(const string &var_name,
                                       std::unordered_set<uint64_t> &out_key_ids);
    // Recursive helper to walk a BoundExpression tree
    static void CollectPropertyRefsFromExpr(const BoundExpression &expr,
                                            const string &var_name,
                                            std::unordered_set<uint64_t> &out_key_ids);
};

} // namespace turbolynx
