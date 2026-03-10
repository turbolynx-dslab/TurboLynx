#pragma once

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

using namespace gpopt;
using namespace gpmd;
using namespace gpos;

namespace duckdb {

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
    Cypher2OrcaConverter(CMemoryPool *mp,
                         ClientContext *context,
                         MDProviderTBGPP *provider,
                         std::map<CColRef *, std::string> &col_name_map);

    // Entry point: convert a fully-bound regular query into a ORCA LogicalPlan.
    s62::LogicalPlan *Convert(const BoundRegularQuery &query);

private:
    // ---- query structure ----
    s62::LogicalPlan *PlanSingleQuery(const NormalizedSingleQuery &sq);
    s62::LogicalPlan *PlanQueryPart(const NormalizedQueryPart &qp,
                                    s62::LogicalPlan *prev_plan);
    s62::LogicalPlan *PlanReadingClause(const BoundReadingClause &rc,
                                        s62::LogicalPlan *prev_plan);
    s62::LogicalPlan *PlanMatchClause(const BoundMatchClause &mc,
                                      s62::LogicalPlan *prev_plan);
    s62::LogicalPlan *PlanProjectionBody(s62::LogicalPlan *plan,
                                         const BoundProjectionBody &proj);
    s62::LogicalPlan *PlanRegularMatch(const BoundQueryGraphCollection &qgc,
                                       s62::LogicalPlan *prev_plan);

    // ---- operator planners ----
    s62::LogicalPlan *PlanSelection(const bound_expression_vector &preds,
                                    s62::LogicalPlan *prev_plan);
    s62::LogicalPlan *PlanProjection(const bound_expression_vector &exprs,
                                     s62::LogicalPlan *prev_plan);
    s62::LogicalPlan *PlanGroupBy(const bound_expression_vector &exprs,
                                  s62::LogicalPlan *prev_plan);
    s62::LogicalPlan *PlanOrderBy(const vector<BoundOrderByItem> &items,
                                  s62::LogicalPlan *prev_plan);
    s62::LogicalPlan *PlanDistinct(const bound_expression_vector &exprs,
                                   CColRefArray *colrefs,
                                   s62::LogicalPlan *prev_plan);
    s62::LogicalPlan *PlanSkipOrLimit(const BoundProjectionBody &proj,
                                      s62::LogicalPlan *prev_plan);

    // ---- graph scan planners ----
    s62::LogicalPlan *PlanNodeScan(const BoundNodeExpression &node);
    s62::LogicalPlan *PlanEdgeScan(const BoundRelExpression &rel);
    s62::LogicalPlan *PlanPathGet(const BoundRelExpression &rel);

    // ---- schema helpers ----
    // Build col_idx → column_pos mapping per graphlet.
    // col_idx 0 = _id.  col_idx i = i-th property in node.GetPropertyExpressions().
    void BuildSchemaProjectionMapping(
        const vector<uint64_t> &graphlet_oids,
        const vector<shared_ptr<BoundExpression>> &prop_exprs,
        bool all_used,
        map<uint64_t, map<uint64_t, uint64_t>> &out_mapping,
        vector<int> &out_used_col_idx);

    void GenerateNodeSchema(const BoundNodeExpression &node,
                            const vector<int> &used_col_idx,
                            CColRefArray *colrefs,
                            s62::LogicalSchema &schema);

    void GenerateEdgeSchema(const BoundRelExpression &rel,
                            const vector<int> &used_col_idx,
                            CColRefArray *colrefs,
                            s62::LogicalSchema &schema);

    // ---- ORCA expression builders (identical logic to Planner) ----
    CExpression *ExprLogicalGet(uint64_t obj_id, const string &name,
                                 bool whole_node_required = false);
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
        bool whole_node_required);

    // Schema-conforming projection (for multi-graphlet UnionAll).
    pair<CExpression *, CColRefArray *> ExprScalarAddSchemaConformProject(
        CExpression *relation,
        vector<uint64_t> &col_ids_to_project,
        vector<pair<gpmd::IMDId *, gpos::INT>> *target_schema_types,
        vector<CColRef *> &union_schema_colrefs);

    CTableDescriptor *CreateTableDescForRel(uint64_t obj_id, const string &rel_name);

    // ---- scalar expression converters ----
    CExpression *ConvertExpression(const BoundExpression &expr,
                                   s62::LogicalPlan *plan);
    CExpression *TryGenScalarIdent(const BoundExpression &expr,
                                    s62::LogicalPlan *plan);
    CExpression *ExprScalarCmpEq(CExpression *left, CExpression *right);
    CExpression *ExprScalarProperty(const string &var_name, uint64_t key_id,
                                     s62::LogicalPlan *plan);

    CExpression *ConvertLiteral(const BoundLiteralExpression &expr);
    CExpression *ConvertProperty(const BoundPropertyExpression &expr,
                                  s62::LogicalPlan *plan);
    CExpression *ConvertVariable(const BoundVariableExpression &expr,
                                  s62::LogicalPlan *plan);
    CExpression *ConvertFunction(const CypherBoundFunctionExpression &expr,
                                  s62::LogicalPlan *plan);
    CExpression *ConvertCastFunction(const CypherBoundFunctionExpression &expr,
                                      s62::LogicalPlan *plan);
    CExpression *ConvertAggFunc(const BoundAggFunctionExpression &expr,
                                 s62::LogicalPlan *plan);
    CExpression *ConvertComparison(const CypherBoundComparisonExpression &expr,
                                    s62::LogicalPlan *plan);
    CExpression *ConvertBoolOp(const BoundBoolExpression &expr,
                                s62::LogicalPlan *plan);
    CExpression *ConvertNullOp(const BoundNullExpression &expr,
                                s62::LogicalPlan *plan);
    CExpression *ConvertCase(const CypherBoundCaseExpression &expr,
                              s62::LogicalPlan *plan);

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
    ClientContext *context_;
    MDProviderTBGPP *provider_;
    std::map<CColRef *, std::string> &col_name_map_;

    GraphCatalogEntry *graph_cat_ = nullptr;

    // System column key IDs (initialized in constructor)
    uint64_t ID_KEY_ID  = 0;   // _id
    uint64_t SID_KEY_ID = 1;   // _sid (updated from catalog)
    uint64_t TID_KEY_ID = 2;   // _tid (updated from catalog)

    // Subquery support
    bool outer_plan_registered_ = false;
    s62::LogicalPlan *outer_plan_ = nullptr;
};

} // namespace duckdb
