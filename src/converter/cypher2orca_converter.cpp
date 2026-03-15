// Cypher2OrcaConverter — structural conversion
// (replaces planner_logical.cpp)
//
// Converts a TurboLynx BoundRegularQuery into an ORCA LogicalPlan.
// Scalar expression conversion lives in cypher2orca_scalar.cpp.

#include "converter/cypher2orca_converter.hpp"
#include "planner/value_ser_des.hpp"
#include "catalog/catalog.hpp"
#include "catalog/catalog_wrapper.hpp"
#include "common/constants.hpp"
#include "main/database.hpp"

#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/operators/CLogicalInnerJoin.h"
#include "gpopt/operators/CLogicalLeftOuterJoin.h"
#include "gpopt/operators/CLogicalRightOuterJoin.h"
#include "gpopt/operators/CLogicalGbAgg.h"
#include "gpopt/mdcache/CMDAccessorUtils.h"

#include <algorithm>
#include <limits>
#include <cassert>

using namespace gpopt;
using namespace gpmd;
using namespace gpos;

namespace duckdb {

// ============================================================
// Constructor
// ============================================================
Cypher2OrcaConverter::Cypher2OrcaConverter(
    CMemoryPool *mp, ClientContext *context, MDProviderTBGPP *provider,
    std::map<CColRef *, std::string> &col_name_map,
    std::unordered_set<idx_t> &both_edge_partitions,
    std::unordered_map<idx_t, std::vector<idx_t>> &multi_edge_partitions,
    std::unordered_map<idx_t, std::vector<idx_t>> &multi_vertex_partitions)
    : mp_(mp), context_(context), provider_(provider),
      col_name_map_(col_name_map),
      both_edge_partitions_(both_edge_partitions),
      multi_edge_partitions_(multi_edge_partitions),
      multi_vertex_partitions_(multi_vertex_partitions)
{
    // Initialize system column key IDs from catalog.
    GraphCatalogEntry *gcat = GetGraphCatalog();
    if (gcat != nullptr) {
        string sid_name = "_sid";
        string tid_name = "_tid";
        SID_KEY_ID = gcat->GetPropertyKeyID(*context_, sid_name);
        TID_KEY_ID = gcat->GetPropertyKeyID(*context_, tid_name);
    }
}

// ============================================================
// Public entry point
// ============================================================
turbolynx::LogicalPlan *Cypher2OrcaConverter::Convert(const BoundRegularQuery &query)
{
    // TODO: handle UNION across multiple single queries (M20 only handles 1)
    D_ASSERT(query.GetNumSingleQueries() == 1);
    return PlanSingleQuery(*query.GetSingleQuery(0));
}

// ============================================================
// Query structure planners
// ============================================================
turbolynx::LogicalPlan *Cypher2OrcaConverter::PlanSingleQuery(const NormalizedSingleQuery &sq)
{
    turbolynx::LogicalPlan *cur_plan = nullptr;
    for (idx_t i = 0; i < sq.GetNumQueryParts(); ++i) {
        cur_plan = PlanQueryPart(*sq.GetQueryPart(i), cur_plan);
    }
    return cur_plan;
}

turbolynx::LogicalPlan *Cypher2OrcaConverter::PlanQueryPart(
    const NormalizedQueryPart &qp, turbolynx::LogicalPlan *prev_plan)
{
    turbolynx::LogicalPlan *cur_plan = prev_plan;

    // Reading clauses (MATCH, UNWIND, ...)
    for (idx_t i = 0; i < qp.GetNumReadingClauses(); i++) {
        cur_plan = PlanReadingClause(*qp.GetReadingClause(i), cur_plan);
    }

    D_ASSERT(qp.GetNumUpdatingClauses() == 0);

    // Projection body (WITH / RETURN)
    if (qp.HasProjectionBody()) {
        cur_plan = PlanProjectionBody(cur_plan, *qp.GetProjectionBody());
        if (qp.HasProjectionBodyPredicate()) {
            // WITH ... WHERE ...
            bound_expression_vector preds;
            preds.push_back(qp.GetProjectionBodyPredicateShared());
            cur_plan = PlanSelection(preds, cur_plan);
        }
    }
    return cur_plan;
}

turbolynx::LogicalPlan *Cypher2OrcaConverter::PlanReadingClause(
    const BoundReadingClause &rc, turbolynx::LogicalPlan *prev_plan)
{
    switch (rc.GetClauseType()) {
        case BoundClauseType::MATCH:
            return PlanMatchClause(static_cast<const BoundMatchClause &>(rc), prev_plan);
        case BoundClauseType::UNWIND:
            D_ASSERT(false);  // not implemented yet
            return nullptr;
        default:
            D_ASSERT(false);
            return nullptr;
    }
}

turbolynx::LogicalPlan *Cypher2OrcaConverter::PlanMatchClause(
    const BoundMatchClause &mc, turbolynx::LogicalPlan *prev_plan)
{
    const BoundQueryGraphCollection *qgc = mc.GetQueryGraphCollection();
    const bound_expression_vector &predicates = mc.GetPredicates();

    turbolynx::LogicalPlan *plan;
    if (!mc.IsOptional()) {
        plan = PlanRegularMatch(*qgc, prev_plan);
    } else {
        // Optional match — handled like regular match with left-outer join.
        // Full optional-match logic (as in lPlanRegularOptionalMatch) is
        // non-trivial; for now fall back to the regular path.
        plan = PlanRegularMatch(*qgc, prev_plan);
    }

    // Apply WHERE predicates
    if (!predicates.empty()) {
        plan = PlanSelection(predicates, plan);
    }
    return plan;
}

// ============================================================
// Regular match: build a join tree from query graphs
// ============================================================
turbolynx::LogicalPlan *Cypher2OrcaConverter::PlanRegularMatch(
    const BoundQueryGraphCollection &qgc, turbolynx::LogicalPlan *prev_plan)
{
    turbolynx::LogicalPlan *qg_plan = prev_plan;

    D_ASSERT(qgc.GetNumQueryGraphs() > 0);
    for (uint32_t qg_idx = 0; qg_idx < qgc.GetNumQueryGraphs(); ++qg_idx) {
        const BoundQueryGraph *qg = qgc.GetQueryGraph(qg_idx);
        const vector<shared_ptr<BoundRelExpression>> &rels = qg->GetQueryRels();

        if (!rels.empty()) {
            // Edge-based join loop
            for (auto &qedge : rels) {
                const string &edge_name = qedge->GetUniqueName();
                const string &lhs_name  = qedge->GetSrcNodeName();
                const string &rhs_name  = qedge->GetDstNodeName();

                // Get the bound node expressions for src/dst
                shared_ptr<BoundNodeExpression> lhs_node = qg->GetQueryNode(lhs_name);
                shared_ptr<BoundNodeExpression> rhs_node = qg->GetQueryNode(rhs_name);
                D_ASSERT(lhs_node && rhs_node);

                bool is_lhs_bound = false;
                bool is_rhs_bound = false;
                if (qg_plan != nullptr) {
                    is_lhs_bound = qg_plan->getSchema()->isNodeBound(lhs_name);
                    is_rhs_bound = qg_plan->getSchema()->isNodeBound(rhs_name);
                }

                bool is_pathjoin = qedge->IsVariableLength();

                // --- Plan LHS (A) ---
                turbolynx::LogicalPlan *lhs_plan;
                if (!is_lhs_bound) {
                    lhs_plan = PlanNodeScan(*lhs_node);
                } else {
                    lhs_plan = qg_plan;
                }
                D_ASSERT(lhs_plan != nullptr);

                // Shortest-path handling (TODO: mirror lPlanShortestPath)
                if (qg->GetPathType() == BoundQueryGraph::PathType::SHORTEST ||
                    qg->GetPathType() == BoundQueryGraph::PathType::ALL_SHORTEST) {
                    // Placeholder — not implemented in this milestone
                    D_ASSERT(false);
                }

                // --- Plan edge scan ---
                turbolynx::LogicalPlan *edge_plan = is_pathjoin
                    ? PlanPathGet(*qedge)
                    : PlanEdgeScan(*qedge);

                // Determine if the LHS vertex is the stored SRC of the edge.
                // Example: (Post)<-[CONTAINER_OF]-(Forum): CONTAINER_OF is stored
                // Forum→Post, so Post is the stored DST (_tid), not the SRC (_sid).
                // For self-referential edges (both endpoints share a partition),
                // we cannot distinguish by type, so fall back to Cypher direction:
                //   direction=LEFT  → LHS is the "to" side = stored DST → lhs_is_src=false
                //   direction=RIGHT → LHS is the "from" side = stored SRC → lhs_is_src=true
                bool lhs_is_src = true;
                bool is_both = (qedge->GetDirection() == RelDirection::BOTH);
                if (!qedge->GetPartitionIDs().empty() && !lhs_node->GetPartitionIDs().empty()) {
                    auto &catalog = context_->db->GetCatalog();
                    auto *epart = static_cast<PartitionCatalogEntry *>(catalog.GetEntry(
                        *context_, DEFAULT_SCHEMA, (idx_t)qedge->GetPartitionIDs()[0]));
                    if (epart) {
                        if (is_both) {
                            // BOTH direction: use forward (SID) as primary.
                            // Physical operator will dual-scan forward + backward.
                            lhs_is_src = true;
                            both_edge_partitions_.insert((idx_t)qedge->GetPartitionIDs()[0]);
                        } else {
                            idx_t stored_src = epart->GetSrcPartOid();
                            idx_t stored_dst = epart->GetDstPartOid();
                            bool lhs_matches_src = false, lhs_matches_dst = false;
                            for (auto lhs_pid : lhs_node->GetPartitionIDs()) {
                                if ((idx_t)lhs_pid == stored_src) lhs_matches_src = true;
                                if ((idx_t)lhs_pid == stored_dst) lhs_matches_dst = true;
                            }
                            if (lhs_matches_src && !lhs_matches_dst) {
                                lhs_is_src = true;
                            } else if (lhs_matches_dst && !lhs_matches_src) {
                                lhs_is_src = false;
                            } else {
                                // Self-referential or ambiguous: use Cypher direction.
                                lhs_is_src = (qedge->GetDirection() != RelDirection::LEFT);
                            }
                        }
                    }
                }

                // M27: Record multi-partition edge info for the planner.
                if (qedge->GetPartitionIDs().size() > 1) {
                    auto primary_oid = (idx_t)qedge->GetPartitionIDs()[0];
                    auto &siblings = multi_edge_partitions_[primary_oid];
                    for (size_t pi = 1; pi < qedge->GetPartitionIDs().size(); pi++) {
                        siblings.push_back((idx_t)qedge->GetPartitionIDs()[pi]);
                    }
                }

                uint64_t lhs_edge_key = lhs_is_src ? SID_KEY_ID : TID_KEY_ID;
                uint64_t rhs_edge_key = lhs_is_src ? TID_KEY_ID : SID_KEY_ID;

                // --- A join R ---
                auto ar_join_type = gpopt::COperator::EOperatorId::EopLogicalInnerJoin;
                CExpression *a_r_join_expr = is_pathjoin
                    ? ExprLogicalPathJoin(
                          lhs_plan->getPlanExpr(), edge_plan->getPlanExpr(),
                          lhs_plan->getSchema()->getColRefOfKey(lhs_name, ID_KEY_ID),
                          edge_plan->getSchema()->getColRefOfKey(edge_name, SID_KEY_ID),
                          (int32_t)qedge->GetLowerBound(),
                          (int32_t)qedge->GetUpperBound(),
                          ar_join_type)
                    : ExprLogicalJoin(
                          lhs_plan->getPlanExpr(), edge_plan->getPlanExpr(),
                          lhs_plan->getSchema()->getColRefOfKey(lhs_name, ID_KEY_ID),
                          edge_plan->getSchema()->getColRefOfKey(edge_name, lhs_edge_key),
                          ar_join_type, nullptr);

                lhs_plan->getSchema()->appendSchema(edge_plan->getSchema());
                lhs_plan->addBinaryParentOp(a_r_join_expr, edge_plan);

                // --- R join B ---
                turbolynx::LogicalPlan *hop_plan;
                if (is_lhs_bound && is_rhs_bound) {
                    // Both bound: add filter edge.rhs_key = rhs.id
                    hop_plan = lhs_plan;
                    CExpression *sel_expr = CUtils::PexprLogicalSelect(
                        mp_, lhs_plan->getPlanExpr(),
                        ExprScalarCmpEq(
                            ExprScalarProperty(edge_name, rhs_edge_key, lhs_plan),
                            ExprScalarProperty(rhs_name, ID_KEY_ID, lhs_plan)));
                    hop_plan->addUnaryParentOp(sel_expr);
                } else {
                    turbolynx::LogicalPlan *rhs_plan;
                    if (!is_rhs_bound) {
                        rhs_plan = PlanNodeScan(*rhs_node);
                    } else {
                        rhs_plan = qg_plan;
                    }
                    auto rb_join_type = gpopt::COperator::EOperatorId::EopLogicalInnerJoin;
                    CExpression *join_expr = ExprLogicalJoin(
                        lhs_plan->getPlanExpr(), rhs_plan->getPlanExpr(),
                        lhs_plan->getSchema()->getColRefOfKey(edge_name, rhs_edge_key),
                        rhs_plan->getSchema()->getColRefOfKey(rhs_name, ID_KEY_ID),
                        rb_join_type, nullptr);
                    lhs_plan->getSchema()->appendSchema(rhs_plan->getSchema());
                    lhs_plan->addBinaryParentOp(join_expr, rhs_plan);
                    hop_plan = lhs_plan;
                }
                D_ASSERT(hop_plan != nullptr);

                // Merge with existing plan via CartProd if both sides were unbound
                if (qg_plan != nullptr && !is_lhs_bound && !is_rhs_bound) {
                    CExpression *cart_expr = ExprLogicalCartProd(
                        qg_plan->getPlanExpr(), hop_plan->getPlanExpr());
                    qg_plan->getSchema()->appendSchema(hop_plan->getSchema());
                    qg_plan->addBinaryParentOp(cart_expr, hop_plan);
                } else {
                    qg_plan = hop_plan;
                }
                D_ASSERT(qg_plan != nullptr);
            }
        } else {
            // No edges: single node scan
            D_ASSERT(qg->GetQueryNodes().size() == 1);
            turbolynx::LogicalPlan *node_plan = PlanNodeScan(*qg->GetQueryNodes()[0]);
            if (qg_plan == nullptr) {
                qg_plan = node_plan;
            } else {
                CExpression *cart_expr = ExprLogicalCartProd(
                    qg_plan->getPlanExpr(), node_plan->getPlanExpr());
                qg_plan->getSchema()->appendSchema(node_plan->getSchema());
                qg_plan->addBinaryParentOp(cart_expr, node_plan);
            }
        }
    }
    D_ASSERT(qg_plan != nullptr);
    return qg_plan;
}

// ============================================================
// Projection body (WITH / RETURN)
// ============================================================
turbolynx::LogicalPlan *Cypher2OrcaConverter::PlanProjectionBody(
    turbolynx::LogicalPlan *plan, const BoundProjectionBody &proj)
{
    // Aggregation
    bool agg_required = proj.HasAggregation();

    if (agg_required) {
        plan = PlanGroupBy(proj.GetProjections(), plan);
    } else {
        plan = PlanProjection(proj.GetProjections(), plan);
    }

    // ORDER BY
    if (proj.HasOrderBy()) {
        plan = PlanOrderBy(proj.GetOrderBy(), plan);
    }

    // DISTINCT
    if (proj.IsDistinct()) {
        CColRefArray *colrefs =
            plan->getPlanExpr()->DeriveOutputColumns()->Pdrgpcr(mp_);
        plan = PlanDistinct(proj.GetProjections(), colrefs, plan);
    }

    // SKIP / LIMIT
    if (proj.HasSkip() || proj.HasLimit()) {
        plan = PlanSkipOrLimit(proj, plan);
    }

    return plan;
}

// ============================================================
// Selection (WHERE predicates → CLogicalSelect)
// ============================================================
turbolynx::LogicalPlan *Cypher2OrcaConverter::PlanSelection(
    const bound_expression_vector &preds, turbolynx::LogicalPlan *prev_plan)
{
    D_ASSERT(!preds.empty());
    CExpressionArray *cnf_exprs = GPOS_NEW(mp_) CExpressionArray(mp_);
    for (auto &pred : preds) {
        cnf_exprs->Append(ConvertExpression(*pred, prev_plan));
    }

    CExpression *pred_expr;
    if (cnf_exprs->Size() > 1) {
        pred_expr = CUtils::PexprScalarBoolOp(
            mp_, CScalarBoolOp::EBoolOperator::EboolopAnd, cnf_exprs);
    } else {
        pred_expr = (*cnf_exprs)[0];
    }

    CExpression *sel_expr =
        CUtils::PexprLogicalSelect(mp_, prev_plan->getPlanExpr(), pred_expr);
    prev_plan->addUnaryParentOp(sel_expr);
    return prev_plan;
}

// ============================================================
// Projection (RETURN / WITH non-aggregate expressions)
// ============================================================
turbolynx::LogicalPlan *Cypher2OrcaConverter::PlanProjection(
    const bound_expression_vector &exprs, turbolynx::LogicalPlan *prev_plan)
{
    CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
    CExpressionArray *proj_array = GPOS_NEW(mp_) CExpressionArray(mp_);
    turbolynx::LogicalSchema new_schema;

    for (auto &expr_ptr : exprs) {
        const BoundExpression &expr = *expr_ptr;
        vector<CExpression *>  gen_exprs;
        vector<CColRef *>      gen_colrefs;

        if (expr.GetExprType() == BoundExpressionType::VARIABLE) {
            // RETURN n — expand to all property columns
            const BoundVariableExpression &var_expr =
                static_cast<const BoundVariableExpression &>(expr);
            const string &var_name = var_expr.GetVarName();
            auto var_colrefs = prev_plan->getSchema()->getAllColRefsOfKey(var_name);
            for (auto *colref : var_colrefs) {
                gen_colrefs.push_back(colref);
                uint64_t prop_key =
                    prev_plan->getSchema()->getPropertyNameOfColRef(var_name, colref);
                gen_exprs.push_back(ExprScalarProperty(var_name, prop_key, prev_plan));
            }
            // Copy schema entry
            if (prev_plan->getSchema()->isNodeBound(var_name)) {
                new_schema.copyNodeFrom(prev_plan->getSchema(), var_name);
            } else if (prev_plan->getSchema()->isEdgeBound(var_name)) {
                new_schema.copyEdgeFrom(prev_plan->getSchema(), var_name);
            } else {
                // Scalar alias from a previous WITH clause (e.g., message.id AS messageId).
                // getAllColRefsOfKey returns empty for alias-only entries; fall back to
                // alias_map via getColRefOfKey.
                if (gen_colrefs.empty()) {
                    CColRef *colref = prev_plan->getSchema()->getColRefOfKey(
                        var_name, std::numeric_limits<uint64_t>::max());
                    if (colref != nullptr) {
                        gen_colrefs.push_back(colref);
                        gen_exprs.push_back(GPOS_NEW(mp_) CExpression(
                            mp_, GPOS_NEW(mp_) CScalarIdent(mp_, colref)));
                    }
                }
                for (auto *colref : gen_colrefs) {
                    new_schema.appendColumn(var_name, colref);
                }
            }
        } else {
            CExpression *c_expr = ConvertExpression(expr, prev_plan);
            D_ASSERT(c_expr->Pop()->FScalar());
            string col_name = expr.HasAlias() ? expr.GetAlias() : expr.GetUniqueName();

            if (CUtils::FScalarIdent(c_expr)) {
                // Reuse existing colref
                CColRef *orig = col_factory->LookupColRef(
                    static_cast<CScalarIdent *>(c_expr->Pop())->Pcr()->Id());
                orig->MarkAsUsed();
                CColRef *prev_colref = col_factory->LookupColRef(
                    static_cast<CScalarIdent *>(c_expr->Pop())->Pcr()->PrevId());
                prev_colref->MarkAsUsed();
                gen_colrefs.push_back(orig);

                if (expr.GetExprType() == BoundExpressionType::PROPERTY) {
                    const BoundPropertyExpression &prop =
                        static_cast<const BoundPropertyExpression &>(expr);
                    if (prev_plan->getSchema()->isNodeBound(prop.GetVarName())) {
                        new_schema.appendNodeProperty(prop.GetVarName(),
                                                      prop.GetPropertyKeyID(), orig);
                    } else {
                        new_schema.appendEdgeProperty(prop.GetVarName(),
                                                      prop.GetPropertyKeyID(), orig);
                    }
                    // Register alias so ORDER BY can find it by alias name
                    if (expr.HasAlias()) {
                        new_schema.registerAlias(expr.GetAlias(), orig);
                    }
                } else {
                    new_schema.appendColumn(col_name, orig);
                }
            } else {
                // Create new colref
                CScalar *scalar_op = static_cast<CScalar *>(c_expr->Pop());
                std::wstring wname(col_name.begin(), col_name.end());
                const CWStringConst col_name_str(wname.c_str());
                CName col_cname(&col_name_str);
                CColRef *new_colref = col_factory->PcrCreate(
                    GetMDAccessor()->RetrieveType(scalar_op->MdidType()),
                    scalar_op->TypeModifier(), col_cname);
                new_colref->MarkAsUsed();
                gen_colrefs.push_back(new_colref);
                new_schema.appendColumn(col_name, new_colref);
            }
            gen_exprs.push_back(c_expr);
        }

        D_ASSERT(!gen_exprs.empty() && gen_exprs.size() == gen_colrefs.size());
        for (size_t idx = 0; idx < gen_exprs.size(); idx++) {
            CExpression *proj_elem = GPOS_NEW(mp_) CExpression(
                mp_,
                GPOS_NEW(mp_) CScalarProjectElement(mp_, gen_colrefs[idx]),
                gen_exprs[idx]);
            proj_array->Append(proj_elem);
        }
    }

    CExpression *proj_list = GPOS_NEW(mp_)
        CExpression(mp_, GPOS_NEW(mp_) CScalarProjectList(mp_), proj_array);
    CExpression *proj_expr = GPOS_NEW(mp_) CExpression(
        mp_, GPOS_NEW(mp_) CLogicalProjectColumnar(mp_),
        prev_plan->getPlanExpr(), proj_list);
    prev_plan->addUnaryParentOp(proj_expr);
    prev_plan->setSchema(std::move(new_schema));
    return prev_plan;
}

// ============================================================
// Group-by / aggregation
// ============================================================
turbolynx::LogicalPlan *Cypher2OrcaConverter::PlanGroupBy(
    const bound_expression_vector &exprs, turbolynx::LogicalPlan *prev_plan)
{
    CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
    CExpressionArray *agg_columns  = GPOS_NEW(mp_) CExpressionArray(mp_);
    CColRefArray     *key_columns  = GPOS_NEW(mp_) CColRefArray(mp_);
    agg_columns->AddRef();
    key_columns->AddRef();
    turbolynx::LogicalSchema new_schema;

    for (auto &expr_ptr : exprs) {
        const BoundExpression &expr = *expr_ptr;
        string col_name = expr.GetUniqueName();
        string col_name_print = expr.HasAlias() ? expr.GetAlias() : expr.GetUniqueName();

        bool is_agg = (expr.GetExprType() == BoundExpressionType::AGG_FUNCTION);

        if (is_agg) {
            // AGG column
            CExpression *c_expr = ConvertExpression(expr, prev_plan);
            CScalar *scalar_op = static_cast<CScalar *>(c_expr->Pop());
            std::wstring wname(col_name_print.begin(), col_name_print.end());
            const CWStringConst col_name_str(wname.c_str());
            CName col_cname(&col_name_str);
            CColRef *new_colref = col_factory->PcrCreate(
                GetMDAccessor()->RetrieveType(scalar_op->MdidType()),
                scalar_op->TypeModifier(), col_cname);
            new_schema.appendColumn(col_name, new_colref);
            // Register alias so ORDER BY can find this AGG column by alias name
            if (expr.HasAlias()) {
                new_schema.registerAlias(expr.GetAlias(), new_colref);
            }

            CExpression *proj_elem = GPOS_NEW(mp_) CExpression(
                mp_, GPOS_NEW(mp_) CScalarProjectElement(mp_, new_colref), c_expr);
            agg_columns->Append(proj_elem);
        } else if (expr.GetExprType() == BoundExpressionType::PROPERTY) {
            // KEY column (group-by key)
            const BoundPropertyExpression &prop =
                static_cast<const BoundPropertyExpression &>(expr);
            CExpression *c_expr = ConvertExpression(expr, prev_plan);
            CColRef *orig = col_factory->LookupColRef(
                static_cast<CScalarIdent *>(c_expr->Pop())->Pcr()->Id());

            // Always index by (var, key_id) so PlanOrderBy can find it by property lookup.
            // Also register any alias for ORDER BY alias-based access.
            if (prev_plan->getSchema()->isNodeBound(prop.GetVarName())) {
                new_schema.appendNodeProperty(prop.GetVarName(),
                                              prop.GetPropertyKeyID(), orig);
            } else {
                new_schema.appendEdgeProperty(prop.GetVarName(),
                                              prop.GetPropertyKeyID(), orig);
            }
            if (expr.HasAlias()) {
                new_schema.registerAlias(expr.GetAlias(), orig);
            }
            key_columns->Append(orig);
        } else if (expr.GetExprType() == BoundExpressionType::VARIABLE) {
            // WITH person, AGG(...),...  — all properties of node are group keys
            const BoundVariableExpression &var_expr =
                static_cast<const BoundVariableExpression &>(expr);
            const string &var_name = var_expr.GetVarName();
            auto prop_colrefs = prev_plan->getSchema()->getAllColRefsOfKey(var_name);
            for (auto *colref : prop_colrefs) {
                key_columns->Append(colref);
            }
            if (prev_plan->getSchema()->isNodeBound(var_name)) {
                new_schema.copyNodeFrom(prev_plan->getSchema(), var_name);
            } else {
                new_schema.copyEdgeFrom(prev_plan->getSchema(), var_name);
            }
        } else {
            // General expression key column
            auto prop_colrefs =
                prev_plan->getSchema()->getAllColRefsOfKey(col_name);
            if (prop_colrefs.empty()) {
                prop_colrefs =
                    prev_plan->getSchema()->getAllColRefsOfKey(expr.GetUniqueName());
            }
            if (prop_colrefs.empty()) {
                // Need to evaluate expression and create new colref
                CExpression *c_expr = ConvertExpression(expr, prev_plan);
                CScalar *scalar_op = static_cast<CScalar *>(c_expr->Pop());
                std::wstring wname(col_name_print.begin(), col_name_print.end());
                const CWStringConst col_name_str(wname.c_str());
                CName col_cname(&col_name_str);
                CColRef *new_colref = col_factory->PcrCreate(
                    GetMDAccessor()->RetrieveType(scalar_op->MdidType()),
                    scalar_op->TypeModifier(), col_cname);
                key_columns->Append(new_colref);
                new_schema.appendColumn(col_name, new_colref);
            } else {
                for (auto *colref : prop_colrefs) {
                    key_columns->Append(colref);
                    if (expr.HasAlias()) {
                        new_schema.appendColumn(col_name, colref);
                    } else {
                        new_schema.appendColumn(col_name, colref);
                    }
                }
            }
        }
    }

    CExpression *proj_list = GPOS_NEW(mp_)
        CExpression(mp_, GPOS_NEW(mp_) CScalarProjectList(mp_), agg_columns);
    CExpression *agg_expr = CUtils::PexprLogicalGbAggGlobal(
        mp_, key_columns, prev_plan->getPlanExpr(), proj_list);

    prev_plan->addUnaryParentOp(agg_expr);
    prev_plan->setSchema(std::move(new_schema));
    return prev_plan;
}

// ============================================================
// ORDER BY
// ============================================================
turbolynx::LogicalPlan *Cypher2OrcaConverter::PlanOrderBy(
    const vector<BoundOrderByItem> &items, turbolynx::LogicalPlan *prev_plan)
{
    vector<CColRef *> sort_colrefs;
    for (auto &item : items) {
        const BoundExpression &expr = *item.expr;
        CColRef *key_colref = nullptr;
        if (expr.GetExprType() == BoundExpressionType::PROPERTY) {
            const BoundPropertyExpression &prop =
                static_cast<const BoundPropertyExpression &>(expr);
            key_colref = prev_plan->getSchema()->getColRefOfKey(
                prop.GetVarName(), prop.GetPropertyKeyID());
            if (!key_colref && expr.HasAlias()) {
                key_colref = prev_plan->getSchema()->getColRefOfKey(
                    expr.GetAlias(), std::numeric_limits<uint64_t>::max());
            }
        } else {
            key_colref = prev_plan->getSchema()->getColRefOfKey(
                expr.GetUniqueName(), std::numeric_limits<uint64_t>::max());
            if (!key_colref && expr.HasAlias()) {
                key_colref = prev_plan->getSchema()->getColRefOfKey(
                    expr.GetAlias(), std::numeric_limits<uint64_t>::max());
            }
        }
        if (key_colref == nullptr) {
            throw std::runtime_error("ORDER BY: column not found in schema — "
                                     "use an alias defined in the RETURN clause");
        }
        sort_colrefs.push_back(key_colref);
    }

    COrderSpec *pos = GPOS_NEW(mp_) COrderSpec(mp_);
    for (size_t i = 0; i < sort_colrefs.size(); i++) {
        CColRef *colref = sort_colrefs[i];
        IMDType::ECmpType sort_type =
            items[i].ascending ? IMDType::EcmptL : IMDType::EcmptG;
        IMDId *mdid = colref->RetrieveType()->GetMdidForCmpType(sort_type);
        pos->Append(mdid, colref, COrderSpec::EntLast);
    }

    CLogicalLimit *pop = GPOS_NEW(mp_) CLogicalLimit(
        mp_, pos, true /* fGlobal */, false /* fHasCount */, true);
    CExpression *offset = CUtils::PexprScalarConstInt8(mp_, 0);
    CExpression *count  = CUtils::PexprScalarConstInt8(mp_, 0, true /* is_null */);
    CExpression *limit_expr = GPOS_NEW(mp_) CExpression(
        mp_, pop, prev_plan->getPlanExpr(), offset, count);

    prev_plan->addUnaryParentOp(limit_expr);
    return prev_plan;
}

// ============================================================
// DISTINCT
// ============================================================
turbolynx::LogicalPlan *Cypher2OrcaConverter::PlanDistinct(
    const bound_expression_vector &exprs, CColRefArray * /*colrefs*/,
    turbolynx::LogicalPlan *prev_plan)
{
    CColRefArray *key_columns = GPOS_NEW(mp_) CColRefArray(mp_);
    key_columns->AddRef();

    for (auto &expr_ptr : exprs) {
        const BoundExpression &expr = *expr_ptr;
        if (expr.GetExprType() == BoundExpressionType::PROPERTY) {
            const BoundPropertyExpression &prop =
                static_cast<const BoundPropertyExpression &>(expr);
            CExpression *c_expr = ConvertExpression(expr, prev_plan);
            CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
            CColRef *orig = col_factory->LookupColRef(
                static_cast<CScalarIdent *>(c_expr->Pop())->Pcr()->Id());
            key_columns->Append(orig);
        } else if (expr.GetExprType() == BoundExpressionType::VARIABLE) {
            const BoundVariableExpression &var_expr =
                static_cast<const BoundVariableExpression &>(expr);
            auto prop_colrefs =
                prev_plan->getSchema()->getAllColRefsOfKey(var_expr.GetVarName());
            for (auto *colref : prop_colrefs) {
                key_columns->Append(colref);
            }
        }
    }

    CLogicalGbAgg *pop = GPOS_NEW(mp_) CLogicalGbAgg(
        mp_, key_columns, COperator::EgbaggtypeGlobal);
    CExpression *gbagg_expr = GPOS_NEW(mp_) CExpression(
        mp_, pop, prev_plan->getPlanExpr(),
        GPOS_NEW(mp_) CExpression(mp_, GPOS_NEW(mp_) CScalarProjectList(mp_)));

    prev_plan->addUnaryParentOp(gbagg_expr);
    return prev_plan;
}

// ============================================================
// SKIP / LIMIT
// ============================================================
turbolynx::LogicalPlan *Cypher2OrcaConverter::PlanSkipOrLimit(
    const BoundProjectionBody &proj, turbolynx::LogicalPlan *prev_plan)
{
    bool has_count = proj.HasLimit();
    COrderSpec *pos = GPOS_NEW(mp_) COrderSpec(mp_);
    CLogicalLimit *pop = GPOS_NEW(mp_) CLogicalLimit(
        mp_, pos, true, has_count, true);

    CExpression *offset = proj.HasSkip()
        ? CUtils::PexprScalarConstInt8(mp_, (LINT)proj.GetSkipNumber())
        : CUtils::PexprScalarConstInt8(mp_, 0);

    CExpression *count = proj.HasLimit()
        ? CUtils::PexprScalarConstInt8(mp_, (LINT)proj.GetLimitNumber(), false)
        : CUtils::PexprScalarConstInt8(mp_, 0, true);

    CExpression *limit_expr = GPOS_NEW(mp_) CExpression(
        mp_, pop, prev_plan->getPlanExpr(), offset, count);
    prev_plan->addUnaryParentOp(limit_expr);
    return prev_plan;
}

// ============================================================
// Node scan
// ============================================================
turbolynx::LogicalPlan *Cypher2OrcaConverter::PlanNodeScan(const BoundNodeExpression &node)
{
    const string &name = node.GetUniqueName();
    vector<uint64_t> graphlet_oids;

    if (node.GetPartitionIDs().size() > 1) {
        // Multi-partition vertex (e.g., Message → Comment + Post).
        // Use only primary partition's graphlets for the ORCA plan (avoids UnionAll).
        // Record sibling graphlets for physical planner expansion.
        auto &catalog = context_->db->GetCatalog();
        auto primary_pid = (idx_t)node.GetPartitionIDs()[0];
        auto *primary_part = static_cast<PartitionCatalogEntry *>(
            catalog.GetEntry(*context_, DEFAULT_SCHEMA, primary_pid));
        if (primary_part) {
            auto *ps_ids = primary_part->GetPropertySchemaIDs();
            if (ps_ids) {
                for (auto ps_id : *ps_ids)
                    graphlet_oids.push_back((uint64_t)ps_id);
            }
        }

        // Collect sibling partition graphlet OIDs
        vector<idx_t> sibling_graphlets;
        for (size_t i = 1; i < node.GetPartitionIDs().size(); i++) {
            auto sib_pid = (idx_t)node.GetPartitionIDs()[i];
            auto *sib_part = static_cast<PartitionCatalogEntry *>(
                catalog.GetEntry(*context_, DEFAULT_SCHEMA, sib_pid));
            if (!sib_part) continue;
            auto *ps_ids = sib_part->GetPropertySchemaIDs();
            if (!ps_ids) continue;
            for (auto ps_id : *ps_ids)
                sibling_graphlets.push_back((idx_t)ps_id);
        }
        if (!sibling_graphlets.empty() && !graphlet_oids.empty()) {
            multi_vertex_partitions_[(idx_t)graphlet_oids[0]] = sibling_graphlets;
        }
    } else {
        graphlet_oids.assign(node.GetGraphletIDs().begin(), node.GetGraphletIDs().end());
    }
    D_ASSERT(!graphlet_oids.empty());

    const auto &prop_exprs = node.GetPropertyExpressions();

    map<uint64_t, map<uint64_t, uint64_t>> mapping;
    vector<int> used_col_idx;
    BuildSchemaProjectionMapping(graphlet_oids, prop_exprs,
                                 node.IsWholeNodeRequired(),
                                 mapping, used_col_idx);

    // M28: When multi-partition, filter out columns that don't exist in any
    // of the primary partition's graphlets (properties exclusive to siblings).
    if (node.GetPartitionIDs().size() > 1) {
        vector<int> filtered_used_col_idx;
        for (auto col_idx : used_col_idx) {
            bool found_in_any = false;
            for (auto &oid : graphlet_oids) {
                auto it = mapping[oid].find((uint64_t)col_idx);
                if (it != mapping[oid].end() &&
                    it->second != std::numeric_limits<uint64_t>::max()) {
                    found_in_any = true;
                    break;
                }
            }
            if (found_in_any) {
                filtered_used_col_idx.push_back(col_idx);
            }
        }
        used_col_idx = filtered_used_col_idx;
    }

    auto planned = ExprLogicalGetNodeOrEdge(name, graphlet_oids,
                                            used_col_idx, &mapping,
                                            node.IsWholeNodeRequired());
    CExpression *plan_expr = planned.first;
    CColRefArray *colrefs  = planned.second;
    D_ASSERT((idx_t)used_col_idx.size() == colrefs->Size());

    turbolynx::LogicalSchema schema;
    GenerateNodeSchema(node, used_col_idx, colrefs, schema);

    turbolynx::LogicalPlan *plan = new turbolynx::LogicalPlan(plan_expr, schema);
    D_ASSERT(!plan->getSchema()->isEmpty());
    return plan;
}

// ============================================================
// Edge scan (non-path)
// ============================================================
turbolynx::LogicalPlan *Cypher2OrcaConverter::PlanEdgeScan(const BoundRelExpression &rel)
{
    const string &name = rel.GetUniqueName();
    vector<uint64_t> graphlet_oids(rel.GetGraphletIDs().begin(),
                                   rel.GetGraphletIDs().end());
    D_ASSERT(!graphlet_oids.empty());
    const auto &prop_exprs = rel.GetPropertyExpressions();

    map<uint64_t, map<uint64_t, uint64_t>> mapping;
    vector<int> used_col_idx;
    BuildSchemaProjectionMapping(graphlet_oids, prop_exprs,
                                 false /* whole_node_required */,
                                 mapping, used_col_idx);

    auto planned = ExprLogicalGetNodeOrEdge(name, graphlet_oids,
                                            used_col_idx, &mapping,
                                            false);
    CExpression *plan_expr = planned.first;
    CColRefArray *colrefs  = planned.second;
    D_ASSERT((idx_t)used_col_idx.size() == colrefs->Size());

    turbolynx::LogicalSchema schema;
    GenerateEdgeSchema(rel, used_col_idx, colrefs, schema);

    turbolynx::LogicalPlan *plan = new turbolynx::LogicalPlan(plan_expr, schema);
    D_ASSERT(!plan->getSchema()->isEmpty());
    return plan;
}

// ============================================================
// Path scan (variable-length)
// ============================================================
turbolynx::LogicalPlan *Cypher2OrcaConverter::PlanPathGet(const BoundRelExpression &rel)
{
    const string &edge_name = rel.GetUniqueName();
    vector<uint64_t> graphlet_oids(rel.GetGraphletIDs().begin(),
                                   rel.GetGraphletIDs().end());
    D_ASSERT(!graphlet_oids.empty());

    // Path get only scans _id, _sid, _tid (3 columns)
    CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
    CColRefArray *path_output_cols = GPOS_NEW(mp_) CColRefArray(mp_);
    CTableDescriptorArray *path_table_descs = GPOS_NEW(mp_) CTableDescriptorArray(mp_);
    path_table_descs->AddRef();

    for (auto &oid : graphlet_oids) {
        path_table_descs->Append(CreateTableDescForRel(oid, edge_name));
    }

    turbolynx::LogicalSchema schema;
    // Build three columns: _id, _sid, _tid
    // Column descriptors are taken from first graphlet
    CColumnDescriptorArray *pdrgpcoldesc =
        path_table_descs->operator[](0)->Pdrgpcoldesc();
    IMDId *mdid_table = path_table_descs->operator[](0)->MDId();

    // Map key IDs to column positions in first graphlet
    struct SysCol {
        uint64_t key_id;
        const WCHAR *wname;
    } sys_cols[] = {
        {ID_KEY_ID,  L"_id"},
        {SID_KEY_ID, L"_sid"},
        {TID_KEY_ID, L"_tid"},
    };

    for (auto &sc : sys_cols) {
        // Find which md column corresponds to this key_id
        uint64_t col_pos = FindKeyColumnInGraphlet(graphlet_oids[0], sc.key_id);
        if (col_pos == std::numeric_limits<uint64_t>::max()) {
            if (sc.key_id == ID_KEY_ID) col_pos = 0;  // always column 0
            else continue;
        }

        const CWStringConst col_name_str(sc.wname);
        CName col_name(&col_name_str);
        CColumnDescriptor *pcoldesc = (*pdrgpcoldesc)[col_pos];
        CColRef *new_colref = col_factory->PcrCreate(
            pcoldesc, col_name, 0, false, mdid_table);
        path_output_cols->Append(new_colref);
        schema.appendEdgeProperty(edge_name, sc.key_id, new_colref);
    }
    D_ASSERT(schema.getNumPropertiesOfKey(edge_name) == 3);
    D_ASSERT(path_output_cols->Size() == 3);

    std::wstring w_alias(edge_name.begin(), edge_name.end());
    const CWStringConst path_name_str(w_alias.c_str());

    CLogicalPathGet *pop = GPOS_NEW(mp_) CLogicalPathGet(
        mp_, GPOS_NEW(mp_) CName(mp_, CName(&path_name_str)),
        path_table_descs, path_output_cols,
        (gpos::INT)rel.GetLowerBound(),
        (gpos::INT)rel.GetUpperBound());
    CExpression *path_expr = GPOS_NEW(mp_) CExpression(mp_, pop);
    path_output_cols->AddRef();

    turbolynx::LogicalPlan *plan = new turbolynx::LogicalPlan(path_expr, schema);
    D_ASSERT(!plan->getSchema()->isEmpty());
    return plan;
}

// ============================================================
// Schema projection mapping builder
// ============================================================
void Cypher2OrcaConverter::BuildSchemaProjectionMapping(
    const vector<uint64_t> &graphlet_oids,
    const vector<shared_ptr<BoundExpression>> &prop_exprs,
    bool all_used,
    map<uint64_t, map<uint64_t, uint64_t>> &out_mapping,
    vector<int> &out_used_col_idx)
{
    // Initialise per-graphlet maps
    for (auto oid : graphlet_oids) {
        out_mapping[oid] = map<uint64_t, uint64_t>();
    }

    for (int col_idx = 0; col_idx < (int)prop_exprs.size(); col_idx++) {
        // Check if this column is used (all_used → always include)
        // In the old Planner, node_expr->isUsedColumn(col_idx) is checked.
        // For TurboLynx, all_used or BoundNodeExpression::IsPropertyUsed(col_idx).
        if (!all_used) {
            // We conservatively include all columns for now.
            // TODO: wire through IsPropertyUsed when refactored.
        }

        out_used_col_idx.push_back(col_idx);

        // Get key_id for this property
        uint64_t key_id;
        if (col_idx == 0) {
            key_id = ID_KEY_ID;  // _id is always column 0
        } else {
            const BoundPropertyExpression *prop =
                static_cast<const BoundPropertyExpression *>(prop_exprs[col_idx].get());
            key_id = prop->GetPropertyKeyID();
        }

        for (auto oid : graphlet_oids) {
            uint64_t col_pos;
            if (key_id == ID_KEY_ID) {
                col_pos = 0;  // _id is always column 0
            } else if (key_id == SID_KEY_ID) {
                col_pos = FindKeyColumnInGraphlet(oid, SID_KEY_ID);
            } else if (key_id == TID_KEY_ID) {
                col_pos = FindKeyColumnInGraphlet(oid, TID_KEY_ID);
            } else {
                col_pos = FindKeyColumnInGraphlet(oid, key_id);
            }
            out_mapping[oid][(uint64_t)col_idx] = col_pos;
        }
    }
}

void Cypher2OrcaConverter::GenerateNodeSchema(
    const BoundNodeExpression &node,
    const vector<int> &used_col_idx,
    CColRefArray *colrefs,
    turbolynx::LogicalSchema &schema)
{
    const string &name = node.GetUniqueName();
    const auto &props = node.GetPropertyExpressions();
    for (int i = 0; i < (int)used_col_idx.size(); i++) {
        int col_idx = used_col_idx[i];
        uint64_t key_id;
        if (col_idx == 0) {
            key_id = ID_KEY_ID;
        } else {
            const BoundPropertyExpression *prop =
                static_cast<const BoundPropertyExpression *>(props[col_idx].get());
            key_id = prop->GetPropertyKeyID();
        }
        schema.appendNodeProperty(name, key_id, (*colrefs)[i]);
    }
}

void Cypher2OrcaConverter::GenerateEdgeSchema(
    const BoundRelExpression &rel,
    const vector<int> &used_col_idx,
    CColRefArray *colrefs,
    turbolynx::LogicalSchema &schema)
{
    const string &name = rel.GetUniqueName();
    const auto &props = rel.GetPropertyExpressions();
    for (int i = 0; i < (int)used_col_idx.size(); i++) {
        int col_idx = used_col_idx[i];
        uint64_t key_id;
        if (col_idx == 0) {
            key_id = ID_KEY_ID;
        } else {
            const BoundPropertyExpression *prop =
                static_cast<const BoundPropertyExpression *>(props[col_idx].get());
            key_id = prop->GetPropertyKeyID();
        }
        schema.appendEdgeProperty(name, key_id, (*colrefs)[i]);
    }
}

// ============================================================
// ORCA expression builders
// ============================================================
CExpression *Cypher2OrcaConverter::ExprLogicalGet(uint64_t obj_id,
                                                    const string &name,
                                                    bool whole_node_required)
{
    CTableDescriptor *ptabdesc = CreateTableDescForRel(obj_id, name);
    ptabdesc->SetInstanceDescriptor(false);

    std::wstring w_alias(name.begin(), name.end());
    CWStringConst str_alias(w_alias.c_str());

    CLogicalGet *pop = GPOS_NEW(mp_) CLogicalGet(
        mp_, GPOS_NEW(mp_) CName(mp_, CName(&str_alias)), ptabdesc);
    CExpression *scan_expr = GPOS_NEW(mp_) CExpression(mp_, pop);

    CColRefArray *arr = pop->PdrgpcrOutput();
    ULONG node_id = (ULONG)-1;
    if (whole_node_required) {
        CColRef *pid_ref = (*arr)[0];
        node_id = pid_ref->Id();
    }
    for (ULONG ul = 0; ul < arr->Size(); ul++) {
        CColRef *ref = (*arr)[ul];
        ref->MarkAsUnknown();
        ref->SetNodeId(node_id);
    }
    return scan_expr;
}

CExpression *Cypher2OrcaConverter::ExprLogicalJoin(
    CExpression *lhs, CExpression *rhs,
    CColRef *lhs_col, CColRef *rhs_col,
    gpopt::COperator::EOperatorId join_op,
    CExpression *additional_pred)
{
    lhs->AddRef();
    rhs->AddRef();

    CExpression *eq = CUtils::PexprScalarEqCmp(mp_, lhs_col, rhs_col);
    CExpression *pred;
    if (additional_pred == nullptr) {
        pred = eq;
    } else {
        CExpressionArray *children = GPOS_NEW(mp_) CExpressionArray(mp_);
        children->Append(additional_pred);
        children->Append(eq);
        children->AddRef();
        pred = CUtils::PexprScalarBoolOp(mp_, CScalarBoolOp::EboolopAnd, children);
    }
    pred->AddRef();

    switch (join_op) {
        case gpopt::COperator::EopLogicalInnerJoin:
            return CUtils::PexprLogicalJoin<CLogicalInnerJoin>(mp_, lhs, rhs, pred);
        case gpopt::COperator::EopLogicalLeftOuterJoin:
            return CUtils::PexprLogicalJoin<CLogicalLeftOuterJoin>(mp_, lhs, rhs, pred);
        case gpopt::COperator::EopLogicalRightOuterJoin:
            return CUtils::PexprLogicalJoin<CLogicalRightOuterJoin>(mp_, lhs, rhs, pred);
        default:
            D_ASSERT(false);
            return nullptr;
    }
}

CExpression *Cypher2OrcaConverter::ExprLogicalPathJoin(
    CExpression *lhs, CExpression *rhs,
    CColRef *lhs_col, CColRef *rhs_col,
    int32_t lb, int32_t ub,
    gpopt::COperator::EOperatorId join_op)
{
    D_ASSERT(join_op == gpopt::COperator::EOperatorId::EopLogicalInnerJoin);
    lhs->AddRef();
    rhs->AddRef();
    CExpression *eq = CUtils::PexprScalarEqCmp(mp_, lhs_col, rhs_col);
    eq->AddRef();
    return CUtils::PexprLogicalJoin<CLogicalInnerJoin>(mp_, lhs, rhs, eq);
}

CExpression *Cypher2OrcaConverter::ExprLogicalCartProd(CExpression *lhs,
                                                        CExpression *rhs)
{
    lhs->AddRef();
    rhs->AddRef();
    CExpression *true_pred = CUtils::PexprScalarConstBool(mp_, true);
    return CUtils::PexprLogicalJoin<CLogicalInnerJoin>(mp_, lhs, rhs, true_pred);
}

pair<CExpression *, CColRefArray *> Cypher2OrcaConverter::ExprLogicalGetNodeOrEdge(
    const string &name,
    vector<uint64_t> &graphlet_oids,
    const vector<int> &used_col_idx,
    map<uint64_t, map<uint64_t, uint64_t>> *mapping,
    bool whole_node_required)
{
    // Collect union schema types from the first valid graphlet per column
    vector<pair<gpmd::IMDId *, gpos::INT>> union_schema_types;
    vector<CColRef *> union_schema_colrefs;

    for (int i = 0; i < (int)used_col_idx.size(); i++) {
        int col_idx = used_col_idx[i];
        uint64_t valid_oid = 0;
        uint64_t valid_cid = std::numeric_limits<uint64_t>::max();

        for (auto &oid : graphlet_oids) {
            auto it = (*mapping)[oid].find((uint64_t)col_idx);
            uint64_t idx_to_try = (it != (*mapping)[oid].end())
                ? it->second
                : std::numeric_limits<uint64_t>::max();
            if (idx_to_try != std::numeric_limits<uint64_t>::max()) {
                valid_oid = oid;
                valid_cid = idx_to_try;
                break;
            }
        }
        D_ASSERT(valid_cid != std::numeric_limits<uint64_t>::max());

        const IMDRelation *relmd = GetRelMd(valid_oid);
        const IMDColumn  *mdcol = relmd->GetMdCol(valid_cid);
        union_schema_types.push_back({mdcol->MdidType(), mdcol->TypeModifier()});
    }

    CColRefArray *idx0_output = nullptr;
    CColRef2dArray *pdrgdrgpcr = nullptr;
    CExpressionArray *pdrgpexpr = nullptr;
    CExpression *union_plan = nullptr;

    if (graphlet_oids.size() > 1) {
        pdrgdrgpcr = GPOS_NEW(mp_) CColRef2dArray(mp_);
        pdrgpexpr  = GPOS_NEW(mp_) CExpressionArray(mp_);
    }

    for (int idx = 0; idx < (int)graphlet_oids.size(); idx++) {
        uint64_t oid = graphlet_oids[idx];
        CExpression *expr = ExprLogicalGet(oid, name, whole_node_required);

        // Build projection conforming to union schema
        auto &m = (*mapping)[oid];
        vector<uint64_t> project_col_ids;
        for (int i = 0; i < (int)used_col_idx.size(); i++) {
            int proj_idx = used_col_idx[i];
            project_col_ids.push_back(m.find((uint64_t)proj_idx)->second);
        }
        D_ASSERT(!project_col_ids.empty());

        auto proj_result = ExprScalarAddSchemaConformProject(
            expr, project_col_ids, &union_schema_types, union_schema_colrefs);
        CExpression *proj_expr = proj_result.first;
        CColRefArray *output   = proj_result.second;

        if (graphlet_oids.size() == 1) {
            union_plan     = proj_expr;
            idx0_output    = output;
        } else {
            if (idx == 0) {
                idx0_output = output;
                pdrgdrgpcr->Append(output);
            } else {
                pdrgdrgpcr->Append(output);
            }
            pdrgpexpr->Append(proj_expr);
        }
    }

    if (graphlet_oids.size() > 1) {
        union_plan = GPOS_NEW(mp_) CExpression(
            mp_,
            GPOS_NEW(mp_) CLogicalUnionAll(mp_, idx0_output, pdrgdrgpcr),
            pdrgpexpr);
    }

    return {union_plan, idx0_output};
}

pair<CExpression *, CColRefArray *>
Cypher2OrcaConverter::ExprScalarAddSchemaConformProject(
    CExpression *relation,
    vector<uint64_t> &col_ids,
    vector<pair<gpmd::IMDId *, gpos::INT>> *target_types,
    vector<CColRef *> &union_schema_colrefs)
{
    CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
    CExpressionArray *proj_array  = GPOS_NEW(mp_) CExpressionArray(mp_);
    CColRefArray     *output_cols = GPOS_NEW(mp_) CColRefArray(mp_);
    output_cols->AddRef();

    for (uint64_t t_col_id = 0; t_col_id < col_ids.size(); t_col_id++) {
        uint64_t col_id = col_ids[t_col_id];
        CExpression *proj_elem;

        if (col_id == std::numeric_limits<uint64_t>::max()) {
            // Null projection
            D_ASSERT(target_types != nullptr);
            auto &type_info = (*target_types)[t_col_id];
            CExpression *null_expr = CUtils::PexprScalarConstNull(
                mp_, GetMDAccessor()->RetrieveType(type_info.first),
                type_info.second);
            const CWStringConst col_name_str(GPOS_WSZ_LIT("const_null"));
            CName col_name(&col_name_str);
            CColRef *new_colref = col_factory->PcrCreate(
                GetMDAccessor()->RetrieveType(type_info.first),
                type_info.second, col_name);
            proj_elem = GPOS_NEW(mp_) CExpression(
                mp_, GPOS_NEW(mp_) CScalarProjectElement(mp_, new_colref), null_expr);
            proj_array->Append(proj_elem);
            output_cols->Append(new_colref);
        } else {
            // Non-null column
            CColRef *colref = relation->DeriveOutputColumns()->Pdrgpcr(mp_)
                                   ->operator[](col_id);
            CColRef *new_colref;
            // System columns (_id, _sid, _tid) use identity projection
            const wchar_t *colname = colref->Name().Pstr()->GetBuffer();
            if (std::wcscmp(colname, L"_id") == 0 ||
                std::wcscmp(colname, L"_sid") == 0 ||
                std::wcscmp(colname, L"_tid") == 0) {
                CExpression *ident = GPOS_NEW(mp_) CExpression(
                    mp_, GPOS_NEW(mp_) CScalarIdent(mp_, colref));
                proj_elem = GPOS_NEW(mp_) CExpression(
                    mp_, GPOS_NEW(mp_) CScalarProjectElement(mp_, colref), ident);
                colref->MarkAsUsed();
                proj_array->Append(proj_elem);
                output_cols->Append(colref);
            } else {
                new_colref = col_factory->PcrCopy(colref);
                new_colref->SetPrevId(colref->Id());
                CExpression *ident = GPOS_NEW(mp_) CExpression(
                    mp_, GPOS_NEW(mp_) CScalarIdent(mp_, colref));
                proj_elem = GPOS_NEW(mp_) CExpression(
                    mp_, GPOS_NEW(mp_) CScalarProjectElement(mp_, new_colref), ident);
                proj_array->Append(proj_elem);
                output_cols->Append(new_colref);
            }
        }
    }

    CExpression *proj_list = GPOS_NEW(mp_) CExpression(
        mp_, GPOS_NEW(mp_) CScalarProjectList(mp_), proj_array);
    CExpression *proj_expr = GPOS_NEW(mp_) CExpression(
        mp_, GPOS_NEW(mp_) CLogicalProjectColumnar(mp_), relation, proj_list);

    return {proj_expr, output_cols};
}

CTableDescriptor *Cypher2OrcaConverter::CreateTableDescForRel(uint64_t obj_id,
                                                               const string &rel_name)
{
    CMDIdGPDB *mdid = GPOS_NEW(mp_) CMDIdGPDB(IMDId::EmdidRel, obj_id, 0, 0);
    const IMDRelation *md_rel = GetMDAccessor()->RetrieveRel(mdid);

    std::wstring w_name(rel_name.begin(), rel_name.end());
    const CWStringConst tname_str(w_name.c_str());
    CName tname(&tname_str);

    CTableDescriptor *ptabdesc =
        GPOS_NEW(mp_) CTableDescriptor(mp_, mdid, tname, false, IMDRelation::EreldistrMasterOnly,
                                        IMDRelation::ErelstorageSentinel, 0 /* lock*/);
    for (ULONG ul = 0; ul < md_rel->ColumnCount(); ul++) {
        const IMDColumn *md_col = md_rel->GetMdCol(ul);
        if (md_col->IsDropped()) continue;
        CColumnDescriptor *col_desc = GPOS_NEW(mp_) CColumnDescriptor(
            mp_, GetMDAccessor()->RetrieveType(md_col->MdidType()),
            md_col->TypeModifier(), CName(md_col->Mdname().GetMDName()),
            md_col->AttrNum() /* attno matches MD relation's attrno map*/, md_col->IsNullable(), false);
        ptabdesc->AddColumn(col_desc);
    }
    return ptabdesc;
}

// ============================================================
// Scalar utilities
// ============================================================
CExpression *Cypher2OrcaConverter::ExprScalarCmpEq(CExpression *left, CExpression *right)
{
    CMDAccessor *mda = GetMDAccessor();
    IMDId *left_mdid  = CScalar::PopConvert(left->Pop())->MdidType();
    IMDId *right_mdid = CScalar::PopConvert(right->Pop())->MdidType();
    IMDId *func = CMDAccessorUtils::GetScCmpMdIdConsiderCasts(
        mda, left_mdid, right_mdid, IMDType::EcmptEq);
    D_ASSERT(func != nullptr);
    return CUtils::PexprScalarCmp(mp_, left, right, IMDType::EcmptEq);
}

CExpression *Cypher2OrcaConverter::ExprScalarProperty(const string &var_name,
                                                        uint64_t key_id,
                                                        turbolynx::LogicalPlan *plan)
{
    CColRef *colref = plan->getSchema()->getColRefOfKey(var_name, key_id);
    D_ASSERT(colref != nullptr);
    return GPOS_NEW(mp_) CExpression(mp_, GPOS_NEW(mp_) CScalarIdent(mp_, colref));
}

// ============================================================
// Catalog helpers
// ============================================================
PropertySchemaCatalogEntry *Cypher2OrcaConverter::GetPropertySchema(uint64_t graphlet_oid)
{
    return context_->db->GetCatalogWrapper().RelationIdGetRelation(*context_, graphlet_oid);
}

uint64_t Cypher2OrcaConverter::FindKeyColumnInGraphlet(uint64_t graphlet_oid,
                                                        uint64_t key_id)
{
    if (key_id == ID_KEY_ID) return 0;  // _id is always column 0

    PropertySchemaCatalogEntry *ps = GetPropertySchema(graphlet_oid);
    if (ps == nullptr) return std::numeric_limits<uint64_t>::max();

    const PropertyKeyID_vector &keys = ps->property_keys;
    for (size_t i = 0; i < keys.size(); i++) {
        if ((uint64_t)keys[i] == key_id) {
            return i + 1;  // 1-based (col 0 = _id)
        }
    }
    return std::numeric_limits<uint64_t>::max();
}

GraphCatalogEntry *Cypher2OrcaConverter::GetGraphCatalog()
{
    if (graph_cat_ != nullptr) return graph_cat_;
    auto &catalog = context_->db->GetCatalog();
    graph_cat_ = (GraphCatalogEntry *)catalog.GetEntry(
        *context_, CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, DEFAULT_GRAPH);
    return graph_cat_;
}

// ============================================================
// Type helpers
// ============================================================
INT Cypher2OrcaConverter::GetTypeMod(const LogicalType &type)
{
    INT mod = 0;
    if (type.id() == LogicalTypeId::DECIMAL) {
        uint16_t ws = DecimalType::GetWidth(type);
        ws = ws << 8 | DecimalType::GetScale(type);
        mod = ws;
    } else if (type.id() == LogicalTypeId::LIST) {
        if (ListType::GetChildType(type).id() == LogicalTypeId::LIST) {
            INT child_mod = GetTypeMod(ListType::GetChildType(type));
            mod = (INT)LogicalTypeId::LIST | child_mod << 8;
        } else {
            mod = (INT)ListType::GetChildType(type).id();
        }
    }
    return mod;
}

bool Cypher2OrcaConverter::IsCastingFunction(const string &func_name)
{
    return (func_name == "TO_DOUBLE" ||
            func_name == "TO_FLOAT"  ||
            func_name == "TO_INTEGER");
}

} // namespace duckdb
