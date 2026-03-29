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
#include "gpopt/operators/CLogicalConstTableGet.h"
#include "gpopt/operators/CLogicalUnnest.h"
#include "gpopt/operators/CScalarProjectElement.h"
#include "gpopt/operators/CScalarProjectList.h"

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
    std::unordered_map<idx_t, std::vector<idx_t>> &multi_vertex_partitions,
    std::unordered_map<ULONG, MpvNullPropInfo> &mpv_null_colref_props,
    std::unordered_map<INT, LogicalType> &complex_type_registry,
    INT &next_complex_type_id)
    : mp_(mp), context_(context), provider_(provider),
      col_name_map_(col_name_map),
      both_edge_partitions_(both_edge_partitions),
      multi_edge_partitions_(multi_edge_partitions),
      multi_vertex_partitions_(multi_vertex_partitions),
      mpv_null_colref_props_(mpv_null_colref_props),
      complex_type_registry_(complex_type_registry),
      next_complex_type_id_(next_complex_type_id)
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
// Detect the collect(var)+IN anti-pattern across consecutive query parts:
//   Part N:   WITH ..., collect(var) AS listAlias
//   Part N+1: OPTIONAL MATCH ... WHERE var IN listAlias
//
// This is semantically equivalent to:
//   Part N:   WITH DISTINCT ..., var
//   Part N+1: OPTIONAL MATCH ...  (var is now bound, no IN filter needed)
//
// The rewritten form produces an optimal plan (index joins) instead of a
// full node scan + late filter.  Returns (collect_var_name, list_alias)
// if detected, or empty strings if not.
// Detect collect(DISTINCT var) AS alias in Part N + UNWIND alias AS newVar in Part N+1.
// Returns {collect_var, list_alias, unwind_alias} or empty strings.
static std::tuple<string, string, string> DetectCollectUnwindPattern(
    const NormalizedQueryPart &partN, const NormalizedQueryPart &partN1)
{
    if (!partN.HasProjectionBody()) return {};

    // Step 1: Find collect(distinct var) AS alias in Part N's projections.
    // Only apply when collect() is the SOLE projection (simple pattern).
    auto &projs = partN.GetProjectionBody()->GetProjections();
    if (projs.size() != 1) return {};  // skip complex multi-projection cases
    string collect_var, list_alias;
    for (auto &ep : projs) {
        if (ep->GetExprType() != BoundExpressionType::AGG_FUNCTION) continue;
        auto &agg = static_cast<const BoundAggFunctionExpression &>(*ep);
        if (agg.GetFuncName() != "collect" || !agg.HasChild()) continue;
        auto *child = agg.GetChild();
        if (child->GetExprType() != BoundExpressionType::VARIABLE) continue;
        collect_var = static_cast<const BoundVariableExpression &>(*child).GetVarName();
        list_alias = agg.HasAlias() ? agg.GetAlias() : agg.GetUniqueName();
        break;
    }
    if (collect_var.empty()) return {};

    // Step 2: Check if Part N+1 starts with UNWIND alias AS newVar
    if (partN1.GetNumReadingClauses() < 1) return {};
    auto *rc0 = partN1.GetReadingClause(0);
    if (rc0->GetClauseType() != BoundClauseType::UNWIND) return {};
    auto &uc = static_cast<const BoundUnwindClause &>(*rc0);
    // The UNWIND expression should reference the list alias
    auto *unwind_expr = uc.GetExpression();
    if (unwind_expr->GetExprType() != BoundExpressionType::VARIABLE) return {};
    auto &unwind_var = static_cast<const BoundVariableExpression &>(*unwind_expr);
    if (unwind_var.GetVarName() != list_alias) return {};

    return {collect_var, list_alias, uc.GetAlias()};
}

static std::pair<string, string> DetectCollectInPattern(
    const NormalizedQueryPart &partN, const NormalizedQueryPart &partN1)
{
    if (!partN.HasProjectionBody()) return {};

    // Step 1: Find collect(var) AS alias in Part N's projections
    string collect_var, list_alias;
    for (auto &ep : partN.GetProjectionBody()->GetProjections()) {
        if (ep->GetExprType() != BoundExpressionType::AGG_FUNCTION) continue;
        auto &agg = static_cast<const BoundAggFunctionExpression &>(*ep);
        if (agg.GetFuncName() != "collect" || !agg.HasChild()) continue;
        auto *child = agg.GetChild();
        if (child->GetExprType() != BoundExpressionType::VARIABLE) continue;
        collect_var = static_cast<const BoundVariableExpression &>(*child).GetVarName();
        list_alias = agg.HasAlias() ? agg.GetAlias() : agg.GetUniqueName();
        break;
    }
    if (collect_var.empty()) return {};

    // Step 2: Find list_contains(listAlias, var) in Part N+1's OPTIONAL MATCH predicates
    for (idx_t i = 0; i < partN1.GetNumReadingClauses(); i++) {
        auto *rc = partN1.GetReadingClause(i);
        if (rc->GetClauseType() != BoundClauseType::MATCH) continue;
        auto &mc = static_cast<const BoundMatchClause &>(*rc);
        if (!mc.IsOptional()) continue;
        for (auto &pred : mc.GetPredicates()) {
            if (pred->GetExprType() != BoundExpressionType::FUNCTION) continue;
            auto &fn = static_cast<const CypherBoundFunctionExpression &>(*pred);
            if (fn.GetFuncName() != "list_contains" || fn.GetNumChildren() != 2) continue;
            // child[0] = list, child[1] = element
            auto *list_arg = fn.GetChild(0);
            auto *elem_arg = fn.GetChild(1);
            string list_name = list_arg->HasAlias() ? list_arg->GetAlias()
                             : list_arg->GetUniqueName();
            string elem_name = elem_arg->HasAlias() ? elem_arg->GetAlias()
                             : elem_arg->GetUniqueName();
            if (elem_name == collect_var && list_name == list_alias) {
                return {collect_var, list_alias};
            }
        }
    }
    return {};
}

turbolynx::LogicalPlan *Cypher2OrcaConverter::PlanSingleQuery(const NormalizedSingleQuery &sq)
{
    // Pre-scan for collect+IN pattern to enable rewrite
    unordered_set<idx_t> rewrite_parts;  // Part N indices to rewrite
    unordered_map<idx_t, string> rewrite_collect_var; // collect variable name
    unordered_map<idx_t, string> rewrite_list_alias;  // list alias name
    for (idx_t i = 0; i + 1 < sq.GetNumQueryParts(); ++i) {
        auto [var, alias] = DetectCollectInPattern(
            *sq.GetQueryPart(i), *sq.GetQueryPart(i + 1));
        if (!var.empty()) {
            rewrite_parts.insert(i);
            rewrite_collect_var[i] = var;
            rewrite_list_alias[i] = alias;
        }
    }

    // Pre-scan for collect(DISTINCT)+UNWIND pattern to enable rewrite
    // collect(distinct var) AS list + UNWIND list AS newVar → WITH DISTINCT var
    unordered_set<idx_t> unwind_rewrite_parts;  // Part N indices
    unordered_map<idx_t, string> unwind_collect_var;
    unordered_map<idx_t, string> unwind_alias;  // the UNWIND alias (newVar)
    for (idx_t i = 0; i + 1 < sq.GetNumQueryParts(); ++i) {
        auto [var, list_alias, uw_alias] = DetectCollectUnwindPattern(
            *sq.GetQueryPart(i), *sq.GetQueryPart(i + 1));
        if (!var.empty()) {
            unwind_rewrite_parts.insert(i);
            unwind_collect_var[i] = var;
            unwind_alias[i] = uw_alias;
        }
    }

    turbolynx::LogicalPlan *cur_plan = nullptr;
    for (idx_t i = 0; i < sq.GetNumQueryParts(); ++i) {
        if (rewrite_parts.count(i)) {
            // Rewrite Part N: replace collect(var) with DISTINCT var.
            // Build modified projection list: swap collect(var) for var.
            auto &proj = *sq.GetQueryPart(i)->GetProjectionBody();
            bound_expression_vector new_projs;
            for (auto &ep : proj.GetProjections()) {
                if (ep->GetExprType() == BoundExpressionType::AGG_FUNCTION) {
                    auto &agg = static_cast<const BoundAggFunctionExpression &>(*ep);
                    if (agg.GetFuncName() == "collect" && agg.HasChild()) {
                        // Replace collect(var) with just var
                        new_projs.push_back(agg.GetChild()->Copy());
                        continue;
                    }
                }
                new_projs.push_back(ep);
            }
            // Process reading clauses normally
            for (idx_t j = 0; j < sq.GetQueryPart(i)->GetNumReadingClauses(); j++) {
                cur_plan = PlanReadingClause(*sq.GetQueryPart(i)->GetReadingClause(j), cur_plan);
            }
            // Use DISTINCT projection (no aggregation) instead of GROUP BY with collect
            cur_plan = PlanProjection(new_projs, cur_plan);
            // Add DISTINCT
            CColRefArray *colrefs =
                cur_plan->getPlanExpr()->DeriveOutputColumns()->Pdrgpcr(mp_);
            cur_plan = PlanDistinct(new_projs, colrefs, cur_plan);
        } else if (i > 0 && rewrite_parts.count(i - 1)) {
            // Part N+1 after rewrite: process OPTIONAL MATCH, but skip the
            // list_contains predicate (var is now directly bound).
            const string &skip_alias = rewrite_list_alias[i - 1];
            for (idx_t j = 0; j < sq.GetQueryPart(i)->GetNumReadingClauses(); j++) {
                auto *rc = sq.GetQueryPart(i)->GetReadingClause(j);
                if (rc->GetClauseType() == BoundClauseType::MATCH) {
                    auto &mc = static_cast<const BoundMatchClause &>(*rc);
                    if (mc.IsOptional()) {
                        // Filter out the list_contains predicate
                        bound_expression_vector filtered_preds;
                        for (auto &pred : mc.GetPredicates()) {
                            if (pred->GetExprType() == BoundExpressionType::FUNCTION) {
                                auto &fn = static_cast<const CypherBoundFunctionExpression &>(*pred);
                                if (fn.GetFuncName() == "list_contains") continue;
                            }
                            filtered_preds.push_back(pred);
                        }
                        // The collect+IN pattern makes this effectively an
                        // inner join (only matching friends are kept), so use
                        // PlanRegularMatch for optimal index-based execution.
                        cur_plan = PlanRegularMatch(*mc.GetQueryGraphCollection(), cur_plan);
                        if (!filtered_preds.empty()) {
                            cur_plan = PlanSelection(filtered_preds, cur_plan);
                        }
                        continue;
                    }
                }
                cur_plan = PlanReadingClause(*rc, cur_plan);
            }
            // Projection body
            if (sq.GetQueryPart(i)->HasProjectionBody()) {
                cur_plan = PlanProjectionBody(cur_plan, *sq.GetQueryPart(i)->GetProjectionBody());
                if (sq.GetQueryPart(i)->HasProjectionBodyPredicate()) {
                    bound_expression_vector preds;
                    preds.push_back(sq.GetQueryPart(i)->GetProjectionBodyPredicateShared());
                    cur_plan = PlanSelection(preds, cur_plan);
                }
            }
        } else if (unwind_rewrite_parts.count(i)) {
            // Rewrite Part N: replace collect(distinct var) with DISTINCT var.
            // Same rewrite as collect+IN pattern.
            auto &proj = *sq.GetQueryPart(i)->GetProjectionBody();
            bound_expression_vector new_projs;
            for (auto &ep : proj.GetProjections()) {
                if (ep->GetExprType() == BoundExpressionType::AGG_FUNCTION) {
                    auto &agg = static_cast<const BoundAggFunctionExpression &>(*ep);
                    if (agg.GetFuncName() == "collect" && agg.HasChild()) {
                        new_projs.push_back(agg.GetChild()->Copy());
                        continue;
                    }
                }
                new_projs.push_back(ep);
            }
            for (idx_t j = 0; j < sq.GetQueryPart(i)->GetNumReadingClauses(); j++) {
                cur_plan = PlanReadingClause(*sq.GetQueryPart(i)->GetReadingClause(j), cur_plan);
            }
            cur_plan = PlanProjection(new_projs, cur_plan);
            CColRefArray *colrefs =
                cur_plan->getPlanExpr()->DeriveOutputColumns()->Pdrgpcr(mp_);
            cur_plan = PlanDistinct(new_projs, colrefs, cur_plan);
        } else if (i > 0 && unwind_rewrite_parts.count(i - 1)) {
            // Part N+1 after collect+UNWIND rewrite: skip UNWIND clause,
            // process remaining reading clauses normally.
            // The UNWIND alias maps to the original collect variable which is
            // now directly available via DISTINCT projection.
            const string &orig_var = unwind_collect_var[i - 1];
            const string &uw_alias = unwind_alias[i - 1];
            // Register alias mapping so the schema lookup uses orig_var name
            if (uw_alias != orig_var && cur_plan) {
                cur_plan->getSchema()->addAlias(uw_alias, orig_var);
            }
            for (idx_t j = 0; j < sq.GetQueryPart(i)->GetNumReadingClauses(); j++) {
                auto *rc = sq.GetQueryPart(i)->GetReadingClause(j);
                if (rc->GetClauseType() == BoundClauseType::UNWIND) continue; // skip UNWIND
                cur_plan = PlanReadingClause(*rc, cur_plan);
            }
            if (sq.GetQueryPart(i)->HasProjectionBody()) {
                cur_plan = PlanProjectionBody(cur_plan, *sq.GetQueryPart(i)->GetProjectionBody());
                if (sq.GetQueryPart(i)->HasProjectionBodyPredicate()) {
                    bound_expression_vector preds;
                    preds.push_back(sq.GetQueryPart(i)->GetProjectionBodyPredicateShared());
                    cur_plan = PlanSelection(preds, cur_plan);
                }
            }
        } else {
            cur_plan = PlanQueryPart(*sq.GetQueryPart(i), cur_plan);
        }
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
        if (!cur_plan)
            throw std::runtime_error("Query has no MATCH clause — cannot project without data source");
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
            return PlanUnwindClause(
                static_cast<const BoundUnwindClause &>(rc), prev_plan);
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
        plan = PlanRegularMatch(*qgc, prev_plan, predicates);
    } else {
        plan = PlanOptionalMatch(*qgc, prev_plan);
    }

    // Apply remaining WHERE predicates
    if (!predicates.empty()) {
        plan = PlanSelection(predicates, plan);
    }
    return plan;
}

// ============================================================
// UNWIND clause: expand a list expression into individual rows
// ============================================================
turbolynx::LogicalPlan *Cypher2OrcaConverter::PlanUnwindClause(
    const BoundUnwindClause &uc, turbolynx::LogicalPlan *prev_plan)
{
    CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
    const string &alias = uc.GetAlias();

    // If no prior plan (standalone UNWIND), create a single-row source
    if (!prev_plan) {
        // Build a ConstTableGet with one empty row
        CColumnDescriptorArray *pdrgpcoldesc =
            GPOS_NEW(mp_) CColumnDescriptorArray(mp_);
        IDatum2dArray *pdrgpdrgpdatum = GPOS_NEW(mp_) IDatum2dArray(mp_);
        IDatumArray *pdrgpdatum = GPOS_NEW(mp_) IDatumArray(mp_);
        pdrgpdrgpdatum->Append(pdrgpdatum);  // one empty row

        CExpression *pexprCTG = GPOS_NEW(mp_) CExpression(
            mp_, GPOS_NEW(mp_) CLogicalConstTableGet(
                mp_, pdrgpcoldesc, pdrgpdrgpdatum));

        turbolynx::LogicalSchema empty_schema;
        prev_plan = new turbolynx::LogicalPlan(pexprCTG, empty_schema);
    }

    // Convert the UNWIND expression to an ORCA scalar
    CExpression *scalar_expr = ConvertExpression(*uc.GetExpression(), prev_plan);

    // Create output column: use the scalar expression's return type.
    // For LIST types, the element type is encoded in the type modifier.
    CScalar *scalar_op = static_cast<CScalar *>(scalar_expr->Pop());

    std::wstring walias(alias.begin(), alias.end());
    const CWStringConst col_name_str(walias.c_str());
    CName col_name(&col_name_str);

    // Determine element type from the LIST type.
    // The type modifier for LIST encodes: child_type_mod(upper) | child_LogicalTypeId(lower 8 bits).
    // We use the child type OID derived from the modifier.
    IMDId *elem_mdid = scalar_op->MdidType();  // fallback: same as list
    INT elem_mod = default_type_modifier;
    INT list_mod = scalar_op->TypeModifier();
    if (list_mod > 0) {
        // Extract child type OID from type modifier
        OID list_oid = CMDIdGPDB::CastMdid(scalar_op->MdidType())->Oid();
        OID base = list_oid - ((OID)duckdb::LogicalTypeId::LIST % 256);
        OID child_type_id = list_mod & 0xFF;
        OID child_oid = base + child_type_id;
        elem_mdid = GPOS_NEW(mp_) CMDIdGPDB(IMDId::EmdidGeneral, child_oid, 1, 0);
        elem_mod = (list_mod >> 8);
    }
    CColRef *output_colref = col_factory->PcrCreate(
        GetMDAccessor()->RetrieveType(elem_mdid), elem_mod, col_name);
    if (list_mod > 0) elem_mdid->Release();

    // Wrap scalar expression in a project element + project list
    CExpression *proj_elem = GPOS_NEW(mp_) CExpression(
        mp_, GPOS_NEW(mp_) CScalarProjectElement(mp_, output_colref),
        scalar_expr);
    CExpression *proj_list = GPOS_NEW(mp_) CExpression(
        mp_, GPOS_NEW(mp_) CScalarProjectList(mp_), proj_elem);

    // Create CLogicalUnnest expression
    CExpression *unnest_expr = GPOS_NEW(mp_) CExpression(
        mp_, GPOS_NEW(mp_) CLogicalUnnest(mp_, output_colref),
        prev_plan->getPlanExpr(), proj_list);

    // Determine if the UNWIND variable should be registered as a node.
    // When collect(node_var) is unwound, each element is a node VID.
    // Check if the source expression references a node variable.
    bool is_node_unwind = false;
    const BoundExpression *unwind_expr_raw = uc.GetExpression();
    if (unwind_expr_raw->GetExprType() == BoundExpressionType::VARIABLE) {
        const string &src_var = static_cast<const BoundVariableExpression &>(
            *unwind_expr_raw).GetVarName();
        // If the source variable name matches a list alias from collect(),
        // and the list element type is ID (node VID), register as node.
        // Heuristic: if the list element type is ID/UBIGINT (node VID type),
        // treat it as a node binding. The element type is encoded in list_mod.
        if (list_mod > 0) {
            uint8_t child_type_id = list_mod & 0xFF;
            // LogicalTypeId::UBIGINT = 31, BIGINT = 14
            if (child_type_id == (uint8_t)LogicalTypeId::UBIGINT ||
                child_type_id == (uint8_t)LogicalTypeId::BIGINT) {
                is_node_unwind = true;
            }
        }
    }

    if (is_node_unwind) {
        prev_plan->getSchema()->appendNodeProperty(alias, ID_KEY_ID, output_colref);
    } else {
        prev_plan->getSchema()->appendColumn(alias, output_colref);
    }
    prev_plan->addUnaryParentOp(unnest_expr);

    return prev_plan;
}

// ============================================================
// Optional match: chain LEFT OUTER JOIN hops from prev_plan
// ============================================================
// Strategy: process each edge in the optional pattern as a LOJ hop
// from the current plan. Shared nodes (already bound in prev_plan)
// are reused; only new nodes/edges are scanned. Using LOJ ensures
// that rows from prev_plan are preserved even when the optional
// pattern doesn't match.
// ============================================================
turbolynx::LogicalPlan *Cypher2OrcaConverter::PlanOptionalMatch(
    const BoundQueryGraphCollection &qgc, turbolynx::LogicalPlan *prev_plan)
{
    D_ASSERT(prev_plan != nullptr);

    auto loj_type = gpopt::COperator::EopLogicalLeftOuterJoin;
    auto ij_type  = gpopt::COperator::EopLogicalInnerJoin;

    for (uint32_t qg_idx = 0; qg_idx < qgc.GetNumQueryGraphs(); ++qg_idx) {
        const BoundQueryGraph *qg = qgc.GetQueryGraph(qg_idx);
        const auto &rels = qg->GetQueryRels();

        if (rels.empty()) {
            // No edges: single node scan LOJ'd with prev_plan
            D_ASSERT(qg->GetQueryNodes().size() == 1);
            auto &node = qg->GetQueryNodes()[0];
            const string &name = node->GetUniqueName();
            if (!prev_plan->getSchema()->isNodeBound(name)) {
                turbolynx::LogicalPlan *node_plan = PlanNodeScan(*node);
                CExpression *cart = ExprLogicalCartProd(
                    prev_plan->getPlanExpr(), node_plan->getPlanExpr());
                prev_plan->getSchema()->appendSchema(node_plan->getSchema());
                prev_plan->addBinaryParentOp(cart, node_plan);
            }
            continue;
        }

        // ============================================================
        // Cypher OPTIONAL MATCH semantics: the entire multi-hop pattern
        // either matches or fails atomically. This is NOT a chain of
        // independent LEFT OUTER JOINs.
        //
        // Correct SQL translation:
        //   prev_plan LOJ (edge1 IJ node1 IJ edge2 IJ node2 ...)
        //
        // The inner subquery uses inner joins — if any hop fails,
        // the entire subquery returns nothing, and the LOJ preserves
        // the left row with NULLs for all optional columns.
        // ============================================================

        turbolynx::LogicalPlan *subquery = nullptr;
        string loj_anchor_name;      // bound node name in prev_plan
        uint64_t loj_anchor_edge_key = 0; // edge key to join with anchor
        string loj_anchor_edge_name; // edge name for the LOJ key
        CExpression *loj_additional_pred = nullptr; // extra pred for both-bound case

        // Edge reordering: if the first edge has no bound endpoint but a
        // later edge does (e.g. OPTIONAL MATCH (a)<-[r1]-(b)<-[r2]-(c)
        // where only c is bound), reverse the edge list so we start from
        // the bound end.  This avoids the assertion failure when neither
        // endpoint of the first edge is bound in prev_plan.
        auto ordered_rels = rels;  // copy of edge references
        if (!ordered_rels.empty()) {
            auto &first = ordered_rels[0];
            bool first_has_bound =
                prev_plan->getSchema()->isNodeBound(first->GetSrcNodeName()) ||
                prev_plan->getSchema()->isNodeBound(first->GetDstNodeName());
            if (!first_has_bound && ordered_rels.size() > 1) {
                auto &last = ordered_rels.back();
                bool last_has_bound =
                    prev_plan->getSchema()->isNodeBound(last->GetSrcNodeName()) ||
                    prev_plan->getSchema()->isNodeBound(last->GetDstNodeName());
                if (last_has_bound) {
                    std::reverse(ordered_rels.begin(), ordered_rels.end());
                }
            }
        }

        for (auto &qedge : ordered_rels) {
            const string &edge_name = qedge->GetUniqueName();
            const string &lhs_name  = qedge->GetSrcNodeName();
            const string &rhs_name  = qedge->GetDstNodeName();
            auto lhs_node = qg->GetQueryNode(lhs_name);
            auto rhs_node = qg->GetQueryNode(rhs_name);
            D_ASSERT(lhs_node && rhs_node);

            // Check binding in prev_plan and subquery separately
            bool is_lhs_bound_prev = prev_plan->getSchema()->isNodeBound(lhs_name);
            bool is_rhs_bound_prev = prev_plan->getSchema()->isNodeBound(rhs_name);
            bool is_lhs_bound_sub  = subquery && subquery->getSchema()->isNodeBound(lhs_name);
            bool is_rhs_bound_sub  = subquery && subquery->getSchema()->isNodeBound(rhs_name);

            // --- Determine edge direction (same logic as PlanRegularMatch) ---
            bool lhs_is_src = true;
            bool is_both = (qedge->GetDirection() == RelDirection::BOTH);
            if (!qedge->GetPartitionIDs().empty() && !lhs_node->GetPartitionIDs().empty()) {
                auto &catalog = context_->db->GetCatalog();
                bool any_src_only = false, any_dst_only = false, any_self_ref = false;
                for (auto ep_oid : qedge->GetPartitionIDs()) {
                    auto *ep = static_cast<PartitionCatalogEntry *>(catalog.GetEntry(
                        *context_, DEFAULT_SCHEMA, (idx_t)ep_oid));
                    if (!ep) continue;
                    idx_t stored_src = ep->GetSrcPartOid();
                    idx_t stored_dst = ep->GetDstPartOid();
                    bool m_src = false, m_dst = false;
                    for (auto lhs_pid : lhs_node->GetPartitionIDs()) {
                        if ((idx_t)lhs_pid == stored_src) m_src = true;
                        if ((idx_t)lhs_pid == stored_dst) m_dst = true;
                    }
                    if (m_src && m_dst)       any_self_ref = true;
                    else if (m_src && !m_dst) any_src_only = true;
                    else if (m_dst && !m_src) any_dst_only = true;
                }

                if (is_both && any_self_ref) {
                    lhs_is_src = true;
                    for (auto ep_oid : qedge->GetPartitionIDs()) {
                        both_edge_partitions_.insert((idx_t)ep_oid);
                    }
                } else if (is_both) {
                    if (any_src_only)      lhs_is_src = true;
                    else if (any_dst_only) lhs_is_src = false;
                    else                   lhs_is_src = true;
                } else {
                    if (any_self_ref || (any_src_only && any_dst_only))
                        lhs_is_src = (qedge->GetDirection() != RelDirection::LEFT);
                    else if (any_src_only) lhs_is_src = true;
                    else if (any_dst_only) lhs_is_src = false;
                    else lhs_is_src = (qedge->GetDirection() != RelDirection::LEFT);
                }
            }

            // MPE single-edge optimization
            bool lhs_multi = lhs_node->GetPartitionIDs().size() > 1;
            bool edge_multi = qedge->GetPartitionIDs().size() > 1;
            bool use_single_edge = edge_multi && !lhs_multi && !qedge->IsVariableLength();
            if (use_single_edge) {
                auto primary_graphlet = (idx_t)qedge->GetGraphletIDs()[0];
                auto &siblings = multi_edge_partitions_[primary_graphlet];
                for (size_t pi = 1; pi < qedge->GetPartitionIDs().size(); pi++) {
                    siblings.push_back((idx_t)qedge->GetPartitionIDs()[pi]);
                }
            }

            uint64_t lhs_edge_key = lhs_is_src ? SID_KEY_ID : TID_KEY_ID;
            uint64_t rhs_edge_key = lhs_is_src ? TID_KEY_ID : SID_KEY_ID;

            // --- Plan edge scan ---
            turbolynx::LogicalPlan *edge_plan;
            if (qedge->IsVariableLength()) {
                edge_plan = PlanPathGet(*qedge);
            } else if (use_single_edge) {
                edge_plan = PlanEdgeScanSinglePartition(*qedge, 0);
            } else {
                edge_plan = PlanEdgeScan(*qedge);
            }

            // If subsequent edge has endpoint bound only in prev_plan
            // (not in subquery), close the current subquery first to avoid
            // building a huge unfiltered join.  The closed subquery gets
            // LOJ'd with prev_plan, and this edge starts a fresh subquery
            // where both endpoints are now in prev_plan.
            if (subquery) {
                bool lhs_prev_only = is_lhs_bound_prev && !is_lhs_bound_sub;
                bool rhs_prev_only = is_rhs_bound_prev && !is_rhs_bound_sub;
                if (lhs_prev_only || rhs_prev_only) {
                    // Close current atomic subquery with LOJ
                    CExpression *loj = ExprLogicalJoin(
                        prev_plan->getPlanExpr(), subquery->getPlanExpr(),
                        prev_plan->getSchema()->getColRefOfKey(loj_anchor_name, ID_KEY_ID),
                        subquery->getSchema()->getColRefOfKey(loj_anchor_edge_name, loj_anchor_edge_key),
                        loj_type, loj_additional_pred);
                    prev_plan->getSchema()->appendSchema(subquery->getSchema());
                    prev_plan->addBinaryParentOp(loj, subquery);

                    // Reset for fresh subquery
                    subquery = nullptr;
                    loj_additional_pred = nullptr;

                    // Refresh binding (subquery nodes are now in prev_plan)
                    is_lhs_bound_prev = prev_plan->getSchema()->isNodeBound(lhs_name);
                    is_rhs_bound_prev = prev_plan->getSchema()->isNodeBound(rhs_name);
                    is_lhs_bound_sub = false;
                    is_rhs_bound_sub = false;
                }
            }

            if (!subquery) {
                // First edge: one or both endpoints are bound in prev_plan.
                // Start building the optional subquery from this edge.
                bool is_lhs_bound = is_lhs_bound_prev;
                bool is_rhs_bound = is_rhs_bound_prev;

                if (is_lhs_bound && is_rhs_bound) {
                    // Both endpoints bound in prev_plan (single-edge optional)
                    // Will be a single LOJ with compound predicate.
                    subquery = edge_plan;
                    loj_anchor_name = lhs_name;
                    loj_anchor_edge_key = lhs_edge_key;
                    loj_anchor_edge_name = edge_name;
                    // Build additional predicate for the other bound endpoint
                    loj_additional_pred = ExprScalarCmpEq(
                        GPOS_NEW(mp_) CExpression(mp_,
                            GPOS_NEW(mp_) CScalarIdent(mp_,
                                edge_plan->getSchema()->getColRefOfKey(edge_name, rhs_edge_key))),
                        GPOS_NEW(mp_) CExpression(mp_,
                            GPOS_NEW(mp_) CScalarIdent(mp_,
                                prev_plan->getSchema()->getColRefOfKey(rhs_name, ID_KEY_ID))));
                } else {
                    // One endpoint bound in prev_plan
                    D_ASSERT(is_lhs_bound || is_rhs_bound);
                    const string &bound_name = is_lhs_bound ? lhs_name : rhs_name;
                    uint64_t bound_edge_key = is_lhs_bound ? lhs_edge_key : rhs_edge_key;
                    const string &new_name = is_lhs_bound ? rhs_name : lhs_name;
                    auto &new_node_expr = is_lhs_bound ? rhs_node : lhs_node;
                    uint64_t new_edge_key = is_lhs_bound ? rhs_edge_key : lhs_edge_key;

                    // Record LOJ anchor
                    loj_anchor_name = bound_name;
                    loj_anchor_edge_key = bound_edge_key;
                    loj_anchor_edge_name = edge_name;

                    // Start subquery with edge scan
                    subquery = edge_plan;

                    // Inner join edge → new node within subquery
                    turbolynx::LogicalPlan *new_node_plan = PlanNodeScan(*new_node_expr);
                    CExpression *ij = ExprLogicalJoin(
                        subquery->getPlanExpr(), new_node_plan->getPlanExpr(),
                        subquery->getSchema()->getColRefOfKey(edge_name, new_edge_key),
                        new_node_plan->getSchema()->getColRefOfKey(new_name, ID_KEY_ID),
                        ij_type, nullptr);
                    subquery->getSchema()->appendSchema(new_node_plan->getSchema());
                    subquery->addBinaryParentOp(ij, new_node_plan);
                }
            } else {
                // Subsequent edges: inner join within the optional subquery.
                // Determine which endpoint is bound within the subquery vs prev_plan.
                // If an endpoint is bound in prev_plan (not subquery), its match
                // condition goes into the LOJ predicate, not the inner join.

                // Prefer subquery-bound endpoint for inner join
                string sub_bound_name, prev_only_name;
                uint64_t sub_bound_edge_key = 0, prev_only_edge_key = 0;
                bool has_prev_only_bound = false;

                if (is_lhs_bound_sub) {
                    sub_bound_name = lhs_name;
                    sub_bound_edge_key = lhs_edge_key;
                    if (is_rhs_bound_prev && !is_rhs_bound_sub) {
                        // rhs is bound in prev_plan only — add as LOJ pred
                        has_prev_only_bound = true;
                        prev_only_name = rhs_name;
                        prev_only_edge_key = rhs_edge_key;
                    }
                } else if (is_rhs_bound_sub) {
                    sub_bound_name = rhs_name;
                    sub_bound_edge_key = rhs_edge_key;
                    if (is_lhs_bound_prev && !is_lhs_bound_sub) {
                        has_prev_only_bound = true;
                        prev_only_name = lhs_name;
                        prev_only_edge_key = lhs_edge_key;
                    }
                } else if (is_lhs_bound_prev) {
                    // Neither in subquery, but lhs in prev_plan
                    sub_bound_name = lhs_name;
                    sub_bound_edge_key = lhs_edge_key;
                    if (is_rhs_bound_prev) {
                        has_prev_only_bound = true;
                        prev_only_name = rhs_name;
                        prev_only_edge_key = rhs_edge_key;
                    }
                } else {
                    sub_bound_name = rhs_name;
                    sub_bound_edge_key = rhs_edge_key;
                }

                // Inner join: subquery IJ edge on sub_bound._id = edge.key
                CExpression *ij1 = ExprLogicalJoin(
                    subquery->getPlanExpr(), edge_plan->getPlanExpr(),
                    subquery->getSchema()->getColRefOfKey(sub_bound_name, ID_KEY_ID),
                    edge_plan->getSchema()->getColRefOfKey(edge_name, sub_bound_edge_key),
                    ij_type, nullptr);
                subquery->getSchema()->appendSchema(edge_plan->getSchema());
                subquery->addBinaryParentOp(ij1, edge_plan);

                // If other endpoint is bound in prev_plan only, add LOJ pred
                if (has_prev_only_bound) {
                    CExpression *extra_pred = ExprScalarCmpEq(
                        GPOS_NEW(mp_) CExpression(mp_,
                            GPOS_NEW(mp_) CScalarIdent(mp_,
                                subquery->getSchema()->getColRefOfKey(edge_name, prev_only_edge_key))),
                        GPOS_NEW(mp_) CExpression(mp_,
                            GPOS_NEW(mp_) CScalarIdent(mp_,
                                prev_plan->getSchema()->getColRefOfKey(prev_only_name, ID_KEY_ID))));
                    if (loj_additional_pred) {
                        // Combine with existing additional predicate using AND
                        loj_additional_pred = GPOS_NEW(mp_) CExpression(mp_,
                            GPOS_NEW(mp_) CScalarBoolOp(mp_, CScalarBoolOp::EboolopAnd),
                            loj_additional_pred, extra_pred);
                    } else {
                        loj_additional_pred = extra_pred;
                    }
                }

                bool is_lhs_new = !is_lhs_bound_sub && !is_lhs_bound_prev;
                bool is_rhs_new = !is_rhs_bound_sub && !is_rhs_bound_prev;
                if (is_lhs_new || is_rhs_new) {
                    const string &new_name = is_lhs_new ? lhs_name : rhs_name;
                    auto &new_node_expr = is_lhs_new ? lhs_node : rhs_node;
                    uint64_t new_edge_key = is_lhs_new ? lhs_edge_key : rhs_edge_key;

                    // Inner join: subquery IJ new_node on edge.new_key = node._id
                    turbolynx::LogicalPlan *new_node_plan = PlanNodeScan(*new_node_expr);
                    CExpression *ij2 = ExprLogicalJoin(
                        subquery->getPlanExpr(), new_node_plan->getPlanExpr(),
                        subquery->getSchema()->getColRefOfKey(edge_name, new_edge_key),
                        new_node_plan->getSchema()->getColRefOfKey(new_name, ID_KEY_ID),
                        ij_type, nullptr);
                    subquery->getSchema()->appendSchema(new_node_plan->getSchema());
                    subquery->addBinaryParentOp(ij2, new_node_plan);
                }
            }
        }

        // Apply a single LOJ: prev_plan LOJ subquery
        D_ASSERT(subquery);
        CExpression *loj = ExprLogicalJoin(
            prev_plan->getPlanExpr(), subquery->getPlanExpr(),
            prev_plan->getSchema()->getColRefOfKey(loj_anchor_name, ID_KEY_ID),
            subquery->getSchema()->getColRefOfKey(loj_anchor_edge_name, loj_anchor_edge_key),
            loj_type, loj_additional_pred);
        prev_plan->getSchema()->appendSchema(subquery->getSchema());
        prev_plan->addBinaryParentOp(loj, subquery);
    }
    return prev_plan;
}

// ============================================================
// Regular match: build a join tree from query graphs
// ============================================================
turbolynx::LogicalPlan *Cypher2OrcaConverter::PlanRegularMatch(
    const BoundQueryGraphCollection &qgc, turbolynx::LogicalPlan *prev_plan,
    const bound_expression_vector &predicates)
{
    turbolynx::LogicalPlan *qg_plan = prev_plan;

    D_ASSERT(qgc.GetNumQueryGraphs() > 0);
    // Process QueryGraphs in two phases:
    // Phase 1: non-shortestPath QGs (build up joins/cartprods)
    // Phase 2: shortestPath QGs (process separately on the completed plan)
    // This avoids ORCA's CNormalizer crash when CartProd is a direct child
    // of CLogicalShortestPath in the same plan tree.
    vector<uint32_t> normal_order;
    vector<uint32_t> sp_order;
    for (uint32_t i = 0; i < qgc.GetNumQueryGraphs(); ++i) {
        auto *qg = qgc.GetQueryGraph(i);
        if (qg->GetPathType() == BoundQueryGraph::PathType::SHORTEST ||
            qg->GetPathType() == BoundQueryGraph::PathType::ALL_SHORTEST) {
            sp_order.push_back(i);
        } else {
            normal_order.push_back(i);
        }
    }


    // Phase 1: process normal QGs
    for (uint32_t qg_idx : normal_order) {
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
                // Deferred: PlanNodeScan is called below, after we know whether
                // per-partition joins will be used. PlanNodeScan sets up
                // multi_vertex_partitions_ which would incorrectly expand
                // single-partition scans in the per-partition path.
                turbolynx::LogicalPlan *lhs_plan = nullptr;
                if (is_lhs_bound) {
                    lhs_plan = qg_plan;
                }

                // Shortest-path handling
                if (qg->GetPathType() == BoundQueryGraph::PathType::SHORTEST ||
                    qg->GetPathType() == BoundQueryGraph::PathType::ALL_SHORTEST) {
                    D_ASSERT(qedge->IsVariableLength());
                    D_ASSERT(is_lhs_bound && is_rhs_bound);
                    qg_plan = PlanShortestPath(*qg, lhs_plan);
                    break;
                }

                // --- Plan edge scan (deferred for per-partition case) ---
                turbolynx::LogicalPlan *edge_plan = nullptr;

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

                    // Expand virtual vertex partition OIDs to real sub-partition OIDs
                    // so comparisons with edge stored src/dst work correctly.
                    vector<idx_t> expanded_lhs_pids;
                    for (auto lhs_pid : lhs_node->GetPartitionIDs()) {
                        auto *part = static_cast<PartitionCatalogEntry *>(
                            catalog.GetEntry(*context_, DEFAULT_SCHEMA, (idx_t)lhs_pid, true));
                        if (part && !part->sub_partition_oids.empty()) {
                            for (auto sub_oid : part->sub_partition_oids)
                                expanded_lhs_pids.push_back(sub_oid);
                        } else {
                            expanded_lhs_pids.push_back((idx_t)lhs_pid);
                        }
                    }

                    // Classify edge partitions by how LHS matches src/dst
                    bool any_src_only = false, any_dst_only = false, any_self_ref = false;
                    for (auto ep_oid : qedge->GetPartitionIDs()) {
                        auto *ep = static_cast<PartitionCatalogEntry *>(catalog.GetEntry(
                            *context_, DEFAULT_SCHEMA, (idx_t)ep_oid));
                        if (!ep) continue;
                        idx_t stored_src = ep->GetSrcPartOid();
                        idx_t stored_dst = ep->GetDstPartOid();
                        bool m_src = false, m_dst = false;
                        for (auto lhs_pid : expanded_lhs_pids) {
                            if (lhs_pid == stored_src) m_src = true;
                            if (lhs_pid == stored_dst) m_dst = true;
                        }
                        if (m_src && m_dst)       any_self_ref = true;
                        else if (m_src && !m_dst) any_src_only = true;
                        else if (m_dst && !m_src) any_dst_only = true;
                    }

                    if (is_both && any_self_ref) {
                        // Self-referential BOTH: dual-phase scan
                        lhs_is_src = true;
                        for (auto ep_oid : qedge->GetPartitionIDs()) {
                            both_edge_partitions_.insert((idx_t)ep_oid);
                        }
                    } else if (is_both) {
                        // Heterogeneous BOTH: resolve to single direction
                        if (any_src_only) {
                            lhs_is_src = true;
                        } else if (any_dst_only) {
                            lhs_is_src = false;
                        } else {
                            lhs_is_src = true;
                        }
                    } else {
                        // Directed pattern
                        if (any_self_ref || (any_src_only && any_dst_only)) {
                            lhs_is_src = (qedge->GetDirection() != RelDirection::LEFT);
                        } else if (any_src_only) {
                            lhs_is_src = true;
                        } else if (any_dst_only) {
                            lhs_is_src = false;
                        } else {
                            lhs_is_src = (qedge->GetDirection() != RelDirection::LEFT);
                        }
                    }
                }

                uint64_t lhs_edge_key = lhs_is_src ? SID_KEY_ID : TID_KEY_ID;
                uint64_t rhs_edge_key = lhs_is_src ? TID_KEY_ID : SID_KEY_ID;

                // --- A join R ---
                // When edge has multiple partitions, create per-partition (node, edge) joins
                // then union the results. This ensures ORCA generates IndexNLJoin (→ AdjIdxJoin)
                // for each pair, instead of HashJoin over two UnionAlls.
                // Works for both multi-partition LHS (Message=Comment+Post) and single-partition
                // LHS (Person) — each edge partition gets its own AdjIdxJoin.
                bool use_per_partition_join = !is_pathjoin
                    && !qedge->IsVariableLength()
                    && qedge->GetPartitionIDs().size() > 1;

                if (use_per_partition_join) {
                    // Build per-partition A→R joins.
                    auto &catalog = context_->db->GetCatalog();

                    // Expand virtual vertex partition OIDs to real sub-partition OIDs
                    // so we can match edge endpoints to real node partitions.
                    vector<idx_t> expanded_node_pids;
                    for (auto np_oid : lhs_node->GetPartitionIDs()) {
                        auto *part = static_cast<PartitionCatalogEntry *>(
                            catalog.GetEntry(*context_, DEFAULT_SCHEMA, (idx_t)np_oid, true));
                        if (part && !part->sub_partition_oids.empty()) {
                            for (auto sub_oid : part->sub_partition_oids)
                                expanded_node_pids.push_back(sub_oid);
                        } else {
                            expanded_node_pids.push_back((idx_t)np_oid);
                        }
                    }

                    // Match each edge partition to its corresponding node partition.
                    struct PartPair {
                        idx_t node_part_id;
                        idx_t edge_part_id;
                    };
                    vector<PartPair> pairs;
                    for (auto ep_oid : qedge->GetPartitionIDs()) {
                        auto *ep = static_cast<PartitionCatalogEntry *>(
                            catalog.GetEntry(*context_, DEFAULT_SCHEMA, (idx_t)ep_oid));
                        if (!ep) continue;
                        idx_t match_pid = lhs_is_src ? ep->GetSrcPartOid() : ep->GetDstPartOid();
                        for (auto np_oid : expanded_node_pids) {
                            if (np_oid == match_pid) {
                                pairs.push_back({np_oid, (idx_t)ep_oid});
                                break;
                            }
                        }
                    }
                    D_ASSERT(!pairs.empty());

                    // Build union schemas from ALL graphlets (for conforming projections).
                    // Node side: expand virtual partition graphlets to real sub-partition graphlets.
                    vector<uint64_t> all_node_graphlets;
                    for (auto np_oid : expanded_node_pids) {
                        auto *npart = static_cast<PartitionCatalogEntry *>(
                            catalog.GetEntry(*context_, DEFAULT_SCHEMA, np_oid));
                        if (!npart) continue;
                        auto *nps = npart->GetPropertySchemaIDs();
                        if (!nps) continue;
                        for (auto ps_id : *nps) {
                            auto *ps = static_cast<PropertySchemaCatalogEntry *>(
                                catalog.GetEntry(*context_, DEFAULT_SCHEMA, ps_id));
                            if (ps && !ps->is_fake)
                                all_node_graphlets.push_back((uint64_t)ps_id);
                        }
                    }
                    if (all_node_graphlets.empty()) {
                        // Fallback to original graphlets if expansion produced nothing
                        all_node_graphlets.assign(lhs_node->GetGraphletIDs().begin(),
                                                  lhs_node->GetGraphletIDs().end());
                    }
                    const auto &node_props = lhs_node->GetPropertyExpressions();
                    map<uint64_t, map<uint64_t, uint64_t>> node_mapping;
                    vector<int> node_used_col_idx;
                    BuildSchemaProjectionMapping(all_node_graphlets, node_props,
                                                 lhs_node->IsWholeNodeRequired(),
                                                 node_mapping, node_used_col_idx);

                    // Edge side:
                    vector<uint64_t> all_edge_graphlets(qedge->GetGraphletIDs().begin(),
                                                        qedge->GetGraphletIDs().end());
                    const auto &edge_props = qedge->GetPropertyExpressions();
                    map<uint64_t, map<uint64_t, uint64_t>> edge_mapping;
                    vector<int> edge_used_col_idx;
                    BuildSchemaProjectionMapping(all_edge_graphlets, edge_props,
                                                 false, edge_mapping, edge_used_col_idx);

                    // Build node union schema types (needed for conforming projections).
                    vector<pair<gpmd::IMDId *, gpos::INT>> node_union_types;
                    for (int ci : node_used_col_idx) {
                        for (auto &goid : all_node_graphlets) {
                            auto it = node_mapping[goid].find((uint64_t)ci);
                            if (it != node_mapping[goid].end() &&
                                it->second != std::numeric_limits<uint64_t>::max()) {
                                const IMDRelation *relmd = GetRelMd(goid);
                                const IMDColumn *mdcol = relmd->GetMdCol(it->second);
                                node_union_types.push_back({mdcol->MdidType(), mdcol->TypeModifier()});
                                break;
                            }
                        }
                    }

                    // Create per-partition joins and collect for UnionAll.
                    CExpressionArray *join_exprs = GPOS_NEW(mp_) CExpressionArray(mp_);
                    CColRef2dArray  *join_colrefs = GPOS_NEW(mp_) CColRef2dArray(mp_);
                    CColRefArray *first_output = nullptr;
                    turbolynx::LogicalSchema first_combined_schema;
                    vector<CColRef *> node_union_colrefs;  // filled by first partition

                    for (size_t pi = 0; pi < pairs.size(); pi++) {
                        auto &pp = pairs[pi];

                        // Get graphlet OIDs for this node partition
                        auto *npart = static_cast<PartitionCatalogEntry *>(
                            catalog.GetEntry(*context_, DEFAULT_SCHEMA, pp.node_part_id));
                        D_ASSERT(npart);
                        auto *nps = npart->GetPropertySchemaIDs();
                        vector<uint64_t> np_graphlets(nps->begin(), nps->end());

                        // Get graphlet OIDs for this edge partition
                        auto *epart = static_cast<PartitionCatalogEntry *>(
                            catalog.GetEntry(*context_, DEFAULT_SCHEMA, pp.edge_part_id));
                        D_ASSERT(epart);
                        auto *eps = epart->GetPropertySchemaIDs();
                        vector<uint64_t> ep_graphlets(eps->begin(), eps->end());

                        // Create single-partition node scan with schema-conforming projection
                        auto np_planned = ExprLogicalGetNodeOrEdge(
                            lhs_name, np_graphlets, node_used_col_idx, &node_mapping,
                            lhs_node->IsWholeNodeRequired());
                        CExpression *np_expr = np_planned.first;
                        CColRefArray *np_colrefs = np_planned.second;

                        // Create single-partition edge scan
                        auto ep_planned = ExprLogicalGetNodeOrEdge(
                            edge_name, ep_graphlets, edge_used_col_idx, &edge_mapping,
                            false);
                        CExpression *ep_expr = ep_planned.first;
                        CColRefArray *ep_colrefs = ep_planned.second;

                        // Build node schema to find join key colref
                        turbolynx::LogicalSchema np_schema;
                        GenerateNodeSchema(*lhs_node, node_used_col_idx, np_colrefs, np_schema);
                        turbolynx::LogicalSchema ep_schema;
                        GenerateEdgeSchema(*qedge, edge_used_col_idx, ep_colrefs, ep_schema);

                        // Create A→R join for this partition pair
                        CColRef *node_id_col = np_schema.getColRefOfKey(lhs_name, ID_KEY_ID);
                        CColRef *edge_key_col = ep_schema.getColRefOfKey(edge_name, lhs_edge_key);
                        CExpression *join_expr = ExprLogicalJoin(
                            np_expr, ep_expr, node_id_col, edge_key_col,
                            gpopt::COperator::EOperatorId::EopLogicalInnerJoin, nullptr);

                        // Collect output columns: node cols + edge cols
                        CColRefArray *join_output = GPOS_NEW(mp_) CColRefArray(mp_);
                        for (ULONG i = 0; i < np_colrefs->Size(); i++)
                            join_output->Append((*np_colrefs)[i]);
                        for (ULONG i = 0; i < ep_colrefs->Size(); i++)
                            join_output->Append((*ep_colrefs)[i]);

                        join_exprs->Append(join_expr);
                        join_colrefs->Append(join_output);

                        if (pi == 0) {
                            first_output = join_output;
                            first_combined_schema = np_schema;
                            first_combined_schema.appendSchema(&ep_schema);
                        }
                    }

                    // Build the result: UnionAll of all per-partition joins (or single join)
                    CExpression *ar_result;
                    if (pairs.size() == 1) {
                        ar_result = (*join_exprs)[0];
                        ar_result->AddRef();
                    } else {
                        ar_result = GPOS_NEW(mp_) CExpression(
                            mp_,
                            GPOS_NEW(mp_) CLogicalUnionAll(mp_, first_output, join_colrefs),
                            join_exprs);
                    }

                    if (is_lhs_bound) {
                        // Bound LHS with multi-edge: use single-partition edge scan
                        // so ORCA generates IndexNLJoin → AdjIdxJoin. Remaining
                        // partitions are expanded via multi_edge_partitions_ siblings.
                        if (qedge->GetPartitionIDs().size() > 1) {
                            auto primary_graphlet = (idx_t)qedge->GetGraphletIDs()[0];
                            auto &siblings = multi_edge_partitions_[primary_graphlet];
                            for (size_t pi = 1; pi < qedge->GetPartitionIDs().size(); pi++) {
                                siblings.push_back((idx_t)qedge->GetPartitionIDs()[pi]);
                            }
                        }
                        edge_plan = is_pathjoin
                            ? PlanPathGet(*qedge)
                            : (qedge->GetPartitionIDs().size() > 1
                                ? PlanEdgeScanSinglePartition(*qedge, 0)
                                : PlanEdgeScan(*qedge));
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
                    } else {
                        // Unbound LHS: the per-partition A→R result replaces lhs_plan.
                        lhs_plan = new turbolynx::LogicalPlan(ar_result, first_combined_schema);
                    }
                    // edge_plan is consumed; don't use it below

                } else {
                    // Standard single A→R join.
                    // Call PlanNodeScan here (deferred from above) so that
                    // multi_vertex_partitions_ is only set in the standard path.
                    if (!is_lhs_bound) {
                        lhs_plan = PlanNodeScan(*lhs_node);
                    }
                    // M27: Record multi-partition edge info for the planner.
                    // Use single-partition edge scan when bound so ORCA generates
                    // IndexNLJoin → AdjIdxJoin. Siblings handle remaining partitions.
                    if (qedge->GetPartitionIDs().size() > 1) {
                        auto primary_graphlet = (idx_t)qedge->GetGraphletIDs()[0];
                        auto &siblings = multi_edge_partitions_[primary_graphlet];
                        for (size_t pi = 1; pi < qedge->GetPartitionIDs().size(); pi++) {
                            siblings.push_back((idx_t)qedge->GetPartitionIDs()[pi]);
                        }
                    }
                    edge_plan = is_pathjoin
                        ? PlanPathGet(*qedge)
                        : (is_lhs_bound && qedge->GetPartitionIDs().size() > 1
                            ? PlanEdgeScanSinglePartition(*qedge, 0)
                            : PlanEdgeScan(*qedge));
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
                }

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
    // Phase 2: process shortestPath QGs on the completed plan.
    // This mirrors what happens with separate MATCH clauses — shortestPath
    // gets the fully built plan as prev_plan, not as a subtree.
    for (uint32_t sp_idx : sp_order) {
        const BoundQueryGraph *qg = qgc.GetQueryGraph(sp_idx);
        const vector<shared_ptr<BoundRelExpression>> &rels = qg->GetQueryRels();
        D_ASSERT(!rels.empty());
        auto &qedge = rels[0];
        D_ASSERT(qedge->IsVariableLength());

        const string &lhs_name = qedge->GetSrcNodeName();
        const string &rhs_name = qedge->GetDstNodeName();

        // Ensure endpoints are bound from Phase 1
        // If not bound, scan them first
        if (!qg_plan || !qg_plan->getSchema()->isNodeBound(lhs_name)) {
            auto lhs_node = qg->GetQueryNode(lhs_name);
            if (lhs_node) {
                auto *node_plan = PlanNodeScan(*lhs_node);
                if (qg_plan) {
                    CExpression *cart = ExprLogicalCartProd(
                        qg_plan->getPlanExpr(), node_plan->getPlanExpr());
                    qg_plan->getSchema()->appendSchema(node_plan->getSchema());
                    qg_plan->addBinaryParentOp(cart, node_plan);
                } else {
                    qg_plan = node_plan;
                }
            }
        }
        if (!qg_plan || !qg_plan->getSchema()->isNodeBound(rhs_name)) {
            auto rhs_node = qg->GetQueryNode(rhs_name);
            if (rhs_node && (lhs_name != rhs_name)) {  // skip if same variable (self-path)
                auto *node_plan = PlanNodeScan(*rhs_node);
                if (qg_plan) {
                    CExpression *cart = ExprLogicalCartProd(
                        qg_plan->getPlanExpr(), node_plan->getPlanExpr());
                    qg_plan->getSchema()->appendSchema(node_plan->getSchema());
                    qg_plan->addBinaryParentOp(cart, node_plan);
                } else {
                    qg_plan = node_plan;
                }
            }
        }

        // Apply predicates (inline property filters) after all endpoints are
        // scanned but BEFORE shortestPath. This ensures the id filters are applied
        // so shortestPath receives specific nodes, not the full table.
        if (!predicates.empty()) {
            qg_plan = PlanSelection(predicates, qg_plan);
        }

        qg_plan = PlanShortestPath(*qg, qg_plan);
    }

    D_ASSERT(qg_plan != nullptr);
    return qg_plan;
}

// ============================================================
// Projection body (WITH / RETURN)
// ============================================================
// Helper: check if expression is size(__list_comprehension(...))
static bool IsSizeOfListComprehension(const BoundExpression &expr) {
    if (expr.GetExprType() != BoundExpressionType::FUNCTION) return false;
    auto &fn = static_cast<const CypherBoundFunctionExpression &>(expr);
    if (fn.GetFuncName() != "list_size" || fn.GetNumChildren() != 1) return false;
    auto *child = fn.GetChild(0);
    if (child->GetExprType() != BoundExpressionType::FUNCTION) return false;
    auto &inner = static_cast<const CypherBoundFunctionExpression &>(*child);
    return inner.GetFuncName() == "__list_comprehension";
}

// Extract list comprehension components from size(__list_comprehension(source, var, filter))
struct ListCompInfo {
    const BoundExpression *source;      // source list expression
    string loop_var;                     // loop variable name
    const BoundExpression *filter;       // filter expression (may contain __pattern_exists)
    const BoundExpression *mapping;      // optional mapping expression
};

static ListCompInfo ExtractListCompInfo(const BoundExpression &expr) {
    auto &fn = static_cast<const CypherBoundFunctionExpression &>(expr);
    auto &lc = static_cast<const CypherBoundFunctionExpression &>(*fn.GetChild(0));
    ListCompInfo info;
    info.source = lc.GetChild(0);
    if (lc.GetChild(1)->GetExprType() == BoundExpressionType::LITERAL) {
        info.loop_var = static_cast<const BoundLiteralExpression &>(*lc.GetChild(1))
            .GetValue().GetValue<string>();
    }
    info.filter = lc.GetNumChildren() > 2 ? lc.GetChild(2) : nullptr;
    info.mapping = lc.GetNumChildren() > 3 ? lc.GetChild(3) : nullptr;
    return info;
}

turbolynx::LogicalPlan *Cypher2OrcaConverter::PlanProjectionBody(
    turbolynx::LogicalPlan *plan, const BoundProjectionBody &proj)
{
    // Check for list comprehension decorrelation before standard processing.
    // If size(__list_comprehension(...)) is found, decorrelate it:
    //   size([p IN posts WHERE cond]) → UNWIND posts AS p, filter, GROUP BY + count
    bool has_list_comp = false;
    for (auto &ep : proj.GetProjections()) {
        if (IsSizeOfListComprehension(*ep)) { has_list_comp = true; break; }
    }

    if (has_list_comp) {
        // Decorrelation: size([p IN posts WHERE cond]) → UNWIND + filter + GROUP BY(count)
        // Must be done BEFORE PlanProjection, because UNWIND needs the source list
        // (e.g., "posts") which would be dropped by PlanProjection.
        bound_expression_vector simple_projs;
        vector<pair<string, ListCompInfo>> lc_items;

        for (auto &ep : proj.GetProjections()) {
            if (IsSizeOfListComprehension(*ep)) {
                string alias = ep->HasAlias() ? ep->GetAlias() : ep->GetUniqueName();
                lc_items.push_back({alias, ExtractListCompInfo(*ep)});
            } else {
                simple_projs.push_back(ep);
            }
        }

        // Phase 0: Skip pre-UNWIND projections.
        // Simple function projections like size(posts) AS postCount will be
        // added as separate count aggregates in the GROUP BY (same as commonPostCount
        // but counting ALL rows instead of filtered rows).

        // Phase 1: For each list comprehension, decorrelate using existing
        // PlanUnwindClause and PlanGroupBy infrastructure.
        for (auto &[alias, info] : lc_items) {
            string source_var;
            if (info.source->GetExprType() == BoundExpressionType::VARIABLE) {
                source_var = static_cast<const BoundVariableExpression &>(*info.source).GetVarName();
            }

            // 1a: UNWIND source list AS loop_var — use PlanUnwindClause
            auto unwind_expr = make_shared<BoundVariableExpression>(
                source_var, LogicalType::LIST(LogicalType::UBIGINT), source_var);
            BoundUnwindClause virtual_unwind(unwind_expr, info.loop_var);
            plan = PlanUnwindClause(virtual_unwind, plan);

            // 1b: Compute filter as CLogicalProject boolean column.
            // NOT using selection — we need both count(all) and count(filtered).
            CColRef *filter_colref = nullptr;
            bool has_real_filter = false;
            if (info.filter) {
                bool is_literal_true = false;
                if (info.filter->GetExprType() == BoundExpressionType::LITERAL) {
                    auto &lit = static_cast<const BoundLiteralExpression &>(*info.filter);
                    if (!lit.GetValue().IsNull() && lit.GetValue().GetValue<bool>()) {
                        is_literal_true = true;
                    }
                }
                if (!is_literal_true) {
                    has_real_filter = true;
                    CColumnFactory *fcf = COptCtxt::PoctxtFromTLS()->Pcf();
                    CExpression *filter_expr = ConvertExpression(*info.filter, plan);
                    std::wstring wf(L"_filter_match");
                    const CWStringConst wfs(wf.c_str());
                    CName fn(&wfs);
                    IMDId *bool_mdid = GPOS_NEW(mp_) CMDIdGPDB(IMDId::EmdidGeneral,
                        LOGICAL_TYPE_BASE_ID + (OID)LogicalTypeId::BOOLEAN, 1, 0);
                    filter_colref = fcf->PcrCreate(
                        GetMDAccessor()->RetrieveType(bool_mdid), default_type_modifier, fn);
                    CExpression *fpe = GPOS_NEW(mp_) CExpression(
                        mp_, GPOS_NEW(mp_) CScalarProjectElement(mp_, filter_colref), filter_expr);
                    CExpression *fpl = GPOS_NEW(mp_) CExpression(
                        mp_, GPOS_NEW(mp_) CScalarProjectList(mp_), fpe);
                    CExpression *fproj = GPOS_NEW(mp_) CExpression(
                        mp_, GPOS_NEW(mp_) CLogicalProject(mp_),
                        plan->getPlanExpr(), fpl);
                    plan->getSchema()->appendColumn("_filter_match", filter_colref);
                    plan->addUnaryParentOp(fproj);
                }
            }

            // 1c: GROUP BY with TWO aggregates:
            //   commonPostCount = count(CASE WHEN filter THEN elem ELSE NULL END)
            //   postCount = count(elem)  (for each size(source) in simple_projs)
            {
                CColumnFactory *cf = COptCtxt::PoctxtFromTLS()->Pcf();
                CColRef *elem_colref = plan->getSchema()->getColRefOfKey(
                    info.loop_var, ID_KEY_ID);
                CColRef *source_colref = plan->getSchema()->getColRefOfKey(
                    source_var, std::numeric_limits<uint64_t>::max());

                // GROUP BY keys: exclude loop_var, source list, filter col, LIST-typed
                CColRefArray *output_arr = plan->getPlanExpr()->DeriveOutputColumns()->Pdrgpcr(mp_);
                CColRefArray *grp_cols = GPOS_NEW(mp_) CColRefArray(mp_);
                for (ULONG ci = 0; ci < output_arr->Size(); ci++) {
                    CColRef *cr = (*output_arr)[ci];
                    if (cr == elem_colref) continue;
                    if (source_colref && cr == source_colref) continue;
                    if (filter_colref && cr == filter_colref) continue;
                    OID cr_oid = CMDIdGPDB::CastMdid(cr->RetrieveType()->MDId())->Oid();
                    OID list_oid = LOGICAL_TYPE_BASE_ID + (OID)LogicalTypeId::LIST;
                    if (cr_oid == list_oid) continue;
                    grp_cols->Append(cr);
                }

                // Helper: lookup count aggregate metadata
                string count_name = "count";
                vector<LogicalType> count_types = {LogicalType::UBIGINT};
                idx_t count_func_id = context_->db->GetCatalogWrapper().GetAggFuncMdId(
                    *context_, count_name, count_types);
                CMDIdGPDB *count_mdid = GPOS_NEW(mp_) CMDIdGPDB(IMDId::EmdidGeneral, count_func_id, 0, 0);
                count_mdid->AddRef();
                const IMDAggregate *count_md = GetMDAccessor()->RetrieveAgg(count_mdid);
                AggregateFunctionCatalogEntry *agg_cat; idx_t agg_func_idx;
                context_->db->GetCatalogWrapper().GetAggFuncAndIdx(*context_, count_func_id, agg_cat, agg_func_idx);
                INT count_type_mod = GetTypeMod(agg_cat->functions.get()->functions[agg_func_idx].return_type);

                IMDId *bigint_mdid = GPOS_NEW(mp_) CMDIdGPDB(IMDId::EmdidGeneral,
                    LOGICAL_TYPE_BASE_ID + (OID)LogicalTypeId::BIGINT, 1, 0);
                CExpressionArray *agg_elems = GPOS_NEW(mp_) CExpressionArray(mp_);

                // Aggregate 1: commonPostCount = count(CASE WHEN filter THEN elem ELSE NULL END)
                {
                    CExpression *count_input;
                    if (has_real_filter && filter_colref) {
                        // CASE WHEN filter THEN elem ELSE NULL
                        OID elem_oid = CMDIdGPDB::CastMdid(elem_colref->RetrieveType()->MDId())->Oid();
                        CMDIdGPDB *if_mdid = GPOS_NEW(mp_) CMDIdGPDB(IMDId::EmdidGeneral, elem_oid, 1, 0);
                        CExpressionArray *if_children = GPOS_NEW(mp_) CExpressionArray(mp_);
                        if_children->Append(GPOS_NEW(mp_) CExpression(mp_, GPOS_NEW(mp_) CScalarIdent(mp_, filter_colref)));
                        if_children->Append(GPOS_NEW(mp_) CExpression(mp_, GPOS_NEW(mp_) CScalarIdent(mp_, elem_colref)));
                        // NULL constant of elem type
                        CMDIdGPDB *null_type_mdid = GPOS_NEW(mp_) CMDIdGPDB(IMDId::EmdidGeneral, elem_oid, 1, 0);
                        IDatum *null_datum = (IDatum *)(GPOS_NEW(mp_) CDatumGenericGPDB(
                            mp_, null_type_mdid, default_type_modifier, nullptr, 0, true, 0, CDouble(0.0)));
                        if_children->Append(GPOS_NEW(mp_) CExpression(mp_, GPOS_NEW(mp_) CScalarConst(mp_, null_datum)));
                        count_input = GPOS_NEW(mp_) CExpression(mp_, GPOS_NEW(mp_) CScalarIf(mp_, if_mdid), if_children);
                    } else {
                        count_input = GPOS_NEW(mp_) CExpression(mp_, GPOS_NEW(mp_) CScalarIdent(mp_, elem_colref));
                    }

                    IMDId *cc_agg_mdid = count_md->MDId(); cc_agg_mdid->AddRef();
                    CWStringConst *cc_wstr = GPOS_NEW(mp_) CWStringConst(mp_, count_md->Mdname().GetMDName()->GetBuffer());
                    CScalarAggFunc *cc_op = CUtils::PopAggFunc(mp_, cc_agg_mdid, count_type_mod, cc_wstr,
                        false, EaggfuncstageGlobal, false, nullptr, EaggfunckindNormal);

                    std::wstring walias(alias.begin(), alias.end());
                    const CWStringConst ws(walias.c_str());
                    CName nm(&ws);
                    CColRef *cc_colref = cf->PcrCreate(GetMDAccessor()->RetrieveType(bigint_mdid), default_type_modifier, nm);

                    CExpressionArray *args = GPOS_NEW(mp_) CExpressionArray(mp_);
                    args->Append(count_input);
                    CExpression *agg = GPOS_NEW(mp_) CExpression(mp_, cc_op, CUtils::PexprAggFuncArgs(mp_, args));
                    agg_elems->Append(GPOS_NEW(mp_) CExpression(mp_, GPOS_NEW(mp_) CScalarProjectElement(mp_, cc_colref), agg));
                    plan->getSchema()->appendColumn(alias, cc_colref);
                    plan->getSchema()->registerAlias(alias, cc_colref);
                }

                // Aggregate 2: postCount = count(elem) for each size(source) in simple_projs
                for (auto &sp : simple_projs) {
                    if (sp->GetExprType() != BoundExpressionType::FUNCTION) continue;
                    auto &fn = static_cast<const CypherBoundFunctionExpression &>(*sp);
                    if (fn.GetFuncName() != "list_size") continue;
                    string pc_alias = sp->HasAlias() ? sp->GetAlias() : sp->GetUniqueName();

                    IMDId *pc_agg_mdid = count_md->MDId(); pc_agg_mdid->AddRef();
                    CWStringConst *pc_wstr = GPOS_NEW(mp_) CWStringConst(mp_, count_md->Mdname().GetMDName()->GetBuffer());
                    CScalarAggFunc *pc_op = CUtils::PopAggFunc(mp_, pc_agg_mdid, count_type_mod, pc_wstr,
                        false, EaggfuncstageGlobal, false, nullptr, EaggfunckindNormal);

                    std::wstring wpc(pc_alias.begin(), pc_alias.end());
                    const CWStringConst wps(wpc.c_str());
                    CName pnm(&wps);
                    CColRef *pc_colref = cf->PcrCreate(GetMDAccessor()->RetrieveType(bigint_mdid), default_type_modifier, pnm);

                    CExpression *pc_ident = GPOS_NEW(mp_) CExpression(mp_, GPOS_NEW(mp_) CScalarIdent(mp_, elem_colref));
                    CExpressionArray *pc_args = GPOS_NEW(mp_) CExpressionArray(mp_);
                    pc_args->Append(pc_ident);
                    CExpression *pc_agg = GPOS_NEW(mp_) CExpression(mp_, pc_op, CUtils::PexprAggFuncArgs(mp_, pc_args));
                    agg_elems->Append(GPOS_NEW(mp_) CExpression(mp_, GPOS_NEW(mp_) CScalarProjectElement(mp_, pc_colref), pc_agg));
                    plan->getSchema()->appendColumn(pc_alias, pc_colref);
                    plan->getSchema()->registerAlias(pc_alias, pc_colref);
                }

                CExpression *agg_proj_list = GPOS_NEW(mp_) CExpression(
                    mp_, GPOS_NEW(mp_) CScalarProjectList(mp_), agg_elems);
                CExpression *gb_expr = GPOS_NEW(mp_) CExpression(
                    mp_, GPOS_NEW(mp_) CLogicalGbAgg(mp_, grp_cols, COperator::EgbaggtypeGlobal),
                    plan->getPlanExpr(), agg_proj_list);
                plan->addUnaryParentOp(gb_expr);
            }
        }

        // Phase 2: Project all items as variables (they're now all in schema)
        // Simple function projections (postCount) are computed in Phase 0.
        // List comp results (commonPostCount) are computed in Phase 1 GROUP BY.
        // Variable projections (friend, city) are GROUP BY keys.
        bound_expression_vector all_projs;
        for (auto &ep : simple_projs) {
            // Convert function projections to variable references (already computed)
            if (ep->GetExprType() == BoundExpressionType::FUNCTION && ep->HasAlias()) {
                auto var = make_shared<BoundVariableExpression>(
                    ep->GetAlias(), LogicalType::BIGINT, ep->GetAlias());
                all_projs.push_back(std::move(var));
            } else {
                all_projs.push_back(ep);
            }
        }
        for (auto &[alias, info] : lc_items) {
            auto var = make_shared<BoundVariableExpression>(alias, LogicalType::BIGINT, alias);
            all_projs.push_back(std::move(var));
        }
        plan = PlanProjection(all_projs, plan);

        // ORDER BY / DISTINCT / LIMIT
        if (proj.HasOrderBy()) plan = PlanOrderBy(proj.GetOrderBy(), plan);
        if (proj.IsDistinct()) {
            CColRefArray *colrefs = plan->getPlanExpr()->DeriveOutputColumns()->Pdrgpcr(mp_);
            plan = PlanDistinct(proj.GetProjections(), colrefs, plan);
        }
        if (proj.HasSkip() || proj.HasLimit()) plan = PlanSkipOrLimit(proj, plan);
        return plan;
    }

    // Aggregation
    bool agg_required = proj.HasAggregation();

    // Collect ORDER BY columns that reference properties not in RETURN.
    // These must be carried through the projection so PlanOrderBy can find them.
    bound_expression_vector augmented_projs;
    for (auto &ep : proj.GetProjections()) augmented_projs.push_back(ep);

    if (proj.HasOrderBy() && !agg_required) {
        // If ORDER BY references a property not in RETURN, add the owning
        // variable to the projection (expands all its properties).
        unordered_set<string> fully_projected;
        for (auto &ep : proj.GetProjections()) {
            if (ep->GetExprType() == BoundExpressionType::VARIABLE)
                fully_projected.insert(
                    static_cast<const BoundVariableExpression &>(*ep).GetVarName());
        }
        // Recursively find property references in ORDER BY expressions
        std::function<void(const BoundExpression &)> findProps;
        findProps = [&](const BoundExpression &e) {
            if (e.GetExprType() == BoundExpressionType::PROPERTY) {
                const auto &prop = static_cast<const BoundPropertyExpression &>(e);
                if (!fully_projected.count(prop.GetVarName())) {
                    auto var_copy = make_shared<BoundVariableExpression>(
                        prop.GetVarName(), LogicalType::ANY, prop.GetVarName());
                    augmented_projs.push_back(var_copy);
                    fully_projected.insert(prop.GetVarName());
                }
            } else if (e.GetExprType() == BoundExpressionType::FUNCTION) {
                auto &fn = static_cast<const CypherBoundFunctionExpression &>(e);
                for (idx_t i = 0; i < fn.GetNumChildren(); i++) findProps(*fn.GetChild(i));
            }
        };
        for (auto &ob : proj.GetOrderBy()) findProps(*ob.expr);
    }

    if (agg_required) {
        // Detect function-over-aggregate patterns like head(collect(x)).
        // Split into: PlanGroupBy with collect(x), then PlanProjection with head().
        bound_expression_vector agg_projs;
        bound_expression_vector post_agg_projs;
        bool has_func_over_agg = false;

        // Count how many non-agg expressions there are (GROUP BY keys)
        idx_t num_non_agg = 0;
        for (auto &ep : proj.GetProjections()) {
            if (ep->GetExprType() != BoundExpressionType::AGG_FUNCTION &&
                ep->GetExprType() != BoundExpressionType::FUNCTION) num_non_agg++;
        }

        for (auto &ep : proj.GetProjections()) {
            if (ep->GetExprType() == BoundExpressionType::FUNCTION) {
                auto &fn = static_cast<const CypherBoundFunctionExpression &>(*ep);
                // Check if any child is an aggregate
                for (idx_t ci = 0; ci < fn.GetNumChildren(); ci++) {
                    if (fn.GetChild(ci)->GetExprType() == BoundExpressionType::AGG_FUNCTION) {
                        has_func_over_agg = true;
                        // Replace with the inner aggregate in the GROUP BY step
                        auto inner = fn.GetChildShared(ci);
                        string tmp_alias = "_agg_" + (ep->HasAlias() ? ep->GetAlias() : ep->GetUniqueName());
                        inner = inner->Copy();
                        inner->SetAlias(tmp_alias);
                        agg_projs.push_back(inner);
                        // Record post-agg: apply outer function on the alias
                        bound_expression_vector new_ch;
                        for (idx_t ci2 = 0; ci2 < fn.GetNumChildren(); ci2++) {
                            if (ci2 == ci) {
                                new_ch.push_back(make_shared<BoundVariableExpression>(
                                    tmp_alias, inner->GetDataType(), tmp_alias));
                            } else {
                                new_ch.push_back(fn.GetChildShared(ci2));
                            }
                        }
                        // Resolve post-agg return type:
                        // list_extract(LIST(T), 1) → T
                        LogicalType post_ret_type = fn.GetDataType();
                        if (fn.GetFuncName() == "list_extract" && inner->GetDataType().id() == LogicalTypeId::LIST) {
                            post_ret_type = ListType::GetChildType(inner->GetDataType());
                        }
                        auto post = make_shared<CypherBoundFunctionExpression>(
                            fn.GetFuncName(), post_ret_type, std::move(new_ch),
                            ep->GetUniqueName());
                        if (ep->HasAlias()) post->SetAlias(ep->GetAlias());
                        post_agg_projs.push_back(std::move(post));
                        goto next_proj;
                    }
                }
            }
            agg_projs.push_back(ep);
            // Pass through non-func-over-agg as identity in post-agg.
            // Keep original expression so PlanProjection properly expands
            // node/edge variables with their properties.
            if (has_func_over_agg) {
                post_agg_projs.push_back(ep->Copy());
            }
            next_proj:;
        }

        plan = PlanGroupBy(agg_projs, plan);

        // Apply post-agg projection.
        // When there are GROUP BY keys (node variables etc.), use CLogicalProject
        // which passes through child columns. Without keys, use PlanProjection
        // (ProjectColumnar) which only outputs projected columns.
        if (has_func_over_agg && num_non_agg == 0) {
            // Pure aggregate — use PlanProjection (no passthrough needed)
            plan = PlanProjection(post_agg_projs, plan);
        } else if (has_func_over_agg) {
            CColumnFactory *post_cf = COptCtxt::PoctxtFromTLS()->Pcf();
            CExpressionArray *post_arr = GPOS_NEW(mp_) CExpressionArray(mp_);
            for (auto &pep : post_agg_projs) {
                // Only process func-over-agg results (skip passthrough vars)
                if (pep->GetExprType() != BoundExpressionType::FUNCTION) continue;
                CExpression *ce = ConvertExpression(*pep, plan);
                CScalar *so = static_cast<CScalar *>(ce->Pop());
                string cn = pep->HasAlias() ? pep->GetAlias() : pep->GetUniqueName();
                std::wstring wn(cn.begin(), cn.end());
                const CWStringConst ws(wn.c_str());
                CName nm(&ws);
                CColRef *ncr = post_cf->PcrCreate(
                    GetMDAccessor()->RetrieveType(so->MdidType()),
                    so->TypeModifier(), nm);
                post_arr->Append(GPOS_NEW(mp_) CExpression(
                    mp_, GPOS_NEW(mp_) CScalarProjectElement(mp_, ncr), ce));
                plan->getSchema()->appendColumn(cn, ncr);
                if (pep->HasAlias())
                    plan->getSchema()->registerAlias(pep->GetAlias(), ncr);
            }
            if (post_arr->Size() > 0) {
                CExpression *pl = GPOS_NEW(mp_) CExpression(
                    mp_, GPOS_NEW(mp_) CScalarProjectList(mp_), post_arr);
                CExpression *pe = GPOS_NEW(mp_) CExpression(
                    mp_, GPOS_NEW(mp_) CLogicalProject(mp_),
                    plan->getPlanExpr(), pl);
                plan->addUnaryParentOp(pe);
            } else {
                post_arr->Release();
            }
        }
    } else {
        plan = PlanProjection(augmented_projs, plan);
    }

    // ORDER BY
    if (proj.HasOrderBy()) {
        plan = PlanOrderBy(proj.GetOrderBy(), plan);
    }

    // DISTINCT (skip if aggregation is already present — GROUP BY handles dedup)
    if (proj.IsDistinct() && !agg_required) {
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

            // Check binding type first — scalar aliases don't have property expansion
            if (prev_plan->getSchema()->isNodeBound(var_name)) {
                auto var_colrefs = prev_plan->getSchema()->getAllColRefsOfKey(var_name);
                for (auto *colref : var_colrefs) {
                    gen_colrefs.push_back(colref);
                    uint64_t prop_key =
                        prev_plan->getSchema()->getPropertyNameOfColRef(var_name, colref);
                    gen_exprs.push_back(ExprScalarProperty(var_name, prop_key, prev_plan));
                }
                new_schema.copyNodeFrom(prev_plan->getSchema(), var_name);
            } else if (prev_plan->getSchema()->isEdgeBound(var_name)) {
                auto var_colrefs = prev_plan->getSchema()->getAllColRefsOfKey(var_name);
                for (auto *colref : var_colrefs) {
                    gen_colrefs.push_back(colref);
                    uint64_t prop_key =
                        prev_plan->getSchema()->getPropertyNameOfColRef(var_name, colref);
                    gen_exprs.push_back(ExprScalarProperty(var_name, prop_key, prev_plan));
                }
                new_schema.copyEdgeFrom(prev_plan->getSchema(), var_name);
            } else {
                // Scalar alias from a previous WITH clause (e.g., distance).
                CColRef *colref = prev_plan->getSchema()->getColRefOfKey(
                    var_name, std::numeric_limits<uint64_t>::max());
                if (colref != nullptr) {
                    gen_colrefs.push_back(colref);
                    gen_exprs.push_back(GPOS_NEW(mp_) CExpression(
                        mp_, GPOS_NEW(mp_) CScalarIdent(mp_, colref)));
                }
                string col_name = var_expr.HasAlias() ? var_expr.GetAlias() : var_name;
                for (auto *cr : gen_colrefs) {
                    new_schema.appendColumn(col_name, cr);
                }
                // Register alias for ORDER BY access
                if (var_expr.HasAlias() && !gen_colrefs.empty()) {
                    new_schema.registerAlias(var_expr.GetAlias(), gen_colrefs[0]);
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

                // For MPV sibling-only properties (NULL constants from ConvertProperty),
                // record the property key ID so the planner can add real columns.
                if (expr.GetExprType() == BoundExpressionType::PROPERTY &&
                    c_expr->Pop()->Eopid() == COperator::EopScalarConst) {
                    const BoundPropertyExpression &prop =
                        static_cast<const BoundPropertyExpression &>(expr);
                    mpv_null_colref_props_[new_colref->Id()] = {
                        prop.GetPropertyKeyID(), col_name, expr.GetDataType()};
                }
            }
            gen_exprs.push_back(c_expr);
        }

        // gen_exprs may be empty if a node variable was pruned (e.g., node used
        // only as a group-by key without property access). Skip in that case.
        if (gen_exprs.empty()) continue;
        D_ASSERT(gen_exprs.size() == gen_colrefs.size());
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

    // Post-aggregate projections for function-over-aggregate patterns

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
            } else if (prev_plan->getSchema()->isEdgeBound(var_name)) {
                new_schema.copyEdgeFrom(prev_plan->getSchema(), var_name);
            } else {
                // Scalar alias (e.g., distance from a previous WITH aggregation)
                if (prop_colrefs.empty()) {
                    CColRef *colref = prev_plan->getSchema()->getColRefOfKey(
                        var_name, std::numeric_limits<uint64_t>::max());
                    if (colref != nullptr) {
                        key_columns->Append(colref);
                        prop_colrefs.push_back(colref);
                    }
                }
                for (auto *colref : prop_colrefs) {
                    new_schema.appendColumn(var_name, colref);
                }
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
    CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
    // We may need to prepend a projection for computed ORDER BY expressions
    CExpressionArray *extra_proj_array = GPOS_NEW(mp_) CExpressionArray(mp_);
    vector<CColRef *> sort_colrefs;
    vector<CColRef *> extra_computed_colrefs;  // track temp columns to remove after sort
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
        } else if (expr.GetExprType() == BoundExpressionType::FUNCTION) {
            // Casting functions like toInteger(x) — resolve the inner column directly
            // since the cast is a no-op for sorting purposes.
            const auto &func_expr = static_cast<const CypherBoundFunctionExpression &>(expr);
            string fn_upper = func_expr.GetFuncName();
            std::transform(fn_upper.begin(), fn_upper.end(), fn_upper.begin(), ::toupper);
            bool is_cast_like = IsCastingFunction(func_expr.GetFuncName()) ||
                fn_upper == "TOINTEGER" || fn_upper == "TOFLOAT" || fn_upper == "FLOOR";
            if (is_cast_like && func_expr.GetNumChildren() == 1) {
                const auto &child = *func_expr.GetChild(0);
                if (child.GetExprType() == BoundExpressionType::PROPERTY) {
                    const auto &prop = static_cast<const BoundPropertyExpression &>(child);
                    key_colref = prev_plan->getSchema()->getColRefOfKey(
                        prop.GetVarName(), prop.GetPropertyKeyID());
                } else {
                    // Variable or alias reference
                    string child_name = child.HasAlias() ? child.GetAlias() : child.GetUniqueName();
                    key_colref = prev_plan->getSchema()->getColRefOfKey(
                        child_name, std::numeric_limits<uint64_t>::max());
                    if (!key_colref && child.GetExprType() == BoundExpressionType::VARIABLE) {
                        const auto &var = static_cast<const BoundVariableExpression &>(child);
                        key_colref = prev_plan->getSchema()->getColRefOfKey(
                            var.GetVarName(), std::numeric_limits<uint64_t>::max());
                    }
                }
            }
            if (!key_colref) {
                // Try to simplify: if expression is a cast function on a property
                // (e.g., toInteger(message.id) where message.id is already integer),
                // just use the inner property column directly for ORDER BY.
                if (expr.GetExprType() == BoundExpressionType::FUNCTION) {
                    auto &fn = static_cast<const CypherBoundFunctionExpression &>(expr);
                    if (fn.GetNumChildren() == 1 &&
                        fn.GetChild(0)->GetExprType() == BoundExpressionType::PROPERTY) {
                        auto &prop = static_cast<const BoundPropertyExpression &>(*fn.GetChild(0));
                        key_colref = prev_plan->getSchema()->getColRefOfKey(
                            prop.GetVarName(), prop.GetPropertyKeyID());
                    }
                }
            }
            if (!key_colref) {
                // General computed expression — evaluate via projection
                CExpression *c_expr = ConvertExpression(expr, prev_plan);
                CScalar *scalar_op = static_cast<CScalar *>(c_expr->Pop());
                string cname = expr.HasAlias() ? expr.GetAlias() : expr.GetUniqueName();
                std::wstring wname(cname.begin(), cname.end());
                const CWStringConst col_name_str(wname.c_str());
                CName col_cname(&col_name_str);
                CColRef *new_colref = col_factory->PcrCreate(
                    GetMDAccessor()->RetrieveType(scalar_op->MdidType()),
                    scalar_op->TypeModifier(), col_cname);
                new_colref->MarkAsUsed();
                CExpression *proj_elem = GPOS_NEW(mp_) CExpression(
                    mp_, GPOS_NEW(mp_) CScalarProjectElement(mp_, new_colref), c_expr);
                extra_proj_array->Append(proj_elem);
                prev_plan->getSchema()->appendColumn(cname, new_colref);
                extra_computed_colrefs.push_back(new_colref);
                key_colref = new_colref;
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

    // If there are computed ORDER BY expressions, add a projection.
    // Use CLogicalProject which passes through child columns automatically.
    if (extra_proj_array->Size() > 0) {
        CExpression *proj_list = GPOS_NEW(mp_)
            CExpression(mp_, GPOS_NEW(mp_) CScalarProjectList(mp_), extra_proj_array);
        CExpression *proj_expr = GPOS_NEW(mp_) CExpression(
            mp_, GPOS_NEW(mp_) CLogicalProject(mp_),
            prev_plan->getPlanExpr(), proj_list);
        prev_plan->addUnaryParentOp(proj_expr);
    } else {
        extra_proj_array->Release();
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

    // Remove temporary computed columns from schema so they don't leak
    // into subsequent WITH/RETURN stages
    for (auto *cr : extra_computed_colrefs) {
        prev_plan->getSchema()->removeColumnByColRef(cr);
    }

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
    // LIMIT 0 → empty result (ORCA can't handle count=0)
    if (proj.HasLimit() && proj.GetLimitNumber() == 0) {
        throw std::runtime_error("LIMIT 0 produces empty result");
    }

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

    // For multi-partition vertices (e.g., Message → Comment + Post), use ALL
    // graphlet OIDs from all partitions.  ORCA's UnionAll handles per-partition
    // NULL projection for missing columns (e.g., Post.imageFile absent in Comment).
    // The physical planner translates each branch independently — no separate
    // MPV expansion needed at the NodeScan level.
    graphlet_oids.assign(node.GetGraphletIDs().begin(), node.GetGraphletIDs().end());

    if (node.GetPartitionIDs().size() > 1) {
        // Record sibling partition info for IdSeek MPV expansion (NLJ inner side).
        // Key = first partition's first graphlet OID.
        auto &catalog = context_->db->GetCatalog();
        auto primary_pid = (idx_t)node.GetPartitionIDs()[0];
        auto *primary_part = static_cast<PartitionCatalogEntry *>(
            catalog.GetEntry(*context_, DEFAULT_SCHEMA, primary_pid));
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
        if (!sibling_graphlets.empty() && primary_part) {
            auto *ps_ids = primary_part->GetPropertySchemaIDs();
            if (ps_ids && !ps_ids->empty()) {
                multi_vertex_partitions_[(idx_t)(*ps_ids)[0]] = sibling_graphlets;
            }
        }
    } else if (node.GetPartitionIDs().size() == 1) {
        // Virtual unified vertex partition: single partition with sub_partition_oids
        // pointing to real sub-partitions (e.g., Message → Comment + Post).
        // ORCA sees one CLogicalGet (no UnionAll), but the physical planner needs
        // the real sub-partition PS OIDs for actual data scanning.
        auto &catalog = context_->db->GetCatalog();
        auto pid = (idx_t)node.GetPartitionIDs()[0];
        auto *part = static_cast<PartitionCatalogEntry *>(
            catalog.GetEntry(*context_, DEFAULT_SCHEMA, pid));
        if (part && !part->sub_partition_oids.empty()) {
            vector<idx_t> real_ps_oids;
            for (auto sub_part_oid : part->sub_partition_oids) {
                auto *sub_part = static_cast<PartitionCatalogEntry *>(
                    catalog.GetEntry(*context_, DEFAULT_SCHEMA, sub_part_oid));
                if (!sub_part) continue;
                PropertySchemaID_vector sub_ps_ids;
                sub_part->GetPropertySchemaIDs(sub_ps_ids);
                for (auto ps_id : sub_ps_ids) {
                    auto *ps = static_cast<PropertySchemaCatalogEntry *>(
                        catalog.GetEntry(*context_, DEFAULT_SCHEMA, ps_id));
                    if (ps && !ps->is_fake)
                        real_ps_oids.push_back((idx_t)ps_id);
                }
            }
            if (!real_ps_oids.empty()) {
                multi_vertex_partitions_[(idx_t)graphlet_oids[0]] = real_ps_oids;
            }
        }
    }
    D_ASSERT(!graphlet_oids.empty());

    const auto &prop_exprs = node.GetPropertyExpressions();

    // ── DSI: Coalesce multiple graphlets into representative groups ──
    // DSI applies ONLY within a single partition (multi-PropertySchema case, e.g. DBpedia
    // with 1304 graphlets for NODE partition). Multi-partition cases (e.g. LDBC Message →
    // Comment + Post) are already handled by the converter's MPV logic.
    std::vector<std::vector<uint64_t>> table_oids_in_groups;
    bool use_dsi = false;
#ifdef DYNAMIC_SCHEMA_INSTANTIATION
    if (graphlet_oids.size() > 1 && node.GetPartitionIDs().size() == 1) {
        vector<uint64_t> prop_key_ids;  // empty → MERGEALL grouping

        vector<idx_t> table_oids(graphlet_oids.begin(), graphlet_oids.end());
        vector<idx_t> representative_oids;
        vector<vector<uint64_t>> prop_location;
        vector<bool> has_temp_table;

        context_->db->GetCatalogWrapper().ConvertTableOidsIntoRepresentativeOids(
            *context_, prop_key_ids, table_oids, provider_,
            representative_oids, table_oids_in_groups,
            prop_location, has_temp_table);

        graphlet_oids.assign(representative_oids.begin(), representative_oids.end());
        use_dsi = true;
    }
#endif

    map<uint64_t, map<uint64_t, uint64_t>> mapping;
    vector<int> used_col_idx;
    BuildSchemaProjectionMapping(graphlet_oids, prop_exprs,
                                 node.IsWholeNodeRequired(),
                                 mapping, used_col_idx);

    auto planned = ExprLogicalGetNodeOrEdge(name, graphlet_oids,
                                            used_col_idx, &mapping,
                                            node.IsWholeNodeRequired(),
                                            use_dsi ? &table_oids_in_groups : nullptr);
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

    // ── DSI: Coalesce edge graphlets ──
    std::vector<std::vector<uint64_t>> table_oids_in_groups;
    bool use_dsi = false;
#ifdef DYNAMIC_SCHEMA_INSTANTIATION
    if (graphlet_oids.size() > 1) {
        vector<uint64_t> prop_key_ids;  // empty → MERGEALL mode
        vector<idx_t> table_oids(graphlet_oids.begin(), graphlet_oids.end());
        vector<idx_t> representative_oids;
        vector<vector<uint64_t>> prop_location;
        vector<bool> has_temp_table;
        context_->db->GetCatalogWrapper().ConvertTableOidsIntoRepresentativeOids(
            *context_, prop_key_ids, table_oids, provider_,
            representative_oids, table_oids_in_groups,
            prop_location, has_temp_table);
        graphlet_oids.assign(representative_oids.begin(), representative_oids.end());
        use_dsi = true;
    }
#endif

    map<uint64_t, map<uint64_t, uint64_t>> mapping;
    vector<int> used_col_idx;
    BuildSchemaProjectionMapping(graphlet_oids, prop_exprs,
                                 false /* whole_node_required */,
                                 mapping, used_col_idx);

    auto planned = ExprLogicalGetNodeOrEdge(name, graphlet_oids,
                                            used_col_idx, &mapping,
                                            false,
                                            use_dsi ? &table_oids_in_groups : nullptr);
    CExpression *plan_expr = planned.first;
    CColRefArray *colrefs  = planned.second;
    D_ASSERT((idx_t)used_col_idx.size() == colrefs->Size());

    turbolynx::LogicalSchema schema;
    GenerateEdgeSchema(rel, used_col_idx, colrefs, schema);

    turbolynx::LogicalPlan *plan = new turbolynx::LogicalPlan(plan_expr, schema);
    D_ASSERT(!plan->getSchema()->isEmpty());
    return plan;
}

turbolynx::LogicalPlan *Cypher2OrcaConverter::PlanEdgeScanSinglePartition(
    const BoundRelExpression &rel, size_t partition_idx)
{
    const string &name = rel.GetUniqueName();
    D_ASSERT(partition_idx < rel.GetPartitionIDs().size());

    // Get graphlet OIDs for the specified partition only.
    auto &catalog = context_->db->GetCatalog();
    auto part_oid = (idx_t)rel.GetPartitionIDs()[partition_idx];
    auto *epart = static_cast<PartitionCatalogEntry *>(
        catalog.GetEntry(*context_, DEFAULT_SCHEMA, part_oid));
    D_ASSERT(epart);
    auto *eps = epart->GetPropertySchemaIDs();
    vector<uint64_t> single_graphlets(eps->begin(), eps->end());
    D_ASSERT(!single_graphlets.empty());

    // Build mapping using ALL graphlets (for correct column index resolution),
    // but the scan expression uses only the single partition's graphlets.
    vector<uint64_t> all_graphlets(rel.GetGraphletIDs().begin(),
                                   rel.GetGraphletIDs().end());
    const auto &prop_exprs = rel.GetPropertyExpressions();

    map<uint64_t, map<uint64_t, uint64_t>> mapping;
    vector<int> used_col_idx;
    BuildSchemaProjectionMapping(all_graphlets, prop_exprs,
                                 false /* whole_node_required */,
                                 mapping, used_col_idx);

    auto planned = ExprLogicalGetNodeOrEdge(name, single_graphlets,
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
// Shortest path planner
// ============================================================
turbolynx::LogicalPlan *Cypher2OrcaConverter::PlanShortestPath(
    const BoundQueryGraph &qg, turbolynx::LogicalPlan *prev_plan)
{
    D_ASSERT(prev_plan != nullptr);
    D_ASSERT(qg.GetNumQueryRels() == 1);

    const auto &qedge = qg.GetQueryRels()[0];
    const string &lhs_name = qedge->GetSrcNodeName();
    const string &rhs_name = qedge->GetDstNodeName();
    const string &edge_name = qedge->GetUniqueName();

    // Both endpoints must already be bound
    D_ASSERT(prev_plan->getSchema()->isNodeBound(lhs_name));
    D_ASSERT(prev_plan->getSchema()->isNodeBound(rhs_name));

    CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();

    // Get src/dst _id column references from prev plan
    CColRef *lhs_col_ref = prev_plan->getSchema()->getColRefOfKey(lhs_name, ID_KEY_ID);
    CColRef *rhs_col_ref = prev_plan->getSchema()->getColRefOfKey(rhs_name, ID_KEY_ID);
    if (!lhs_col_ref || !rhs_col_ref) {
        throw std::runtime_error("ShortestPath: endpoint '" +
            (lhs_col_ref ? rhs_name : lhs_name) + "' not bound in schema");
    }
    if (lhs_name == rhs_name || lhs_col_ref == rhs_col_ref) {
        // Self-path: return prev_plan as-is (no path found)
        // Add an empty path column to satisfy downstream references
        string path_name = qg.GetPathName();
        if (path_name.empty()) path_name = "_path_self";
        prev_plan->getSchema()->appendColumn(path_name,
            prev_plan->getSchema()->getColRefOfKey(lhs_name, ID_KEY_ID));
        return prev_plan;
    }
    lhs_col_ref->MarkAsUsed();
    rhs_col_ref->MarkAsUsed();

    // Create path column ref (PATH type)
    string path_name = qg.GetPathName();
    if (path_name.empty()) path_name = "_path_" + edge_name;
    std::wstring w_path(path_name.begin(), path_name.end());
    const CWStringConst path_name_wstr(w_path.c_str());
    CName path_cname(&path_name_wstr);
    CColRef *path_col_ref = col_factory->PcrCreate(
        GetMDAccessor()->RetrieveType(&CMDIdGPDB::m_mdid_turbolynx_path),
        default_type_modifier, path_cname);
    prev_plan->getSchema()->appendColumn(path_name, path_col_ref);

    // Create projection list: [src_id, dst_id]
    CExpressionArray *ident_array = GPOS_NEW(mp_) CExpressionArray(mp_);
    ident_array->Append(GPOS_NEW(mp_) CExpression(mp_, GPOS_NEW(mp_) CScalarIdent(mp_, lhs_col_ref)));
    ident_array->Append(GPOS_NEW(mp_) CExpression(mp_, GPOS_NEW(mp_) CScalarIdent(mp_, rhs_col_ref)));
    CExpression *pexpr_value_list = GPOS_NEW(mp_) CExpression(
        mp_, GPOS_NEW(mp_) CScalarValuesList(mp_), ident_array);
    CExpression *proj_elem = GPOS_NEW(mp_) CExpression(
        mp_, GPOS_NEW(mp_) CScalarProjectElement(mp_, path_col_ref), pexpr_value_list);
    CExpressionArray *proj_array = GPOS_NEW(mp_) CExpressionArray(mp_);
    proj_array->Append(proj_elem);
    CExpression *pexprPrjList = GPOS_NEW(mp_) CExpression(
        mp_, GPOS_NEW(mp_) CScalarProjectList(mp_), proj_array);

    // Create edge table descriptors
    vector<uint64_t> graphlet_oids(qedge->GetGraphletIDs().begin(),
                                   qedge->GetGraphletIDs().end());
    CTableDescriptorArray *path_table_descs = GPOS_NEW(mp_) CTableDescriptorArray(mp_);
    path_table_descs->AddRef();
    for (auto &oid : graphlet_oids) {
        path_table_descs->Append(CreateTableDescForRel(oid, edge_name));
    }

    // Create CLogicalShortestPath name
    std::wstring w_edge(edge_name.begin(), edge_name.end());
    const CWStringConst edge_name_wstr(w_edge.c_str());

    // Build the ORCA operator
    bool is_all_shortest = (qg.GetPathType() == BoundQueryGraph::PathType::ALL_SHORTEST);
    CExpression *sp_expr;
    if (is_all_shortest) {
        sp_expr = GPOS_NEW(mp_) CExpression(mp_,
            GPOS_NEW(mp_) CLogicalAllShortestPath(mp_,
                GPOS_NEW(mp_) CName(mp_, CName(&edge_name_wstr)),
                path_table_descs, lhs_col_ref, rhs_col_ref,
                (gpos::INT)qedge->GetLowerBound(),
                (gpos::INT)qedge->GetUpperBound()),
            prev_plan->getPlanExpr(), pexprPrjList);
    } else {
        sp_expr = GPOS_NEW(mp_) CExpression(mp_,
            GPOS_NEW(mp_) CLogicalShortestPath(mp_,
                GPOS_NEW(mp_) CName(mp_, CName(&edge_name_wstr)),
                path_table_descs, lhs_col_ref, rhs_col_ref,
                (gpos::INT)qedge->GetLowerBound(),
                (gpos::INT)qedge->GetUpperBound()),
            prev_plan->getPlanExpr(), pexprPrjList);
    }

    prev_plan->addUnaryParentOp(sp_expr);
    return prev_plan;
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
                                                    bool whole_node_required,
                                                    bool is_instance,
                                                    std::vector<uint64_t> *table_oids_in_group)
{
    CTableDescriptor *ptabdesc = CreateTableDescForRel(obj_id, name);
    ptabdesc->SetInstanceDescriptor(is_instance);
    if (is_instance && table_oids_in_group) {
        for (auto grp_oid : *table_oids_in_group) {
            ptabdesc->AddTableInTheGroup(GenRelMdid(grp_oid));
        }
    }

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
    bool whole_node_required,
    std::vector<std::vector<uint64_t>> *table_oids_in_groups)
{
    // Collect union schema types from the first valid graphlet per column
    vector<pair<gpmd::IMDId *, gpos::INT>> union_schema_types;
    vector<CColRef *> union_schema_colrefs;

    for (int i = 0; i < (int)used_col_idx.size(); i++) {
        int col_idx = used_col_idx[i];
        uint64_t valid_oid = 0;
        uint64_t valid_cid = std::numeric_limits<uint64_t>::max();

        // Search primary graphlets first
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

        // If not found in provided graphlets, search ALL entries in the mapping.
        // This handles properties that exist only in other partitions' graphlets
        // (e.g., Post.imageFile when scanning Comment-only graphlets in per-partition path,
        //  or sibling-only properties in multi-vertex partition scenarios).
        if (valid_cid == std::numeric_limits<uint64_t>::max() && mapping) {
            for (auto &map_entry : *mapping) {
                // Skip graphlets we already searched
                bool already_searched = false;
                for (auto &oid : graphlet_oids) {
                    if (oid == map_entry.first) { already_searched = true; break; }
                }
                if (already_searched) continue;

                auto it = map_entry.second.find((uint64_t)col_idx);
                uint64_t idx_to_try = (it != map_entry.second.end())
                    ? it->second
                    : std::numeric_limits<uint64_t>::max();
                if (idx_to_try != std::numeric_limits<uint64_t>::max()) {
                    valid_oid = map_entry.first;
                    valid_cid = idx_to_try;
                    break;
                }
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
        bool is_instance = (table_oids_in_groups != nullptr);
        std::vector<uint64_t> *grp = is_instance ? &(*table_oids_in_groups)[idx] : nullptr;
        CExpression *expr = ExprLogicalGet(oid, name, whole_node_required, is_instance, grp);

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
        auto &child = ListType::GetChildType(type);
        if (child.id() == LogicalTypeId::LIST) {
            INT child_mod = GetTypeMod(child);
            mod = (INT)LogicalTypeId::LIST | child_mod << 8;
        } else if (child.id() == LogicalTypeId::STRUCT) {
            // Store STRUCT child in registry, use registry index as modifier
            INT reg_id = next_complex_type_id_++;
            complex_type_registry_[reg_id] = type;  // store full LIST(STRUCT(...))
            mod = reg_id;
        } else {
            mod = (INT)child.id();
        }
    } else if (type.id() == LogicalTypeId::STRUCT) {
        // STRUCT as standalone (not in LIST) — also use registry
        INT reg_id = next_complex_type_id_++;
        complex_type_registry_[reg_id] = type;
        mod = reg_id;
    }
    return mod;
}

bool Cypher2OrcaConverter::IsCastingFunction(const string &func_name)
{
    string upper = func_name;
    std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);
    // Note: tointeger/tofloat are now registered as DuckDB scalar functions
    // and go through ConvertFunction, not ConvertCastFunction.
    return (upper == "TO_DOUBLE" ||
            upper == "TO_FLOAT"  ||
            upper == "TO_INTEGER");
}

} // namespace duckdb
