#include "planner/planner.hpp"
#include "catalog/catalog_entry/partition_catalog_entry.hpp"

#include <wchar.h>
#include <algorithm>
#include <limits>
#include <numeric>
#include <set>
#include <string>

// locally used duckdb operators
#include "execution/physical_operator/physical_adjidxjoin.hpp"
#include "execution/physical_operator/physical_blockwise_nl_join.hpp"
#include "execution/physical_operator/physical_cross_product.hpp"
#include "execution/physical_operator/physical_filter.hpp"
#include "execution/physical_operator/physical_hash_aggregate.hpp"
#include "execution/physical_operator/physical_hash_join.hpp"
#include "execution/physical_operator/physical_id_seek.hpp"
#include "execution/physical_operator/physical_node_scan.hpp"
#include "execution/physical_operator/physical_piecewise_merge_join.hpp"
#include "execution/physical_operator/physical_produce_results.hpp"
#include "execution/physical_operator/physical_projection.hpp"
#include "execution/physical_operator/physical_sort.hpp"
#include "execution/physical_operator/physical_top.hpp"
#include "execution/physical_operator/physical_top_n_sort.hpp"
#include "execution/physical_operator/physical_varlen_adjidxjoin.hpp"
#include "execution/physical_operator/physical_shortestpathjoin.hpp"
#include "execution/physical_operator/physical_all_shortestpathjoin.hpp"
#include "execution/physical_operator/physical_unwind.hpp"
#include "execution/physical_operator/physical_const_scan.hpp"

#include "planner/expression/bound_between_expression.hpp"
#include "planner/expression/bound_case_expression.hpp"
#include "planner/expression/bound_cast_expression.hpp"
#include "planner/expression/bound_comparison_expression.hpp"
#include "planner/expression/bound_conjunction_expression.hpp"
#include "planner/expression/bound_constant_expression.hpp"
#include "planner/expression/bound_function_expression.hpp"
#include "planner/expression/bound_operator_expression.hpp"
#include "planner/expression/bound_parameter_expression.hpp"
#include "planner/expression/bound_reference_expression.hpp"

using namespace duckdb;

namespace turbolynx {

void Planner::pGenPhysicalPlan(CExpression *orca_plan_root)
{
    duckdb::CypherPhysicalOperator::operator_version = 0;
    pInitializeSchemaFlowGraph();
    duckdb::CypherPhysicalOperatorGroups final_pipeline_ops =
        *pTraverseTransformPhysicalPlan(orca_plan_root);

    // Append PhysicalProduceResults
    duckdb::Schema final_output_schema =
        final_pipeline_ops[final_pipeline_ops.size() - 1]->schema;
    vector<duckdb::Schema> prev_local_schemas;
    duckdb::CypherPhysicalOperator *op;

    // calculate mapping for produceresults
    vector<uint64_t> projection_mapping;
    vector<vector<uint64_t>> projection_mappings;
    // TODO strange code..
    if (!generate_sfg) {
        for (uint64_t log_idx = 0; log_idx < logical_plan_output_colrefs.size();
             log_idx++) {
            for (uint64_t phy_idx = 0;
                 phy_idx < physical_plan_output_colrefs.size(); phy_idx++) {
                if (logical_plan_output_colrefs[log_idx]->Id() ==
                    physical_plan_output_colrefs[phy_idx]->Id()) {
                    projection_mapping.push_back(phy_idx);
                }
            }
        }
        D_ASSERT(projection_mapping.size() ==
                 logical_plan_output_colrefs.size());
        op = new duckdb::PhysicalProduceResults(final_output_schema,
                                                projection_mapping);
    }
    else {
        projection_mappings.push_back(std::vector<uint64_t>());
        for (uint64_t log_idx = 0; log_idx < logical_plan_output_colrefs.size();
             log_idx++) {
            for (uint64_t phy_idx = 0;
                 phy_idx < physical_plan_output_colrefs.size(); phy_idx++) {
                if (logical_plan_output_colrefs[log_idx]->Id() ==
                    physical_plan_output_colrefs[phy_idx]->Id()) {
                    projection_mappings[0].push_back(phy_idx);
                }
            }
        }
        op = new duckdb::PhysicalProduceResults(final_output_schema,
                                                projection_mappings);
    }

    final_pipeline_ops.push_back(op);
    D_ASSERT(final_pipeline_ops.size() > 0);

    pBuildSchemaFlowGraphForUnaryOperator(final_output_schema);
    pGenerateSchemaFlowGraph(final_pipeline_ops);

    auto final_pipeline =
        new duckdb::CypherPipeline(final_pipeline_ops, pipelines.size());

    pipelines.push_back(final_pipeline);

    // validate plan
    D_ASSERT(pValidatePipelines());
    return;
}

bool Planner::pValidatePipelines()
{
    bool ok = true;
    ok = ok && pipelines.size() > 0;
    for (auto &pipeline : pipelines) {
        ok = ok && (pipeline->pipelineLength >= 2);
    }
    ok = ok && (pipelines[pipelines.size() - 1]->GetSink()->type ==
                duckdb::PhysicalOperatorType::PRODUCE_RESULTS);
    return ok;
}

duckdb::CypherPhysicalOperatorGroups *
Planner::pTraverseTransformPhysicalPlan(CExpression *plan_expr)
{

    duckdb::CypherPhysicalOperatorGroups *result = nullptr;

    /* Matching order
		- UnionAll-ComputeScalar-TableScan|IndexScan => NodeScan|NodeIndexScan
			- UnionAll-ComputeScalar-Filter-TableScan|IndexScan => NodeScan|NodeIndexScan or Filter-NodeScan|NodeIndexScan
		- Projection => Projection
		- TableScan => EdgeScan
		// TODO fillme
	*/

    // based on op pass call to the corresponding func
    D_ASSERT(plan_expr != nullptr);
    D_ASSERT(plan_expr->Pop()->FPhysical());

    switch (plan_expr->Pop()->Eopid()) {
        case COperator::EOperatorId::EopPhysicalSerialUnionAll: {
            if (pIsUnionAllOpAccessExpression(plan_expr)) {
                result = pTransformEopUnionAllForNodeOrEdgeScan(plan_expr);
            }
            else {
                result = pTransformEopUnionAll(plan_expr);
            }
            break;
        }
        case COperator::EOperatorId::EopPhysicalIndexPathJoin: {
            result = pTransformEopPhysicalInnerIndexNLJoinToVarlenAdjIdxJoin(
                plan_expr);
            break;
        }
        case COperator::EOperatorId::EopPhysicalInnerNLJoin: {
            // cartesian product only
            if (pIsCartesianProduct(plan_expr)) {
                result = pTransformEopPhysicalInnerNLJoinToCartesianProduct(
                    plan_expr);
                break;
            }
            // otherwise handle NLJ
            result = pTransformEopPhysicalNLJoinToBlockwiseNLJoin(plan_expr);
            break;
        }
        case COperator::EOperatorId::EopPhysicalCorrelatedLeftSemiNLJoin:
        case COperator::EOperatorId::EopPhysicalCorrelatedLeftAntiSemiNLJoin: {
            result =
                pTransformEopPhysicalNLJoinToBlockwiseNLJoin(plan_expr, true);
            break;
        }
        case COperator::EOperatorId::EopPhysicalInnerHashJoin:
        case COperator::EOperatorId::EopPhysicalLeftOuterHashJoin:
        case COperator::EOperatorId::EopPhysicalRightOuterHashJoin: {
            if (pIsComplexPred(plan_expr->operator[](2))) {
                result = pTransformEopPhysicalNLJoinToBlockwiseNLJoin(plan_expr, false);
            } else {
                result = pTransformEopPhysicalHashJoinToHashJoin(plan_expr);
            }
            break;
        }
        case COperator::EOperatorId::EopPhysicalLeftAntiSemiHashJoin:
        case COperator::EOperatorId::EopPhysicalLeftSemiHashJoin: {
            if (pIsComplexPred(plan_expr->operator[](2))) {
                result = pTransformEopPhysicalNLJoinToBlockwiseNLJoin(plan_expr,
                                                                      false);
            }
            else {
                result = pTransformEopPhysicalHashJoinToHashJoin(plan_expr);
            }
            break;
        }
        case COperator::EOperatorId::EopPhysicalInnerMergeJoin: {
            result = pTransformEopPhysicalMergeJoinToMergeJoin(plan_expr);
            break;
        }
        case COperator::EOperatorId::EopPhysicalLeftOuterNLJoin:       // LEFT
        case COperator::EOperatorId::EopPhysicalLeftSemiNLJoin:        // SEMI
        case COperator::EOperatorId::EopPhysicalLeftAntiSemiNLJoin: {  // ANTI
            result = pTransformEopPhysicalNLJoinToBlockwiseNLJoin(plan_expr);
            break;
        }
        case COperator::EOperatorId::EopPhysicalInnerIndexNLJoin:
        case COperator::EOperatorId::EopPhysicalLeftOuterIndexNLJoin: {
            bool is_left_outer =
                plan_expr->Pop()->Eopid() ==
                COperator::EOperatorId::EopPhysicalLeftOuterIndexNLJoin;
            CExpression *pexprInner = (*plan_expr)[1];
            if (pIsUnionAllOpAccessExpression(pexprInner)) {
                if (pIsIndexJoinOnPhysicalID(plan_expr)) {
                    result =
                        pTransformEopPhysicalInnerIndexNLJoinToIdSeekForUnionAllInner(
                            plan_expr);
                }
                else {
                    GPOS_ASSERT(false);
                    throw NotImplementedException("Schemaless adjidxjoin case");
                }
            }
            else {
                if (pIsIndexJoinOnPhysicalID(plan_expr)) {
                    result = pTransformEopPhysicalInnerIndexNLJoinToIdSeek(
                        plan_expr);
                }
                else {
                    result = pTransformEopPhysicalInnerIndexNLJoinToAdjIdxJoin(
                        plan_expr, is_left_outer);
                }
            }
            break;
        }
        // Try filter projection
        case COperator::EOperatorId::EopPhysicalFilter: {
            if (plan_expr->operator[](0)->Pop()->Eopid() ==
                COperator::EOperatorId::EopPhysicalTableScan) {
                // Filter + Scan
                auto scan_p1 = vector<COperator::EOperatorId>(
                    {COperator::EOperatorId::EopPhysicalFilter,
                     COperator::EOperatorId::EopPhysicalTableScan});
                if (pMatchExprPattern(plan_expr, scan_p1, 0, true)
                    /* && pIsFilterPushdownAbleIntoScan(plan_expr) */) {
                    result = pTransformEopTableScan(plan_expr);
                }
                else {
                    result = pTransformEopPhysicalFilter(plan_expr);
                }
            }
            else {
                result = pTransformEopPhysicalFilter(plan_expr);
            }
            break;
        }
        case COperator::EOperatorId::EopPhysicalTableScan: {
            result = pTransformEopTableScan(plan_expr);
            break;
        }
        // Unary operators (projection / compute scalar)
        case COperator::EOperatorId::EopPhysicalComputeScalar:
        case COperator::EOperatorId::EopPhysicalComputeScalarColumnar: {
            result = pTransformEopProjectionColumnar(plan_expr);
            break;
        }
        case COperator::EOperatorId::EopPhysicalLimit: {
            auto topnsort_p = vector<COperator::EOperatorId>(
                {COperator::EOperatorId::EopPhysicalLimit,
                 COperator::EOperatorId::EopPhysicalLimit,
                 COperator::EOperatorId::EopPhysicalSort});
            if (pMatchExprPattern(plan_expr, topnsort_p, 0, true)) {
                result = pTransformEopTopNSort(plan_expr);
            }
            else {
                result = pTransformEopLimit(plan_expr);
            }
            break;
        }
        case COperator::EOperatorId::EopPhysicalSort: {
            result = pTransformEopSort(plan_expr);
            break;
        }
        case COperator::EOperatorId::EopPhysicalHashAgg:
        case COperator::EOperatorId::EopPhysicalScalarAgg: {
            // PhysicalStreamAgg is not supported
            result = pTransformEopAgg(plan_expr);
            break;
        }
		case COperator::EOperatorId::EopPhysicalShortestPath: {
			result = pTransformEopShortestPath(plan_expr);
			break;
		}
		case COperator::EOperatorId::EopPhysicalAllShortestPath: {
			result = pTransformEopAllShortestPath(plan_expr);
			break;
		}
		case COperator::EOperatorId::EopPhysicalUnnest: {
			result = pTransformEopUnnest(plan_expr);
			break;
		}
		case COperator::EOperatorId::EopPhysicalConstTableGet: {
			// Constant row source — used for standalone UNWIND.
			// Extract datum values from ORCA's ConstTableGet and create a
			// PhysicalConstScan that emits those rows.
			CPhysicalConstTableGet *ctg_op =
			    CPhysicalConstTableGet::PopConvert(plan_expr->Pop());
			auto *pdrgpdrgpdatum = ctg_op->Pdrgpdrgpdatum();

			// Determine output types from ORCA output columns
			CMemoryPool *ctg_mp = this->memory_pool;
			CColRefArray *ctg_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(ctg_mp);
			duckdb::Schema ctg_schema;
			vector<duckdb::LogicalType> ctg_types;
			if (ctg_cols->Size() > 0) {
				pGetColumnsDuckDBType(ctg_cols, ctg_types);
			} else {
				// No output columns — produce one dummy row
				ctg_types.push_back(duckdb::LogicalType::INTEGER);
			}
			ctg_schema.setStoredTypes(ctg_types);

			// Build rows from datums (may be empty for single-row source)
			vector<vector<duckdb::Value>> const_rows;
			if (pdrgpdrgpdatum->Size() == 0 ||
			    (pdrgpdrgpdatum->Size() == 1 && (*pdrgpdrgpdatum)[0]->Size() == 0)) {
				// Single empty row
				const_rows.push_back(vector<duckdb::Value>(ctg_types.size()));
			}
			// TODO: extract actual datum values for non-empty ConstTableGet

			auto *op = new duckdb::PhysicalConstScan(ctg_schema, std::move(const_rows));
			result = new duckdb::CypherPhysicalOperatorGroups();
			result->push_back(op);
			pBuildSchemaFlowGraphForSingleSchemaScan(ctg_schema);
			break;
		}
        default:
            fprintf(stderr, "[PLANNER-ERR] Unhandled physical operator: %d (%s)\n",
                (int)plan_expr->Pop()->Eopid(),
                plan_expr->Pop()->SzId());
            D_ASSERT(false);
            break;
    }
    D_ASSERT(result != nullptr);

    /* Update latest plan output columns */
    auto *mp = this->memory_pool;
    CColRefArray *output_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    physical_plan_output_colrefs.clear();  // TODO strange.. multiple times?
    for (ULONG idx = 0; idx < output_cols->Size(); idx++) {
        physical_plan_output_colrefs.push_back(output_cols->operator[](idx));
    }

    return result;
}

duckdb::CypherPhysicalOperatorGroups *Planner::pTransformEopTableScan(CExpression *plan_expr) {
#ifdef DYNAMIC_SCHEMA_INSTANTIATION
    CPhysicalTableScan *scan_op = NULL;
    if (plan_expr->Pop()->Eopid() ==
        COperator::EOperatorId::EopPhysicalFilter) {
        scan_op = (CPhysicalTableScan *)plan_expr->operator[](0)->Pop();
    }
    else {
        scan_op = (CPhysicalTableScan *)plan_expr->Pop();
    }

    CTableDescriptor *tab_desc = scan_op->Ptabdesc();
    if (tab_desc->IsInstanceDescriptor()) {
        return pTransformEopDSITableScan(plan_expr);
    } else {
        return pTransformEopNormalTableScan(plan_expr);
    }
#else
    return pTransformEopNormalTableScan(plan_expr);
#endif
}

duckdb::CypherPhysicalOperatorGroups *Planner::pTransformEopNormalTableScan(CExpression* plan_expr) {
	/*
		handles
		 - F + S
		 - S
	*/
    // for orca's pushdown mechanism, refer to CTranslatorExprToDXL::PdxlnFromFilter(CExpression *pexprFilter,
    auto *mp = this->memory_pool;

    // leaf node
    auto result = new duckdb::CypherPhysicalOperatorGroups();

    CExpression *scan_expr = NULL;
    CExpression *filter_expr = NULL;
    CPhysicalTableScan *scan_op = NULL;
    CPhysicalFilter *filter_op = NULL;
    CExpression *filter_pred_expr = NULL;

    if (plan_expr->Pop()->Eopid() ==
        COperator::EOperatorId::EopPhysicalFilter) {
        filter_expr = plan_expr;
        filter_op = (CPhysicalFilter *)filter_expr->Pop();
        filter_pred_expr = filter_expr->operator[](1);
        scan_expr = filter_expr->operator[](0);
        scan_op = (CPhysicalTableScan *)scan_expr->Pop();
    }
    else {
        scan_expr = plan_expr;
        scan_op = (CPhysicalTableScan *)scan_expr->Pop();
    }

    bool do_filter_pushdown = filter_op != NULL;
    bool is_simple_filter = do_filter_pushdown;
    if (do_filter_pushdown) {
        is_simple_filter = is_simple_filter && pIsFilterPushdownAbleIntoScan(plan_expr);
    }

    CMDIdGPDB *table_mdid = CMDIdGPDB::CastMdid(scan_op->Ptabdesc()->MDId());
    OID table_obj_id = table_mdid->Oid();

    CColRefSet *output_cols =
        plan_expr->Prpp()
            ->PcrsRequired();  // columns required for the output of NodeScan
    CColRefSet *scan_cols =
        scan_expr->Prpp()
            ->PcrsRequired();  // columns required to be scanned from storage
    D_ASSERT(scan_cols->ContainsAll(
        output_cols));  // output_cols is the subset of scan_cols

    // scan projection mapping - when doing filter pushdown, two mappings MAY BE different.
    vector<vector<uint64_t>> scan_projection_mapping;
    vector<uint64_t> scan_ident_mapping;
    pGenerateScanMapping(table_obj_id, scan_cols->Pdrgpcr(mp),
                         scan_ident_mapping);
    vector<duckdb::LogicalType> scan_types;
    pGenerateTypes(scan_cols->Pdrgpcr(mp), scan_types);
    D_ASSERT(scan_ident_mapping.size() == scan_cols->Size());
    scan_projection_mapping.push_back(scan_ident_mapping);

    // oids / projection_mapping
    vector<vector<uint64_t>> output_projection_mapping;
    vector<uint64_t> output_to_original_table_mapping;
    vector<uint64_t> output_to_scanned_table_mapping;
    pGenerateScanMapping(table_obj_id, output_cols->Pdrgpcr(mp),
                         output_to_original_table_mapping);
    D_ASSERT(output_to_original_table_mapping.size() == output_cols->Size());

    for (auto i = 0; i < output_to_original_table_mapping.size(); i++) {
        auto col_id = output_to_original_table_mapping[i];
        auto it = std::find(scan_ident_mapping.begin(),
                            scan_ident_mapping.end(), col_id);
        D_ASSERT(it != scan_ident_mapping.end());
        output_to_scanned_table_mapping.push_back(
            std::distance(scan_ident_mapping.begin(), it));
    }

    output_projection_mapping.push_back(output_to_scanned_table_mapping);
    vector<duckdb::LogicalType> types;
    vector<string> out_col_names;
    pGenerateTypes(output_cols->Pdrgpcr(mp), types);
    pGenerateColumnNames(output_cols->Pdrgpcr(mp), out_col_names);

    gpos::ULONG pred_attr_pos;
    duckdb::Value literal_val;
    if (do_filter_pushdown && is_simple_filter) {
        pGetFilterAttrPosAndValue(filter_pred_expr, pred_attr_pos, literal_val);
    }

    // oids
    vector<uint64_t> oids;
    oids.push_back(table_obj_id);

    // Check for multi-partition vertex (MPV) expansion
    auto vp_it = multi_vertex_partitions.find(table_obj_id);
    bool is_mpv = (vp_it != multi_vertex_partitions.end() && !vp_it->second.empty());

    duckdb::Schema tmp_schema;
    tmp_schema.setStoredTypes(types);
    tmp_schema.setStoredColumnNames(out_col_names);
    duckdb::CypherPhysicalOperator *op = nullptr;

    if (is_mpv) {
        // Multi-partition vertex: expand to schemaless (DSI-like) scan.
        // Build per-partition scan mappings by matching property key IDs.
        duckdb::Catalog &cat_instance = context->db->GetCatalog();
        auto *primary_ps = static_cast<duckdb::PropertySchemaCatalogEntry *>(
            cat_instance.GetEntry(*context, DEFAULT_SCHEMA, (duckdb::idx_t)table_obj_id));
        duckdb::PropertyKeyID_vector *primary_keys = primary_ps->GetKeyIDs();

        // Get source MD relation for property key resolution
        CMDIdGPDB src_mdid(IMDId::EmdidRel, table_obj_id, 0, 0);
        const IMDRelation *src_rel = lGetMDAccessor()->RetrieveRel(&src_mdid);

        // Map scanned columns to property names (using MD relation).
        // NOTE: We match by property NAME, not key_id/PropId, because different
        // partitions may assign different key_ids to the same property name.
        vector<string> scanned_prop_names;
        for (size_t sci = 0; sci < scan_ident_mapping.size(); sci++) {
            uint64_t md_pos = scan_ident_mapping[sci];
            if (md_pos < src_rel->ColumnCount()) {
                const IMDColumn *md_col = src_rel->GetMdCol(md_pos);
                // Convert wide string name to narrow string
                const WCHAR *wbuf = md_col->Mdname().GetMDName()->GetBuffer();
                string narrow_name;
                for (ULONG ci = 0; wbuf[ci] != L'\0'; ci++)
                    narrow_name += (char)wbuf[ci];
                scanned_prop_names.push_back(narrow_name);
            } else {
                scanned_prop_names.push_back("");
            }
        }

        // Build local schemas, scan mappings, and filter params for all partitions
        vector<duckdb::Schema> local_schemas;
        vector<vector<duckdb::LogicalType>> all_scan_types;
        vector<int64_t> filter_key_idxs;
        vector<duckdb::Value> filter_values;

        // Detect virtual partition: primary has sub_partition_oids (no real data).
        // If virtual, replace oids[0] with first real sub-partition and rebuild
        // its scan mapping using column name matching.
        size_t sibling_start_idx = 0;
        {
            auto *primary_part = static_cast<duckdb::PartitionCatalogEntry *>(
                cat_instance.GetEntry(*context, DEFAULT_SCHEMA, primary_ps->partition_oid));
            if (primary_part && !primary_part->sub_partition_oids.empty()
                && !vp_it->second.empty()) {
                // Virtual partition: replace with first real sub-partition
                oids[0] = vp_it->second[0];
                sibling_start_idx = 1;

                auto *first_ps = static_cast<duckdb::PropertySchemaCatalogEntry *>(
                    cat_instance.GetEntry(*context, DEFAULT_SCHEMA,
                                          (duckdb::idx_t)vp_it->second[0]));
                auto first_key_names = first_ps ? first_ps->GetKeysWithCopy()
                                                : vector<string>{};
                std::unordered_map<string, duckdb::idx_t> first_name_pos;
                for (duckdb::idx_t k = 0; k < first_key_names.size(); k++)
                    first_name_pos[first_key_names[k]] = k;

                // Rebuild scan_projection_mapping[0] for first real sub-partition
                scan_projection_mapping[0].clear();
                vector<duckdb::LogicalType> first_types;
                for (size_t sci = 0; sci < scanned_prop_names.size(); sci++) {
                    if (scan_types[sci] == duckdb::LogicalType::ID) {
                        scan_projection_mapping[0].push_back(0);
                        first_types.push_back(duckdb::LogicalType::ID);
                        continue;
                    }
                    auto &name = scanned_prop_names[sci];
                    if (!name.empty()) {
                        auto nit = first_name_pos.find(name);
                        if (nit != first_name_pos.end()) {
                            scan_projection_mapping[0].push_back(nit->second + 1);
                            first_types.push_back(scan_types[sci]);
                        } else {
                            scan_projection_mapping[0].push_back(
                                std::numeric_limits<uint64_t>::max());
                            first_types.push_back(duckdb::LogicalType::SQLNULL);
                        }
                    } else {
                        scan_projection_mapping[0].push_back(
                            std::numeric_limits<uint64_t>::max());
                        first_types.push_back(duckdb::LogicalType::SQLNULL);
                    }
                }
                local_schemas.push_back(duckdb::Schema());
                local_schemas.back().setStoredTypes(first_types);
                all_scan_types.push_back(first_types);

                // Filter pushdown for first real sub-partition
                if (do_filter_pushdown && is_simple_filter) {
                    // Find filter column by name in first real sub-partition
                    CColRefTable *filter_colref = (CColRefTable *)(
                        COptCtxt::PoctxtFromTLS()->Pcf()->LookupColRef(
                            ((CScalarIdent *)filter_pred_expr->operator[](0)->Pop())
                                ->Pcr()->Id()));
                    ULONG filter_src_pos = src_rel->GetPosFromAttno(
                        filter_colref->AttrNum());
                    const IMDColumn *filter_md_col = src_rel->GetMdCol(filter_src_pos);
                    const WCHAR *wbuf = filter_md_col->Mdname().GetMDName()->GetBuffer();
                    string filter_prop_name;
                    for (ULONG ci = 0; wbuf[ci] != L'\0'; ci++)
                        filter_prop_name += (char)wbuf[ci];
                    auto fit = first_name_pos.find(filter_prop_name);
                    if (fit != first_name_pos.end()) {
                        filter_key_idxs.push_back((int64_t)(fit->second + 1));
                    } else {
                        filter_key_idxs.push_back(-1);
                    }
                    filter_values.push_back(literal_val);
                }
            } else {
                // Real primary partition (old-style MPV or non-virtual)
                local_schemas.push_back(duckdb::Schema());
                local_schemas.back().setStoredTypes(scan_types);
                all_scan_types.push_back(scan_types);

                if (do_filter_pushdown && is_simple_filter) {
                    filter_key_idxs.push_back(pred_attr_pos);
                    filter_values.push_back(literal_val);
                }
            }
        }

        // Sibling partitions
        for (size_t si = sibling_start_idx; si < vp_it->second.size(); si++) {
            auto sib_oid = vp_it->second[si];
            oids.push_back(sib_oid);

            auto *sib_ps2 = static_cast<duckdb::PropertySchemaCatalogEntry *>(
                cat_instance.GetEntry(*context, DEFAULT_SCHEMA, sib_oid));
            auto sib_key_names2 = sib_ps2 ? sib_ps2->GetKeysWithCopy() : vector<string>{};

            // Build property name → column position map for sibling
            std::unordered_map<string, duckdb::idx_t> sib_name_pos2;
            for (duckdb::idx_t k = 0; k < sib_key_names2.size(); k++)
                sib_name_pos2[sib_key_names2[k]] = k;

            // Build sibling scan projection mapping and types.
            // NOTE: ExtentIterator expects 1-based attr_no convention
            // (0 = _id, 1+ = property columns) and subtracts target_idxs_offset=1
            // when indexing into extent_cat_entry->chunks[].
            // GetKeysWithCopy() returns 0-based key positions, so we add +1
            // to convert to the attr_no convention.
            vector<uint64_t> sib_scan_mapping;
            vector<duckdb::LogicalType> sib_types;
            for (size_t sci = 0; sci < scanned_prop_names.size(); sci++) {
                // _id (physical VID) is not a stored property — handle like primary
                if (scan_types[sci] == duckdb::LogicalType::ID) {
                    sib_scan_mapping.push_back(0);  // attr_no=0 for _id
                    sib_types.push_back(duckdb::LogicalType::ID);
                    continue;
                }
                auto &name = scanned_prop_names[sci];
                if (!name.empty()) {
                    auto nit = sib_name_pos2.find(name);
                    if (nit != sib_name_pos2.end()) {
                        sib_scan_mapping.push_back(nit->second + 1);  // +1: 0-based key pos → 1-based attr_no
                        sib_types.push_back(scan_types[sci]);  // use same logical type
                    } else {
                        // Column missing in sibling — use max as sentinel for NULL
                        sib_scan_mapping.push_back(std::numeric_limits<uint64_t>::max());
                        sib_types.push_back(duckdb::LogicalType::SQLNULL);
                    }
                } else {
                    sib_scan_mapping.push_back(std::numeric_limits<uint64_t>::max());
                    sib_types.push_back(duckdb::LogicalType::SQLNULL);
                }
            }

            scan_projection_mapping.push_back(sib_scan_mapping);
            output_projection_mapping.push_back(output_to_scanned_table_mapping);
            local_schemas.push_back(duckdb::Schema());
            local_schemas.back().setStoredTypes(sib_types);
            all_scan_types.push_back(sib_types);

            // Sibling filter params
            if (do_filter_pushdown && is_simple_filter) {
                // Find the filter column's property name from the primary MD relation
                CColRefTable *filter_colref = (CColRefTable *)(
                    COptCtxt::PoctxtFromTLS()->Pcf()->LookupColRef(
                        ((CScalarIdent *)filter_pred_expr->operator[](0)->Pop())->Pcr()->Id()));
                ULONG filter_src_pos = src_rel->GetPosFromAttno(filter_colref->AttrNum());
                const IMDColumn *filter_md_col = src_rel->GetMdCol(filter_src_pos);
                const WCHAR *wbuf = filter_md_col->Mdname().GetMDName()->GetBuffer();
                string filter_prop_name;
                for (ULONG ci = 0; wbuf[ci] != L'\0'; ci++)
                    filter_prop_name += (char)wbuf[ci];
                auto fit = sib_name_pos2.find(filter_prop_name);
                if (fit != sib_name_pos2.end()) {
                    filter_key_idxs.push_back((int64_t)(fit->second + 1));  // +1: 0-based key pos → 1-based attr_no
                } else {
                    filter_key_idxs.push_back(-1);  // column not in sibling
                }
                filter_values.push_back(literal_val);
            }
        }

        // Add sibling-only columns to scan output for MPV null properties
        // (e.g., imageFile exists on Post but not Comment).
        if (!mpv_null_colref_props.empty()) {
            // Collect all sibling name → position maps
            std::vector<std::unordered_map<string, duckdb::idx_t>> all_sib_name_pos;
            for (size_t si = 0; si < vp_it->second.size(); si++) {
                auto sib_oid = vp_it->second[si];
                auto *sib_ps3 = static_cast<duckdb::PropertySchemaCatalogEntry *>(
                    cat_instance.GetEntry(*context, DEFAULT_SCHEMA, sib_oid));
                auto sib_names3 = sib_ps3 ? sib_ps3->GetKeysWithCopy() : vector<string>{};
                std::unordered_map<string, duckdb::idx_t> spmap;
                for (duckdb::idx_t k = 0; k < sib_names3.size(); k++)
                    spmap[sib_names3[k]] = k;
                all_sib_name_pos.push_back(spmap);
            }

            for (auto &[colref_id, info] : mpv_null_colref_props) {
                const string &prop_name = info.prop_name;
                // Skip if already scanned
                if (std::find(scanned_prop_names.begin(), scanned_prop_names.end(),
                              prop_name) != scanned_prop_names.end())
                    continue;

                // Check if property exists in any sibling
                bool found_in_sibling = false;
                for (auto &spmap : all_sib_name_pos) {
                    if (spmap.find(prop_name) != spmap.end()) {
                        found_in_sibling = true;
                        break;
                    }
                }
                if (!found_in_sibling) continue;

                // Add to scan output
                size_t new_scan_idx = scan_projection_mapping[0].size();
                size_t new_output_idx = types.size();

                // Primary partition: NULL (max sentinel)
                scan_projection_mapping[0].push_back(std::numeric_limits<uint64_t>::max());
                local_schemas[0].stored_types.push_back(duckdb::LogicalType::SQLNULL);

                // Sibling partitions
                for (size_t si = 0; si < vp_it->second.size(); si++) {
                    auto &spmap = all_sib_name_pos[si];
                    auto kit = spmap.find(prop_name);
                    if (kit != spmap.end()) {
                        scan_projection_mapping[si + 1].push_back(kit->second);
                        local_schemas[si + 1].stored_types.push_back(info.type);
                    } else {
                        scan_projection_mapping[si + 1].push_back(
                            std::numeric_limits<uint64_t>::max());
                        local_schemas[si + 1].stored_types.push_back(
                            duckdb::LogicalType::SQLNULL);
                    }
                }

                // Update output mapping for all partitions
                for (auto &opm : output_projection_mapping) {
                    opm.push_back(new_scan_idx);
                }

                // Update output types and names
                types.push_back(info.type);
                out_col_names.push_back("_mpv_extra_" + prop_name);

                // Record mapping for pTransformEopProjectionColumnar
                mpv_colref_to_scan_idx_[colref_id] = new_output_idx;

                scanned_prop_names.push_back(prop_name);
            }

            // Rebuild tmp_schema with updated types (must create fresh Schema
            // because setStoredTypes appends without clearing).
            tmp_schema = duckdb::Schema();
            tmp_schema.setStoredTypes(types);
            tmp_schema.setStoredColumnNames(out_col_names);
        }

        // Create schemaless PhysicalNodeScan
        if (!do_filter_pushdown) {
            op = new duckdb::PhysicalNodeScan(local_schemas, tmp_schema, oids,
                                              output_projection_mapping,
                                              scan_projection_mapping);
        } else if (is_simple_filter) {
            if (((CScalarCmp *)(filter_pred_expr->Pop()))->ParseCmpType() ==
                IMDType::ECmpType::EcmptEq) {
                op = new duckdb::PhysicalNodeScan(local_schemas, tmp_schema, oids,
                                                  output_projection_mapping,
                                                  scan_projection_mapping,
                                                  filter_key_idxs, filter_values);
            } else {
                // Range filters: build per-partition range filter values
                vector<duckdb::RangeFilterValue> range_values;
                auto cmp_type = ((CScalarCmp *)(filter_pred_expr->Pop()))->ParseCmpType();
                for (size_t i = 0; i < oids.size(); i++) {
                    duckdb::Value l_val, r_val;
                    bool l_inc = true, r_inc = true;
                    if (cmp_type == IMDType::ECmpType::EcmptL) {
                        l_val = duckdb::Value::MinimumValue(literal_val.type());
                        r_val = literal_val; l_inc = true; r_inc = false;
                    } else if (cmp_type == IMDType::ECmpType::EcmptLEq) {
                        l_val = duckdb::Value::MinimumValue(literal_val.type());
                        r_val = literal_val; l_inc = true; r_inc = true;
                    } else if (cmp_type == IMDType::ECmpType::EcmptG) {
                        l_val = literal_val;
                        r_val = duckdb::Value::MaximumValue(literal_val.type());
                        l_inc = false; r_inc = true;
                    } else if (cmp_type == IMDType::ECmpType::EcmptGEq) {
                        l_val = literal_val;
                        r_val = duckdb::Value::MaximumValue(literal_val.type());
                        l_inc = true; r_inc = true;
                    }
                    range_values.push_back({l_val, r_val, l_inc, r_inc});
                }
                op = new duckdb::PhysicalNodeScan(local_schemas, tmp_schema, oids,
                                                  output_projection_mapping,
                                                  scan_projection_mapping,
                                                  filter_key_idxs, range_values);
            }
        } else {
            // Complex filter — fall back to single-schema (no MPV expansion for now)
            vector<unique_ptr<duckdb::Expression>> filter_exprs;
            filter_exprs.push_back(
                std::move(pTransformScalarExpr(filter_pred_expr, scan_cols->Pdrgpcr(mp), nullptr)));
            op = new duckdb::PhysicalNodeScan(local_schemas, tmp_schema, oids,
                                              output_projection_mapping,
                                              scan_projection_mapping, move(filter_exprs));
        }
    } else {
        // Single-partition: original code path
        if (!do_filter_pushdown) {
            op = new duckdb::PhysicalNodeScan(tmp_schema, oids,
                                              output_projection_mapping,
                                              scan_types,
                                              scan_projection_mapping);
        }
        else {
            if (is_simple_filter) {
                if (((CScalarCmp *)(filter_pred_expr->Pop()))->ParseCmpType() ==
                    IMDType::ECmpType::EcmptEq) {
                    op = new duckdb::PhysicalNodeScan(
                        tmp_schema, oids, output_projection_mapping, scan_types,
                        scan_projection_mapping, pred_attr_pos, literal_val);
                }
                else if (((CScalarCmp *)(filter_pred_expr->Pop()))->ParseCmpType() ==
                        IMDType::ECmpType::EcmptL) {
                    op = new duckdb::PhysicalNodeScan(
                        tmp_schema, oids, output_projection_mapping, scan_types,
                        scan_projection_mapping, pred_attr_pos,
                        duckdb::Value::MinimumValue(literal_val.type()), literal_val,
                        true, false);
                }
                else if (((CScalarCmp *)(filter_pred_expr->Pop()))->ParseCmpType() ==
                        IMDType::ECmpType::EcmptLEq) {
                    op = new duckdb::PhysicalNodeScan(
                        tmp_schema, oids, output_projection_mapping, scan_types,
                        scan_projection_mapping, pred_attr_pos,
                        duckdb::Value::MinimumValue(literal_val.type()), literal_val,
                        true, true);
                }
                else if (((CScalarCmp *)(filter_pred_expr->Pop()))->ParseCmpType() ==
                        IMDType::ECmpType::EcmptG) {
                    op = new duckdb::PhysicalNodeScan(
                        tmp_schema, oids, output_projection_mapping, scan_types,
                        scan_projection_mapping, pred_attr_pos, literal_val,
                        duckdb::Value::MaximumValue(literal_val.type()), false, true);
                }
                else if (((CScalarCmp *)(filter_pred_expr->Pop()))->ParseCmpType() ==
                        IMDType::ECmpType::EcmptGEq) {
                    op = new duckdb::PhysicalNodeScan(
                        tmp_schema, oids, output_projection_mapping, scan_types,
                        scan_projection_mapping, pred_attr_pos, literal_val,
                        duckdb::Value::MaximumValue(literal_val.type()), true, true);
                }
                else {
                    D_ASSERT(false);
                }
            }
            else {
                vector<unique_ptr<duckdb::Expression>> filter_exprs;
                filter_exprs.push_back(
                    std::move(pTransformScalarExpr(filter_pred_expr, scan_cols->Pdrgpcr(mp), nullptr)));
                op = new duckdb::PhysicalNodeScan(
                    tmp_schema, oids, output_projection_mapping, scan_types,
                    scan_projection_mapping, move(filter_exprs));
            }
        }
    }

    pBuildSchemaFlowGraphForSingleSchemaScan(tmp_schema);

    D_ASSERT(op != nullptr);

    // Set human-readable display name from catalog (graphlet → partition → name)
    op->display_name = pResolvePartitionName(table_obj_id);

    result->push_back(op);

    return result;
}

duckdb::CypherPhysicalOperatorGroups *Planner::pTransformEopDSITableScan(CExpression *plan_expr) {
    // expand TableScan -> UnionAll - TableScan

    auto *mp = this->memory_pool;
    duckdb::Catalog &cat_instance = context->db->GetCatalog();
    auto result = new duckdb::CypherPhysicalOperatorGroups();

    // variables for scan op
    vector<uint64_t> oids;
    duckdb::Schema global_schema;
    vector<duckdb::Schema> local_schemas;
    vector<duckdb::LogicalType> global_types;
    vector<vector<uint64_t>> projection_mappings;
    vector<vector<uint64_t>> scan_projection_mappings;
    vector<duckdb::idx_t> scan_cols_id;

    // variables for filter op
    bool is_filter_exist = false;
    bool is_filter_only_column_exist = false;
    bool is_simple_filter;
    vector<ULONG> unionall_output_original_col_ids;
    vector<duckdb::idx_t> bound_ref_idxs;

    // variables for expression
    CExpression *scan_expr;
    CPhysicalTableScan *scan_op = NULL;
    CExpression *filter_expr = NULL;
    CPhysicalFilter *filter_op = NULL;
    CExpression *filter_pred_expr = NULL;
    vector<int64_t> pred_attr_pos_vec;
    vector<duckdb::Value> literal_val_vec;
    vector<duckdb::RangeFilterValue> range_filter_values;

    // get table descriptor
    if (plan_expr->Pop()->Eopid() ==
        COperator::EOperatorId::EopPhysicalFilter) {
        scan_expr = plan_expr->operator[](0);

        is_filter_exist = true;
        filter_expr = plan_expr;
        filter_op = (CPhysicalFilter *)filter_expr->Pop();
        filter_pred_expr = filter_expr->operator[](1);
    }
    else {
        scan_expr = plan_expr;
    }
    scan_op = (CPhysicalTableScan *)scan_expr->Pop();

    CTableDescriptor *tab_desc = scan_op->Ptabdesc();
    D_ASSERT(tab_desc->IsInstanceDescriptor());

    if (is_filter_exist) {
        is_simple_filter = pIsFilterPushdownAbleIntoScan(filter_expr);
    }

    CColRefArray *output_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CColRefArray *scan_cols = scan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);

    duckdb::GraphCatalogEntry *graph_cat =
        (duckdb::GraphCatalogEntry *)cat_instance.GetEntry(
            *context, duckdb::CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA,
            DEFAULT_GRAPH);

    pGetColumnsDuckDBType(scan_cols, global_types, scan_cols_id);
    
    // generate scan for each oid
    IMdIdArray *mdid_array = tab_desc->GetTableIdsInGroup();
    local_schemas.resize(mdid_array->Size());
    pred_attr_pos_vec.resize(mdid_array->Size());
    literal_val_vec.resize(mdid_array->Size());
    for (int i = 0; i < mdid_array->Size(); i++) {
        CMDIdGPDB *table_mdid = CMDIdGPDB::CastMdid((*mdid_array)[i]);
        OID table_obj_id = table_mdid->Oid();
        oids.push_back(table_obj_id);

        projection_mappings.push_back(vector<uint64_t>());
        scan_projection_mappings.push_back(vector<uint64_t>());

        vector<duckdb::LogicalType> local_types;

        duckdb::PropertySchemaCatalogEntry *ps_cat =
            (duckdb::PropertySchemaCatalogEntry *)cat_instance.GetEntry(
                *context, DEFAULT_SCHEMA, table_obj_id);
        duckdb::PropertyKeyID_vector *key_ids = ps_cat->GetKeyIDs();

        pGenerateMappingInfo(scan_cols_id, key_ids, global_types, local_types,
                             projection_mappings.back(),
                             scan_projection_mappings.back());

        local_schemas[i].setStoredTypes(local_types);

        if (is_filter_exist && is_simple_filter) {
            gpos::ULONG pred_attr_pos;
            duckdb::Value literal_val;

            pGetFilterAttrPosAndValue(
                filter_pred_expr,
                (*mdid_array)[i],
                pred_attr_pos,
                literal_val);

            pred_attr_pos_vec[i] = pred_attr_pos;
            literal_val_vec[i] = move(literal_val);
        }
    }

    global_schema.setStoredTypes(global_types);

    duckdb::CypherPhysicalOperator *op = nullptr;

    if (is_filter_exist) {
        if (is_simple_filter) {
            if (((CScalarCmp *)(filter_pred_expr->Pop()))->ParseCmpType() ==
                IMDType::ECmpType::EcmptEq) {
                op = new duckdb::PhysicalNodeScan(
                    local_schemas, global_schema, oids, projection_mappings,
                    scan_projection_mappings, pred_attr_pos_vec,
                    literal_val_vec);
            }
            else if (((CScalarCmp *)(filter_pred_expr->Pop()))
                         ->ParseCmpType() == IMDType::ECmpType::EcmptL) {
                for (int i = 0; i < literal_val_vec.size(); i++) {
                    range_filter_values.push_back(
                        {duckdb::Value::MinimumValue(literal_val_vec[i].type()),
                         literal_val_vec[i], true, false});
                }
                op = new duckdb::PhysicalNodeScan(
                    local_schemas, global_schema, oids, projection_mappings,
                    scan_projection_mappings, pred_attr_pos_vec,
                    range_filter_values);
            }
            else if (((CScalarCmp *)(filter_pred_expr->Pop()))
                         ->ParseCmpType() == IMDType::ECmpType::EcmptLEq) {
                for (int i = 0; i < literal_val_vec.size(); i++) {
                    range_filter_values.push_back(
                        {duckdb::Value::MinimumValue(literal_val_vec[i].type()),
                         literal_val_vec[i], true, true});
                }
                op = new duckdb::PhysicalNodeScan(
                    local_schemas, global_schema, oids, projection_mappings,
                    scan_projection_mappings, pred_attr_pos_vec,
                    range_filter_values);
            }
            else if (((CScalarCmp *)(filter_pred_expr->Pop()))
                         ->ParseCmpType() == IMDType::ECmpType::EcmptG) {
                for (int i = 0; i < literal_val_vec.size(); i++) {
                    range_filter_values.push_back(
                        {literal_val_vec[i],
                         duckdb::Value::MaximumValue(literal_val_vec[i].type()),
                         false, true});
                }
                op = new duckdb::PhysicalNodeScan(
                    local_schemas, global_schema, oids, projection_mappings,
                    scan_projection_mappings, pred_attr_pos_vec,
                    range_filter_values);
            }
            else if (((CScalarCmp *)(filter_pred_expr->Pop()))
                         ->ParseCmpType() == IMDType::ECmpType::EcmptGEq) {
                for (int i = 0; i < literal_val_vec.size(); i++) {
                    range_filter_values.push_back(
                        {literal_val_vec[i],
                         duckdb::Value::MaximumValue(literal_val_vec[i].type()),
                         true, true});
                }
                op = new duckdb::PhysicalNodeScan(
                    local_schemas, global_schema, oids, projection_mappings,
                    scan_projection_mappings, pred_attr_pos_vec,
                    range_filter_values);
            }
            else {
                D_ASSERT(false);
            }
        }
        else {
            vector<unique_ptr<duckdb::Expression>> filter_exprs;
            filter_exprs.push_back(
                std::move(pTransformScalarExpr(filter_pred_expr, scan_cols, nullptr)));
            op = new duckdb::PhysicalNodeScan(
                local_schemas, global_schema, oids, projection_mappings,
                scan_projection_mappings, move(filter_exprs));
        }
    } else {
        op = new duckdb::PhysicalNodeScan(
            local_schemas, global_schema, oids, projection_mappings,
            scan_projection_mappings);
    }

    pBuildSchemaFlowGraphForMultiSchemaScan(global_schema, local_schemas);

    D_ASSERT(op != nullptr);

    // Set display name from first graphlet OID
    if (!oids.empty()) {
        op->display_name = pResolvePartitionName(oids[0]);
    }

    result->push_back(op);

    return result;
}

duckdb::CypherPhysicalOperatorGroups *
Planner::pTransformEopUnionAllForNodeOrEdgeScan(CExpression *plan_expr)
{
    // constants
    const int REPR_IDX = 0;

    // Initialize memory pool pointer
    auto *mp = this->memory_pool;
    D_ASSERT(plan_expr->Pop()->Eopid() ==
             COperator::EOperatorId::EopPhysicalSerialUnionAll);

    // Result containers for processing projections and schemas
    auto result = new duckdb::CypherPhysicalOperatorGroups();
    vector<uint64_t> oids;
    vector<vector<uint64_t>> projection_mapping;
    vector<vector<uint64_t>> scan_projection_mapping;

    duckdb::Schema global_schema;
    vector<duckdb::Schema> local_schemas;
    vector<duckdb::LogicalType> global_types;

    // Retrieve projection expressions
    CExpressionArray *projections = plan_expr->PdrgPexpr();
    const ULONG num_projections = projections->Size();

    // Construct params for each node scan
    for (int i = 0; i < num_projections; i++) {
        CExpression *projection_expr = projections->operator[](i);
        if (i == REPR_IDX)
            global_types.resize(projection_expr->PdrgPexpr()
                                    ->operator[](1)
                                    ->PdrgPexpr()
                                    ->Size(),
                                duckdb::LogicalType::SQLNULL);

        pConstructNodeScanParams(projection_expr, oids, projection_mapping,
                                 scan_projection_mapping, global_types,
                                 local_schemas);
    }
    D_ASSERT(oids.size() == projection_mapping.size());
    D_ASSERT(projection_mapping.size() == scan_projection_mapping.size());

    global_schema.setStoredTypes(global_types);
    pBuildSchemaFlowGraphForMultiSchemaScan(global_schema, local_schemas);

    // Handle filter expression, if exists
    CExpression *repr_proj_expr = projections->operator[](REPR_IDX);
    CExpression *repr_filter_expr = pFindFilterExpr(repr_proj_expr);
    bool has_filter = repr_filter_expr != NULL;
    if (has_filter) {
        /**
         * Strong assumption: filter predicates are only on non-schemaless columns
         * Binder will not include tables without the columns used by filter predicates.
         * Therefore, representative projection should have all valid column infos.
         * 
         * However, if we need to process IS NULL predicate,
         * we may need to handle schemaless columns.
        */

        vector<ULONG> output_original_colref_ids;
        vector<duckdb::idx_t> non_filter_only_column_idxs;
        bool has_filter_only_column = pConstructColumnInfosRegardingFilter(
            repr_proj_expr, output_original_colref_ids,
            non_filter_only_column_idxs);

        if (pIsFilterPushdownAbleIntoScan(repr_filter_expr)) {
            vector<int64_t> pred_attr_poss;
            vector<duckdb::Value> literal_vals;

            for (int i = 0; i < num_projections; i++) {
                CExpression *projection_expr = projections->operator[](i);
                pConstructFilterColPosVals(projection_expr,
                                           pred_attr_poss, literal_vals);
            }

            D_ASSERT(pred_attr_poss.size() == literal_vals.size() && literal_vals.size() == projection_mapping.size());
            D_ASSERT(literal_vals[0].type().id() != duckdb::LogicalTypeId::VARCHAR);

            /* add expression type for pushdown */
            duckdb::CypherPhysicalOperator *op = nullptr;
            vector<duckdb::RangeFilterValue> range_filter_values;
            auto cmp_type =
                ((CScalarCmp *)(repr_filter_expr->operator[](1)->Pop()))
                    ->ParseCmpType();
            auto num_vals = literal_vals.size();
            switch (cmp_type) {
                case IMDType::ECmpType::EcmptEq:
                    op = new duckdb::PhysicalNodeScan(
                        local_schemas, global_schema, oids, projection_mapping,
                        scan_projection_mapping, pred_attr_poss, literal_vals);
                    break;
                case IMDType::ECmpType::EcmptL:
                    for (int i = 0; i < num_vals; i++)
                        range_filter_values.push_back(
                            {duckdb::Value::MinimumValue(
                                 literal_vals[i].type()),
                             literal_vals[i], true, false});
                    op = new duckdb::PhysicalNodeScan(
                        local_schemas, global_schema, oids, projection_mapping,
                        scan_projection_mapping, pred_attr_poss,
                        range_filter_values);
                    break;
                case IMDType::ECmpType::EcmptLEq:
                    for (int i = 0; i < num_vals; i++)
                        range_filter_values.push_back(
                            {duckdb::Value::MinimumValue(
                                 literal_vals[i].type()),
                             literal_vals[i], true, true});
                    op = new duckdb::PhysicalNodeScan(
                        local_schemas, global_schema, oids, projection_mapping,
                        scan_projection_mapping, pred_attr_poss,
                        range_filter_values);
                    break;
                case IMDType::ECmpType::EcmptG:
                    for (int i = 0; i < num_vals; i++)
                        range_filter_values.push_back(
                            {literal_vals[i],
                             duckdb::Value::MaximumValue(
                                 literal_vals[i].type()),
                             false, true});
                    op = new duckdb::PhysicalNodeScan(
                        local_schemas, global_schema, oids, projection_mapping,
                        scan_projection_mapping, pred_attr_poss,
                        range_filter_values);
                    break;
                case IMDType::ECmpType::EcmptGEq:
                    for (int i = 0; i < num_vals; i++)
                        range_filter_values.push_back(
                            {literal_vals[i],
                             duckdb::Value::MaximumValue(
                                 literal_vals[i].type()),
                             true, true});
                    op = new duckdb::PhysicalNodeScan(
                        local_schemas, global_schema, oids, projection_mapping,
                        scan_projection_mapping, pred_attr_poss,
                        range_filter_values);
                    break;
                default:
                    D_ASSERT(false);
                    break;
            }
            if (!oids.empty()) op->display_name = pResolvePartitionName(oids[0]);
            result->push_back(op);
        }
        else {
            // Generate filter exprs
            vector<unique_ptr<duckdb::Expression>> filter_exprs;
            CExpression *filter_pred_expr = repr_filter_expr->operator[](1);
            CColRefArray *repr_scan_cols = repr_filter_expr->PdrgPexpr()
                                               ->operator[](0)
                                               ->Prpp()
                                               ->PcrsRequired()
                                               ->Pdrgpcr(mp);
            auto repr_filter_expr =
                pTransformScalarExpr(filter_pred_expr, repr_scan_cols, nullptr);
            pConvertLocalFilterExprToUnionAllFilterExpr(
                repr_filter_expr, repr_scan_cols, output_original_colref_ids);
            filter_exprs.push_back(std::move(repr_filter_expr));

            duckdb::CypherPhysicalOperator *scan_cypher_op =
                new duckdb::PhysicalNodeScan(
                    local_schemas, global_schema, oids, projection_mapping,
                    scan_projection_mapping, move(filter_exprs));
            if (!oids.empty()) scan_cypher_op->display_name = pResolvePartitionName(oids[0]);
            result->push_back(scan_cypher_op);
        }

        /* If we don't need filter-only-used column anymore, create projection operator */
        /**
         * CPhyiscalFilter's required output columns have filter-only-used columns.
         * However, CPhysicalComputeScalarC's required output columns does not have filter-only-used columns.
         * This is because CPhyiscalFilter thinks that CPhysicalComputeScalarC uses those columns.
         * 
         * Therefore, to create projection operator to remove filter-only-used columns, 
         * we need to compare CPhyiscalFilter's required output columns and CPhysicalComputeScalarC's required output columns.
         * 
         * Fortunately, we made NodeScan to output columns in order of CPhysicalComputeScalarC's projected columns.
         * And, the output columns of CPhyiscalFilter is as same as the NodeScan's output columns.
         * Therefore, to solve this problem, we just check each of projected column is in the required output columns of
         * CPhysicalComputeScalarC.
         * 
         * See non_filter_only_column_idxs for code level implementation
         */
        CColRefArray *unionall_output_cols =
            plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
        if (has_filter_only_column) {
            // Obtain union schema
            duckdb::Schema proj_op_output_union_schema;
            vector<duckdb::LogicalType> proj_op_output_types;
            pGetColumnsDuckDBType(unionall_output_cols, proj_op_output_types);
            proj_op_output_union_schema.setStoredTypes(proj_op_output_types);

            // Create projection exprs
            vector<unique_ptr<duckdb::Expression>> proj_exprs;
            pGetProjectionExprs(proj_op_output_types, non_filter_only_column_idxs,
                                proj_exprs);

            // Create projection operator
            D_ASSERT(proj_exprs.size() != 0);
            D_ASSERT(proj_exprs.size() == unionall_output_cols->Size());
            duckdb::CypherPhysicalOperator *proj_op =
                new duckdb::PhysicalProjection(proj_op_output_union_schema,
                                               std::move(proj_exprs));
            result->push_back(proj_op);
            pBuildSchemaFlowGraphForUnaryOperator(proj_op_output_union_schema);
        }
    }
    else {
        duckdb::CypherPhysicalOperator *op = new duckdb::PhysicalNodeScan(
            local_schemas, global_schema, oids, projection_mapping,
            scan_projection_mapping);
        if (!oids.empty()) op->display_name = pResolvePartitionName(oids[0]);
        result->push_back(op);
    }

    return result;
}

duckdb::CypherPhysicalOperatorGroups *
Planner::pTransformEopUnionAll(CExpression *plan_expr)
{
    auto *mp = this->memory_pool;

    duckdb::CypherPhysicalOperatorGroups *result = new duckdb::CypherPhysicalOperatorGroups();
    duckdb::CypherPhysicalOperatorGroup *union_group = new duckdb::CypherPhysicalOperatorGroup();

    CExpressionArray *childs = plan_expr->PdrgPexpr();
    const ULONG num_childs = childs->Size();

    for (int i = 0; i < num_childs; i++) {
        generate_sfg = (i == 0);
        restrict_generate_sfg_for_unionall = (i != 0);
        CExpression *child_expr = childs->operator[](i);
        auto child_result = pTraverseTransformPhysicalPlan(child_expr);
        union_group->PushBack(child_result->GetGroups());
    }
    generate_sfg = true;
    restrict_generate_sfg_for_unionall = false;

    result->push_back(union_group);
    return result;
}

void Planner::pConstructNodeScanParams(
    CExpression *projection_expr, vector<uint64_t> &oids,
    vector<vector<uint64_t>> &projection_mapping,
    vector<vector<uint64_t>> &scan_projection_mapping,
    vector<duckdb::LogicalType> &global_types,
    vector<duckdb::Schema> &local_schemas)
{
    auto *mp = this->memory_pool;
    CExpression *proj_list_expr = projection_expr->PdrgPexpr()->operator[](1);
    CColRefArray *proj_output_cols =
        projection_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);

    // Obtain scan and filter expression
    CExpression *filter_expr = pFindFilterExpr(projection_expr);
    bool has_filter = filter_expr != NULL;
    CExpression *scan_expr = has_filter
                                 ? filter_expr->operator[](0)
                                 : projection_expr->PdrgPexpr()->operator[](0);

    // Obtain scan operator for table descriptor
    CPhysicalTableScan *scan_op = (CPhysicalTableScan *)scan_expr->Pop();
    CTableDescriptor *tab_desc = scan_op->Ptabdesc();
    bool is_dsi_table = tab_desc->IsInstanceDescriptor();

    // Pattern to find
    auto scalarident_pattern = vector<COperator::EOperatorId>(
        {COperator::EOperatorId::EopScalarProjectElement,
            COperator::EOperatorId::EopScalarIdent});

    if (!is_dsi_table) {
        // allocate outputs
        projection_mapping.emplace_back();
        scan_projection_mapping.emplace_back();
        local_schemas.emplace_back();
        vector<duckdb::LogicalType> local_types;

        // collect object ids
        OID table_obj_id = pGetTableOidFromScanExpr(scan_expr);
        oids.push_back(table_obj_id);

        // Construct outputs
        for (int i = 0; i < proj_list_expr->PdrgPexpr()->Size(); i++) {
            CExpression *proj_elem_expr = proj_list_expr->PdrgPexpr()->operator[](i);
            if (pMatchExprPattern(proj_elem_expr, scalarident_pattern)) {
                /* CScalarProjectList - CScalarIdent */
                CScalarIdent *ident_op = (CScalarIdent *)proj_elem_expr->PdrgPexpr()
                                             ->operator[](0)
                                             ->Pop();
                projection_mapping.back().push_back(i);
                scan_projection_mapping.back().push_back(
                    pGetColIdxFromTable(table_obj_id, ident_op->Pcr()));
                auto duckdb_type = pGetColumnsDuckDBType(ident_op->Pcr());
                local_types.push_back(duckdb_type);
                if (global_types[i] == duckdb::LogicalType::SQLNULL) {
                    global_types[i] = duckdb_type;
                }
            }
            else {
                /* CScalarProjectList - CScalarConst (null) */
                projection_mapping.back().push_back(i);
                scan_projection_mapping.back().push_back(
                    std::numeric_limits<uint64_t>::max());
                local_types.push_back(duckdb::LogicalTypeId::SQLNULL);
            }
        }

        local_schemas.back().setStoredTypes(local_types);
    }
    else {
        IMdIdArray *mdid_array = tab_desc->GetTableIdsInGroup();
        vector<duckdb::LogicalType> local_global_types;
        vector<duckdb::idx_t> col_prop_ids;

        // Abstract DSI tables as a single table, consturct column infos
        for (int i = 0; i < proj_list_expr->PdrgPexpr()->Size(); i++) {
            CExpression *proj_elem_expr = proj_list_expr->PdrgPexpr()->operator[](i);
            if (pMatchExprPattern(proj_elem_expr, scalarident_pattern)) {
                CScalarIdent *ident_op = (CScalarIdent *)proj_elem_expr->PdrgPexpr()
                                             ->operator[](0)
                                             ->Pop();
                auto duckdb_type = pGetColumnsDuckDBType(ident_op->Pcr());
                local_global_types.push_back(duckdb_type);
                col_prop_ids.push_back(ident_op->Pcr()->PropId());
                if (global_types[i] == duckdb::LogicalType::SQLNULL) {
                    global_types[i] = duckdb_type;
                }
            }
            else {
                local_global_types.push_back(duckdb::LogicalTypeId::SQLNULL);
                col_prop_ids.push_back(std::numeric_limits<uint64_t>::max());
            }
        }

        // For each DSI table, construct outputs
        for (int i = 0; i < mdid_array->Size(); i++) {
            projection_mapping.emplace_back();
            scan_projection_mapping.emplace_back();
            local_schemas.emplace_back();
            vector<duckdb::LogicalType> local_local_types;

            CMDIdGPDB *table_mdid = CMDIdGPDB::CastMdid((*mdid_array)[i]);
            OID table_obj_id = table_mdid->Oid();
            oids.push_back(table_obj_id);

            duckdb::Catalog &cat_instance = context->db->GetCatalog();
            duckdb::PropertySchemaCatalogEntry *ps_cat =
                (duckdb::PropertySchemaCatalogEntry *)cat_instance.GetEntry(
                    *context, DEFAULT_SCHEMA, table_obj_id);
            duckdb::PropertyKeyID_vector *key_ids = ps_cat->GetPropKeyIDs();

            pGenerateMappingInfo(col_prop_ids, key_ids, local_global_types,
                                 local_local_types, projection_mapping.back(),
                                 scan_projection_mapping.back());

            local_schemas.back().setStoredTypes(local_local_types);
        }
    }
}

duckdb::CypherPhysicalOperatorGroups *
Planner::pTransformEopPhysicalInnerIndexNLJoinToAdjIdxJoin(
    CExpression *plan_expr, bool is_left_outer)
{
    /**
     * If edge proeprty in output, create IdSeek operator.
     * In this case, AdjIdxJoin only output non-edge property columns, including edge ID column.
     * Then, IdSeek will output edge property columns, removing edge ID column, if necessary.
    */

    CMemoryPool *mp = this->memory_pool;
    duckdb::CypherPhysicalOperatorGroups *result =
        pTraverseTransformPhysicalPlan(plan_expr->PdrgPexpr()->operator[](0));

    // ORCA data structures
    CColRefArray *output_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CColRefSet *output_colset = plan_expr->Prpp()->PcrsRequired();
    CColRefArray *outer_cols =
        (*plan_expr)[0]->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CColRefSet *outer_colset =
        (*plan_expr)[0]->Prpp()->PcrsRequired();
    CColRefArray *inner_cols =
        (*plan_expr)[1]->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CColRefSet *inner_colset =
        (*plan_expr)[1]->Prpp()->PcrsRequired();
    CColRefArray *adj_inner_cols = GPOS_NEW(mp) CColRefArray(mp);
    CColRefSet *adj_inner_colset = GPOS_NEW(mp) CColRefSet(mp);
    CColRefArray *adj_output_cols = GPOS_NEW(mp) CColRefArray(mp);
    CColRefArray *seek_inner_cols = GPOS_NEW(mp) CColRefArray(mp);
    CColRefSet *seek_inner_colset = GPOS_NEW(mp) CColRefSet(mp);
    CColRefArray *seek_output_cols = output_cols;
    CColRefArray *idxscan_pred_cols = GPOS_NEW(mp) CColRefArray(mp);
    CColRefArray *idxscan_cols = NULL;
    CColRefSet *idxscan_colset = NULL;
    CColRefSet *adj_output_colset = GPOS_NEW(mp) CColRefSet(mp);
    CColRefSet *adj_input_colset = GPOS_NEW(mp) CColRefSet(mp);
    CColRef *edge_physical_id_col;
    CExpression *filter_expr = NULL;

    // DuckDB data structures
    size_t num_outer_schemas = pGetNumOuterSchemas();
    duckdb::idx_t outer_join_key_col_idx;
    duckdb::idx_t tgt_key_col_idx;
    duckdb::idx_t edge_id_col_idx;
    uint64_t adjidx_obj_id;
    vector<uint64_t> seek_obj_ids;
    vector<vector<uint32_t>> inner_col_maps_adj(1);
    vector<vector<uint32_t>> outer_col_maps_adj(1);
    vector<vector<uint32_t>> inner_col_maps_seek(1);
    vector<uint32_t> outer_col_maps_seek;
    vector<duckdb::LogicalType> output_types_adj;
    vector<duckdb::LogicalType> output_types_seek;
    vector<vector<uint64_t>> scan_projection_mappings_seek(1);
    vector<vector<uint64_t>> output_projection_mappings_seek(1);
    vector<vector<duckdb::LogicalType>> scan_types_seek(1);
    unique_ptr<duckdb::Expression> filter_duckdb_expr;
    vector<vector<duckdb::idx_t>> filter_col_idxs(1);
    duckdb::Schema schema_adj;
    duckdb::Schema schema_seek;
    duckdb::Schema schema_proj;
    size_t ID_COL_SIZE = 1;

    // Flags
    bool is_edge_prop_in_output = pIsPropertyInCols(inner_cols);
    bool is_filter_exist = pIsFilterExist(plan_expr->operator[](1));
    bool is_adjidxjoin_into = false;

    // Calculate join key columns index
    auto idxscan_expr = pFindIndexScanExpr(plan_expr->operator[](1));
    D_ASSERT(idxscan_expr != NULL);
    idxscan_colset = idxscan_expr->Prpp()->PcrsRequired();
    idxscan_cols = idxscan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    outer_join_key_col_idx =
        pGetColIndexInPred(idxscan_expr->operator[](0), outer_cols);
    D_ASSERT(outer_join_key_col_idx != gpos::ulong_max);
    D_ASSERT(idxscan_expr->Pop()->Eopid() == COperator::EopPhysicalIndexScan);
    edge_physical_id_col = ((CPhysicalIndexScan *)idxscan_expr->Pop())->PdrgpcrOutput()->operator[](0);

    // Get filter
    if (is_filter_exist) {
        CExpression *adjidxjoin_into_expr = nullptr;
        filter_expr = pFindFilterExpr(plan_expr->operator[](1));

        // check if filter contains adjidxjoin_into condition
        is_adjidxjoin_into = pIsAdjIdxJoinInto(
            (*filter_expr)[1], (*plan_expr)[0]->Prpp()->PcrsRequired(),
            idxscan_expr->Prpp()->PcrsRequired(), adjidxjoin_into_expr);

        // if so, rebuild filter expr without adjidxjoin_into condition
        if (is_adjidxjoin_into) {
            filter_expr = reBuildFilterExpr(filter_expr, adjidxjoin_into_expr);
            if (filter_expr == nullptr) {
                is_filter_exist = false;
            }

            // get tgt_col_idx
            tgt_key_col_idx =
                pGetColIndexInPred(adjidxjoin_into_expr, outer_cols);
        }
    }

    bool filter_after_adj =
        is_filter_exist && !pIsEdgePropertyInFilter(plan_expr->operator[](1)) &&
        !is_left_outer;
    bool filter_in_seek = is_filter_exist && !filter_after_adj;
    bool generate_seek = is_edge_prop_in_output || filter_in_seek;

    CColRefSet *output_cols_copy = GPOS_NEW(mp) CColRefSet(mp, *(plan_expr->Prpp()->PcrsRequired()));
    CColRefSet *inner_cols_set = (*plan_expr)[1]->Prpp()->PcrsRequired();
    output_cols_copy->Difference(inner_cols_set);

    // Calculate adj_inner_cols and adj_output_cols and seek_inner_cols
    /**
     * 1) AdjIdxJoin + Filter + Seek
     *  - AdjIdxJoin output: outer_cols + inner_cols (w/o edge properties) + filter only columns + edge ID
     *  - Filter output: outer_cols + inner_cols (w/o edge properties) + filter only columns
     *  - Seek inner: edge properties
     *  - Seek output: output_cols
     * 2) AdjIdxJoin + Filter + [Projection]
     *  - AdjIdxJoin inner: inner_cols + filter only columns
     *  - AdjIdxJoin output: outer_cols + inner_cols + filter only columns
     *  - Filter output: output_cols
     * 3) AdjIdxJoin + Seek
     *  - AdjIdxJoin inner: inner_cols (w/o edge properties)
     *  - AdjIdxJoin output: outer_cols + inner_cols (w/o edge properties) + edge ID
     *  - Seek inner: edge properties + filter only columns (if filter exists)
     *  - Seek output: output_cols
     * 4) AdjIdxJoin
     *  - AdjIdxJoin inner: inner cols
     *  - AdjIdxJoin output: output cols
    **/
    if (filter_after_adj && generate_seek) {
        if (!is_adjidxjoin_into) {
            D_ASSERT(false);
            D_ASSERT(filter_expr != NULL);
            adj_output_cols->AppendArray(outer_cols);
            adj_output_cols->AppendArray(adj_inner_cols);
            pAppendFilterOnlyCols(filter_expr, idxscan_cols, inner_cols,
                                adj_inner_cols);
            pAppendFilterOnlyCols(filter_expr, idxscan_cols, inner_cols,
                                adj_output_cols);
        } else {
            D_ASSERT(false);
        }
    }
    else if (filter_after_adj && !generate_seek) {
        if (!is_adjidxjoin_into) {
            D_ASSERT(false);
            D_ASSERT(filter_expr != NULL);
            adj_output_cols->AppendArray(outer_cols);
            adj_output_cols->AppendArray(inner_cols);
            adj_inner_cols->AppendArray(inner_cols);
            pAppendFilterOnlyCols(filter_expr, idxscan_cols, inner_cols,
                                adj_inner_cols);
            pAppendFilterOnlyCols(filter_expr, idxscan_cols, inner_cols,
                                adj_output_cols);
        } else {
            D_ASSERT(false);
        }
    }
    else if (!filter_after_adj && generate_seek) {
        adj_output_colset->Include(output_cols_copy);
        pSeperatePropertyNonPropertyCols(inner_colset, seek_inner_colset,
                                        adj_inner_colset);
        adj_output_cols->AppendArray(adj_inner_cols);
        adj_inner_colset->Include(edge_physical_id_col);
        adj_output_colset->Include(adj_inner_colset);
        seek_inner_cols = seek_inner_colset->Pdrgpcr(mp);
        if (filter_in_seek) {
            D_ASSERT(filter_expr != NULL);
            pAppendFilterOnlyCols(filter_expr, idxscan_cols, inner_cols,
                                    seek_inner_cols);
        }
    }
    else {
        adj_output_colset->Include(output_colset);
        adj_inner_colset->Include(inner_colset);
    }

    // TODO remove colrefarray
    adj_output_cols = adj_output_colset->Pdrgpcr(mp);
    adj_inner_cols = adj_inner_colset->Pdrgpcr(mp);
    // seek_inner_cols = seek_inner_colset->Pdrgpcr(mp);

    // D_ASSERT(adj_output_cols->Size() > 0);
    // D_ASSERT(adj_inner_cols->Size() > 0); // release this condition

    // Construct inner_col_maps_adj
    if (!generate_seek) {
        pConstructColMapping(adj_inner_cols, adj_output_cols,
                             inner_col_maps_adj[0]);
    }
    else {
        pConstructColMapping(adj_inner_cols, adj_output_cols,
                             inner_col_maps_adj[0]);
        edge_id_col_idx = adj_output_cols->IndexOf(edge_physical_id_col);     
    }
    output_cols_copy->Release();
    // D_ASSERT(inner_col_maps_adj[0].size() > 0); // release this condition

    // Construct outer_cols_maps_adj
    pConstructColMapping(outer_cols, adj_output_cols, outer_col_maps_adj[0]);

    // Construct AdjIdxJoin schema
    pGetDuckDBTypesFromColRefs(adj_output_cols, output_types_adj);
    schema_adj.setStoredTypes(output_types_adj);

    // Construct adjidx_obj_id
    CPhysicalIndexScan *idxscan_op = (CPhysicalIndexScan *)idxscan_expr->Pop();
    CMDIdGPDB *index_mdid =
        CMDIdGPDB::CastMdid(idxscan_op->Pindexdesc()->MDId());
    adjidx_obj_id = index_mdid->Oid();

    // Construct adjacency index
    auto adjidx_idx_idxs = pGetAdjIdxIdIdxs(adj_inner_cols, idxscan_op->Pindexdesc()->IndexType());
    auto *duckdb_adjidx_op =
        new duckdb::PhysicalAdjIdxJoin(
            schema_adj, adjidx_obj_id,
            is_left_outer ? duckdb::JoinType::LEFT : duckdb::JoinType::INNER,
            is_adjidxjoin_into, outer_join_key_col_idx, tgt_key_col_idx,
            outer_col_maps_adj[0], inner_col_maps_adj[0], adjidx_idx_idxs);

    // M26: For BOTH direction, find the complementary (backward) index OID.
    // M27-C: For multi-partition edge types, find extra partition adj indexes.
    {
        auto &cat = context->db->GetCatalog();
        CMDIdGPDB *tab_mdid = CMDIdGPDB::CastMdid(idxscan_op->Ptabdesc()->MDId());
        duckdb::idx_t graphlet_oid = tab_mdid->Oid();
        // Ptabdesc returns the graphlet (PropertySchema) OID, not the partition OID.
        // Resolve to the actual partition OID for both_edge_partitions lookup.
        duckdb::idx_t edge_part_oid = graphlet_oid;
        {
            auto *ps_entry = dynamic_cast<duckdb::PropertySchemaCatalogEntry *>(
                cat.GetEntry(*context, DEFAULT_SCHEMA, graphlet_oid, true));
            if (ps_entry) {
                edge_part_oid = ps_entry->partition_oid;
            }
        }
        auto *gcat = static_cast<duckdb::GraphCatalogEntry *>(
            cat.GetEntry(*context, duckdb::CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, DEFAULT_GRAPH));

        // Helper: find backward CSR index OID for a given edge partition
        auto find_bwd_index = [&](duckdb::PartitionCatalogEntry *epart, duckdb::idx_t skip_fwd_oid) -> duckdb::idx_t {
            auto *adj_indexes = epart->GetAdjIndexOidVec();
            for (auto idx_oid : *adj_indexes) {
                if (idx_oid == skip_fwd_oid) continue;
                auto *idx_entry = static_cast<duckdb::IndexCatalogEntry *>(
                    cat.GetEntry(*context, DEFAULT_SCHEMA, idx_oid, true));
                if (idx_entry && idx_entry->GetIndexType() == duckdb::IndexType::BACKWARD_CSR) {
                    return idx_oid;
                }
            }
            return 0;
        };

        // Helper: find forward CSR index OID for a given edge partition
        auto find_fwd_index = [&](duckdb::PartitionCatalogEntry *epart) -> duckdb::idx_t {
            auto *adj_indexes = epart->GetAdjIndexOidVec();
            for (auto idx_oid : *adj_indexes) {
                auto *idx_entry = static_cast<duckdb::IndexCatalogEntry *>(
                    cat.GetEntry(*context, DEFAULT_SCHEMA, idx_oid, true));
                if (idx_entry && idx_entry->GetIndexType() == duckdb::IndexType::FORWARD_CSR) {
                    return idx_oid;
                }
            }
            return 0;
        };

        bool is_both = both_edge_partitions.count(edge_part_oid) > 0;

        // Primary partition: BOTH backward
        if (is_both) {
            auto *epart = static_cast<duckdb::PartitionCatalogEntry *>(
                cat.GetEntry(*context, DEFAULT_SCHEMA, edge_part_oid));
            if (epart) {
                duckdb_adjidx_op->bwd_adjidx_obj_id = find_bwd_index(epart, adjidx_obj_id);
                // No dedup needed: from a single vertex's perspective,
                // forward CSR and backward CSR contain disjoint edge sets.
                // Edge A→B appears in A's forward and B's backward only.
            }
        }

        // M27-C: Add sibling partitions from converter-provided multi_edge_partitions map.
        // Use graphlet_oid (not resolved edge_part_oid) because ORCA already handles
        // multi-partition edges via UnionAll at the logical level. The converter stores
        // partition OIDs in multi_edge_partitions_, so using graphlet_oid ensures we
        // don't double-add partitions that ORCA already planned for.
        // IMPORTANT: Match the primary index direction — if primary is backward CSR,
        // siblings must also use backward CSR (not forward).
        {
            auto it = multi_edge_partitions.find(graphlet_oid);
            if (it != multi_edge_partitions.end()) {
                // Determine primary index direction
                auto *primary_idx = static_cast<duckdb::IndexCatalogEntry *>(
                    cat.GetEntry(*context, DEFAULT_SCHEMA, adjidx_obj_id, true));
                bool primary_is_fwd = primary_idx &&
                    primary_idx->GetIndexType() == duckdb::IndexType::FORWARD_CSR;

                for (auto sib_oid : it->second) {
                    auto *sib_epart = static_cast<duckdb::PartitionCatalogEntry *>(
                        cat.GetEntry(*context, DEFAULT_SCHEMA, sib_oid));
                    if (!sib_epart) continue;

                    // Match sibling direction to primary direction
                    duckdb::idx_t sib_idx;
                    if (primary_is_fwd) {
                        sib_idx = find_fwd_index(sib_epart);
                    } else {
                        // Primary is backward — find sibling's backward CSR
                        duckdb::idx_t sib_fwd = find_fwd_index(sib_epart);
                        sib_idx = find_bwd_index(sib_epart, sib_fwd);
                    }
                    if (sib_idx == 0) continue;

                    duckdb_adjidx_op->extra_fwd_obj_ids.push_back(sib_idx);

                    if (is_both) {
                        // For BOTH: also need the complementary direction
                        duckdb::idx_t sib_complement;
                        if (primary_is_fwd) {
                            sib_complement = find_bwd_index(sib_epart, sib_idx);
                        } else {
                            sib_complement = find_fwd_index(sib_epart);
                        }
                        duckdb_adjidx_op->extra_bwd_obj_ids.push_back(sib_complement);
                        duckdb_adjidx_op->extra_dedup_flags.push_back(false);
                    }
                }
            }
        }

        // M30: Virtual unified edge partition — partition-aware CSR dispatch.
        // For virtual partitions with sub_partition_oids, each sub-partition's CSR
        // is stored in its own source (fwd) or destination (bwd) vertex extents.
        // We must only scan the CSR that matches the input vertex's partition.
        {
            auto *epart = static_cast<duckdb::PartitionCatalogEntry *>(
                cat.GetEntry(*context, DEFAULT_SCHEMA, edge_part_oid, true));
            if (epart && !epart->sub_partition_oids.empty()) {
                // Determine primary adj index direction
                auto *primary_idx_entry = static_cast<duckdb::IndexCatalogEntry *>(
                    cat.GetEntry(*context, DEFAULT_SCHEMA, adjidx_obj_id, true));
                bool primary_is_fwd = primary_idx_entry &&
                    primary_idx_entry->GetIndexType() == duckdb::IndexType::FORWARD_CSR;

                // Helper: get vertex partition pid from partition OID
                auto get_vertex_pid = [&](duckdb::idx_t vpart_oid) -> uint16_t {
                    auto *vpart = static_cast<duckdb::PartitionCatalogEntry *>(
                        cat.GetEntry(*context, DEFAULT_SCHEMA, vpart_oid, true));
                    return vpart ? vpart->GetPartitionID() : 0;
                };

                // Find which sub-partition owns the primary adj index
                duckdb::idx_t primary_sub_oid = 0;
                for (auto sub_oid : epart->sub_partition_oids) {
                    auto *sub = static_cast<duckdb::PartitionCatalogEntry *>(
                        cat.GetEntry(*context, DEFAULT_SCHEMA, sub_oid, true));
                    if (!sub) continue;
                    auto *adj_idxs = sub->GetAdjIndexOidVec();
                    for (auto idx_oid : *adj_idxs) {
                        if (idx_oid == adjidx_obj_id) {
                            primary_sub_oid = sub_oid;
                            break;
                        }
                    }
                    if (primary_sub_oid) break;
                }

                // Compute dispatch pid for the primary CSR entry.
                // Forward CSR → dispatch by src vertex pid (CSR stored in src extents)
                // Backward CSR → dispatch by dst vertex pid (CSR stored in dst extents)
                if (primary_sub_oid) {
                    auto *primary_sub = static_cast<duckdb::PartitionCatalogEntry *>(
                        cat.GetEntry(*context, DEFAULT_SCHEMA, primary_sub_oid, true));
                    if (primary_is_fwd) {
                        duckdb_adjidx_op->adj_dispatch_pids.push_back(
                            get_vertex_pid(primary_sub->GetSrcPartOid()));
                    } else {
                        duckdb_adjidx_op->adj_dispatch_pids.push_back(
                            get_vertex_pid(primary_sub->GetDstPartOid()));
                    }
                    if (is_both) {
                        // bwd dispatch pid = dst vertex partition pid
                        duckdb_adjidx_op->bwd_dispatch_pids.push_back(
                            get_vertex_pid(primary_sub->GetDstPartOid()));
                    }
                }



                // Add CSR indexes from non-primary sub-partitions + their dispatch pids
                for (auto sub_oid : epart->sub_partition_oids) {
                    if (sub_oid == primary_sub_oid) continue;
                    auto *sub = static_cast<duckdb::PartitionCatalogEntry *>(
                        cat.GetEntry(*context, DEFAULT_SCHEMA, sub_oid, true));
                    if (!sub) continue;

                    // Find matching-direction CSR for this sub-partition
                    duckdb::idx_t sub_idx_oid = 0;
                    if (primary_is_fwd) {
                        sub_idx_oid = find_fwd_index(sub);
                    } else {
                        sub_idx_oid = find_bwd_index(sub, 0 /* skip none */);
                    }
                    if (sub_idx_oid == 0) continue;
                    duckdb_adjidx_op->extra_fwd_obj_ids.push_back(sub_idx_oid);
                    fprintf(stderr, "[M30]   extra sub=%llu, idx_oid=%llu\n",
                        (unsigned long long)sub_oid, (unsigned long long)sub_idx_oid);

                    // Dispatch pid for this extra CSR
                    if (primary_is_fwd) {
                        duckdb_adjidx_op->adj_dispatch_pids.push_back(
                            get_vertex_pid(sub->GetSrcPartOid()));
                    } else {
                        duckdb_adjidx_op->adj_dispatch_pids.push_back(
                            get_vertex_pid(sub->GetDstPartOid()));
                    }

                    if (is_both) {
                        duckdb::idx_t sub_bwd = find_bwd_index(sub, sub_idx_oid);
                        duckdb_adjidx_op->extra_bwd_obj_ids.push_back(sub_bwd);
                        duckdb_adjidx_op->extra_dedup_flags.push_back(false);
                        // bwd dispatch pid = dst vertex partition pid
                        duckdb_adjidx_op->bwd_dispatch_pids.push_back(
                            get_vertex_pid(sub->GetDstPartOid()));
                    }
                }
            }
        }

        // M30: For non-virtual partitions, always set dispatch_pids to filter
        // VIDs from wrong vertex partitions. NodeScan may scan all vertex types
        // but the CSR adj_col_idx is only valid for the edge's source (fwd) or
        // destination (bwd) vertex partition's extents.
        if (duckdb_adjidx_op->adj_dispatch_pids.empty()) {
            auto *epart = static_cast<duckdb::PartitionCatalogEntry *>(
                cat.GetEntry(*context, DEFAULT_SCHEMA, edge_part_oid, true));
            if (epart && epart->GetSrcPartOid() != 0 && epart->GetDstPartOid() != 0) {
                auto *primary_idx_entry = static_cast<duckdb::IndexCatalogEntry *>(
                    cat.GetEntry(*context, DEFAULT_SCHEMA, adjidx_obj_id, true));
                bool primary_is_fwd = primary_idx_entry &&
                    primary_idx_entry->GetIndexType() == duckdb::IndexType::FORWARD_CSR;

                auto get_vertex_pid = [&](duckdb::idx_t vpart_oid) -> uint16_t {
                    auto *vpart = static_cast<duckdb::PartitionCatalogEntry *>(
                        cat.GetEntry(*context, DEFAULT_SCHEMA, vpart_oid, true));
                    return vpart ? vpart->GetPartitionID() : 0;
                };

                uint16_t dispatch_pid;
                if (primary_is_fwd) {
                    dispatch_pid = get_vertex_pid(epart->GetSrcPartOid());
                } else {
                    dispatch_pid = get_vertex_pid(epart->GetDstPartOid());
                }
                duckdb_adjidx_op->adj_dispatch_pids.push_back(dispatch_pid);
                if (is_both) {
                    if (primary_is_fwd) {
                        duckdb_adjidx_op->bwd_dispatch_pids.push_back(
                            get_vertex_pid(epart->GetDstPartOid()));
                    } else {
                        duckdb_adjidx_op->bwd_dispatch_pids.push_back(
                            get_vertex_pid(epart->GetSrcPartOid()));
                    }
                }

                // Also set dispatch for extra (M27-C sibling) partitions
                for (size_t pi = 0; pi < duckdb_adjidx_op->extra_fwd_obj_ids.size(); pi++) {
                    auto *extra_idx = static_cast<duckdb::IndexCatalogEntry *>(
                        cat.GetEntry(*context, DEFAULT_SCHEMA,
                            duckdb_adjidx_op->extra_fwd_obj_ids[pi], true));
                    if (!extra_idx) continue;
                    auto *extra_epart = static_cast<duckdb::PartitionCatalogEntry *>(
                        cat.GetEntry(*context, DEFAULT_SCHEMA, extra_idx->pid, true));
                    if (!extra_epart) continue;
                    if (primary_is_fwd) {
                        duckdb_adjidx_op->adj_dispatch_pids.push_back(
                            get_vertex_pid(extra_epart->GetSrcPartOid()));
                    } else {
                        duckdb_adjidx_op->adj_dispatch_pids.push_back(
                            get_vertex_pid(extra_epart->GetDstPartOid()));
                    }
                    if (is_both) {
                        if (primary_is_fwd) {
                            duckdb_adjidx_op->bwd_dispatch_pids.push_back(
                                get_vertex_pid(extra_epart->GetDstPartOid()));
                        } else {
                            duckdb_adjidx_op->bwd_dispatch_pids.push_back(
                                get_vertex_pid(extra_epart->GetSrcPartOid()));
                        }
                    }
                }
            }
        }
    }

    /**
     * TOOD: this code assumes that the edge table is single schema.
     * Extend this code to handle multiple schemas.
     *
     * TODO: is pipeline_schemas necessary?
     * In the current logic, we intialize the chunk with UNION schema
     * and use col map to invalid each vector.
     * Therefore, pipeline schema is not actually used.
    */
    // Set human-readable display name from catalog
    {
        CMDIdGPDB *tab_mdid = CMDIdGPDB::CastMdid(idxscan_op->Ptabdesc()->MDId());
        duckdb_adjidx_op->display_name = pResolvePartitionName(tab_mdid->Oid());
    }

    pBuildSchemaFlowGraphForUnaryOperator(schema_adj);
    result->push_back(duckdb_adjidx_op);

    // System col only filter will be processed after adj
    if (filter_after_adj) {
        D_ASSERT(!filter_in_seek);
        vector<unique_ptr<duckdb::Expression>> filter_duckdb_exprs;
        pGetFilterDuckDBExprs(filter_expr, adj_output_cols, nullptr,
                              adj_output_cols->Size(), filter_duckdb_exprs);
        duckdb::CypherPhysicalOperator *duckdb_filter_op =
            new duckdb::PhysicalFilter(schema_adj, move(filter_duckdb_exprs));
        result->push_back(duckdb_filter_op);
        pBuildSchemaFlowGraphForUnaryOperator(schema_adj);

        // Construct projection
        if (!generate_seek) {
            if (output_cols->Size() != adj_output_cols->Size()) {
                vector<duckdb::LogicalType> output_types_proj;
                vector<unique_ptr<duckdb::Expression>> proj_exprs;
                pGetDuckDBTypesFromColRefs(output_cols, output_types_proj);
                schema_proj.setStoredTypes(output_types_proj);
                pGetProjectionExprs(adj_output_cols, output_cols,
                                    output_types_proj, proj_exprs);
                if (proj_exprs.size() != 0) {
                    duckdb::CypherPhysicalOperator *duckdb_proj_op =
                        new duckdb::PhysicalProjection(schema_proj,
                                                       move(proj_exprs));
                    result->push_back(duckdb_proj_op);
                    pBuildSchemaFlowGraphForUnaryOperator(schema_proj);
                }
            }
            return result;
        }
    }

    // Construct IdSeek
    if (generate_seek) {
        // Construct inner col map
        pConstructColMapping(seek_inner_cols, seek_output_cols,
                             inner_col_maps_seek[0]);
        D_ASSERT(inner_col_maps_seek[0].size() > 0);

        // Construct union_inner_col_map_seek
        vector<uint32_t> union_inner_col_map_seek;
        union_inner_col_map_seek = inner_col_maps_seek[0];

        // Construct outer col map
        pConstructColMapping(adj_output_cols, seek_output_cols,
                             outer_col_maps_seek);
        D_ASSERT(outer_col_maps_seek.size() > 0);

        // Construct scan_projection_mappings_seek and output_projection_mappings_seek and scan_types
        for (ULONG col_idx = 0; col_idx < seek_inner_cols->Size(); col_idx++) {
            CColRef *col = seek_inner_cols->operator[](col_idx);
            CColRefTable *colref_table = (CColRefTable *)col;
            CMDIdGPDB *type_mdid =
                CMDIdGPDB::CastMdid(colref_table->RetrieveType()->MDId());
            scan_projection_mappings_seek[0].push_back(colref_table->AttrNum());
            output_projection_mappings_seek[0].push_back(
                col_idx);  // not actually used
            scan_types_seek[0].push_back(pConvertTypeOidToLogicalType(
                type_mdid->Oid(), col->TypeModifier()));
        }

        // Get OIDs
        CColRefArray *idxscan_output_cols =
            idxscan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
        pGetObjetIdsForColRefs(idxscan_output_cols, seek_obj_ids);

        // Construct seek scheam
        pGetDuckDBTypesFromColRefs(seek_output_cols, output_types_seek);
        schema_seek.setStoredTypes(output_types_seek);

        // Construct IdSeek Operator
        duckdb::CypherPhysicalOperator *duckdb_idseek_op;
        if (!filter_in_seek) {
            duckdb_idseek_op = new duckdb::PhysicalIdSeek(
                schema_seek, edge_id_col_idx, seek_obj_ids,
                output_projection_mappings_seek /* not used */,
                outer_col_maps_seek, inner_col_maps_seek,
                union_inner_col_map_seek, scan_projection_mappings_seek,
                scan_types_seek, false /* is output UNION Schema */,
                is_left_outer ? duckdb::JoinType::LEFT : duckdb::JoinType::INNER,
                num_outer_schemas);
        }
        else {
            // Get filter_exprs
            vector<vector<unique_ptr<duckdb::Expression>>> filter_duckdb_exprs(1);
            pGetFilterDuckDBExprs(filter_expr, adj_output_cols, seek_inner_cols,
                                  adj_output_cols->Size(),
                                  filter_duckdb_exprs[0]);
            pGetIdentIndices(filter_duckdb_exprs[0][0], filter_col_idxs[0]);
            // Construct IdSeek Operator for filter
            duckdb_idseek_op = new duckdb::PhysicalIdSeek(
                schema_seek, edge_id_col_idx, seek_obj_ids,
                output_projection_mappings_seek /* not used */,
                outer_col_maps_seek, inner_col_maps_seek,
                union_inner_col_map_seek, scan_projection_mappings_seek,
                scan_types_seek, filter_duckdb_exprs, filter_col_idxs,
                false /* is output UNION Schema */,
                is_left_outer ? duckdb::JoinType::LEFT : duckdb::JoinType::INNER,
                num_outer_schemas);
        }

        // Construct schema flow graph
        pPushCartesianProductSchema(schema_seek, scan_types_seek[0]);

        // Set human-readable display name from catalog
        {
            CMDIdGPDB *tab_mdid = CMDIdGPDB::CastMdid(idxscan_op->Ptabdesc()->MDId());
            duckdb_idseek_op->display_name = pResolvePartitionName(tab_mdid->Oid());
        }

        // Pushback
        result->push_back(duckdb_idseek_op);
    }

    return result;
}

duckdb::CypherPhysicalOperatorGroups *
Planner::pTransformEopPhysicalInnerIndexNLJoinToVarlenAdjIdxJoin(
    CExpression *plan_expr)
{

    CMemoryPool *mp = this->memory_pool;

    /* Non-root - call single child */
    duckdb::CypherPhysicalOperatorGroups *result =
        pTraverseTransformPhysicalPlan(plan_expr->PdrgPexpr()->operator[](0));

    vector<duckdb::LogicalType> types;
    CPhysicalIndexPathJoin *join_op =
        (CPhysicalIndexPathJoin *)plan_expr->Pop();
    CColRefArray *output_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CExpression *pexprOuter = (*plan_expr)[0];
    CColRefArray *outer_cols = pexprOuter->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CExpression *pexprInner = (*plan_expr)[1];
    CColRefArray *inner_cols = pexprInner->Prpp()->PcrsRequired()->Pdrgpcr(mp);

    D_ASSERT(pexprInner->Pop()->Eopid() == COperator::EopPhysicalIndexPathScan);
    CPhysicalIndexPathScan *pathscan_op =
        (CPhysicalIndexPathScan *)pexprInner->Pop();

    unordered_map<ULONG, uint64_t> id_map;
    std::vector<uint32_t> outer_col_map;
    std::vector<uint32_t> inner_col_map;

    for (ULONG col_idx = 0; col_idx < output_cols->Size(); col_idx++) {
        CColRef *col = (*output_cols)[col_idx];
        ULONG col_id = col->Id();
        id_map.insert(std::make_pair(col_id, col_idx));
        types.push_back(pGetColumnsDuckDBType(col));
    }

    // Get JoinColumnID
    std::vector<uint32_t> sccmp_colids;
    CExpression *parent_exp = pexprInner;
    while (true) {
        CExpression *child_exp = (*parent_exp)[0];
        if (child_exp->Pop()->Eopid() ==
            COperator::EOperatorId::EopScalarIdent) {
            for (uint32_t i = 0; i < parent_exp->Arity(); i++) {
                CScalarIdent *sc_ident =
                    (CScalarIdent *)(parent_exp->operator[](i)->Pop());
                sccmp_colids.push_back(sc_ident->Pcr()->Id());
            }
            break;
        }
        else {
            parent_exp = child_exp;
            continue;
        }
    }

    uint64_t sid_col_idx = 0;
    bool sid_col_idx_found = false;
    // Construct mapping info
    for (ULONG col_idx = 0; col_idx < outer_cols->Size(); col_idx++) {
        CColRef *col = outer_cols->operator[](col_idx);
        ULONG col_id = col->Id();
        // find key column
        auto it = std::find(sccmp_colids.begin(), sccmp_colids.end(), col_id);
        if (it != sccmp_colids.end()) {
            D_ASSERT(!sid_col_idx_found);
            sid_col_idx = col_idx;
            sid_col_idx_found = true;
        }
        // construct outer col map
        auto it_ = id_map.find(col_id);
        if (it_ == id_map.end()) {
            outer_col_map.push_back(std::numeric_limits<uint32_t>::max());
        }
        else {
            auto id_idx = id_map.at(col_id);
            outer_col_map.push_back(id_idx);
        }
    }
    D_ASSERT(sid_col_idx_found);

    // construct inner col map. // TODO we don't consider edge property now..
    for (ULONG col_idx = 0; col_idx < inner_cols->Size(); col_idx++) {
        // if (col_idx == 0) continue; // 0405 we don't need this condition anymore?
        CColRef *col = inner_cols->operator[](col_idx);
        ULONG col_id = col->Id();
        auto id_idx = id_map.at(
            col_id);  // std::out_of_range exception if col_id does not exist in id_map
        inner_col_map.push_back(id_idx);
    }

    // Collect all index OIDs (one per edge type)
    vector<uint64_t> path_index_oids;
    for (ULONG i = 0; i < pathscan_op->Pindexdesc()->Size(); i++) {
        OID oid = CMDIdGPDB::CastMdid(
            pathscan_op->Pindexdesc()->operator[](i)->MDId())->Oid();
        path_index_oids.push_back(oid);
    }

    // M26-C: For BOTH direction, add backward index OIDs alongside forward ones.
    {
        auto &cat = context->db->GetCatalog();
        vector<uint64_t> bwd_oids_to_add;
        for (auto fwd_oid : path_index_oids) {
            auto *idx_entry = static_cast<duckdb::IndexCatalogEntry *>(
                cat.GetEntry(*context, DEFAULT_SCHEMA, fwd_oid, true));
            if (!idx_entry) continue;
            duckdb::idx_t part_oid = idx_entry->GetPartitionID();
            if (!both_edge_partitions.count(part_oid)) continue;
            // Find backward index in the same partition
            auto *epart = static_cast<duckdb::PartitionCatalogEntry *>(
                cat.GetEntry(*context, DEFAULT_SCHEMA, part_oid));
            if (!epart) continue;
            for (auto adj_oid : *epart->GetAdjIndexOidVec()) {
                if (adj_oid == fwd_oid) continue;
                auto *adj_entry = static_cast<duckdb::IndexCatalogEntry *>(
                    cat.GetEntry(*context, DEFAULT_SCHEMA, adj_oid, true));
                if (adj_entry && adj_entry->GetIndexType() == duckdb::IndexType::BACKWARD_CSR) {
                    bwd_oids_to_add.push_back(adj_oid);
                    break;
                }
            }
        }
        for (auto bwd_oid : bwd_oids_to_add) {
            path_index_oids.push_back(bwd_oid);
        }
    }

    // Compute terminal destination partitions for VarLen output filtering.
    // Terminal = vertex partitions reachable as dst but NOT used as src (can't recurse further).
    // E.g., REPLY_OF: Comment→Comment, Comment→Post → terminal = {Post}.
    std::unordered_set<uint16_t> dst_partition_ids;
    {
        auto &cat = context->db->GetCatalog();
        std::unordered_set<uint16_t> src_pids, dst_pids;
        for (auto idx_oid : path_index_oids) {
            auto *idx_entry = static_cast<duckdb::IndexCatalogEntry *>(
                cat.GetEntry(*context, DEFAULT_SCHEMA, idx_oid, true));
            if (!idx_entry) continue;
            duckdb::idx_t edge_part_oid = idx_entry->GetPartitionID();
            auto *epart = static_cast<duckdb::PartitionCatalogEntry *>(
                cat.GetEntry(*context, DEFAULT_SCHEMA, edge_part_oid, true));
            if (!epart) continue;
            // Resolve src/dst vertex partition OIDs to their PartitionID (16-bit VID prefix)
            bool is_forward = (idx_entry->GetIndexType() == duckdb::IndexType::FORWARD_CSR);
            duckdb::idx_t src_vp_oid = is_forward ? epart->GetSrcPartOid() : epart->GetDstPartOid();
            duckdb::idx_t dst_vp_oid = is_forward ? epart->GetDstPartOid() : epart->GetSrcPartOid();
            auto *src_vpart = static_cast<duckdb::PartitionCatalogEntry *>(
                cat.GetEntry(*context, DEFAULT_SCHEMA, src_vp_oid, true));
            auto *dst_vpart = static_cast<duckdb::PartitionCatalogEntry *>(
                cat.GetEntry(*context, DEFAULT_SCHEMA, dst_vp_oid, true));
            if (src_vpart) src_pids.insert((uint16_t)src_vpart->GetPartitionID());
            if (dst_vpart) dst_pids.insert((uint16_t)dst_vpart->GetPartitionID());
        }
        for (auto dp : dst_pids) {
            if (src_pids.count(dp) == 0) {
                dst_partition_ids.insert(dp);
            }
        }
    }

    /* Generate operator and push */
    duckdb::Schema tmp_schema;
    tmp_schema.setStoredTypes(types);
    uint64_t upper_bound = pathscan_op->UpperBound();
    uint64_t lower_bound = pathscan_op->LowerBound();
    if (upper_bound == -1) upper_bound = std::numeric_limits<uint64_t>::max();
    duckdb::CypherPhysicalOperator *op = new duckdb::PhysicalVarlenAdjIdxJoin(
        tmp_schema, path_index_oids, duckdb::JoinType::INNER, sid_col_idx, false,
        lower_bound, upper_bound, outer_col_map,
        inner_col_map, std::move(dst_partition_ids));

    result->push_back(op);

    pBuildSchemaFlowGraphForUnaryOperator(tmp_schema);

    return result;
}

duckdb::CypherPhysicalOperatorGroups *
Planner::pTransformEopPhysicalInnerIndexNLJoinToIdSeek(CExpression *plan_expr)
{
#ifdef DYNAMIC_SCHEMA_INSTANTIATION
    bool is_dsi = false;

    CExpression *inner_root = (*plan_expr)[1];
    while (true) {
        if (inner_root->Pop()->Eopid() ==
            COperator::EOperatorId::EopPhysicalIndexScan) {
            auto *idxscan_op =
                (CPhysicalIndexScan *)inner_root->Pop();
            auto *index_desc = idxscan_op->Pindexdesc();
            if (index_desc->IsInstanceDescriptor()) {
                is_dsi = true;
            }
            break;
        }
        else if (inner_root->Pop()->Eopid() ==
                 COperator::EOperatorId::EopPhysicalIndexOnlyScan) {
            auto *idxscan_op =
                (CPhysicalIndexOnlyScan *)inner_root->Pop();
            auto *index_desc = idxscan_op->Pindexdesc();
            if (index_desc->IsInstanceDescriptor()) {
                is_dsi = true;
            }
            break;
        }
        // reached to the bottom
        if (inner_root->Arity() == 0) {
            break;
        }
        else {
            inner_root = inner_root->operator[](0);
        }
    }

    if (is_dsi) {
        return pTransformEopPhysicalInnerIndexNLJoinToIdSeekDSI(plan_expr);
    } else {
        return pTransformEopPhysicalInnerIndexNLJoinToIdSeekNormal(plan_expr);
    }
#else
    return pTransformEopPhysicalInnerIndexNLJoinToIdSeekNormal(plan_expr);
#endif
}

duckdb::CypherPhysicalOperatorGroups *
Planner::pTransformEopPhysicalInnerIndexNLJoinToIdSeekNormal(CExpression *plan_expr)
{
    CMemoryPool *mp = this->memory_pool;

    /* Non-root - call single child */
    duckdb::CypherPhysicalOperatorGroups *result =
        pTraverseTransformPhysicalPlan(plan_expr->PdrgPexpr()->operator[](0));

    vector<duckdb::LogicalType> types;

    CColRefArray *output_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CExpression *pexprOuter = (*plan_expr)[0];
    CColRefArray *outer_cols = pexprOuter->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CExpression *pexprInner = (*plan_expr)[1];
    CColRefArray *inner_cols = pexprInner->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CColRefArray *idxscan_cols = nullptr;


    unordered_map<ULONG, uint64_t> id_map;
    vector<vector<duckdb::LogicalType>> scan_types;
    vector<uint32_t> outer_col_map;
    vector<vector<uint32_t>> inner_col_maps;
    vector<vector<uint64_t>> scan_projection_mapping;
    vector<vector<uint64_t>> output_projection_mapping;

    duckdb::JoinType join_type = pTranslateJoinType(plan_expr->Pop());

    scan_types.push_back(std::vector<duckdb::LogicalType>());
    scan_projection_mapping.push_back(std::vector<uint64_t>());
    output_projection_mapping.push_back(std::vector<uint64_t>());
    inner_col_maps.push_back(std::vector<uint32_t>());

    for (ULONG col_idx = 0; col_idx < output_cols->Size(); col_idx++) {
        CColRef *col = (*output_cols)[col_idx];
        CColRefTable *colref_table = (CColRefTable *)col;
        ULONG col_id = col->Id();
        id_map.insert(std::make_pair(col_id, col_idx));

        CMDIdGPDB *type_mdid = CMDIdGPDB::CastMdid(col->RetrieveType()->MDId());
        OID type_oid = type_mdid->Oid();
        INT type_mod = col->TypeModifier();
        types.push_back(pConvertTypeOidToLogicalType(type_oid, type_mod));
    }

    uint64_t idx_obj_id;  // 230303
    uint64_t sid_col_idx;
    CExpression *inner_root = pexprInner;

    vector<uint64_t> oids;
    vector<uint32_t> sccmp_colids;
    vector<uint32_t> scident_colids;

    bool do_projection_on_idxscan = false;
    bool do_filter_pushdown = false;
    bool has_filter = false;

    CExpression *filter_expr = NULL;
    CExpression *filter_pred_expr = NULL;
    CExpression *idxscan_expr = NULL;
    duckdb::ExpressionType exp_type;
    CColRefArray *filter_pred_cols = GPOS_NEW(mp) CColRefArray(mp);
    vector<vector<unique_ptr<duckdb::Expression>>> per_schema_filter_exprs(1);
    auto &filter_exprs = per_schema_filter_exprs[0];
    vector<vector<duckdb::idx_t>> filter_col_idxs(1);
    size_t num_outer_schemas = pGetNumOuterSchemas();
    vector<ULONG> inner_filter_only_cols_idx;
    size_t scan_types_before_filter = 0;

    while (true) {
        if (inner_root->Pop()->Eopid() ==
            COperator::EOperatorId::EopPhysicalIndexScan) {
            // IdxScan
            idxscan_expr = inner_root;
            idxscan_cols = inner_root->Prpp()->PcrsRequired()->Pdrgpcr(mp);
            CPhysicalIndexScan *idxscan_op =
                (CPhysicalIndexScan *)inner_root->Pop();
            CMDIdGPDB *index_mdid =
                CMDIdGPDB::CastMdid(idxscan_op->Pindexdesc()->MDId());
            gpos::ULONG oid = index_mdid->Oid();
            idx_obj_id = (uint64_t)oid;

            // check complex comparison
            bool is_complex_comparison = false;
            if (idxscan_expr->Arity() > 0) {
                CExpression *scalar_expr_child = idxscan_expr->operator[](0);
                if (scalar_expr_child->Pop()->Eopid() ==
                    COperator::EOperatorId::EopScalarBoolOp) {
                    has_filter = true;
                    is_complex_comparison = true;
                }
            }

            // Traverse down until we met ScalarCmp (We assume binary tree of BoolOp)
            CExpression *scalar_cmp_expr = NULL;
            CExpression *cursor = idxscan_expr;
            while (true) {
                cursor = cursor->operator[](0);
                if (cursor->Pop()->Eopid() ==
                    COperator::EOperatorId::EopScalarCmp) {
                    scalar_cmp_expr = cursor;
                    break;
                }
            }
            D_ASSERT(scalar_cmp_expr->Pop()->Eopid() ==
                     COperator::EOperatorId::EopScalarCmp);

            // Generate filter expression for
            if (is_complex_comparison) {
                /**
                 * We consider the case where ._id column have multiple CMP with other ._id cols.
                 * Since we don't load ._id column, we consider this as a AND of CMP of other .id cols.
                */

                unique_ptr<duckdb::Expression> filter_duckdb_expr;
                auto org_filter_pred_expr = idxscan_expr->operator[](0);
                D_ASSERT(org_filter_pred_expr->Pop()->Eopid() ==
                         COperator::EOperatorId::EopScalarBoolOp);

                // Create modified filter_pred_expr
                CExpression *filter_pred_expr;
                vector<uint64_t> outer_filter_cols_idx;
                for (ULONG col_idx = 0; col_idx < org_filter_pred_expr->Arity();
                     col_idx++) {
                    CExpression *child_cmp_expr =
                        org_filter_pred_expr->operator[](col_idx);
                    for (ULONG child_col_idx = 0;
                         child_col_idx < child_cmp_expr->Arity();
                         child_col_idx++) {
                        CScalarIdent *sc_ident = (CScalarIdent *)child_cmp_expr
                                                     ->
                                                     operator[](child_col_idx)
                                                     ->Pop();
                        const CColRef *col = sc_ident->Pcr();
                        if (idxscan_cols->IndexOf(col) == gpos::ulong_max) {
                            auto col_idx = outer_cols->IndexOf(col);
                            D_ASSERT(col_idx != gpos::ulong_max);
                            outer_filter_cols_idx.push_back(col_idx);
                        }
                    }
                }
                D_ASSERT(outer_filter_cols_idx.size() != 1);

                CExpressionArray *cnf_exprs = GPOS_NEW(mp) CExpressionArray(mp);
                for (ULONG i = 0; i < outer_filter_cols_idx.size() - 1; i++) {
                    CColRef *lpcr =
                        outer_cols->operator[](outer_filter_cols_idx[i]);
                    CColRef *rpcr =
                        outer_cols->operator[](outer_filter_cols_idx[i + 1]);
                    cnf_exprs->Append(CUtils::PexprScalarEqCmp(mp, lpcr, rpcr));
                }

                if (cnf_exprs->Size() == 1) {
                    filter_pred_expr = cnf_exprs->operator[](0);
                }
                else {
                    filter_pred_expr = CUtils::PexprScalarBoolOp(
                        mp, CScalarBoolOp::EBoolOperator::EboolopAnd,
                        cnf_exprs);
                }

                filter_duckdb_expr =
                    pTransformScalarExpr(filter_pred_expr, outer_cols,
                                         nullptr);  // only outer cols exist
                pGetIdentIndices(filter_duckdb_expr, filter_col_idxs[0]);
                filter_exprs.push_back(std::move(filter_duckdb_expr));
            }

            // Get JoinColumnID (We assume binary tree of BoolOp)
            for (uint32_t i = 0; i < scalar_cmp_expr->Arity(); i++) {
                CScalarIdent *sc_ident =
                    (CScalarIdent *)(scalar_cmp_expr->operator[](i)->Pop());
                sccmp_colids.push_back(sc_ident->Pcr()->Id());
            }

            // CColRefArray *output =
            //     inner_root->Prpp()->PcrsRequired()->Pdrgpcr(mp);

            // // try seek bypassing
            // if ((output->Size() == 0) ||
            //     (output->Size() == 1 &&
            //      pGetColIdxFromTable(
            //          CMDIdGPDB::CastMdid(((CColRefTable *)output->operator[](0))
            //                                  ->GetMdidTable())
            //              ->Oid(),
            //          output->operator[](0)) == 0)) {
            //     // nothing changes, we don't need seek, pass directly
            //     return result;
            // }
        }
        else if (inner_root->Pop()->Eopid() ==
                 COperator::EOperatorId::EopPhysicalIndexOnlyScan) {
            // IndexOnlyScan on physical id index. We don't need to do idseek
            // maybe we need to process filter expression
            D_ASSERT(inner_root->Arity() >= 1);
            CExpression *scalar_expr = inner_root;
            CExpression *scalar_expr_child = scalar_expr->operator[](0);
            vector<unique_ptr<duckdb::Expression>> scalar_filter_exprs;
            if (scalar_expr_child->Pop()->Eopid() != COperator::EopScalarCmp) {
                // we need additional filter
            }

            // if output_cols size != outer_cols, we need to do projection
            /* Generate operator and push */
            duckdb::Schema tmp_schema;
            vector<unique_ptr<duckdb::Expression>> proj_exprs;
            tmp_schema.setStoredTypes(types);

            // bool project_physical_id_column = (output_cols->Size() == outer_cols->Size()); // TODO always works?
            bool project_physical_id_column = true;  // TODO we need a logic..
            pGetAllScalarIdents(inner_root->operator[](0), sccmp_colids);
            for (ULONG col_idx = 0; col_idx < output_cols->Size(); col_idx++) {
                CColRef *col = (*output_cols)[col_idx];
                ULONG idx = outer_cols->IndexOf(col);
                if (idx ==
                    gpos::
                        ulong_max) {  // if not found, it is ID column and to be replaced with IdSeek's column
                    if (!project_physical_id_column) {
                        continue;
                    }
                    else {
                        for (ULONG outer_col_idx = 0;
                             outer_col_idx < outer_cols->Size();
                             outer_col_idx++) {
                            CColRef *col = (*outer_cols)[outer_col_idx];
                            ULONG outer_col_id = col->Id();
                            auto it =
                                std::find(sccmp_colids.begin(),
                                          sccmp_colids.end(), outer_col_id);
                            if (it != sccmp_colids.end()) {
                                idx = outer_col_idx;
                                break;
                            }
                        }
                    }
                    if (idx == gpos::ulong_max)
                        continue;
                }
                D_ASSERT(idx != gpos::ulong_max);
                proj_exprs.push_back(
                    make_unique<duckdb::BoundReferenceExpression>(
                        types[col_idx], (int)idx));
            }
            if (proj_exprs.size() != 0) {
                D_ASSERT(proj_exprs.size() == output_cols->Size());
                duckdb::CypherPhysicalOperator *op =
                    new duckdb::PhysicalProjection(tmp_schema,
                                                   std::move(proj_exprs));
                result->push_back(op);

                pBuildSchemaFlowGraphForUnaryOperator(tmp_schema);
            }

            return result;
        }
        else if (inner_root->Pop()->Eopid() ==
                 COperator::EOperatorId::EopPhysicalFilter) {
            has_filter = true;
            do_filter_pushdown = false;
            filter_expr = inner_root;
            filter_pred_expr = filter_expr->operator[](1);
            // D_ASSERT(filter_expr->operator[](0)->Pop()->Eopid() ==
            //          COperator::EOperatorId::EopPhysicalIndexScan);
            idxscan_cols =
                filter_expr->operator[](0)->Prpp()->PcrsRequired()->Pdrgpcr(mp);

            /**
             * TODO: revive filter pushdown
            */

            // Get filter only columns in idxscan_cols
            pGetFilterOnlyInnerColsIdx(
                filter_pred_expr, idxscan_cols /* all inner cols */,
                inner_cols /* output inner cols */, inner_filter_only_cols_idx);

            // Record how many scan_types entries exist BEFORE we add filter-only cols.
            scan_types_before_filter = scan_types[0].size();

            // Build inner_required_cols: inner_cols first, then filter-only cols.
            CColRefArray *inner_required_cols = GPOS_NEW(mp) CColRefArray(mp);
            for (ULONG col_idx = 0; col_idx < inner_cols->Size(); col_idx++) {
                inner_required_cols->Append(inner_cols->operator[](col_idx));
            }

            // Add filter-only cols to scan_types and inner_required_cols.
            for (auto col_idx : inner_filter_only_cols_idx) {
                CColRef *col = idxscan_cols->operator[](col_idx);
                CColRefTable *colreftbl = (CColRefTable *)col;
                inner_required_cols->Append(col);
                inner_col_maps[0].push_back(
                    std::numeric_limits<uint32_t>::max());
                scan_projection_mapping[0].push_back(colreftbl->AttrNum());
                CMDIdGPDB *type_mdid =
                    CMDIdGPDB::CastMdid(col->RetrieveType()->MDId());
                scan_types[0].push_back(pConvertTypeOidToLogicalType(
                    type_mdid->Oid(), col->TypeModifier()));
            }

            // Convert filter expression. BoundRef indices are relative to
            // inner_required_cols = [inner_cols..., filter_only_cols...].
            // Shift inner column refs by outer_cols->Size().
            unique_ptr<duckdb::Expression> filter_duckdb_expr;
            filter_duckdb_expr = pTransformScalarExpr(
                filter_pred_expr, outer_cols, inner_required_cols);
            pShiftFilterPredInnerColumnIndices(filter_duckdb_expr,
                                               outer_cols->Size());

            // Remap filter-only BoundRef indices to match scan_types order.
            //
            // After pShiftFilterPredInnerColumnIndices, inner BoundRefs are:
            //   inner_cols[i] → outer + i
            //   filter_only[j] → outer + N + j  (where N = inner_cols->Size())
            //
            // But in scan_types, filter-only cols start at scan_types_before_filter.
            // So filter_only[j] should be: outer + scan_types_before_filter + j.
            // Adjustment: shift by (scan_types_before_filter - N) for refs >= outer+N.
            if (scan_types_before_filter != inner_cols->Size()) {
                int adjustment = (int)scan_types_before_filter - (int)inner_cols->Size();
                size_t threshold = outer_cols->Size() + inner_cols->Size();
                pAdjustBoundRefIndices(filter_duckdb_expr, threshold, adjustment);
            }

            pGetIdentIndices(filter_duckdb_expr, filter_col_idxs[0]);
            filter_exprs.push_back(std::move(filter_duckdb_expr));
        }
        else if (inner_root->Pop()->Eopid() ==
                 COperator::EOperatorId::EopPhysicalComputeScalarColumnar) {
            size_t num_filter_only_cols = 0;
            bool load_system_col = false;

            // generate inner_col_maps, scan_projection_mapping, output_projection_mapping, scan_types
            for (ULONG i = 0; i < inner_cols->Size(); i++) {
                CColRef *col = inner_cols->operator[](i);
                CColRefTable *colreftbl = (CColRefTable *)col;
                INT attr_no = colreftbl->AttrNum();
                ULONG col_id = colreftbl->Id();
                auto it = id_map.find(col_id);
                if (it != id_map.end()) {
                    inner_col_maps[0].push_back(it->second);
                    if (attr_no == INT(-1))
                        load_system_col = true;
                }

                // generate scan_projection_mapping
                if ((attr_no == (INT)-1)) {
                    if (load_system_col) {
                        scan_projection_mapping[0].push_back(0);
                        scan_types[0].push_back(duckdb::LogicalType::ID);
                    }
                }
                else {
                    scan_projection_mapping[0].push_back(attr_no);
                    CMDIdGPDB *type_mdid =
                        CMDIdGPDB::CastMdid(col->RetrieveType()->MDId());
                    scan_types[0].push_back(pConvertTypeOidToLogicalType(
                        type_mdid->Oid(), col->TypeModifier()));
                }
            }

            // Construct outer mapping info
            outer_col_map.reserve(outer_cols->Size());
            for (ULONG col_idx = 0; col_idx < outer_cols->Size();
                    col_idx++) {
                CColRef *col = outer_cols->operator[](col_idx);
                ULONG col_id = col->Id();
                // construct outer_col_map
                auto it_ = id_map.find(col_id);
                if (it_ == id_map.end()) {
                    outer_col_map.push_back(
                        std::numeric_limits<uint32_t>::max());
                }
                else {
                    auto id_idx = id_map.at(
                        col_id);  // std::out_of_range exception if col_id does not exist in id_map
                    outer_col_map.push_back(id_idx);
                }
            }
        }
        // reached to the bottom
        if (inner_root->Arity() == 0) {
            break;
        }
        else {
            inner_root =
                inner_root->operator[](0);  // pass first child in linear plan
        }
    }

    D_ASSERT(inner_root != pexprInner);

    // Fallback: construct column mappings when ComputeScalarColumnar is absent.
    //
    // The converter always wraps scans in CLogicalProjectColumnar, which
    // becomes CPhysicalComputeScalarColumnar in physical plans. The loop
    // above builds inner_col_maps/outer_col_map/scan_types only in the
    // ComputeScalarColumnar case, relying on that invariant.
    //
    // However, ORCA may eliminate ComputeScalarColumnar during optimization
    // (e.g., EXISTS subquery decorrelation produces Filter → IndexScan
    // without an intermediate projection). When this happens, the column
    // mappings are left empty and the IdSeek fails at execution time.
    //
    // This fallback applies the same mapping logic using inner_cols directly,
    // mirroring the ComputeScalarColumnar handler above.
    if (inner_col_maps[0].empty() && outer_col_map.empty()) {
        // Build inner_col_maps[0] and scan_types/scan_projection_mapping
        // from inner_cols (the NLJ's required inner output columns).
        bool load_system_col = false;
        for (ULONG i = 0; i < inner_cols->Size(); i++) {
            CColRef *col = inner_cols->operator[](i);
            CColRefTable *colreftbl = (CColRefTable *)col;
            INT attr_no = colreftbl->AttrNum();
            ULONG col_id = col->Id();
            auto it = id_map.find(col_id);
            if (it != id_map.end()) {
                inner_col_maps[0].push_back(it->second);
                if (attr_no == INT(-1))
                    load_system_col = true;
            }

            if (attr_no == (INT)-1) {
                if (load_system_col) {
                    scan_projection_mapping[0].push_back(0);
                    scan_types[0].push_back(duckdb::LogicalType::ID);
                }
            } else {
                scan_projection_mapping[0].push_back(attr_no);
                CMDIdGPDB *type_mdid =
                    CMDIdGPDB::CastMdid(col->RetrieveType()->MDId());
                scan_types[0].push_back(pConvertTypeOidToLogicalType(
                    type_mdid->Oid(), col->TypeModifier()));
            }
        }

        // Build outer_col_map
        outer_col_map.reserve(outer_cols->Size());
        for (ULONG col_idx = 0; col_idx < outer_cols->Size(); col_idx++) {
            CColRef *col = outer_cols->operator[](col_idx);
            ULONG col_id = col->Id();
            auto it_ = id_map.find(col_id);
            if (it_ == id_map.end()) {
                outer_col_map.push_back(
                    std::numeric_limits<uint32_t>::max());
            } else {
                outer_col_map.push_back(id_map.at(col_id));
            }
        }
    }

    D_ASSERT(idxscan_expr != NULL);
    CColRefSet *inner_output_cols = pexprInner->Prpp()->PcrsRequired();
    CColRefSet *idxscan_output_cols = idxscan_expr->Prpp()->PcrsRequired();

    CPhysicalIndexScan *idxscan_op = (CPhysicalIndexScan *)idxscan_expr->Pop();
    CMDIdGPDB *table_mdid = CMDIdGPDB::CastMdid(idxscan_op->Ptabdesc()->MDId());
    OID table_obj_id = table_mdid->Oid();
    oids.push_back(table_obj_id);

    // M28: Expand IdSeek with sibling vertex partitions (operator-level multi-partition).
    // When a vertex label maps to multiple partitions (e.g., Message → Comment + Post),
    // the converter emits only the primary partition's graphlet to ORCA. Here we inject
    // sibling graphlets so IdSeek can look up vertices from any partition.
    {
        auto vp_it = multi_vertex_partitions.find(table_obj_id);
        if (vp_it != multi_vertex_partitions.end()) {
            duckdb::Catalog &cat_instance = context->db->GetCatalog();

            // Get primary graphlet's key_ids to match columns across partitions
            auto *primary_ps = static_cast<duckdb::PropertySchemaCatalogEntry *>(
                cat_instance.GetEntry(*context, DEFAULT_SCHEMA, (duckdb::idx_t)table_obj_id));
            duckdb::PropertyKeyID_vector *primary_keys = primary_ps ? primary_ps->GetKeyIDs() : nullptr;

            // For each scanned column in the primary, record its property name.
            // System column (_id) has type LogicalType::ID; regular columns use their attr_no
            // to look up the property name from the property schema.
            // NOTE: We match by property NAME, not key_id, because different partitions
            // may assign different key_ids to the same property name.
            auto primary_key_names = primary_ps ? primary_ps->GetKeysWithCopy() : vector<string>{};
            vector<string> scanned_prop_names;
            for (size_t sci = 0; sci < scan_projection_mapping[0].size(); sci++) {
                if (sci < scan_types[0].size() &&
                    scan_types[0][sci] == duckdb::LogicalType::ID) {
                    // System _id column
                    scanned_prop_names.push_back("_id");
                } else {
                    auto attr_no = scan_projection_mapping[0][sci];
                    // attr_no is 1-based (0=_id, 1+=properties), key_names is 0-based
                    if (attr_no > 0 && (attr_no - 1) < primary_key_names.size()) {
                        scanned_prop_names.push_back(primary_key_names[attr_no - 1]);
                    } else {
                        scanned_prop_names.push_back("");
                    }
                }
            }

            // Detect virtual partition: primary has sub_partition_oids (no real data).
            // If virtual, replace oids[0] with the first real sub-partition
            // and rebuild its scan mapping, then start siblings from index 1.
            size_t idseek_sib_start = 0;
            {
                auto *primary_part = static_cast<duckdb::PartitionCatalogEntry *>(
                    cat_instance.GetEntry(*context, DEFAULT_SCHEMA,
                                          primary_ps->partition_oid));
                if (primary_part && !primary_part->sub_partition_oids.empty()
                    && !vp_it->second.empty()) {
                    // Virtual partition: replace with first real sub-partition
                    oids[0] = vp_it->second[0];
                    idseek_sib_start = 1;

                    auto *first_ps = static_cast<duckdb::PropertySchemaCatalogEntry *>(
                        cat_instance.GetEntry(*context, DEFAULT_SCHEMA,
                                              (duckdb::idx_t)vp_it->second[0]));
                    auto first_key_names = first_ps ? first_ps->GetKeysWithCopy()
                                                    : vector<string>{};
                    std::unordered_map<string, duckdb::idx_t> first_name_pos;
                    for (duckdb::idx_t k = 0; k < first_key_names.size(); k++)
                        first_name_pos[first_key_names[k]] = k;

                    spdlog::info("[IdSeek-MPV] Virtual partition detected. "
                        "Replacing oids[0]={} with first real sub {}. "
                        "scanned_prop_names.size()={}", table_obj_id,
                        vp_it->second[0], scanned_prop_names.size());
                    // Rebuild scan_projection_mapping[0] and scan_types[0]
                    // for the first real sub-partition.
                    // Save originals before clearing (needed for type lookup).
                    auto orig_scan_types0 = scan_types[0];
                    auto orig_inner_col_map0 = inner_col_maps[0];
                    scan_projection_mapping[0].clear();
                    scan_types[0].clear();
                    vector<uint32_t> first_inner_col_map;
                    for (size_t ci = 0; ci < scanned_prop_names.size(); ci++) {
                        auto &prop_name = scanned_prop_names[ci];
                        if (prop_name == "_id") {
                            scan_projection_mapping[0].push_back(0);
                            scan_types[0].push_back(duckdb::LogicalType::ID);
                            if (ci < orig_inner_col_map0.size())
                                first_inner_col_map.push_back(orig_inner_col_map0[ci]);
                        } else if (!prop_name.empty()) {
                            auto nit = first_name_pos.find(prop_name);
                            if (nit != first_name_pos.end()) {
                                scan_projection_mapping[0].push_back(nit->second + 1);
                                scan_types[0].push_back(
                                    ci < orig_scan_types0.size()
                                        ? orig_scan_types0[ci]
                                        : duckdb::LogicalType::SQLNULL);
                            } else {
                                scan_projection_mapping[0].push_back(
                                    std::numeric_limits<uint64_t>::max());
                                scan_types[0].push_back(duckdb::LogicalType::SQLNULL);
                            }
                            if (ci < orig_inner_col_map0.size())
                                first_inner_col_map.push_back(orig_inner_col_map0[ci]);
                        } else {
                            scan_projection_mapping[0].push_back(
                                std::numeric_limits<uint64_t>::max());
                            scan_types[0].push_back(duckdb::LogicalType::SQLNULL);
                            if (ci < orig_inner_col_map0.size())
                                first_inner_col_map.push_back(orig_inner_col_map0[ci]);
                        }
                    }
                    inner_col_maps[0] = first_inner_col_map;

                    // Debug: log the rebuilt mappings
                    {
                        string spm_str, st_str, icm_str, spn_str;
                        for (auto v : scan_projection_mapping[0])
                            spm_str += std::to_string(v) + " ";
                        for (auto &t : scan_types[0])
                            st_str += std::to_string((int)t.id()) + " ";
                        for (auto v : inner_col_maps[0])
                            icm_str += std::to_string(v) + " ";
                        for (auto &n : scanned_prop_names)
                            spn_str += n + " ";
                        spdlog::info("[IdSeek-MPV] scan_proj=[{}] scan_types=[{}] "
                            "inner_col_map=[{}] prop_names=[{}]",
                            spm_str, st_str, icm_str, spn_str);
                    }
                }
            }

            for (size_t si = idseek_sib_start; si < vp_it->second.size(); si++) {
                auto sib_graphlet_oid = vp_it->second[si];
                oids.push_back(sib_graphlet_oid);

                auto *sib_ps = static_cast<duckdb::PropertySchemaCatalogEntry *>(
                    cat_instance.GetEntry(*context, DEFAULT_SCHEMA, sib_graphlet_oid));
                auto sib_types = sib_ps ? sib_ps->GetTypesWithCopy() : vector<duckdb::LogicalType>{};
                auto sib_key_names = sib_ps ? sib_ps->GetKeysWithCopy() : vector<string>{};

                // Build property name → column position map for sibling
                std::unordered_map<string, duckdb::idx_t> sib_name_pos;
                for (duckdb::idx_t k = 0; k < sib_key_names.size(); k++)
                    sib_name_pos[sib_key_names[k]] = k;

                // Build sibling column mappings by matching property names
                vector<uint32_t> sib_inner_col_map;
                vector<uint64_t> sib_scan_proj;
                vector<duckdb::LogicalType> sib_scan_types;

                for (size_t ci = 0; ci < scanned_prop_names.size(); ci++) {
                    auto &prop_name = scanned_prop_names[ci];
                    if (prop_name == "_id") {
                        // System _id column — always at position 0, type ID
                        sib_scan_proj.push_back(0);
                        sib_scan_types.push_back(duckdb::LogicalType::ID);
                        if (ci < inner_col_maps[0].size())
                            sib_inner_col_map.push_back(inner_col_maps[0][ci]);
                    } else if (!prop_name.empty()) {
                        auto nit = sib_name_pos.find(prop_name);
                        if (nit != sib_name_pos.end()) {
                            // Column exists in sibling at possibly different position.
                            // +1: convert 0-based key pos to 1-based attr_no convention
                            // (ExtentIterator subtracts target_idxs_offset=1 when indexing chunks[]).
                            sib_scan_proj.push_back(nit->second + 1);
                            sib_scan_types.push_back(scan_types[0][ci]);
                        } else {
                            // Column missing in sibling — use NULL sentinel
                            sib_scan_proj.push_back(std::numeric_limits<uint64_t>::max());
                            sib_scan_types.push_back(duckdb::LogicalType::SQLNULL);
                        }
                        if (ci < inner_col_maps[0].size())
                            sib_inner_col_map.push_back(inner_col_maps[0][ci]);
                    } else {
                        // Unknown column — use NULL sentinel
                        sib_scan_proj.push_back(std::numeric_limits<uint64_t>::max());
                        sib_scan_types.push_back(duckdb::LogicalType::SQLNULL);
                        if (ci < inner_col_maps[0].size())
                            sib_inner_col_map.push_back(inner_col_maps[0][ci]);
                    }
                }

                inner_col_maps.push_back(sib_inner_col_map);
                scan_projection_mapping.push_back(sib_scan_proj);
                scan_types.push_back(sib_scan_types);
                // output_projection_mapping must match oids in size (asserted by PhysicalIdSeek ctor)
                output_projection_mapping.push_back(std::vector<uint64_t>());
            }
        }
    }

    // Construct sid_col_idx
    for (auto i = 0; i < num_outer_schemas; i++) {
        bool sid_col_idx_found = false;
        for (ULONG col_idx = 0; col_idx < outer_cols->Size(); col_idx++) {
            CColRef *col = outer_cols->operator[](col_idx);
            ULONG col_id = col->Id();
            // match _tid
            auto it =
                std::find(sccmp_colids.begin(), sccmp_colids.end(), col_id);
            if (it != sccmp_colids.end()) {
                D_ASSERT(!sid_col_idx_found);
                sid_col_idx = col_idx;
                sid_col_idx_found = true;
            }
        }
        D_ASSERT(sid_col_idx_found);
    }

    gpos::ULONG pred_attr_pos, pred_pos;
    duckdb::Value literal_val;
    duckdb::LogicalType pred_attr_type;
    if (has_filter && do_filter_pushdown) {
        CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
        CColRef *lhs_colref_o = col_factory->LookupColRef(
            ((CScalarIdent *)filter_pred_expr->operator[](0)->Pop())
                ->Pcr()
                ->Id());
        CColRefTable *lhs_colref = (CColRefTable *)lhs_colref_o;
        gpos::INT lhs_attrnum = lhs_colref->AttrNum();
        pred_attr_pos = lGetMDAccessor()
                            ->RetrieveRel(lhs_colref->GetMdidTable())
                            ->GetPosFromAttno(lhs_attrnum);
        pred_attr_type = pConvertTypeOidToLogicalType(
            CMDIdGPDB::CastMdid(lhs_colref->RetrieveType()->MDId())->Oid(),
            lhs_colref_o->TypeModifier());
        pred_pos = output_cols->IndexOf((CColRef *)lhs_colref);
        CDatumGenericGPDB *datum =
            (CDatumGenericGPDB
                 *)(((CScalarConst *)filter_pred_expr->operator[](1)->Pop())
                        ->GetDatum());
        literal_val = DatumSerDes::DeserializeOrcaByteArrayIntoDuckDBValue(
            CMDIdGPDB::CastMdid(datum->MDId())->Oid(), datum->TypeModifier(),
            datum->GetByteArrayValue(), (uint64_t)datum->Size());
    }


    // Get human-readable table name for display
    string idseek_display_name = pResolvePartitionName(table_obj_id);

    /* Generate operator and push */
    duckdb::Schema tmp_schema;
    tmp_schema.setStoredTypes(types);

    /* Generate schema flow graph for IdSeek */
    /* Note: to prevent destruction of inner_col_maps due to move, call this before PhysicalIdSeek */
    pBuildSchemaFlowGraphForBinaryOperator(tmp_schema, inner_col_maps.size());
    vector<uint32_t> union_inner_col_map = inner_col_maps[0];
    bool force_output_union = (inner_col_maps.size() > 1);
    if (!do_filter_pushdown) {
        if (has_filter) {
            // Expand per_schema_filter_exprs for sibling schemas (no filter for siblings)
            while (per_schema_filter_exprs.size() < inner_col_maps.size()) {
                per_schema_filter_exprs.push_back(vector<unique_ptr<duckdb::Expression>>());
                filter_col_idxs.push_back(vector<duckdb::idx_t>());
            }
            duckdb::CypherPhysicalOperator *op = new duckdb::PhysicalIdSeek(
                tmp_schema, sid_col_idx, oids, output_projection_mapping,
                outer_col_map, inner_col_maps, union_inner_col_map,
                scan_projection_mapping, scan_types, per_schema_filter_exprs, filter_col_idxs,
                force_output_union, join_type, num_outer_schemas);
            op->display_name = idseek_display_name;
            result->push_back(op);
        }
        else {
            duckdb::CypherPhysicalOperator *op = new duckdb::PhysicalIdSeek(
                tmp_schema, sid_col_idx, oids, output_projection_mapping,
                outer_col_map, inner_col_maps, union_inner_col_map,
                scan_projection_mapping, scan_types, force_output_union, join_type,
                num_outer_schemas);
            op->display_name = idseek_display_name;
            result->push_back(op);
        }
    }
    else {
        D_ASSERT(false);
        duckdb::CypherPhysicalOperator *op = new duckdb::PhysicalIdSeek(
            tmp_schema, sid_col_idx, oids, output_projection_mapping,
            outer_col_map, inner_col_maps, union_inner_col_map,
            scan_projection_mapping, scan_types, false, join_type,
            num_outer_schemas);
        result->push_back(op);
    }

    return result;
}

duckdb::CypherPhysicalOperatorGroups *
Planner::pTransformEopPhysicalInnerIndexNLJoinToIdSeekForUnionAllInner(
    CExpression *plan_expr)
{
    /* Non-root - call single child */
    duckdb::CypherPhysicalOperatorGroups *result =
        pTraverseTransformPhysicalPlan(plan_expr->PdrgPexpr()->operator[](0));

    CDrvdPropPlan *drvd_prop_plan = plan_expr->GetDrvdPropPlan();

    if (pIsJoinRhsOutputPhysicalIdOnly(plan_expr)) {
        pTransformEopPhysicalInnerIndexNLJoinToProjectionForUnionAllInner(
            plan_expr, result);
    }
    else {
        if (drvd_prop_plan->Pos()->UlSortColumns() > 0) {
            pTransformEopPhysicalInnerIndexNLJoinToIdSeekForUnionAllInnerWithSortOrder(
                plan_expr, result);
        }
        else {
            pTransformEopPhysicalInnerIndexNLJoinToIdSeekForUnionAllInnerWithoutSortOrder(
                plan_expr, result);
        }
    }

    return result;
}

void Planner::
    pTransformEopPhysicalInnerIndexNLJoinToIdSeekForUnionAllInnerWithSortOrder(
        CExpression *plan_expr,
        duckdb::CypherPhysicalOperatorGroups *result)
{
    CMemoryPool *mp = this->memory_pool;
    vector<duckdb::LogicalType> types;

    CColRefArray *output_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CExpression *pexprOuter = (*plan_expr)[0];
    CColRefArray *outer_cols = pexprOuter->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CExpression *pexprInner = (*plan_expr)[1];
    CColRefArray *inner_cols = pexprInner->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CColRefSet *outer_inner_cols = GPOS_NEW(mp) CColRefSet(mp, outer_cols);
    outer_inner_cols->Include(pexprInner->Prpp()->PcrsRequired());

    unordered_map<ULONG, uint64_t> id_map;
    vector<uint32_t> outer_col_map;
    vector<vector<uint32_t>> inner_col_maps;
    vector<uint32_t> union_inner_col_map;
    vector<vector<uint64_t>> output_projection_mapping;
    vector<vector<uint64_t>> scan_projection_mapping;
    vector<vector<duckdb::LogicalType>> scan_types;

    duckdb::JoinType join_type = pTranslateJoinType(plan_expr->Pop());

    for (ULONG col_idx = 0; col_idx < output_cols->Size(); col_idx++) {
        CColRef *col = (*output_cols)[col_idx];
        ULONG col_id = col->Id();
        id_map.insert(std::make_pair(col_id, col_idx));
        types.push_back(pGetColumnsDuckDBType(col));
    }

    uint64_t idx_obj_id;  // 230303
    uint64_t sid_col_idx;
    CExpression *inner_root = pexprInner;

    vector<uint64_t> oids;
    vector<uint32_t> sccmp_colids;
    vector<uint32_t> scident_colids;

    bool do_projection_on_idxscan = false;
    bool do_filter_pushdown = false;
    bool has_filter = false;

    while (true) {
        if (inner_root->Pop()->Eopid() ==
            COperator::EOperatorId::EopPhysicalSerialUnionAll) {
            for (uint32_t i = 0; i < inner_root->Arity();
                 i++) {  // for each idx(only)scan expression
                // TODO currently support this pattern type only
                D_ASSERT(
                    inner_root->operator[](i)->Pop()->Eopid() ==
                    COperator::EOperatorId::EopPhysicalComputeScalarColumnar);
                D_ASSERT(
                    inner_root->operator[](i)->operator[](0)->Pop()->Eopid() ==
                    COperator::EOperatorId::EopPhysicalIndexScan);
                D_ASSERT(
                    inner_root->operator[](i)->operator[](1)->Pop()->Eopid() ==
                    COperator::EOperatorId::EopScalarProjectList);

                CExpression *unionall_expr = inner_root;
                CExpression *inner_idxscan_expr =
                    inner_root->operator[](i)->operator[](0);
                CExpression *projectlist_expr =
                    inner_root->operator[](i)->operator[](1);

                // Get JoinColumnID
                for (uint32_t j = 0;
                     j < inner_idxscan_expr->operator[](0)->Arity(); j++) {
                    CScalarIdent *sc_ident =
                        (CScalarIdent *)(inner_idxscan_expr->operator[](0)
                                             ->
                                             operator[](j)
                                             ->Pop());
                    sccmp_colids.push_back(sc_ident->Pcr()->Id());
                }

                D_ASSERT(inner_idxscan_expr != NULL);
                CColRefSet *unionall_output_cols =
                    unionall_expr->Prpp()->PcrsRequired();
                CColRefSet *inner_output_cols =
                    pexprInner->Prpp()->PcrsRequired();
                CColRefSet *idxscan_output_cols =
                    inner_idxscan_expr->Prpp()->PcrsRequired();
                // D_ASSERT(idxscan_output_cols->ContainsAll(inner_output_cols));

                CPhysicalIndexScan *idxscan_op =
                    (CPhysicalIndexScan *)inner_idxscan_expr->Pop();
                OID table_obj_id =
                    CMDIdGPDB::CastMdid(idxscan_op->Ptabdesc()->MDId())->Oid();
                oids.push_back(table_obj_id);

                // oids / projection_mapping
                vector<uint64_t> output_ident_mapping;
                pGenerateScanMapping(table_obj_id,
                                     inner_output_cols->Pdrgpcr(mp),
                                     output_ident_mapping);
                D_ASSERT(output_ident_mapping.size() ==
                         inner_output_cols->Size());
                output_projection_mapping.push_back(output_ident_mapping);
                vector<duckdb::LogicalType> output_types;
                pGenerateTypes(inner_output_cols->Pdrgpcr(mp), output_types);
                D_ASSERT(output_types.size() == output_ident_mapping.size());

                // scan projection mapping - when doing filter pushdown, two mappings MAY BE different.
                vector<uint64_t> scan_ident_mapping;
                vector<duckdb::LogicalType> scan_type;

                bool load_system_col = false;
                bool sid_col_idx_found = false;

                inner_col_maps.push_back(std::vector<uint32_t>());

                if (i == 0) {
                    for (uint32_t j = 0; j < projectlist_expr->Arity(); j++) {
                        D_ASSERT(
                            projectlist_expr->operator[](j)->Pop()->Eopid() ==
                            COperator::EOperatorId::EopScalarProjectElement);
                        CScalarProjectElement *proj_elem =
                            (CScalarProjectElement
                                 *)(projectlist_expr->operator[](j)->Pop());
                        CColRefTable *proj_col =
                            (CColRefTable *)proj_elem->Pcr();
                        auto it = id_map.find(proj_col->Id());
                        if (it != id_map.end()) {
                            union_inner_col_map.push_back(it->second);
                            if (proj_col->AttrNum() == INT(-1))
                                load_system_col = true;
                        }
                    }
                }

                // Construct inner mapping, scan projection mapping, scan type infos
                for (uint32_t j = 0; j < projectlist_expr->Arity(); j++) {
                    D_ASSERT(projectlist_expr->operator[](j)->Pop()->Eopid() ==
                             COperator::EOperatorId::EopScalarProjectElement);
                    CScalarProjectElement *proj_elem =
                        (CScalarProjectElement
                             *)(projectlist_expr->operator[](j)->Pop());
                    CColRefTable *proj_col = (CColRefTable *)proj_elem->Pcr();

                    if (projectlist_expr->operator[](j)
                            ->
                            operator[](0)
                            ->Pop()
                            ->Eopid() ==
                        COperator::EOperatorId::EopScalarIdent) {
                        // non-null column
                        // This logic is built on the assumption that all columns except the system column will be included in the seek output
                        if (load_system_col) {
                            inner_col_maps[i].push_back(union_inner_col_map[j]);
                        }
                        else if (!load_system_col && (j != 0)) {
                            inner_col_maps[i].push_back(
                                union_inner_col_map[j - 1]);
                        }

                        INT attr_no = proj_col->AttrNum();
                        if ((attr_no == (INT)-1)) {
                            if (load_system_col) {
                                scan_ident_mapping.push_back(0);
                                scan_type.push_back(duckdb::LogicalType::ID);
                            }
                        }
                        else {
                            scan_ident_mapping.push_back(attr_no);
                            CMDIdGPDB *type_mdid = CMDIdGPDB::CastMdid(
                                proj_col->RetrieveType()->MDId());
                            OID type_oid = type_mdid->Oid();
                            INT type_mod = proj_elem->Pcr()->TypeModifier();
                            scan_type.push_back(pConvertTypeOidToLogicalType(
                                type_oid, type_mod));
                        }
                    }
                    else if (projectlist_expr->operator[](j)
                                 ->
                                 operator[](0)
                                 ->Pop()
                                 ->Eopid() ==
                             COperator::EOperatorId::EopScalarConst) {
                        // null column
                    }
                    else {
                        GPOS_ASSERT(false);
                        throw duckdb::InvalidInputException(
                            "Project element types other than ident & const is "
                            "not desired");
                    }
                }

                scan_projection_mapping.push_back(scan_ident_mapping);
                scan_types.push_back(std::move(scan_type));

                // Construct outer mapping info
                if (i == 0) {
                    for (ULONG col_idx = 0; col_idx < outer_cols->Size();
                        col_idx++) {
                        CColRef *col = outer_cols->operator[](col_idx);
                        ULONG col_id = col->Id();
                        // match _tid
                        auto it = std::find(sccmp_colids.begin(),
                                            sccmp_colids.end(), col_id);
                        if (it != sccmp_colids.end()) {
                            D_ASSERT(!sid_col_idx_found);
                            sid_col_idx = col_idx;
                            sid_col_idx_found = true;
                        }
                        // construct outer_col_map
                        auto it_ = id_map.find(col_id);
                        if (it_ == id_map.end()) {
                            outer_col_map.push_back(
                                std::numeric_limits<uint32_t>::max());
                        }
                        else {
                            auto id_idx = id_map.at(
                                col_id);  // std::out_of_range exception if col_id does not exist in id_map
                            outer_col_map.push_back(id_idx);
                        }
                    }
                    D_ASSERT(sid_col_idx_found);
                }
            }
        }

        // reached to the bottom
        if (inner_root->Arity() == 0) {
            break;
        }
        else {
            inner_root =
                inner_root->operator[](0);  // pass first child in linear plan
        }
    }

    gpos::ULONG pred_attr_pos, pred_pos;
    duckdb::Value literal_val;
    duckdb::LogicalType pred_attr_type;
    if (has_filter && do_filter_pushdown) {
        GPOS_ASSERT(false);
        throw NotImplementedException("InnerIdxNLJoin for Filter case");
    }

    /* Generate operator and push */
    duckdb::Schema tmp_schema;
    tmp_schema.setStoredTypes(types);

    if (!do_filter_pushdown) {
        if (has_filter) {
            GPOS_ASSERT(false);
            throw NotImplementedException("InnerIdxNLJoin for Filter case");
        }
        else {
            size_t n_outer = pGetNumOuterSchemas();
            duckdb::CypherPhysicalOperator *op = new duckdb::PhysicalIdSeek(
                tmp_schema, sid_col_idx, oids, output_projection_mapping,
                outer_col_map, inner_col_maps, union_inner_col_map,
                scan_projection_mapping, scan_types, true, join_type,
                n_outer);
            result->push_back(op);
        }
    }
    else {
        GPOS_ASSERT(false);
        throw NotImplementedException("InnerIdxNLJoin for Filter case");
    }

    pBuildSchemaFlowGraphForUnaryOperator(tmp_schema);
}

// TODO: Merge with pTransformEopPhysicalInnerIndexNLJoinToIdSeekForUnionAllInnerWithSortOrder
// Currently, DSI results in single tables goes to that function
// While DSI results in UNION ALL goes to this function
void Planner::
    pTransformEopPhysicalInnerIndexNLJoinToIdSeekForUnionAllInnerWithoutSortOrder(
        CExpression *plan_expr,
        duckdb::CypherPhysicalOperatorGroups *result)
{
    CMemoryPool *mp = this->memory_pool;

    const int REPR_IDX = 0;
    CExpression *outer_expr = (*plan_expr)[0];
    CExpression *unionall_expr = (*plan_expr)[1];
    CExpressionArray *projections = unionall_expr->PdrgPexpr();
    const ULONG num_projections = projections->Size();

    // Filter related variables
    CColRefArray *pushed_filter_output_cols  = GPOS_NEW(mp) CColRefArray(mp);
    bool seperate_filter_from_condition = false;

    // Operator Parameters
    uint64_t sid_col_idx = std::numeric_limits<uint64_t>::max();
    vector<uint64_t> oids;
    vector<uint32_t> union_inner_col_map;
    vector<uint32_t> outer_col_map;
    vector<vector<uint32_t>> inner_col_maps;
    vector<vector<uint64_t>> scan_projection_mapping;
    vector<vector<uint64_t>> projection_mapping;
    vector<vector<duckdb::LogicalType>> scan_types;
    vector<unique_ptr<duckdb::Expression>> condition_filter_duckdb_exprs;
    vector<unique_ptr<duckdb::Expression>> pushed_filter_duckdb_exprs;
    duckdb::JoinType join_type = pTranslateJoinType(plan_expr->Pop());

    // Outers
    CColRefArray *outer_cols = outer_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);

    // Goal output
    vector<duckdb::LogicalType> seek_output_types;
    CColRefArray *seek_output_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    pGetColumnsDuckDBType(seek_output_cols, seek_output_types);
    CColRefArray *final_output_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);

    // Construct filter expression, if exists
    CExpression *repr_proj_expr = projections->operator[](REPR_IDX);
    CExpression *repr_filter_expr = pFindFilterExpr(repr_proj_expr);
    bool has_filter = repr_filter_expr != NULL;
    bool do_filter_pushdown = false;
    bool has_filter_only_column = false;
    if (has_filter) {
        CExpression *repr_filter_pred_expr = repr_filter_expr->operator[](1);
        CColRefArray *inner_cols = 
            repr_proj_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
        CColRefArray *scan_cols =
            repr_filter_expr->operator[](0)->Prpp()->PcrsRequired()->Pdrgpcr(mp);

        vector<ULONG> inner_filter_only_cols_idx;
        pGetFilterOnlyInnerColsIdx(
            repr_filter_pred_expr, scan_cols /* all inner cols */,
            inner_cols /* output inner cols */, inner_filter_only_cols_idx);
        has_filter_only_column = inner_filter_only_cols_idx.size() > 0;

        pushed_filter_output_cols->AppendArray(seek_output_cols);
        for (auto col_idx : inner_filter_only_cols_idx) {
            CColRef *col = scan_cols->operator[](col_idx);
            pushed_filter_output_cols->Append(col);
            seek_output_cols->Append(col);
            seek_output_types.push_back(pGetColumnsDuckDBType(col));
        }

        pGetFilterDuckDBExprs(repr_filter_expr, pushed_filter_output_cols, nullptr, 0, pushed_filter_duckdb_exprs);
    }

    // Construct outer_col_map
    for (int outer_col_idx = 0; outer_col_idx < outer_cols->Size(); outer_col_idx++) {
        CColRef *col = outer_cols->operator[](outer_col_idx);
        auto col_idx = seek_output_cols->IndexOf(col);
        if (col_idx != gpos::ulong_max) {
            outer_col_map.push_back(col_idx);
        }
        else {
            outer_col_map.push_back(std::numeric_limits<uint32_t>::max());
        }
    }

    // Check skip seek case (do we need this? TODO)
    CExpression *repr_proj_list_expr = repr_proj_expr->PdrgPexpr()->operator[](1);
    if (repr_proj_list_expr->Arity() == 1) {
        CExpression *proj_elem_expr = repr_proj_list_expr->operator[](0);
        CScalarProjectElement *proj_elem = (CScalarProjectElement *)(proj_elem_expr->Pop());
        CColRefTable *proj_col = (CColRefTable *)proj_elem->Pcr();
        if (pIsPhysicalIdCol(proj_col)) {
            duckdb::Schema schema_proj;
            vector<duckdb::LogicalType> output_types_proj;
            vector<unique_ptr<duckdb::Expression>> proj_exprs;
            pGetDuckDBTypesFromColRefs(seek_output_cols, output_types_proj);
            schema_proj.setStoredTypes(output_types_proj);
            pGetProjectionExprs(outer_cols, seek_output_cols,
                                output_types_proj, proj_exprs);
            if (proj_exprs.size() != 0) {
                duckdb::CypherPhysicalOperator *duckdb_proj_op =
                    new duckdb::PhysicalProjection(schema_proj,
                                                    move(proj_exprs));
                result->push_back(duckdb_proj_op);
                pBuildSchemaFlowGraphForUnaryOperator(schema_proj);
            }
            return;
        }
    }

    // Construct output for each projections
    bool load_physical_id_col = false; 
    for (int i = 0; i < num_projections; i++) {
        CExpression *projection_expr = projections->operator[](i);
        CExpression *proj_list_expr = projection_expr->PdrgPexpr()->operator[](1);
        CExpression *filter_expr = pFindFilterExpr(projection_expr);
        bool has_filter = filter_expr != NULL;
        CExpression *scan_expr = has_filter ? filter_expr->operator[](0) : projection_expr->operator[](0);

        CIndexDescriptor *index_desc = NULL;
        if (scan_expr->Pop()->Eopid() == COperator::EOperatorId::EopPhysicalIndexScan) {
            auto *idxscan_op = (CPhysicalIndexScan *)scan_expr->Pop();
            index_desc = idxscan_op->Pindexdesc();
        }
        else if (scan_expr->Pop()->Eopid() == COperator::EOperatorId::EopPhysicalIndexOnlyScan) {
            auto *idxonlyscan_op = (CPhysicalIndexOnlyScan *)scan_expr->Pop();
            index_desc = idxonlyscan_op->Pindexdesc();
        }
        else {
            GPOS_ASSERT(false);
        }

        // Construct union_inner_col_map and sid col idx
        const duckdb::idx_t physical_id_col_idx = 0;
        if (i == REPR_IDX) {
            size_t num_filter_only_cols = 0;
            for (int j = 0; j < proj_list_expr->PdrgPexpr()->Size(); j++) {
                CExpression *proj_elem_expr = proj_list_expr->PdrgPexpr()->operator[](j);
                CScalarProjectElement *proj_elem = (CScalarProjectElement *)(proj_elem_expr->Pop());
                CColRefTable *proj_col = (CColRefTable *)proj_elem->Pcr();
                auto col_idx = seek_output_cols->IndexOf(proj_col);

                if (col_idx != gpos::ulong_max) { // in the output
                    union_inner_col_map.push_back(col_idx);
                    if (pIsPhysicalIdCol(proj_col)) {
                        load_physical_id_col = true;
                    }
                }
                else { // not in the output
                    if (!pIsPhysicalIdCol(proj_col)) {
                        // Filter-only columns are appended to the output columns.
                        union_inner_col_map.push_back(seek_output_cols->Size() + num_filter_only_cols);
                        num_filter_only_cols++;
                    }
                    // Assumption: _id is not used in the filter
                }
            }

            // Construct sid col idx (and filter exprs, if needs)
            CExpression *seek_condition = scan_expr->operator[](0);
            if (pIsComplexCondition(seek_condition)) {
                D_ASSERT(seek_condition->Arity() == 2); 
                seperate_filter_from_condition = true;                
                CExpression *condition_filter_expr =  CUtils::PexprScalarCmp(
                        mp,
                        seek_condition->operator[](0)->operator[](1),
                        seek_condition->operator[](1)->operator[](1),
                        IMDType::ECmpType::EcmptEq);
                pGetFilterDuckDBExprs(condition_filter_expr, outer_cols, nullptr, 0, condition_filter_duckdb_exprs);
                pFindOperandsColIdxs(seek_condition->operator[](0), outer_cols, sid_col_idx);
            }
            else {
                pFindOperandsColIdxs(seek_condition, outer_cols, sid_col_idx);
            }
            GPOS_ASSERT(sid_col_idx != std::numeric_limits<uint64_t>::max());
        }

        bool is_dsi_table = index_desc->IsInstanceDescriptor();

        if (is_dsi_table) {
            IMdIdArray *mdid_array = index_desc->GetTableIdsInGroup();
            vector<duckdb::idx_t> col_prop_ids;
            vector<duckdb::LogicalType> global_types;

            // Abstract DSI tables as a single table, construct column infos
            for (int j = 0; j < proj_list_expr->Arity(); j++) {
                CExpression *proj_elem_expr = proj_list_expr->PdrgPexpr()->operator[](j);
                CExpression *scalar_expr = proj_elem_expr->operator[](0);
                CScalarProjectElement *proj_elem = (CScalarProjectElement *)(proj_elem_expr->Pop());
                if (scalar_expr->Pop()->Eopid() == COperator::EOperatorId::EopScalarIdent) {
                    col_prop_ids.push_back(proj_elem->Pcr()->PropId());
                    global_types.push_back(pGetColumnsDuckDBType(proj_elem->Pcr()));
                }
            }

            // For each DSI table, construct outputs
            for (int j = 0; j < mdid_array->Size(); j++) {
                inner_col_maps.emplace_back();
                scan_types.emplace_back();
                scan_projection_mapping.emplace_back();
                projection_mapping.emplace_back();

                CMDIdGPDB *table_mdid = CMDIdGPDB::CastMdid((*mdid_array)[j]);
                OID table_obj_id = table_mdid->Oid();
                oids.push_back(table_obj_id);

                duckdb::Catalog &cat_instance = context->db->GetCatalog();
                duckdb::PropertySchemaCatalogEntry *ps_cat =
                    (duckdb::PropertySchemaCatalogEntry *)cat_instance.GetEntry(
                        *context, DEFAULT_SCHEMA, table_obj_id);
                duckdb::PropertyKeyID_vector *key_ids = ps_cat->GetPropKeyIDs();

                pGenerateMappingInfo(col_prop_ids, key_ids, global_types, scan_types.back(), 
                                    union_inner_col_map, inner_col_maps.back(),
                                    projection_mapping.back(), scan_projection_mapping.back(),
                                    load_physical_id_col);
            }
        }
        else {
            OID table_obj_id = pGetTableOidFromScanExpr(scan_expr);
            oids.push_back(table_obj_id);
            inner_col_maps.emplace_back();
            scan_types.emplace_back();
            scan_projection_mapping.emplace_back();
            projection_mapping.emplace_back();

            // Construct inner_col_maps, scan_projection_mapping, projection_mapping, scan_types
            for (int j = 0; j < proj_list_expr->Arity(); j++) {
                CExpression *proj_elem_expr = proj_list_expr->PdrgPexpr()->operator[](j);
                CExpression *scalar_expr = proj_elem_expr->operator[](0);
                CScalarProjectElement *proj_elem = (CScalarProjectElement *)(proj_elem_expr->Pop());
                CColRefTable *proj_col = (CColRefTable *)proj_elem->Pcr();

                if (scalar_expr->Pop()->Eopid() == COperator::EOperatorId::EopScalarIdent) {
                    // construct scan_projection_mapping, projection_mapping, scan_types
                    if (pIsPhysicalIdCol(proj_col)) {
                        if (load_physical_id_col) {   
                            scan_types.back().push_back(duckdb::LogicalType::ID);
                            scan_projection_mapping.back().push_back(physical_id_col_idx);
                            projection_mapping.back().push_back(j);
                        }
                    }
                    else {
                        scan_types.back().push_back(pGetColumnsDuckDBType(proj_col));
                        scan_projection_mapping.back().push_back(proj_col->AttrNum());
                        projection_mapping.back().push_back(j);
                    }

                    // construct inner_col_map
                    if (load_physical_id_col) {
                        inner_col_maps.back().push_back(union_inner_col_map[j]);
                    }
                    else if (!load_physical_id_col && (j != physical_id_col_idx)) {
                        inner_col_maps.back().push_back(union_inner_col_map[j - 1]);
                    }
                }
                else if (scalar_expr->Pop()->Eopid() == COperator::EOperatorId::EopScalarConst) {
                    // Null column: skip this
                }
                else {
                    GPOS_ASSERT(false);
                }
            }
        }
    }

    /* Generate operators */
    if (has_filter && do_filter_pushdown) {
        GPOS_ASSERT(false);
        throw NotImplementedException("InnerIdxNLJoin for Filter case");
    }

    if (seperate_filter_from_condition) {
        vector<duckdb::LogicalType> output_types_condition_filter;
        pGetDuckDBTypesFromColRefs(outer_cols, output_types_condition_filter);
        duckdb::Schema schema_condition_filter;
        schema_condition_filter.setStoredTypes(output_types_condition_filter);
        duckdb::CypherPhysicalOperator *duckdb_filter_op =
            new duckdb::PhysicalFilter(schema_condition_filter, move(condition_filter_duckdb_exprs));
        result->push_back(duckdb_filter_op);
        pBuildSchemaFlowGraphForUnaryOperator(schema_condition_filter);
    }

    duckdb::Schema seek_schema;
    seek_schema.setStoredTypes(seek_output_types);

    if (generate_sfg) {
        vector<duckdb::Schema> prev_local_schemas = pipeline_schemas.back();
        auto &num_schemas_of_childs_prev = num_schemas_of_childs.back();
        duckdb::idx_t num_total_schemas_prev = 1;
        for (auto i = 0; i < num_schemas_of_childs_prev.size(); i++) {
            num_total_schemas_prev *= num_schemas_of_childs_prev[i];
        }
        pipeline_operator_types.push_back(duckdb::OperatorType::BINARY);
        num_schemas_of_childs.push_back(
            {num_total_schemas_prev, inner_col_maps.size()});
        pipeline_schemas.push_back(prev_local_schemas);
        pipeline_union_schema.push_back(seek_schema);
    }

    size_t num_outer_schemas = pGetNumOuterSchemas();
    if (!do_filter_pushdown) {
        duckdb::CypherPhysicalOperator *op = new duckdb::PhysicalIdSeek(
            seek_schema, sid_col_idx, oids, projection_mapping,
            outer_col_map, inner_col_maps, union_inner_col_map,
            scan_projection_mapping, scan_types, false, join_type,
            num_outer_schemas);
        result->push_back(op);

        // Construct filter
        if (has_filter) {
            duckdb::Schema schema_filter;
            vector<duckdb::LogicalType> output_types_filter;
            pGetDuckDBTypesFromColRefs(pushed_filter_output_cols, output_types_filter);
            schema_filter.setStoredTypes(output_types_filter);
            duckdb::CypherPhysicalOperator *duckdb_filter_op =
                new duckdb::PhysicalFilter(schema_filter, move(pushed_filter_duckdb_exprs));
            result->push_back(duckdb_filter_op);
            pBuildSchemaFlowGraphForUnaryOperator(schema_filter);

            // Construct projection
            if (has_filter_only_column) {
                duckdb::Schema schema_proj;
                vector<duckdb::LogicalType> output_types_proj;
                vector<unique_ptr<duckdb::Expression>> proj_exprs;
                pGetDuckDBTypesFromColRefs(final_output_cols, output_types_proj);
                schema_proj.setStoredTypes(output_types_proj);
                pGetProjectionExprs(pushed_filter_output_cols, final_output_cols,
                                    output_types_proj, proj_exprs);
                if (proj_exprs.size() != 0) {
                    duckdb::CypherPhysicalOperator *duckdb_proj_op =
                        new duckdb::PhysicalProjection(schema_proj,
                                                       move(proj_exprs));
                    result->push_back(duckdb_proj_op);
                    pBuildSchemaFlowGraphForUnaryOperator(schema_proj);
                }
            }
        }
    }
    else {
        GPOS_ASSERT(false);
        throw NotImplementedException("InnerIdxNLJoin for Filter case");
    }
}


duckdb::CypherPhysicalOperatorGroups *
Planner::pTransformEopPhysicalInnerIndexNLJoinToIdSeekDSI(CExpression *plan_expr)
{
    CMemoryPool *mp = this->memory_pool;

    /* Non-root - call single child */
    duckdb::CypherPhysicalOperatorGroups *result =
        pTraverseTransformPhysicalPlan(plan_expr->PdrgPexpr()->operator[](0));

    vector<duckdb::LogicalType> types;

    CColRefArray *output_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CExpression *pexprOuter = (*plan_expr)[0];
    CColRefArray *outer_cols = pexprOuter->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CExpression *pexprInner = (*plan_expr)[1];
    CColRefArray *inner_cols = pexprInner->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CColRefSet *outer_inner_cols = GPOS_NEW(mp) CColRefSet(mp, outer_cols);
    outer_inner_cols->Include(pexprInner->Prpp()->PcrsRequired());
    CColRefArray *filter_output_cols  = GPOS_NEW(mp) CColRefArray(mp);
    CColRefArray *join_cond_cols = GPOS_NEW(mp) CColRefArray(mp);

    unordered_map<ULONG, uint64_t> id_map;
    vector<uint32_t> outer_col_map;
    vector<vector<uint32_t>> inner_col_maps;
    vector<uint32_t> union_inner_col_map;
    vector<vector<uint64_t>> output_projection_mapping;
    vector<vector<uint64_t>> scan_projection_mapping;
    vector<vector<duckdb::LogicalType>> scan_types;

    duckdb::JoinType join_type = pTranslateJoinType(plan_expr->Pop());

    for (ULONG col_idx = 0; col_idx < output_cols->Size(); col_idx++) {
        CColRef *col = (*output_cols)[col_idx];
        ULONG col_id = col->Id();
        id_map.insert(std::make_pair(col_id, col_idx));
        types.push_back(pGetColumnsDuckDBType(col));
    }

    uint64_t idx_obj_id;  // 230303
    uint64_t sid_col_idx;
    CExpression *inner_root = pexprInner;

    vector<uint64_t> oids;
    vector<uint32_t> sccmp_colids;
    vector<uint32_t> scident_colids;

    bool do_projection_on_idxscan = false;
    bool do_filter_pushdown = false;
    bool has_filter = false;
    bool has_filter_only_column = false;
    bool skip_seek = false; // when only scan _id column
    size_t num_filter_only_col = 0;

    // Cycle handling
    bool construct_filter_for_cycle = false;
    vector<unique_ptr<duckdb::Expression>> cycle_filter_duckdb_exprs;

    CExpression *scalar_cmp_expr = NULL;
    CExpression *filter_expr = NULL;
    CExpression *filter_pred_expr = NULL;
    CExpression *idxscan_expr = NULL;
    vector<unique_ptr<duckdb::Expression>> filter_duckdb_exprs;
    vector<vector<duckdb::idx_t>> filter_col_idxs;
    vector<vector<ULONG>> inner_col_ids;
    vector<duckdb::LogicalType> scan_type_union;

    // TODO improve this logic
    while (true) {
        if (inner_root->Pop()->Eopid() ==
            COperator::EOperatorId::EopPhysicalComputeScalarColumnar)
        {
            D_ASSERT(inner_root->operator[](0)->Pop()->Eopid() ==
                         COperator::EOperatorId::EopPhysicalIndexScan ||
                     inner_root->operator[](0)->Pop()->Eopid() ==
                         COperator::EOperatorId::EopPhysicalFilter);
            
            if (inner_root->operator[](0)->Pop()->Eopid() ==
                COperator::EOperatorId::EopPhysicalFilter) {
                has_filter = true;
            }

            CExpression *idxscan_expr =
                has_filter ? inner_root->operator[](0)->operator[](0)
                           : inner_root->operator[](0);

            if (has_filter) {
                filter_expr = inner_root->operator[](0);
                filter_pred_expr = filter_expr->operator[](1);
                CColRefArray *inner_cols = inner_root->Prpp()->PcrsRequired()->Pdrgpcr(mp);
                CColRefArray *idxscan_cols =
                    filter_expr->operator[](0)->Prpp()->PcrsRequired()->Pdrgpcr(mp);

                filter_output_cols->AppendArray(output_cols);

                // Get filter only columns in idxscan_cols
                vector<ULONG> inner_filter_only_cols_idx;
                pGetFilterOnlyInnerColsIdx(
                    filter_pred_expr, idxscan_cols /* all inner cols */,
                    inner_cols /* output inner cols */, inner_filter_only_cols_idx);

                for (auto col_idx : inner_filter_only_cols_idx) {
                    CColRef *col = idxscan_cols->operator[](col_idx);
                    filter_output_cols->Append(col);
                    // update types
                    ULONG col_id = col->Id();
                    id_map.insert(std::make_pair(col_id, col_idx));
                    types.push_back(pGetColumnsDuckDBType(col));
                }
            }

            // Handle cycle case (where the IdSeek condition have AND)
            CExpression *seek_condition = idxscan_expr->operator[](0);
            if (seek_condition->Pop()->Eopid() == COperator::EOperatorId::EopScalarBoolOp) {
                D_ASSERT(seek_condition->Arity() == 2); 
                construct_filter_for_cycle = true;
                CExpression *cycle_filter_expr =  CUtils::PexprScalarCmp(
                    mp, 
                    seek_condition->operator[](0)->operator[](1),
                    seek_condition->operator[](1)->operator[](1),
                    IMDType::ECmpType::EcmptEq);
                pGetFilterDuckDBExprs(cycle_filter_expr, outer_cols, nullptr, 0, cycle_filter_duckdb_exprs);
            }

            // Get JoinColumnID
            scalar_cmp_expr = construct_filter_for_cycle ? seek_condition->operator[](0) : seek_condition;
            for (uint32_t j = 0; j < scalar_cmp_expr->Arity();
                 j++) {
                CScalarIdent *sc_ident =
                    (CScalarIdent
                         *)(scalar_cmp_expr->operator[](j)->Pop());
                sccmp_colids.push_back(sc_ident->Pcr()->Id());
            }

            bool load_system_col = false;
            CExpression *projectlist_expr = inner_root->operator[](1);

            // first, check if we can skip seek
            if (projectlist_expr->Arity() == 1) {
                CScalarProjectElement *proj_elem =
                    (CScalarProjectElement
                         *)(projectlist_expr->operator[](0)->Pop());
                CColRefTable *proj_col = (CColRefTable *)proj_elem->Pcr();
                if (proj_col->AttrNum() == INT(-1)) {
                    skip_seek = true;
                }
            }
            if (skip_seek) break;
            
            for (uint32_t j = 0; j < projectlist_expr->Arity(); j++) {
                D_ASSERT(
                    projectlist_expr->operator[](j)->Pop()->Eopid() ==
                    COperator::EOperatorId::EopScalarProjectElement);
                CExpression *proj_elem_expr =
                    projectlist_expr->operator[](j);
                CScalarProjectElement *proj_elem =
                    (CScalarProjectElement
                            *)(projectlist_expr->operator[](j)->Pop());
                CColRefTable *proj_col =
                    (CColRefTable *)proj_elem->Pcr();
                auto it = id_map.find(proj_col->Id());

                if (it != id_map.end()) {
                    union_inner_col_map.push_back(it->second);
                    if (proj_col->AttrNum() == INT(-1))
                        load_system_col = true;
                }
                else {
                    if (proj_col->AttrNum() != INT(-1)) {
                        /**
                         * Note: Filter-only columns are appended to the output columns.
                         * IdSeek finds out filter-only columns using inner_col_map
                         * and then appends them to the output columns to generate temp chunk.
                        */
                        union_inner_col_map.push_back(
                            output_cols->Size() + num_filter_only_col);
                        num_filter_only_col++;
                        has_filter_only_column = true;
                    }
                }
            }

            CPhysicalIndexScan *idxscan_op =
                (CPhysicalIndexScan *)idxscan_expr->Pop();
            CIndexDescriptor *index_desc = idxscan_op->Pindexdesc();

            D_ASSERT(index_desc->IsInstanceDescriptor());
            IMdIdArray *table_ids =
                index_desc->GetTableIdsInGroup();
            duckdb::Catalog &cat_instance = context->db->GetCatalog();
            
            // build 
            for (ULONG i = 0; i < table_ids->Size(); i++) {
                CMDIdGPDB *mdid = CMDIdGPDB::CastMdid((*table_ids)[i]);
                OID table_obj_id = mdid->Oid();
                oids.push_back(table_obj_id);

                duckdb::PropertySchemaCatalogEntry *ps_cat =
                    (duckdb::PropertySchemaCatalogEntry *)cat_instance.GetEntry(
                        *context, DEFAULT_SCHEMA, table_obj_id);
                duckdb::PropertyKeyID_vector *prop_key_ids = ps_cat->GetPropKeyIDs();

                // scan projection mapping - when doing filter pushdown, two mappings MAY BE different.
                vector<uint64_t> scan_ident_mapping;
                vector<duckdb::LogicalType> scan_type;

                inner_col_maps.push_back(std::vector<uint32_t>());

                // projection mapping (output to scan table mapping)
                vector<uint64_t> output_ident_mapping;

                // Construct inner mapping, scan projection mapping, scan type infos
                for (uint32_t j = 0; j < projectlist_expr->Arity(); j++) {
                    D_ASSERT(projectlist_expr->operator[](j)->Pop()->Eopid() ==
                             COperator::EOperatorId::EopScalarProjectElement);
                    CExpression *proj_elem_expr =
                        projectlist_expr->operator[](j);
                    CScalarProjectElement *proj_elem =
                        (CScalarProjectElement
                             *)(projectlist_expr->operator[](j)->Pop());
                    CColRefTable *proj_col = (CColRefTable *)proj_elem->Pcr();
                    ULONG col_prop_id = proj_col->PropId();
                    bool found = false;
                    duckdb::idx_t prop_key_idx;
                    if (col_prop_id == 0) {
                        // "_id" col
                        found = true;
                    } else {
                        // TODO inefficient
                        for (ULONG k = 0; k < prop_key_ids->size(); k++) {
                            if (col_prop_id == (*prop_key_ids)[k]) {
                                prop_key_idx = k + 1;
                                found = true;
                                break;
                            }
                        }
                    }
                    if (!found) continue;

                    if (projectlist_expr->operator[](j)
                            ->
                            operator[](0)
                            ->Pop()
                            ->Eopid() ==
                        COperator::EOperatorId::EopScalarIdent) {
                        // we need to identify this column exists

                        // build inner_col_maps
                        if (load_system_col) {
                            inner_col_maps[i].push_back(union_inner_col_map[j]);
                        }
                        else if (!load_system_col && (j != 0)) {
                            inner_col_maps[i].push_back(
                                union_inner_col_map[j - 1]);
                        }

                        // build scan projection mapping, projection mapping, scan_type
                        INT attr_no = proj_col->AttrNum();
                        if ((attr_no == (INT)-1)) {
                            if (load_system_col) {
                                scan_ident_mapping.push_back(0);
                                output_ident_mapping.push_back(j);
                                scan_type.push_back(duckdb::LogicalType::ID);
                            }
                        }
                        else {
                            scan_ident_mapping.push_back(prop_key_idx);
                            output_ident_mapping.push_back(j);
                            CMDIdGPDB *type_mdid = CMDIdGPDB::CastMdid(
                                proj_col->RetrieveType()->MDId());
                            OID type_oid = type_mdid->Oid();
                            INT type_mod = proj_elem->Pcr()->TypeModifier();
                            scan_type.push_back(pConvertTypeOidToLogicalType(
                                type_oid, type_mod));
                        }
                    }
                    else if (projectlist_expr->operator[](j)
                                 ->
                                 operator[](0)
                                 ->Pop()
                                 ->Eopid() ==
                             COperator::EOperatorId::EopScalarConst) {
                        // null column
                    }
                    else {
                        GPOS_ASSERT(false);
                        throw duckdb::InvalidInputException(
                            "Project element types other than ident & const is "
                            "not desired");
                    }
                }

                scan_projection_mapping.push_back(scan_ident_mapping);
                scan_types.push_back(std::move(scan_type));
                output_projection_mapping.push_back(output_ident_mapping);
                // inner_col_ids.push_back(inner_col_id);
            }

            // Construct outer mapping info
            outer_col_map.reserve(outer_cols->Size());
            bool sid_col_idx_found = false;
            for (ULONG col_idx = 0; col_idx < outer_cols->Size();
                    col_idx++) {
                CColRef *col = outer_cols->operator[](col_idx);
                ULONG col_id = col->Id();
                // match _tid
                auto it = std::find(sccmp_colids.begin(),
                                    sccmp_colids.end(), col_id);
                if (it != sccmp_colids.end()) {
                    D_ASSERT(!sid_col_idx_found);
                    sid_col_idx = col_idx;
                    sid_col_idx_found = true;
                }
                // construct outer_col_map
                auto it_ = id_map.find(col_id);
                if (it_ == id_map.end()) {
                    outer_col_map.push_back(
                        std::numeric_limits<uint32_t>::max());
                }
                else {
                    auto id_idx = id_map.at(
                        col_id);  // std::out_of_range exception if col_id does not exist in id_map
                    outer_col_map.push_back(id_idx);
                }
            }
            D_ASSERT(sid_col_idx_found);
        }
        else if (inner_root->Pop()->Eopid() ==
            COperator::EOperatorId::EopPhysicalIndexOnlyScan)
        {
            D_ASSERT(false); // not implemented yet
        }
        else if (inner_root->Pop()->Eopid() ==
            COperator::EOperatorId::EopPhysicalFilter)
        {
            /**
             * Pattern
             * CPhysicalInnerIndexNLJoin
             * |---ComputeScalar
             * |   |---Filter
             * |   |   |---TableScan
            */
            has_filter = true;
            filter_expr = inner_root;
            pGetFilterDuckDBExprs(filter_expr, filter_output_cols, nullptr, 0, filter_duckdb_exprs);
        }

        // reached to the bottom
        if (inner_root->Arity() == 0) {
            break;
        }
        else {
            inner_root =
                inner_root->operator[](0);  // pass first child in linear plan
        }
    }

    if (skip_seek) {
        duckdb::Schema schema_proj;
        vector<duckdb::LogicalType> output_types_proj;
        vector<unique_ptr<duckdb::Expression>> proj_exprs;
        pGetDuckDBTypesFromColRefs(output_cols, output_types_proj);
        schema_proj.setStoredTypes(output_types_proj);
        pGetProjectionExprsWithJoinCond(scalar_cmp_expr, outer_cols, output_cols,
                            output_types_proj, proj_exprs);
        if (proj_exprs.size() != 0) {
            duckdb::CypherPhysicalOperator *duckdb_proj_op =
                new duckdb::PhysicalProjection(schema_proj,
                                                move(proj_exprs));
            result->push_back(duckdb_proj_op);
            pBuildSchemaFlowGraphForUnaryOperator(schema_proj);
        }
        return result;
    }

    gpos::ULONG pred_attr_pos, pred_pos;
    duckdb::Value literal_val;
    duckdb::LogicalType pred_attr_type;
    if (has_filter && do_filter_pushdown) {
        GPOS_ASSERT(false);
        throw NotImplementedException("InnerIdxNLJoin for Filter case");
    }

    /* Generate operator and push */
    duckdb::Schema tmp_schema;
    tmp_schema.setStoredTypes(types);

    if (construct_filter_for_cycle) {
        vector<duckdb::LogicalType> output_types_cycle_filter;
        pGetDuckDBTypesFromColRefs(outer_cols, output_types_cycle_filter);
        duckdb::Schema schema_cycle_filter;
        schema_cycle_filter.setStoredTypes(output_types_cycle_filter);
        duckdb::CypherPhysicalOperator *duckdb_filter_op =
            new duckdb::PhysicalFilter(schema_cycle_filter, move(cycle_filter_duckdb_exprs));
        result->push_back(duckdb_filter_op);
        pBuildSchemaFlowGraphForUnaryOperator(schema_cycle_filter);
    }

    /**
     * TODO: this code is currently wrong. It should be fixed.
     * IdSeek should generate multiple schemas.
     * However, this code now only generates one schema (union schema).
     * Instead, it uses tmp_schema given to the PhysicalIdSeek to initialize.
    */

    if (generate_sfg) {
        vector<duckdb::Schema> prev_local_schemas = pipeline_schemas.back();
        auto &num_schemas_of_childs_prev = num_schemas_of_childs.back();
        duckdb::idx_t num_total_schemas_prev = 1;
        for (auto i = 0; i < num_schemas_of_childs_prev.size(); i++) {
            num_total_schemas_prev *= num_schemas_of_childs_prev[i];
        }
        pipeline_operator_types.push_back(duckdb::OperatorType::BINARY);
        num_schemas_of_childs.push_back(
            {num_total_schemas_prev, inner_col_maps.size()});
        // num_schemas_of_childs.push_back({prev_local_schemas.size()});
        pipeline_schemas.push_back(prev_local_schemas);
        pipeline_union_schema.push_back(tmp_schema);
    }

    size_t num_outer_schemas = pGetNumOuterSchemas();
    if (!do_filter_pushdown) {
        duckdb::CypherPhysicalOperator *op = new duckdb::PhysicalIdSeek(
            tmp_schema, sid_col_idx, oids, output_projection_mapping,
            outer_col_map, inner_col_maps, union_inner_col_map,
            scan_projection_mapping, scan_types, false, join_type,
            num_outer_schemas);
        result->push_back(op);

        // Construct filter
        if (has_filter) {
            vector<duckdb::LogicalType> output_types_filter;
            pGetDuckDBTypesFromColRefs(filter_output_cols, output_types_filter);
            duckdb::Schema schema_filter;
            schema_filter.setStoredTypes(output_types_filter);
            duckdb::CypherPhysicalOperator *duckdb_filter_op =
                new duckdb::PhysicalFilter(schema_filter, move(filter_duckdb_exprs));
            result->push_back(duckdb_filter_op);
            pBuildSchemaFlowGraphForUnaryOperator(schema_filter);

            // Construct projection
            if (has_filter_only_column) {
                duckdb::Schema schema_proj;
                vector<duckdb::LogicalType> output_types_proj;
                vector<unique_ptr<duckdb::Expression>> proj_exprs;
                pGetDuckDBTypesFromColRefs(output_cols, output_types_proj);
                schema_proj.setStoredTypes(output_types_proj);
                pGetProjectionExprs(filter_output_cols, output_cols,
                                    output_types_proj, proj_exprs);
                if (proj_exprs.size() != 0) {
                    duckdb::CypherPhysicalOperator *duckdb_proj_op =
                        new duckdb::PhysicalProjection(schema_proj,
                                                       move(proj_exprs));
                    result->push_back(duckdb_proj_op);
                    pBuildSchemaFlowGraphForUnaryOperator(schema_proj);
                }
            }
        }
    }
    else {
        GPOS_ASSERT(false);
        throw NotImplementedException("InnerIdxNLJoin for Filter case");
    }

    return result;
}

void Planner::pTransformEopPhysicalInnerIndexNLJoinToProjectionForUnionAllInner(
    CExpression *plan_expr, duckdb::CypherPhysicalOperatorGroups *result)
{
    CMemoryPool *mp = this->memory_pool;

    CColRefArray *output_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CExpression *pexprOuter = (*plan_expr)[0];
    CColRefArray *outer_cols = pexprOuter->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CExpression *pexprInner = (*plan_expr)[1];
    CColRefArray *inner_cols = pexprInner->Prpp()->PcrsRequired()->Pdrgpcr(mp);

    // ID column only (n._id)
    vector<duckdb::LogicalType> proj_output_types;
    for (ULONG col_idx = 0; col_idx < output_cols->Size(); col_idx++) {
        CColRef *col = (*output_cols)[col_idx];
        proj_output_types.push_back(pGetColumnsDuckDBType(col));
    }

    // generate projection expressions (I don't assume the _tid is always the last)
    vector<unique_ptr<duckdb::Expression>> proj_exprs(output_cols->Size());
    vector<bool> outer_cols_is_id_col(outer_cols->Size(), true);
    duckdb::idx_t output_id_col_idx = gpos::ulong_max;

    // projection expressions for non _id columns
    for (ULONG output_col_idx = 0; output_col_idx < output_cols->Size();
         output_col_idx++) {
        auto outer_col_idx =
            outer_cols->IndexOf(output_cols->operator[](output_col_idx));
        if (outer_col_idx != gpos::ulong_max) {  // non _id column
            proj_exprs[output_col_idx] =
                make_unique<duckdb::BoundReferenceExpression>(
                    proj_output_types[output_col_idx], (int)outer_col_idx);
            outer_cols_is_id_col[outer_col_idx] = false;
        }
        else {  // _id column
            output_id_col_idx = output_col_idx;
        }
    }

    // projection expressions for _id column
    if (output_id_col_idx != gpos::ulong_max) {
        for (size_t outer_col_idx = 0;
             outer_col_idx < outer_cols_is_id_col.size(); outer_col_idx++) {
            if (outer_cols_is_id_col[outer_col_idx]) {
                proj_exprs[output_id_col_idx] =
                    (make_unique<duckdb::BoundReferenceExpression>(
                        duckdb::LogicalType(duckdb::LogicalTypeId::ID),
                        (int)outer_col_idx));
            }
        }
    }

    // generate schema
    duckdb::Schema proj_schema;
    proj_schema.setStoredTypes(proj_output_types);

    // define op
    duckdb::CypherPhysicalOperator *op =
        new duckdb::PhysicalProjection(proj_schema, move(proj_exprs));
    result->push_back(op);

    // generate schema flow graph
    pBuildSchemaFlowGraphForUnaryOperator(proj_schema);
}

duckdb::CypherPhysicalOperatorGroups *
Planner::pTransformEopPhysicalHashJoinToHashJoin(CExpression *plan_expr)
{
    CMemoryPool *mp = this->memory_pool;

    // Don't need to explicitly convert to LeftAntiJoin, LeftSemiJoin, LeftMarkJoin
    CPhysicalInnerHashJoin *expr_op =
        (CPhysicalInnerHashJoin *)plan_expr->Pop();
    CColRefArray *output_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CColRefArray *hash_output_cols = GPOS_NEW(mp) CColRefArray(mp);
    CColRefArray *filter_output_cols = NULL;
    CColRefArray *proj_output_cols = output_cols;

    // For RightOuterHashJoin, swap left/right children to convert to LeftOuter.
    bool is_right_outer = plan_expr->Pop()->Eopid() ==
        COperator::EOperatorId::EopPhysicalRightOuterHashJoin;

    // Obtain left and right cols (swapped for ROJ → LOJ conversion)
    CExpression *pexprLeft = is_right_outer ? (*plan_expr)[1] : (*plan_expr)[0];
    CColRefArray *left_cols;
    CColRefArray *right_cols;
    if (pexprLeft->Pop()->Eopid() ==
        COperator::EOperatorId::
            EopPhysicalSerialUnionAll) {  // TODO: correctness check (is it okay to call PdrgpcrOutput?)
        CPhysicalSerialUnionAll *unionall_op =
            (CPhysicalSerialUnionAll *)pexprLeft->Pop();
        left_cols = unionall_op->PdrgpcrOutput();
    }
    else {
        left_cols = pexprLeft->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    }
    CExpression *pexprRight = is_right_outer ? (*plan_expr)[0] : (*plan_expr)[1];
    if (pexprRight->Pop()->Eopid() ==
        COperator::EOperatorId::
            EopPhysicalSerialUnionAll) {  // TODO: correctness check (is it okay to call PdrgpcrOutput?)
        CPhysicalSerialUnionAll *unionall_op =
            (CPhysicalSerialUnionAll *)pexprRight->Pop();
        right_cols = unionall_op->PdrgpcrOutput();
    }
    else {
        right_cols = pexprRight->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    }

    vector<duckdb::JoinCondition> join_conds;
    // For ROJ→LOJ, the predicate column order stays the same (ORCA's left=right),
    // but we swapped left/right children, so swap the cols in the predicate too.
    if (is_right_outer) {
        pTranslatePredicateToJoinCondition(plan_expr->operator[](2), join_conds,
                                           right_cols, left_cols);
        // Swap the condition sides to match our swapped children
        for (auto &cond : join_conds) {
            std::swap(cond.left, cond.right);
        }
    } else {
        pTranslatePredicateToJoinCondition(plan_expr->operator[](2), join_conds,
                                           left_cols, right_cols);
    }
    // JoinHashTable requires equality conditions before non-equality conditions.
    // ORCA's AND-tree order is arbitrary, so sort: COMPARE_EQUAL first.
    std::stable_sort(join_conds.begin(), join_conds.end(),
        [](const duckdb::JoinCondition &a, const duckdb::JoinCondition &b) {
            bool a_eq = (a.comparison == duckdb::ExpressionType::COMPARE_EQUAL ||
                         a.comparison == duckdb::ExpressionType::COMPARE_NOT_DISTINCT_FROM ||
                         a.comparison == duckdb::ExpressionType::COMPARE_DISTINCT_FROM);
            bool b_eq = (b.comparison == duckdb::ExpressionType::COMPARE_EQUAL ||
                         b.comparison == duckdb::ExpressionType::COMPARE_NOT_DISTINCT_FROM ||
                         b.comparison == duckdb::ExpressionType::COMPARE_DISTINCT_FROM);
            return a_eq > b_eq;  // equality first
        });

    hash_output_cols = output_cols;

    // Construct col map, types and etc
    vector<duckdb::LogicalType> hash_output_types;
    vector<uint32_t> left_col_map;
    vector<uint32_t> right_col_map;

    pGetDuckDBTypesFromColRefs(hash_output_cols, hash_output_types);
    pConstructColMapping(left_cols, hash_output_cols, left_col_map);
    pConstructColMapping(right_cols, hash_output_cols, right_col_map);

    duckdb::JoinType join_type = pTranslateJoinType(expr_op);

    vector<duckdb::LogicalType> right_build_types;
    vector<uint64_t> right_build_map;
    if (join_type != duckdb::JoinType::ANTI &&
        join_type != duckdb::JoinType::SEMI &&
        join_type != duckdb::JoinType::MARK) {
        // hash build type (output_cols - left_cols; where values are required)
        // non-key columns, which means columns to be outputted after probing
        CColRefSet *cols_to_build = GPOS_NEW(mp) CColRefSet(mp);
        cols_to_build->Include(plan_expr->Prpp()->PcrsRequired());
        cols_to_build->Difference(pexprLeft->Prpp()->PcrsRequired());
        CColRefArray *cols_build_list = cols_to_build->Pdrgpcr(mp);
        // right_build_types, right_build_map
        for (ULONG col_idx = 0; col_idx < cols_build_list->Size(); col_idx++) {
            auto idx =
                right_cols->IndexOf(cols_build_list->operator[](col_idx));
            right_build_map.push_back(idx);
            OID type_oid =
                CMDIdGPDB::CastMdid(cols_build_list->operator[](col_idx)
                                        ->RetrieveType()
                                        ->MDId())
                    ->Oid();
            INT type_mod = cols_build_list->operator[](col_idx)->TypeModifier();
            duckdb::LogicalType col_type =
                pConvertTypeOidToLogicalType(type_oid, type_mod);
            right_build_types.push_back(col_type);
        }
        D_ASSERT(right_build_map.size() == right_build_types.size());
    }
    else if (join_type == duckdb::JoinType::ANTI) {
        // do nothing
    }
    else if (join_type == duckdb::JoinType::SEMI) {
        // do nothing
    }
    else {
        D_ASSERT(false);
    }

    // define op
    duckdb::Schema schema;
    schema.setStoredTypes(hash_output_types);

    duckdb::CypherPhysicalOperator *op = new duckdb::PhysicalHashJoin(
        schema, move(join_conds), join_type, left_col_map, right_col_map,
        right_build_types, right_build_map);

    return pBuildSchemaflowGraphForBinaryJoin(plan_expr, op, schema, is_right_outer);
}

duckdb::CypherPhysicalOperatorGroups *
Planner::pTransformEopPhysicalMergeJoinToMergeJoin(CExpression *plan_expr)
{
    CMemoryPool *mp = this->memory_pool;
    D_ASSERT(plan_expr->Arity() == 3);

    CPhysicalInnerMergeJoin *expr_op =
        (CPhysicalInnerMergeJoin *)plan_expr->Pop();
    CColRefArray *output_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CExpression *pexprLeft = (*plan_expr)[0];
    CColRefArray *left_cols;
    if (pexprLeft->Pop()->Eopid() ==
        COperator::EOperatorId::EopPhysicalSerialUnionAll) {
        CPhysicalSerialUnionAll *unionall_op =
            (CPhysicalSerialUnionAll *)pexprLeft->Pop();
        left_cols = unionall_op->PdrgpcrOutput();
    }
    else {
        left_cols = pexprLeft->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    }
    CExpression *pexprRight = (*plan_expr)[1];
    CColRefArray *right_cols;
    if (pexprRight->Pop()->Eopid() ==
        COperator::EOperatorId::EopPhysicalSerialUnionAll) {
        CPhysicalSerialUnionAll *unionall_op =
            (CPhysicalSerialUnionAll *)pexprRight->Pop();
        right_cols = unionall_op->PdrgpcrOutput();
    }
    else {
        right_cols = pexprRight->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    }

    vector<duckdb::LogicalType> types;
    vector<uint32_t> left_col_map;
    vector<uint32_t> right_col_map;

    for (ULONG col_idx = 0; col_idx < output_cols->Size(); col_idx++) {
        CColRef *col = output_cols->operator[](col_idx);
        types.push_back(pGetColumnsDuckDBType(col));
    }
    for (ULONG col_idx = 0; col_idx < left_cols->Size(); col_idx++) {
        auto idx = output_cols->IndexOf(left_cols->operator[](col_idx));
        if (idx == gpos::ulong_max) {
            left_col_map.push_back(std::numeric_limits<uint32_t>::max());
        }
        else {
            left_col_map.push_back(idx);
        }
    }
    for (ULONG col_idx = 0; col_idx < right_cols->Size(); col_idx++) {
        auto idx = output_cols->IndexOf(right_cols->operator[](col_idx));
        if (idx == gpos::ulong_max) {
            right_col_map.push_back(std::numeric_limits<uint32_t>::max());
        }
        else {
            right_col_map.push_back(idx);
        }
    }

    duckdb::JoinType join_type = pTranslateJoinType(expr_op);
    D_ASSERT(join_type != duckdb::JoinType::RIGHT);

    // define op
    duckdb::Schema schema;
    schema.setStoredTypes(types);

    // generate conditions
    vector<duckdb::JoinCondition> join_conds;
    pTranslatePredicateToJoinCondition(plan_expr->operator[](2), join_conds,
                                       left_cols, right_cols);

    // Calculate lhs and rhs types
    vector<duckdb::LogicalType> lhs_types;
    vector<duckdb::LogicalType> rhs_types;
    for (ULONG col_idx = 0; col_idx < left_cols->Size(); col_idx++) {
        OID type_oid =
            CMDIdGPDB::CastMdid(
                left_cols->operator[](col_idx)->RetrieveType()->MDId())
                ->Oid();
        INT type_mod = left_cols->operator[](col_idx)->TypeModifier();
        lhs_types.push_back(pConvertTypeOidToLogicalType(type_oid, type_mod));
    }
    for (ULONG col_idx = 0; col_idx < right_cols->Size(); col_idx++) {
        OID type_oid =
            CMDIdGPDB::CastMdid(
                right_cols->operator[](col_idx)->RetrieveType()->MDId())
                ->Oid();
        INT type_mod = right_cols->operator[](col_idx)->TypeModifier();
        rhs_types.push_back(pConvertTypeOidToLogicalType(type_oid, type_mod));
    }

    duckdb::CypherPhysicalOperator *op = new duckdb::PhysicalPiecewiseMergeJoin(
        schema, move(join_conds), join_type, lhs_types, rhs_types, left_col_map,
        right_col_map);

    return pBuildSchemaflowGraphForBinaryJoin(plan_expr, op, schema);
}

duckdb::CypherPhysicalOperatorGroups *
Planner::pTransformEopPhysicalInnerNLJoinToCartesianProduct(
    CExpression *plan_expr)
{
    // CMemoryPool *mp = this->memory_pool;

    // D_ASSERT(plan_expr->Arity() == 3);

    // /* Non-root - call LHS first for inner materialization */
    // vector<duckdb::CypherPhysicalOperator *> *lhs_result =
    //     pTraverseTransformPhysicalPlan(
    //         plan_expr->PdrgPexpr()->operator[](0));  // outer
    // vector<duckdb::CypherPhysicalOperator *> *rhs_result =
    //     pTraverseTransformPhysicalPlan(
    //         plan_expr->PdrgPexpr()->operator[](1));  // inner

    // /* finish rhs pipeline */
    // CColRefArray *output_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    // CExpression *pexprOuter = (*plan_expr)[0];
    // CColRefArray *outer_cols = pexprOuter->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    // CExpression *pexprInner = (*plan_expr)[1];
    // CColRefArray *inner_cols = pexprInner->Prpp()->PcrsRequired()->Pdrgpcr(mp);

    // vector<duckdb::LogicalType> types;
    // vector<uint32_t> outer_col_map;
    // vector<uint32_t> inner_col_map;

    // for (ULONG col_idx = 0; col_idx < output_cols->Size(); col_idx++) {
    //     CColRef *col = output_cols->operator[](col_idx);
    //     OID type_oid = CMDIdGPDB::CastMdid(col->RetrieveType()->MDId())->Oid();
    //     duckdb::LogicalType col_type = pConvertTypeOidToLogicalType(type_oid);
    //     types.push_back(col_type);
    // }
    // for (ULONG col_idx = 0; col_idx < outer_cols->Size(); col_idx++) {
    //     outer_col_map.push_back(
    //         output_cols->IndexOf(outer_cols->operator[](col_idx)));
    // }
    // for (ULONG col_idx = 0; col_idx < inner_cols->Size(); col_idx++) {
    //     inner_col_map.push_back(
    //         output_cols->IndexOf(inner_cols->operator[](col_idx)));
    // }

    // // define op
    // duckdb::Schema tmp_schema;
    // tmp_schema.setStoredTypes(types);
    // duckdb::CypherPhysicalOperator *op = new duckdb::PhysicalCrossProduct(
    //     tmp_schema, outer_col_map, inner_col_map);

    // // finish rhs pipeline
    // rhs_result->push_back(op);
    // auto pipeline = new duckdb::CypherPipeline(*rhs_result, pipelines.size());
    // pipelines.push_back(pipeline);

    // // return lhs pipeline
    // lhs_result->push_back(op);
    // return lhs_result;
    CMemoryPool *mp = this->memory_pool;
    D_ASSERT(plan_expr->Arity() == 3);

    CPhysicalInnerNLJoin *expr_op = (CPhysicalInnerNLJoin *)plan_expr->Pop();
    CColRefArray *output_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CExpression *pexprLeft = (*plan_expr)[0];
    CColRefArray *left_cols;
    if (pexprLeft->Pop()->Eopid() ==
        COperator::EOperatorId::EopPhysicalSerialUnionAll) {
        CPhysicalSerialUnionAll *unionall_op =
            (CPhysicalSerialUnionAll *)pexprLeft->Pop();
        left_cols = unionall_op->PdrgpcrOutput();
    }
    else {
        left_cols = pexprLeft->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    }
    CExpression *pexprRight = (*plan_expr)[1];
    CColRefArray *right_cols;
    if (pexprRight->Pop()->Eopid() ==
        COperator::EOperatorId::EopPhysicalSerialUnionAll) {
        CPhysicalSerialUnionAll *unionall_op =
            (CPhysicalSerialUnionAll *)pexprRight->Pop();
        right_cols = unionall_op->PdrgpcrOutput();
    }
    else {
        right_cols = pexprRight->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    }

    vector<duckdb::LogicalType> types;
    vector<uint32_t> left_col_map;
    vector<uint32_t> right_col_map;

    for (ULONG col_idx = 0; col_idx < output_cols->Size(); col_idx++) {
        CColRef *col = output_cols->operator[](col_idx);
        types.push_back(pGetColumnsDuckDBType(col));
    }
    for (ULONG col_idx = 0; col_idx < left_cols->Size(); col_idx++) {
        auto idx = output_cols->IndexOf(left_cols->operator[](col_idx));
        if (idx == gpos::ulong_max) {
            left_col_map.push_back(std::numeric_limits<uint32_t>::max());
        }
        else {
            left_col_map.push_back(idx);
        }
    }
    for (ULONG col_idx = 0; col_idx < right_cols->Size(); col_idx++) {
        auto idx = output_cols->IndexOf(right_cols->operator[](col_idx));
        if (idx == gpos::ulong_max) {
            right_col_map.push_back(std::numeric_limits<uint32_t>::max());
        }
        else {
            right_col_map.push_back(idx);
        }
    }

    // define op
    duckdb::Schema schema;
    schema.setStoredTypes(types);

    // Calculate lhs and rhs types
    // vector<duckdb::LogicalType> lhs_types;
    // vector<duckdb::LogicalType> rhs_types;
    // for (ULONG col_idx = 0; col_idx < left_cols->Size(); col_idx++) {
    //     OID type_oid =
    //         CMDIdGPDB::CastMdid(
    //             left_cols->operator[](col_idx)->RetrieveType()->MDId())
    //             ->Oid();
    //     lhs_types.push_back(pConvertTypeOidToLogicalType(type_oid));
    // }
    // for (ULONG col_idx = 0; col_idx < right_cols->Size(); col_idx++) {
    //     OID type_oid =
    //         CMDIdGPDB::CastMdid(
    //             right_cols->operator[](col_idx)->RetrieveType()->MDId())
    //             ->Oid();
    //     rhs_types.push_back(pConvertTypeOidToLogicalType(type_oid));
    // }

    duckdb::CypherPhysicalOperator *op =
        new duckdb::PhysicalCrossProduct(schema, left_col_map, right_col_map);

    return pBuildSchemaflowGraphForBinaryJoin(plan_expr, op, schema);
}

duckdb::CypherPhysicalOperatorGroups *
Planner::pTransformEopPhysicalNLJoinToBlockwiseNLJoin(CExpression *plan_expr,
                                                      bool is_correlated)
{
    CMemoryPool *mp = this->memory_pool;

    D_ASSERT(plan_expr->Arity() == 3);

    /* Non-root - call left child */
    CColRefArray *output_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CExpression *pexprOuter = (*plan_expr)[0];
    CColRefArray *outer_cols = pexprOuter->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CExpression *pexprInner = (*plan_expr)[1];
    CColRefArray *inner_cols = pexprInner->Prpp()->PcrsRequired()->Pdrgpcr(mp);

    vector<duckdb::LogicalType> types;
    vector<uint32_t> outer_col_map;
    vector<uint32_t> inner_col_map;

    for (ULONG col_idx = 0; col_idx < output_cols->Size(); col_idx++) {
        CColRef *col = output_cols->operator[](col_idx);
        types.push_back(pGetColumnsDuckDBType(col));
    }
    for (ULONG col_idx = 0; col_idx < outer_cols->Size(); col_idx++) {
        auto idx = output_cols->IndexOf(outer_cols->operator[](col_idx));
        if (idx == gpos::ulong_max) {
            outer_col_map.push_back(std::numeric_limits<uint32_t>::max());
        }
        else {
            outer_col_map.push_back(idx);
        }
    }
    for (ULONG col_idx = 0; col_idx < inner_cols->Size(); col_idx++) {
        auto idx = output_cols->IndexOf(inner_cols->operator[](col_idx));
        if (idx == gpos::ulong_max) {
            inner_col_map.push_back(std::numeric_limits<uint32_t>::max());
        }
        else {
            inner_col_map.push_back(idx);
        }
    }

    // define op
    duckdb::Schema schema;
    schema.setStoredTypes(types);

    duckdb::JoinType join_type = pTranslateJoinType(plan_expr->Pop());
    D_ASSERT(join_type != duckdb::JoinType::RIGHT);

    CExpression *join_pred = (*plan_expr)[2];
    if (plan_expr->Pop()->Eopid() ==
        COperator::EOperatorId::EopPhysicalLeftAntiSemiHashJoin) {
        // Temporal code for anti hash join handling
        join_pred = CUtils::PexprNegate(mp, join_pred);
    }

    unique_ptr<duckdb::Expression> join_condition_expr = pTransformScalarExpr(
        join_pred, outer_cols, inner_cols);  // left - right
    pShiftFilterPredInnerColumnIndices(join_condition_expr, outer_cols->Size());

    duckdb::CypherPhysicalOperator *op = new duckdb::PhysicalBlockwiseNLJoin(
        schema, move(join_condition_expr), join_type, outer_col_map,
        inner_col_map);

    if (is_correlated) {
        // Correlated NL join requires re-executing inner pipeline per outer row.
        // Not yet supported — throw a user-friendly error instead of hanging.
        throw duckdb::NotImplementedException(
            "NOT EXISTS subquery is not yet supported. Use EXISTS with negation instead.");
    }
    return pBuildSchemaflowGraphForBinaryJoin(plan_expr, op, schema);
}

duckdb::CypherPhysicalOperatorGroups *Planner::pTransformEopLimit(
    CExpression *plan_expr)
{
    CMemoryPool *mp = this->memory_pool;

    /* Non-root - call single child */
    duckdb::CypherPhysicalOperatorGroups *result =
        pTraverseTransformPhysicalPlan(plan_expr->PdrgPexpr()->operator[](0));

    CExpression *limit_expr = plan_expr;
    CPhysicalLimit *limit_op = (CPhysicalLimit *)limit_expr->Pop();
    D_ASSERT(limit_expr->operator[](1)->Pop()->Eopid() ==
             COperator::EOperatorId::EopScalarConst);
    D_ASSERT(limit_expr->operator[](2)->Pop()->Eopid() ==
             COperator::EOperatorId::EopScalarConst);

    if (!limit_op->FHasCount())
        return result;

    int64_t offset, limit;
    CDatumInt8GPDB *offset_datum =
        (CDatumInt8GPDB *)(((CScalarConst *)limit_expr->operator[](1)->Pop())
                               ->GetDatum());
    CDatumInt8GPDB *limit_datum =
        (CDatumInt8GPDB *)(((CScalarConst *)limit_expr->operator[](2)->Pop())
                               ->GetDatum());
    offset = offset_datum->Value();
    limit = limit_datum->Value();

    duckdb::Schema tmp_schema;
    duckdb::CypherPhysicalOperator *last_op = result->back();
    tmp_schema.setStoredTypes(last_op->GetTypes());

    pBuildSchemaFlowGraphForUnaryOperator(tmp_schema);

    duckdb::CypherPhysicalOperator *op =
        new duckdb::PhysicalTop(tmp_schema, limit, offset);
    result->push_back(op);

    return result;
}

duckdb::CypherPhysicalOperatorGroups *
Planner::pTransformEopProjectionColumnar(CExpression *plan_expr)
{

    CMemoryPool *mp = this->memory_pool;
    CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();

    /* Non-root - call single child */
    duckdb::CypherPhysicalOperatorGroups *result =
        pTraverseTransformPhysicalPlan(plan_expr->PdrgPexpr()->operator[](0));

    vector<unique_ptr<duckdb::Expression>> proj_exprs;
    vector<duckdb::LogicalType> types;
    vector<string> output_column_names;

    CColRefArray *output_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);

    CPhysicalComputeScalarColumnar *proj_op =
        (CPhysicalComputeScalarColumnar *)plan_expr->Pop();
    CExpression *pexprProjRelational = (*plan_expr)[0];  // Prev op
    CColRefArray *child_cols =
        pexprProjRelational->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CExpression *pexprProjList = (*plan_expr)[1];  // Projection list

    // decide which proj elems to project - keep colref orders
    vector<ULONG> indices_to_project;
    for (ULONG ocol = 0; ocol < output_cols->Size(); ocol++) {
        bool found = false;
        for (ULONG elem_idx = 0; elem_idx < pexprProjList->Arity();
             elem_idx++) {
            if (((CScalarProjectElement *)(pexprProjList->operator[](elem_idx)
                                               ->Pop()))
                    ->Pcr()
                    ->Id() == output_cols->operator[](ocol)->Id()) {
                // matching ColRef found
                indices_to_project.push_back(elem_idx);
                found = true;
                break;
            }
        }
        if (!found) {
            // CPhysicalComputeScalar (non-Columnar) passes through child columns.
            // If the colref is in the child output, create a passthrough reference.
            ULONG child_idx = child_cols->IndexOf(output_cols->operator[](ocol));
            if (child_idx != gpos::ulong_max) {
                // Mark as passthrough — will be handled as identity projection
                indices_to_project.push_back(gpos::ulong_max);  // sentinel
            } else {
                throw duckdb::InvalidInputException("Projection column not found");
            }
        }
    }
    
    for (size_t proj_i = 0; proj_i < indices_to_project.size(); proj_i++) {
        auto elem_idx = indices_to_project[proj_i];

        // Passthrough column (not in projection list, from child output)
        if (elem_idx == gpos::ulong_max) {
            CColRef *ocol = output_cols->operator[](proj_i);
            // Find column in the PHYSICAL child output (not ORCA required cols)
            ULONG child_idx = gpos::ulong_max;
            for (size_t pi = 0; pi < physical_plan_output_colrefs.size(); pi++) {
                if (physical_plan_output_colrefs[pi]->Id() == ocol->Id()) {
                    child_idx = pi;
                    break;
                }
            }
            if (child_idx == gpos::ulong_max) {
                child_idx = child_cols->IndexOf(ocol);
            }
            auto child_type = pGetColumnsDuckDBType(ocol);
            proj_exprs.push_back(make_unique<duckdb::BoundReferenceExpression>(
                child_type, child_idx));
            types.push_back(child_type);
            output_column_names.push_back(pGetColNameFromColRef(ocol));
            continue;
        }

        CExpression *pexprProjElem =
            pexprProjList->operator[](elem_idx);  // CScalarProjectElement
        CExpression *pexprScalarExpr =
            pexprProjElem->operator[](0);  // CScalar... - expr tree root

        output_column_names.push_back(pGetColNameFromColRef(
            ((CScalarProjectElement *)pexprProjElem->Pop())->Pcr()));

        // Check if this is an MPV sibling-only column (NULL constant that should
        // be replaced with a reference to the MPV-expanded scan output).
        ULONG colref_id = ((CScalarProjectElement *)pexprProjElem->Pop())->Pcr()->Id();
        auto mpv_it = mpv_colref_to_scan_idx_.find(colref_id);
        if (mpv_it != mpv_colref_to_scan_idx_.end()) {
            // Replace NULL constant with column reference to scan output
            auto &info = mpv_null_colref_props[colref_id];
            auto ref_expr = duckdb::make_unique<duckdb::BoundReferenceExpression>(
                info.type, mpv_it->second);
            proj_exprs.push_back(std::move(ref_expr));
        } else {
            proj_exprs.push_back(
                std::move(pTransformScalarExpr(pexprScalarExpr, child_cols)));
        }
        types.push_back(proj_exprs.back()->return_type);
    }

    // Pass through MPV extra columns that are not projected by this projection
    // but will be needed by an outer projection. This handles nested
    // CPhysicalComputeScalarColumnar where the inner one only projects
    // columns known to ORCA, missing the MPV-expanded columns.
    if (!mpv_colref_to_scan_idx_.empty()) {
        // Collect colref IDs already projected by this projection
        std::unordered_set<ULONG> projected_colrefs;
        for (auto &elem_idx : indices_to_project) {
            CExpression *pexprProjElem = pexprProjList->operator[](elem_idx);
            projected_colrefs.insert(
                ((CScalarProjectElement *)pexprProjElem->Pop())->Pcr()->Id());
        }

        // For each MPV colref NOT projected here, add a passthrough.
        // Collect updates separately to avoid modifying map during iteration.
        std::vector<std::pair<ULONG, size_t>> deferred_updates;
        for (auto &[colref_id, scan_idx] : mpv_colref_to_scan_idx_) {
            if (projected_colrefs.find(colref_id) != projected_colrefs.end())
                continue;  // already projected by this projection

            auto &info = mpv_null_colref_props[colref_id];
            auto ref_expr = duckdb::make_unique<duckdb::BoundReferenceExpression>(
                info.type, scan_idx);
            size_t new_output_idx = types.size();
            proj_exprs.push_back(std::move(ref_expr));
            types.push_back(info.type);
            output_column_names.push_back("_mpv_passthru_" + std::to_string(colref_id));
            deferred_updates.push_back({colref_id, new_output_idx});
        }
        // Update the mapping so outer projections reference this output
        for (auto &[cid, idx] : deferred_updates) {
            mpv_colref_to_scan_idx_[cid] = idx;
        }
    }

    // All column dropped case (ISSUE #108)
    if (types.empty()) {
        return result;
    }

    /* Generate operator and push */
    duckdb::Schema tmp_schema;
    tmp_schema.setStoredTypes(types);
    tmp_schema.setStoredColumnNames(output_column_names);

    pBuildSchemaFlowGraphForUnaryOperator(tmp_schema);

    duckdb::CypherPhysicalOperator *op =
        new duckdb::PhysicalProjection(tmp_schema, std::move(proj_exprs));
    result->push_back(op);

    return result;
}

/**
 * This code is for post-projection implementation.
 * Need to fix someday
*/
// vector<duckdb::CypherPhysicalOperator *> *Planner::pTransformEopAgg(
//     CExpression *plan_expr)
// {
//     CMemoryPool *mp = this->memory_pool;

//     /* Non-root - call single child */
//     vector<duckdb::CypherPhysicalOperator *> *result =
//         pTraverseTransformPhysicalPlan(plan_expr->PdrgPexpr()->operator[](0));

//     vector<duckdb::LogicalType> agg_types;
//     vector<duckdb::LogicalType> proj_types;
//     vector<duckdb::LogicalType> post_proj_type;
//     vector<unique_ptr<duckdb::Expression>> agg_exprs;
//     vector<unique_ptr<duckdb::Expression>> agg_groups;
//     vector<string> output_column_names;
//     vector<string> output_column_names_proj;
//     // vector<duckdb::LogicalType> groups_type;
//     // vector<ULONG> groups_idx;
//     // vector<ULONG> proj_mapping;
//     vector<uint64_t> output_projection_mapping;

//     CPhysicalAgg *agg_op = (CPhysicalAgg *)plan_expr->Pop();
//     CExpression *pexprProjRelational = (*plan_expr)[0];  // Prev op
//     CColRefArray *output_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
//     CColRefArray *child_cols =
//         pexprProjRelational->Prpp()->PcrsRequired()->Pdrgpcr(mp);
//     CColRefArray *interm_output_cols =
//         plan_expr->DeriveOutputColumns()->Pdrgpcr(mp);
//     CExpression *pexprProjList = (*plan_expr)[1];  // Projection list
//     const CColRefArray *grouping_cols = agg_op->PdrgpcrGroupingCols();
//     CColRefSet *grouping_col_set = GPOS_NEW(mp) CColRefSet(mp, grouping_cols);
//     CColRefArray *grouping_cols_sorted = grouping_col_set->Pdrgpcr(mp);

//     // used for pre-projection
//     vector<unique_ptr<duckdb::Expression>> proj_exprs;

//     // used for post-projection
//     vector<unique_ptr<duckdb::Expression>> post_proj_exprs;

//     // get agg groups
//     uint64_t num_outputs_in_grouping_col = 0;
//     for (ULONG group_col_idx = 0; group_col_idx < grouping_cols_sorted->Size();
//          group_col_idx++) {
//         CColRef *col = grouping_cols_sorted->operator[](group_col_idx);
//         OID type_oid = CMDIdGPDB::CastMdid(col->RetrieveType()->MDId())->Oid();
//         INT type_mod = col->TypeModifier();
//         duckdb::LogicalType col_type =
//             pConvertTypeOidToLogicalType(type_oid, type_mod);
//         ULONG child_idx = child_cols->IndexOf(col);
//         agg_groups.push_back(
//             make_unique<duckdb::BoundReferenceExpression>(col_type, child_idx));
//         proj_exprs.push_back(
//             make_unique<duckdb::BoundReferenceExpression>(col_type, child_idx));
//         post_proj_exprs.push_back(
//             make_unique<duckdb::BoundReferenceExpression>(col_type, child_idx));
//         proj_types.push_back(col_type);
//         output_column_names_proj.push_back(pGetColNameFromColRef(col));
//         if (output_cols->IndexOf(col) != gpos::ulong_max) {
//             output_projection_mapping.push_back(num_outputs_in_grouping_col++);
//         }
//         else {
//             output_projection_mapping.push_back(
//                 std::numeric_limits<uint32_t>::max());
//         }
//     }

//     // get output columns
//     for (ULONG output_col_idx = 0; output_col_idx < output_cols->Size();
//          output_col_idx++) {
//         CColRef *col = output_cols->operator[](output_col_idx);
//         if (grouping_cols->IndexOf(col) == gpos::ulong_max)
//             continue;
//         // output_projection_mapping;
//         OID type_oid = CMDIdGPDB::CastMdid(col->RetrieveType()->MDId())->Oid();
//         INT type_mod = col->TypeModifier();
//         duckdb::LogicalType col_type =
//             pConvertTypeOidToLogicalType(type_oid, type_mod);
//         agg_types.push_back(col_type);
//         output_column_names.push_back(pGetColNameFromColRef(col));
//     }

//     /**
//      * Disable this code due to pre projection bugs
//     */
//     bool has_pre_projection = false;
//     bool has_post_projection = false;
//     bool adjust_agg_groups_performed = false;
//     // handle aggregation expressions
//     for (ULONG elem_idx = 0; elem_idx < pexprProjList->Arity(); elem_idx++) {
//         CExpression *pexprProjElem = pexprProjList->operator[](elem_idx);
//         CExpression *pexprScalarExpr = pexprProjElem->operator[](0);
//         CExpression *aggargs_expr = pexprScalarExpr->operator[](0);
//         CExpression *pexprAggExpr;

//         output_column_names.push_back(pGetColNameFromColRef(
//             ((CScalarProjectElement *)pexprProjElem->Pop())->Pcr()));
//         output_column_names_proj.push_back(pGetColNameFromColRef(
//             ((CScalarProjectElement *)pexprProjElem->Pop())->Pcr()));

//         if (pexprScalarExpr->Pop()->Eopid() != COperator::EopScalarAggFunc) {
//             D_ASSERT(pexprScalarExpr->Pop()->Eopid() ==
//                      COperator::EopScalarFunc);
//             has_post_projection = true;

//             // do the same operation for left and right
// 	        vector<unique_ptr<duckdb::Expression>> child_duckdb_expressions;
//             for (ULONG child_idx = 0; child_idx < 2; child_idx++) {
//                 CExpression *pexprChild = (*pexprScalarExpr)[child_idx];
//                 if (pexprChild->Arity() > 0) {
//                     pUpdateProjAggExprs(pexprChild, agg_exprs, agg_groups, proj_exprs,
//                                         agg_types, proj_types, child_cols,
//                                         adjust_agg_groups_performed,
//                                         has_pre_projection);
//                     child_duckdb_expressions.push_back(make_unique<duckdb::BoundReferenceExpression>(agg_exprs.back()->return_type, proj_exprs.size() - 1));
//                 }
//                 else {
//                     child_duckdb_expressions.push_back(pTransformScalarExpr(pexprChild, child_cols));
//                 }
//             }
//             post_proj_exprs.push_back(pTransformScalarFunc(pexprScalarExpr, child_duckdb_expressions));
//             post_proj_type.push_back(post_proj_exprs.back()->return_type);
//         }
//         else {
//             if (aggargs_expr->Arity() == 0) {  // no child
//                 agg_exprs.push_back(std::move(
//                     pTransformScalarExpr(pexprScalarExpr, child_cols)));
//                 agg_types.push_back(agg_exprs.back()->return_type);
//                 continue;
//             }
//             pUpdateProjAggExprs(
//                 pexprScalarExpr, agg_exprs, agg_groups, proj_exprs, agg_types, proj_types,
//                 child_cols, adjust_agg_groups_performed, has_pre_projection);
//         }
//     }

//     duckdb::Schema agg_schema;
//     agg_schema.setStoredTypes(agg_types);
//     agg_schema.setStoredColumnNames(output_column_names);

//     if (has_pre_projection) {
//         duckdb::Schema proj_schema;
//         proj_schema.setStoredTypes(proj_types);
//         proj_schema.setStoredColumnNames(output_column_names_proj);
//         pBuildSchemaFlowGraphForUnaryOperator(proj_schema);
//         duckdb::CypherPhysicalOperator *proj_op =
//             new duckdb::PhysicalProjection(proj_schema, move(proj_exprs));
//         result->push_back(proj_op);
//     }

//     pBuildSchemaFlowGraphForUnaryOperator(agg_schema);

//     duckdb::CypherPhysicalOperator *op;
//     if (agg_groups.empty()) {
//         op = new duckdb::PhysicalHashAggregate(
//             agg_schema, output_projection_mapping, move(agg_exprs));
//     }
//     else {
//         op = new duckdb::PhysicalHashAggregate(
//             agg_schema, output_projection_mapping, move(agg_exprs),
//             move(agg_groups));
//     }

//     result->push_back(op);
//     pGenerateSchemaFlowGraph(*result);

//     // finish pipeline
//     auto pipeline = new duckdb::CypherPipeline(*result, pipelines.size());
//     pipelines.push_back(pipeline);

//     // new pipeline
//     auto new_result = new vector<duckdb::CypherPhysicalOperator *>();
//     new_result->push_back(op);

//     if (generate_sfg) {
//         // Set for the current pipeline. We consider after group by, schema is merged.
//         pClearSchemaFlowGraph();
//         pipeline_operator_types.push_back(duckdb::OperatorType::UNARY);
//         num_schemas_of_childs.push_back({1});
//         pipeline_schemas.push_back({agg_schema});
//         pipeline_union_schema.push_back(agg_schema);
//     }

//     // Projection for post processing
//     if (has_post_projection) {
//         D_ASSERT(false);
//         // duckdb::Schema post_proj_schema;
//         // post_proj_schema.setStoredTypes(post_proj_type);
//         // pBuildSchemaFlowGraphForUnaryOperator(post_proj_schema);
//         // duckdb::CypherPhysicalOperator *post_proj_op =
//         //     new duckdb::PhysicalProjection(post_proj_schema, move(post_proj_exprs));
//         // new_result->push_back(post_proj_op);
//     }
//     return new_result;
// }

duckdb::CypherPhysicalOperatorGroups *Planner::pTransformEopAgg(
    CExpression *plan_expr)
{
    CMemoryPool *mp = this->memory_pool;
    /* Non-root - call single child */
    duckdb::CypherPhysicalOperatorGroups *result =
        pTraverseTransformPhysicalPlan(plan_expr->PdrgPexpr()->operator[](0));
    vector<duckdb::LogicalType> types;
    vector<duckdb::LogicalType> proj_types;
    vector<unique_ptr<duckdb::Expression>> agg_exprs;
    vector<unique_ptr<duckdb::Expression>> agg_groups;
    vector<string> output_column_names;
    vector<string> output_column_names_proj;
    vector<uint64_t> output_projection_mapping;
    vector<unique_ptr<duckdb::Expression>> proj_exprs; // used for pre-projection
    vector<unique_ptr<duckdb::Expression>> post_proj_exprs; // used for post-projection
    bool has_node_grouping_key = false; // e.g. RETURN n, COUNT(neighbors)
    vector<uint32_t> node_pid_idxs;
    
    // ORCA data structures
    CPhysicalAgg *agg_op = (CPhysicalAgg *)plan_expr->Pop();
    CExpression *pexprProjRelational = (*plan_expr)[0];  // Prev op
    CColRefArray *output_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CColRefArray *child_cols =
        pexprProjRelational->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CColRefArray *interm_output_cols =
        plan_expr->DeriveOutputColumns()->Pdrgpcr(mp);
    CExpression *pexprProjList = (*plan_expr)[1];  // Projection list
    const CColRefArray *grouping_cols = agg_op->PdrgpcrGroupingCols();
    CColRefSet *grouping_col_set = GPOS_NEW(mp) CColRefSet(mp, grouping_cols);
    CColRefArray *grouping_cols_sorted = grouping_col_set->Pdrgpcr(mp);

    // get agg groups
    uint64_t num_outputs_in_grouping_col = 0;
    for (ULONG group_col_idx = 0; group_col_idx < grouping_cols_sorted->Size();
         group_col_idx++) {
        CColRef *col = grouping_cols_sorted->operator[](group_col_idx);

        bool is_subordinated_col = false;
        if (col->NodeId() != gpos::ulong_max) {
            has_node_grouping_key = true;
            if (col->NodeId() != col->Id()) {
                is_subordinated_col = true;
            } else {
                node_pid_idxs.push_back(group_col_idx);
            }
        }

        ULONG child_idx = child_cols->IndexOf(col);
        auto col_type = pGetColumnsDuckDBType(col);
        agg_groups.push_back(make_unique<duckdb::BoundReferenceExpression>(
            col_type, child_idx));
        proj_exprs.push_back(make_unique<duckdb::BoundReferenceExpression>(
            col_type, child_idx));
        proj_types.push_back(col_type);
        output_column_names_proj.push_back(pGetColNameFromColRef(col));
        if (output_cols->IndexOf(col) != gpos::ulong_max) {
            output_projection_mapping.push_back(num_outputs_in_grouping_col++);
        } else {
            output_projection_mapping.push_back(std::numeric_limits<uint32_t>::max());
        }
    }
    // get output columns
    for (ULONG output_col_idx = 0; output_col_idx < output_cols->Size();
         output_col_idx++) {
        CColRef *col = output_cols->operator[](output_col_idx);
        if (grouping_cols->IndexOf(col) == gpos::ulong_max)
            continue;
        // output_projection_mapping;
        types.push_back(pGetColumnsDuckDBType(col));
        output_column_names.push_back(pGetColNameFromColRef(col));
    }
    bool has_pre_projection = false;
    bool has_post_projection = false;
    bool adjust_agg_groups_performed = false;
    // handle aggregation expressions
    for (ULONG elem_idx = 0; elem_idx < pexprProjList->Arity(); elem_idx++) {
        CExpression *pexprProjElem = pexprProjList->operator[](elem_idx);
        CExpression *pexprScalarExpr = pexprProjElem->operator[](0);
        CExpression *pexprAggExpr;
        output_column_names.push_back(pGetColNameFromColRef(
            ((CScalarProjectElement *)pexprProjElem->Pop())->Pcr()));
        output_column_names_proj.push_back(pGetColNameFromColRef(
            ((CScalarProjectElement *)pexprProjElem->Pop())->Pcr()));
        if (pexprScalarExpr->Pop()->Eopid() != COperator::EopScalarAggFunc) {
            has_post_projection = true;
        }
        else {
            CExpression *aggargs_expr = pexprScalarExpr->operator[](0);
            if (aggargs_expr->Arity() == 0) {  // no child
                agg_exprs.push_back(std::move(
                    pTransformScalarExpr(pexprScalarExpr, child_cols)));
                types.push_back(agg_exprs.back()->return_type);
                continue;
            }
            if (aggargs_expr->operator[](0)->Pop()->Eopid() !=
                COperator::EopScalarIdent) {
                has_pre_projection = true;
                if (!adjust_agg_groups_performed) {
                    for (ULONG agg_group_idx = 0;
                         agg_group_idx < agg_groups.size(); agg_group_idx++) {
                        auto agg_group_expr =
                            (duckdb::BoundReferenceExpression *)
                                agg_groups[agg_group_idx]
                                    .get();
                        agg_group_expr->index = agg_group_idx;
                    }
                    ULONG accm_agg_expr_idx = 0;
                    for (ULONG agg_expr_idx = 0;
                         agg_expr_idx < agg_exprs.size(); agg_expr_idx++) {
                        auto agg_expr = (duckdb::BoundAggregateExpression *)
                                            agg_exprs[agg_expr_idx]
                                                .get();
                        for (ULONG agg_expr_child_idx = 0;
                             agg_expr_child_idx < agg_expr->children.size();
                             agg_expr_child_idx++) {
                            auto bound_expr =
                                (duckdb::BoundReferenceExpression *)agg_expr
                                    ->children[agg_expr_child_idx]
                                    .get();
                            bound_expr->index =
                                agg_groups.size() + accm_agg_expr_idx++;
                        }
                    }
                    adjust_agg_groups_performed = true;
                }
                proj_exprs.push_back(std::move(pTransformScalarExpr(
                    aggargs_expr->operator[](0), child_cols)));
                agg_exprs.push_back(std::move(pTransformScalarAggFunc(
                    pexprScalarExpr, child_cols, proj_exprs.back()->return_type,
                    proj_exprs.size() - 1)));
                proj_types.push_back(proj_exprs.back()->return_type);
                types.push_back(agg_exprs.back()->return_type);
            }
            else {
                proj_exprs.push_back(std::move(pTransformScalarExpr(
                    aggargs_expr->operator[](0), child_cols)));
                if (has_pre_projection) {
                    agg_exprs.push_back(std::move(
                        pTransformScalarAggFunc(pexprScalarExpr, child_cols,
                                                proj_exprs.back()->return_type,
                                                proj_exprs.size() - 1)));
                }
                else {
                    agg_exprs.push_back(std::move(
                        pTransformScalarExpr(pexprScalarExpr, child_cols)));
                }
                proj_types.push_back(proj_exprs.back()->return_type);
                types.push_back(agg_exprs.back()->return_type);
            }
        }
    }
    // ScalarAgg with no real aggregates (all projection items are pass-through
    // CScalarIdent) — emit a Projection instead of a HashAggregate.
    if (agg_exprs.empty() && agg_groups.empty() && has_post_projection) {
        // Build projection expressions from child columns referenced in output
        vector<unique_ptr<duckdb::Expression>> passthru_exprs;
        vector<duckdb::LogicalType> passthru_types;
        vector<string> passthru_names;
        for (ULONG i = 0; i < pexprProjList->Arity(); i++) {
            CExpression *pe = pexprProjList->operator[](i);
            CExpression *se = pe->operator[](0);
            auto duckdb_expr = pTransformScalarExpr(se, child_cols);
            passthru_types.push_back(duckdb_expr->return_type);
            passthru_names.push_back(pGetColNameFromColRef(
                ((CScalarProjectElement *)pe->Pop())->Pcr()));
            passthru_exprs.push_back(std::move(duckdb_expr));
        }
        duckdb::Schema passthru_schema;
        passthru_schema.setStoredTypes(passthru_types);
        passthru_schema.setStoredColumnNames(passthru_names);
        pBuildSchemaFlowGraphForUnaryOperator(passthru_schema);
        auto *proj_op = new duckdb::PhysicalProjection(passthru_schema,
                                                        move(passthru_exprs));
        result->push_back(proj_op);
        return result;
    }
    duckdb::Schema tmp_schema;
    tmp_schema.setStoredTypes(types);
    tmp_schema.setStoredColumnNames(output_column_names);
    if (has_pre_projection) {
        duckdb::Schema proj_schema;
        proj_schema.setStoredTypes(proj_types);
        proj_schema.setStoredColumnNames(output_column_names_proj);
        pBuildSchemaFlowGraphForUnaryOperator(proj_schema);
        duckdb::CypherPhysicalOperator *proj_op =
            new duckdb::PhysicalProjection(proj_schema, move(proj_exprs));
        result->push_back(proj_op);
    }
    pBuildSchemaFlowGraphForUnaryOperator(tmp_schema);
    duckdb::CypherPhysicalOperator *op;
    if (agg_groups.empty()) {
        op = new duckdb::PhysicalHashAggregate(
            tmp_schema, output_projection_mapping, move(agg_exprs),
            node_pid_idxs);
    }
    else {
        op = new duckdb::PhysicalHashAggregate(
            tmp_schema, output_projection_mapping, move(agg_exprs),
            move(agg_groups), node_pid_idxs);
    }
    result->push_back(op);
    pGenerateSchemaFlowGraph(*result);
    // finish pipeline
    auto pipeline = new duckdb::CypherPipeline(*result, pipelines.size());
    pipelines.push_back(pipeline);
    // new pipeline
    auto new_result = new duckdb::CypherPhysicalOperatorGroups();
    new_result->push_back(op);
    if (generate_sfg) {
        // Set for the current pipeline. We consider after group by, schema is merged.
        pClearSchemaFlowGraph();
        pipeline_operator_types.push_back(duckdb::OperatorType::UNARY);
        num_schemas_of_childs.push_back({1});
        pipeline_schemas.push_back({tmp_schema});
        pipeline_union_schema.push_back(tmp_schema);
    }
    return new_result;
}

duckdb::CypherPhysicalOperatorGroups *Planner::pTransformEopPhysicalFilter(
    CExpression *plan_expr)
{
    CMemoryPool *mp = this->memory_pool;
    /* Non-root - call single child */
    duckdb::CypherPhysicalOperatorGroups *result =
        pTraverseTransformPhysicalPlan(plan_expr->PdrgPexpr()->operator[](0));

    CPhysicalFilter *filter_op = (CPhysicalFilter *)plan_expr->Pop();
    CExpression *filter_expr = plan_expr;
    CExpression *filter_pred_expr = filter_expr->operator[](1);
    vector<unique_ptr<duckdb::Expression>> filter_exprs;

    CColRefArray *output_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CExpression *pexprOuter = (*plan_expr)[0];
    CColRefArray *outer_cols = pexprOuter->Prpp()->PcrsRequired()->Pdrgpcr(mp);

    vector<duckdb::LogicalType> output_types;
    pGetColumnsDuckDBType(output_cols, output_types);

    duckdb::ExpressionType exp_type;
    // D_ASSERT(filter_pred_expr->Pop()->Eopid() == COperator::EOperatorId::EopScalarCmp);
    // CScalarCmp *sccmp = (CScalarCmp *)filter_pred_expr->Pop();
    // exp_type = pTranslateCmpType(sccmp->ParseCmpType());

    filter_exprs.push_back(
        std::move(pTransformScalarExpr(filter_pred_expr, outer_cols, nullptr)));

    duckdb::Schema tmp_schema;
    duckdb::CypherPhysicalOperator *last_op = result->back();
    tmp_schema.setStoredTypes(last_op->GetTypes());
    duckdb::CypherPhysicalOperator *op =
        new duckdb::PhysicalFilter(tmp_schema, move(filter_exprs));
    result->push_back(op);

    // generate schema flow graph for the filter
    pBuildSchemaFlowGraphForUnaryOperator(tmp_schema);

    // we need further projection if we don't need filter column anymore
    if (output_cols->Size() != outer_cols->Size()) {
        duckdb::Schema output_schema;
        output_schema.setStoredTypes(output_types);
        vector<unique_ptr<duckdb::Expression>> proj_exprs;
        for (ULONG col_idx = 0; col_idx < output_cols->Size(); col_idx++) {
            CColRef *col = (*output_cols)[col_idx];
            ULONG idx = outer_cols->IndexOf(col);
            D_ASSERT(idx != gpos::ulong_max);
            proj_exprs.push_back(make_unique<duckdb::BoundReferenceExpression>(
                output_types[col_idx], (int)idx));
        }
        if (proj_exprs.size() != 0) {
            D_ASSERT(proj_exprs.size() == output_cols->Size());
            duckdb::CypherPhysicalOperator *op = new duckdb::PhysicalProjection(
                output_schema, std::move(proj_exprs));
            result->push_back(op);

            pBuildSchemaFlowGraphForUnaryOperator(output_schema);
        }
    }

    return result;
}

duckdb::CypherPhysicalOperatorGroups *Planner::pTransformEopSort(
    CExpression *plan_expr)
{
    CMemoryPool *mp = this->memory_pool;

    /* Non-root - call single child */
    duckdb::CypherPhysicalOperatorGroups *result =
        pTraverseTransformPhysicalPlan(plan_expr->PdrgPexpr()->operator[](0));

    CPhysicalSort *proj_op = (CPhysicalSort *)plan_expr->Pop();

    const COrderSpec *pos = proj_op->Pos();

    CColRefArray *output_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CExpression *pexprOuter = (*plan_expr)[0];
    CColRefArray *outer_cols = pexprOuter->Prpp()->PcrsRequired()->Pdrgpcr(mp);

    duckdb::CypherPhysicalOperator *last_op = result->back();
    auto &last_op_result_types = last_op->GetTypes();

    vector<duckdb::BoundOrderByNode> orders;
    for (ULONG ul = 0; ul < pos->UlSortColumns(); ul++) {
        const CColRef *col = pos->Pcr(ul);
        ULONG idx = outer_cols->IndexOf(col);

        unique_ptr<duckdb::Expression> order_expr =
            make_unique<duckdb::BoundReferenceExpression>(
                last_op_result_types[idx],
                plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp)->IndexOf(col));

        duckdb::OrderType order_type =
            IMDId::MDIdCompare(pos->GetMdIdSortOp(ul),
                               col->RetrieveType()->GetMdidForCmpType(
                                   IMDType::EcmptG)) == false
                ? duckdb::OrderType::ASCENDING
                : duckdb::OrderType::DESCENDING;

        duckdb::BoundOrderByNode order(
            order_type, pTranslateNullType(pos->Ent(ul)), move(order_expr));
        orders.push_back(move(order));
    }

    duckdb::Schema tmp_schema;
    tmp_schema.setStoredTypes(last_op->GetTypes());

    pBuildSchemaFlowGraphForUnaryOperator(tmp_schema);

    duckdb::CypherPhysicalOperator *op =
        new duckdb::PhysicalSort(tmp_schema, move(orders));
    result->push_back(op);
    pGenerateSchemaFlowGraph(*result);

    // break pipeline
    auto pipeline = new duckdb::CypherPipeline(*result, pipelines.size());
    pipelines.push_back(pipeline);

    auto new_result = new duckdb::CypherPhysicalOperatorGroups();
    new_result->push_back(op);

    if (generate_sfg) {
        // Set for the current pipeline. We consider after group by, schema is merged.
        pClearSchemaFlowGraph();
        pipeline_operator_types.push_back(duckdb::OperatorType::UNARY);
        num_schemas_of_childs.push_back({1});
        pipeline_schemas.push_back({tmp_schema});
        pipeline_union_schema.push_back(tmp_schema);
    }

    return new_result;
}

duckdb::CypherPhysicalOperatorGroups *Planner::pTransformEopTopNSort(
    CExpression *plan_expr)
{
    CMemoryPool *mp = this->memory_pool;

    /* Non-root - call single child */
    D_ASSERT(plan_expr->Pop()->Eopid() ==
             COperator::EOperatorId::EopPhysicalLimit);
    D_ASSERT(plan_expr->operator[](0)->Pop()->Eopid() ==
             COperator::EOperatorId::EopPhysicalLimit);
    D_ASSERT(plan_expr->operator[](0)->operator[](0)->Pop()->Eopid() ==
             COperator::EOperatorId::EopPhysicalSort);
    duckdb::CypherPhysicalOperatorGroups *result =
        pTraverseTransformPhysicalPlan(
            plan_expr->operator[](0)->operator[](0)->PdrgPexpr()->operator[](
                0));

    // get limit info
    bool has_limit = false;
    CExpression *limit_expr = plan_expr;
    CPhysicalLimit *limit_op = (CPhysicalLimit *)limit_expr->Pop();
    D_ASSERT(limit_expr->operator[](1)->Pop()->Eopid() ==
             COperator::EOperatorId::EopScalarConst);
    D_ASSERT(limit_expr->operator[](2)->Pop()->Eopid() ==
             COperator::EOperatorId::EopScalarConst);

    int64_t offset, limit;
    if (!limit_op->FHasCount()) {
        has_limit = false;
    }
    else {
        has_limit = true;
        CDatumInt8GPDB *offset_datum =
            (CDatumInt8GPDB *)(((CScalarConst *)limit_expr->operator[](1)
                                    ->Pop())
                                   ->GetDatum());
        CDatumInt8GPDB *limit_datum =
            (CDatumInt8GPDB *)(((CScalarConst *)limit_expr->operator[](2)
                                    ->Pop())
                                   ->GetDatum());
        offset = offset_datum->Value();
        limit = limit_datum->Value();
    }

    // currently, second limit has no count info
    D_ASSERT(!((CPhysicalLimit *)plan_expr->operator[](0)->Pop())->FHasCount());

    // get sort info
    CExpression *sort_expr = plan_expr->operator[](0)->operator[](0);
    CPhysicalSort *proj_op = (CPhysicalSort *)sort_expr->Pop();

    const COrderSpec *pos = proj_op->Pos();

    CColRefArray *output_cols = sort_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CExpression *pexprOuter = (*sort_expr)[0];
    CColRefArray *outer_cols = pexprOuter->Prpp()->PcrsRequired()->Pdrgpcr(mp);

    duckdb::CypherPhysicalOperator *last_op = result->back();
    auto &last_op_result_types = last_op->GetTypes();

    vector<duckdb::BoundOrderByNode> orders;
    for (ULONG ul = 0; ul < pos->UlSortColumns(); ul++) {
        const CColRef *col = pos->Pcr(ul);
        ULONG idx = outer_cols->IndexOf(col);

        unique_ptr<duckdb::Expression> order_expr =
            make_unique<duckdb::BoundReferenceExpression>(
                last_op_result_types[idx],
                plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp)->IndexOf(col));

        duckdb::OrderType order_type =
            IMDId::MDIdCompare(pos->GetMdIdSortOp(ul),
                               col->RetrieveType()->GetMdidForCmpType(
                                   IMDType::EcmptG)) == false
                ? duckdb::OrderType::ASCENDING
                : duckdb::OrderType::DESCENDING;

        duckdb::BoundOrderByNode order(
            order_type, pTranslateNullType(pos->Ent(ul)), move(order_expr));
        orders.push_back(move(order));
    }

    duckdb::Schema tmp_schema;
    tmp_schema.setStoredTypes(last_op->GetTypes());

    pBuildSchemaFlowGraphForUnaryOperator(tmp_schema);

    duckdb::CypherPhysicalOperator *op;
    if (has_limit) {
        op = new duckdb::PhysicalTopNSort(tmp_schema, move(orders), limit,
                                          offset);
    }
    else {
        op = new duckdb::PhysicalSort(tmp_schema, move(orders));
    }

    result->push_back(op);
    pGenerateSchemaFlowGraph(*result);

    // break pipeline
    auto pipeline = new duckdb::CypherPipeline(*result, pipelines.size());
    pipelines.push_back(pipeline);

    auto new_result = new duckdb::CypherPhysicalOperatorGroups();
    new_result->push_back(op);

    if (generate_sfg) {
        // Set for the current pipeline. We consider after group by, schema is merged.
        pClearSchemaFlowGraph();
        pipeline_operator_types.push_back(duckdb::OperatorType::UNARY);
        num_schemas_of_childs.push_back({1});
        pipeline_schemas.push_back({tmp_schema});
        pipeline_union_schema.push_back(tmp_schema);
    }

    return new_result;
}


duckdb::CypherPhysicalOperatorGroups* Planner::pTransformEopShortestPath(CExpression* plan_expr) {
	CMemoryPool* mp = this->memory_pool;
	CPhysicalShortestPath *shrtst_op = (CPhysicalShortestPath*) plan_expr->Pop();
	// Debug removed
    CColRefArray *output_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CColRefArray *input_cols = (*plan_expr)[0]->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CColRef *path_col = ((CScalarProjectElement*)(*plan_expr)[1]->operator[](0)->Pop())->Pcr();
    auto pname = shrtst_op->PnameAlias();
    auto ptabledesc = shrtst_op->PtabdescArray()->operator[](0);
    auto pcr_src = shrtst_op->PcrSource();
    auto pcr_dest = shrtst_op->PcrDestination();
    uint64_t lower_bound = shrtst_op->PathLowerBound();
    uint64_t upper_bound = shrtst_op->PathUpperBound();
    if (upper_bound == -1) upper_bound = std::numeric_limits<uint64_t>::max();

    // DuckDB types
	duckdb::CypherPhysicalOperatorGroups *result = pTraverseTransformPhysicalPlan(plan_expr->PdrgPexpr()->operator[](0));
    vector<duckdb::LogicalType> types;
    vector<uint32_t> input_col_map;
    duckdb::Schema schema;

    // Construct parameters
    pConstructColMapping(input_cols, output_cols, input_col_map);
    pGetDuckDBTypesFromColRefs(output_cols, types);
    schema.setStoredTypes(types);
    duckdb::idx_t src_id_idx = input_cols->IndexOf(pcr_src);
    duckdb::idx_t dest_id_idx = input_cols->IndexOf(pcr_dest);
    duckdb::idx_t output_idx = output_cols->IndexOf(path_col);

    // Get OID
    auto pmdrel = lGetMDAccessor()->RetrieveRel(ptabledesc->MDId());
    D_ASSERT(pmdrel != nullptr);
    D_ASSERT(pmdrel->IndexCount() == 3); // _id, _sid, _tid
    // for fwd
    auto pmdidIndex = pmdrel->IndexMDidAt(1);
    auto pmdindex = lGetMDAccessor()->RetrieveIndex(pmdidIndex);
    D_ASSERT(pmdindex != nullptr);
    OID path_index_oid_fwd = CMDIdGPDB::CastMdid(pmdindex->MDId())->Oid();
    // for bwd
    pmdidIndex = pmdrel->IndexMDidAt(2);
    pmdindex = lGetMDAccessor()->RetrieveIndex(pmdidIndex);
    D_ASSERT(pmdindex != nullptr);
    OID path_index_oid_bwd = CMDIdGPDB::CastMdid(pmdindex->MDId())->Oid();

    duckdb::CypherPhysicalOperator *op = new duckdb::PhysicalShortestPathJoin(schema, path_index_oid_fwd, path_index_oid_bwd, 
                                                    input_col_map, output_idx, src_id_idx, dest_id_idx, lower_bound, upper_bound);
    result->push_back(op);
    pBuildSchemaFlowGraphForUnaryOperator(schema);

	return result;
}


duckdb::CypherPhysicalOperatorGroups* Planner::pTransformEopAllShortestPath(CExpression* plan_expr) {
	CMemoryPool* mp = this->memory_pool;
	CPhysicalShortestPath *shrtst_op = (CPhysicalShortestPath*) plan_expr->Pop();
    CColRefArray *output_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CColRefArray *input_cols = (*plan_expr)[0]->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CColRef *path_col = ((CScalarProjectElement*)(*plan_expr)[1]->operator[](0)->Pop())->Pcr();
    auto pname = shrtst_op->PnameAlias();
    auto ptabledesc = shrtst_op->PtabdescArray()->operator[](0);
    auto pcr_src = shrtst_op->PcrSource();
    auto pcr_dest = shrtst_op->PcrDestination();
    uint64_t lower_bound = shrtst_op->PathLowerBound();
    uint64_t upper_bound = shrtst_op->PathUpperBound();
    if (upper_bound == -1) upper_bound = std::numeric_limits<uint64_t>::max();

    // DuckDB types
	duckdb::CypherPhysicalOperatorGroups *result = pTraverseTransformPhysicalPlan(plan_expr->PdrgPexpr()->operator[](0));
    vector<duckdb::LogicalType> types;
    vector<uint32_t> input_col_map;
    duckdb::Schema schema;

    // Construct parameters
    pConstructColMapping(input_cols, output_cols, input_col_map);
    pGetDuckDBTypesFromColRefs(output_cols, types);
    schema.setStoredTypes(types);
    duckdb::idx_t src_id_idx = input_cols->IndexOf(pcr_src);
    duckdb::idx_t dest_id_idx = input_cols->IndexOf(pcr_dest);
    duckdb::idx_t output_idx = output_cols->IndexOf(path_col);

    // Get OID
    auto pmdrel = lGetMDAccessor()->RetrieveRel(ptabledesc->MDId());
    D_ASSERT(pmdrel != nullptr);
    D_ASSERT(pmdrel->IndexCount() == 3); // _id, _sid, _tid
    // for fwd
    auto pmdidIndex = pmdrel->IndexMDidAt(1);
    auto pmdindex = lGetMDAccessor()->RetrieveIndex(pmdidIndex);
    D_ASSERT(pmdindex != nullptr);
    OID path_index_oid_fwd = CMDIdGPDB::CastMdid(pmdindex->MDId())->Oid();
    // for bwd
    pmdidIndex = pmdrel->IndexMDidAt(2);
    pmdindex = lGetMDAccessor()->RetrieveIndex(pmdidIndex);
    D_ASSERT(pmdindex != nullptr);
    OID path_index_oid_bwd = CMDIdGPDB::CastMdid(pmdindex->MDId())->Oid();

    duckdb::CypherPhysicalOperator *op = new duckdb::PhysicalAllShortestPathJoin(schema, path_index_oid_fwd, path_index_oid_bwd, 
                                                    input_col_map, output_idx, src_id_idx, dest_id_idx, lower_bound, upper_bound);
    result->push_back(op);
    pBuildSchemaFlowGraphForUnaryOperator(schema);

	return result;
}

bool Planner::pIsIndexJoinOnPhysicalID(CExpression *plan_expr)
{
    // if id seek then true, else adjidxjoin

    D_ASSERT(plan_expr->Pop()->Eopid() ==
                 COperator::EOperatorId::EopPhysicalInnerIndexNLJoin ||
             plan_expr->Pop()->Eopid() ==
                 COperator::EOperatorId::EopPhysicalLeftOuterIndexNLJoin);

    CPhysicalInnerIndexNLJoin *proj_op =
        (CPhysicalInnerIndexNLJoin *)plan_expr->Pop();
    CExpression *pexprInner = (*plan_expr)[1];

    CExpression *inner_root = pexprInner;
    uint64_t idx_obj_id;
    while (true) {
        if (inner_root->Pop()->Eopid() ==
                COperator::EOperatorId::EopPhysicalIndexScan ||
            inner_root->Pop()->Eopid() ==
                COperator::EOperatorId::EopPhysicalIndexOnlyScan) {
            // IdxScan
            CPhysicalIndexScan *idxscan_op =
                (CPhysicalIndexScan *)inner_root->Pop();
            CMDIdGPDB *index_mdid =
                CMDIdGPDB::CastMdid(idxscan_op->Pindexdesc()->MDId());
            gpos::ULONG oid = index_mdid->Oid();
            idx_obj_id = (uint64_t)oid;
        }
        // reached to the bottom
        if (inner_root->Arity() == 0) {
            break;
        }
        else {
            inner_root =
                inner_root->operator[](0);  // pass first child in linear plan
        }
    }
    D_ASSERT(inner_root != pexprInner);

    // search catalog
    duckdb::Catalog &cat_instance = context->db->GetCatalog();
    duckdb::IndexCatalogEntry *index_cat =
        (duckdb::IndexCatalogEntry *)cat_instance.GetEntry(
            *context, DEFAULT_SCHEMA, idx_obj_id);
    if (index_cat->GetIndexType() == duckdb::IndexType::PHYSICAL_ID) {
        return true;
    }
    else {
        return false;
    }
}

bool Planner::pMatchExprPattern(CExpression *root,
                                vector<COperator::EOperatorId> &pattern,
                                uint64_t pattern_root_idx,
                                bool physical_op_only)
{

    D_ASSERT(pattern.size() > 0);
    D_ASSERT(pattern_root_idx < pattern.size());

    // conjunctive checks
    bool match = true;

    // recursively iterate
    // if depth shorter than pattern, also return false.
    CExpressionArray *children = root->PdrgPexpr();
    const ULONG children_size = children->Size();

    // construct recursive child_pattern_match
    for (int i = 0; i < children_size; i++) {
        CExpression *child_expr = children->operator[](i);
        // check pattern for child
        if (physical_op_only && !child_expr->Pop()->FPhysical()) {
            // dont care other than phyiscal operator
            continue;
        }
        if (pattern_root_idx + 1 < pattern.size()) {
            // more patterns to check
            match = match &&
                    pMatchExprPattern(child_expr, pattern, pattern_root_idx + 1,
                                      physical_op_only);
        }
    }
    // check pattern for root
    match = match && root->Pop()->Eopid() == pattern[pattern_root_idx];

    return match;
}

bool Planner::pIsUnionAllOpAccessExpression(CExpression *expr)
{
    // FIXME
    auto p1 = vector<COperator::EOperatorId>({
        COperator::EOperatorId::EopPhysicalSerialUnionAll,
        COperator::EOperatorId::EopPhysicalComputeScalarColumnar,
        COperator::EOperatorId::EopPhysicalTableScan,
    });
    auto p2 = vector<COperator::EOperatorId>({
        COperator::EOperatorId::EopPhysicalSerialUnionAll,
        COperator::EOperatorId::EopPhysicalComputeScalarColumnar,
        COperator::EOperatorId::EopPhysicalIndexScan,
    });
    auto p3 = vector<COperator::EOperatorId>({
        COperator::EOperatorId::EopPhysicalSerialUnionAll,
        COperator::EOperatorId::EopPhysicalComputeScalarColumnar,
        COperator::EOperatorId::EopPhysicalIndexOnlyScan,
    });
    auto p4 = vector<COperator::EOperatorId>({
        COperator::EOperatorId::EopPhysicalSerialUnionAll,
        COperator::EOperatorId::EopPhysicalComputeScalarColumnar,
        COperator::EOperatorId::EopPhysicalFilter,
        COperator::EOperatorId::EopPhysicalTableScan,
    });
    auto p5 = vector<COperator::EOperatorId>({
        COperator::EOperatorId::EopPhysicalSerialUnionAll,
        COperator::EOperatorId::EopPhysicalComputeScalarColumnar,
        COperator::EOperatorId::EopPhysicalFilter,
        COperator::EOperatorId::EopPhysicalIndexScan,
    });
    return pMatchExprPattern(expr, p1, 0, true) ||
           pMatchExprPattern(expr, p2, 0, true) ||
           pMatchExprPattern(expr, p3, 0, true) ||
           pMatchExprPattern(expr, p4, 0, true) ||
           pMatchExprPattern(expr, p5, 0, true);
}

uint64_t Planner::pGetColIdxFromTable(OID table_oid, const CColRef *target_col)
{
    CMemoryPool *mp = this->memory_pool;

    CColRefTable *colref_table = (CColRefTable *)target_col;
    INT attr_no = colref_table->AttrNum();
    if (attr_no == (INT)-1) {
        return 0;
    }
    else {
        return (uint64_t)attr_no;
    }
}


void Planner::pGenerateScanMapping(OID table_oid, CColRefArray *columns,
                                   vector<uint64_t> &out_mapping)
{
    columns->AddRef();
    D_ASSERT(out_mapping.size() == 0);  // assert empty

    for (uint64_t i = 0; i < columns->Size(); i++) {
        auto table_col_idx =
            pGetColIdxFromTable(table_oid, columns->operator[](i));
        out_mapping.push_back(table_col_idx);
    }
}

void Planner::pGenerateTypes(CColRefArray *columns,
                             vector<duckdb::LogicalType> &out_types)
{
    columns->AddRef();
    D_ASSERT(out_types.size() == 0);  // assert empty
    for (uint64_t i = 0; i < columns->Size(); i++) {
        CMDIdGPDB *type_mdid =
            CMDIdGPDB::CastMdid(columns->operator[](i)->RetrieveType()->MDId());
        OID type_oid = type_mdid->Oid();
        INT type_mod = columns->operator[](i)->TypeModifier();
        out_types.push_back(pConvertTypeOidToLogicalType(type_oid, type_mod));
    }
}

void Planner::pGenerateColumnNames(CColRefArray *columns,
                                   vector<string> &out_col_names)
{
    columns->AddRef();
    D_ASSERT(out_col_names.size() == 0);
    for (uint64_t i = 0; i < columns->Size(); i++) {
        out_col_names.push_back(pGetColNameFromColRef(columns->operator[](i)));
    }
}

void Planner::pGenerateSchemaFlowGraph(
    duckdb::CypherPhysicalOperatorGroups &final_pipeline_ops)
{
    if (!generate_sfg)
        return;
    duckdb::SchemaFlowGraph sfg(final_pipeline_ops.size(),
                                pipeline_operator_types, num_schemas_of_childs,
                                pipeline_schemas, other_source_schemas,
                                pipeline_union_schema);
    auto &num_schemas_of_childs_ = sfg.GetNumSchemasOfChilds();
    vector<vector<uint64_t>> flow_graph;
    flow_graph.resize(final_pipeline_ops.size());
    for (auto i = 0; i < flow_graph.size(); i++) {
        uint64_t num_total_child_schemas = 1;
        for (auto j = 0; j < num_schemas_of_childs_[i].size(); j++) {
            num_total_child_schemas *= num_schemas_of_childs_[i][j];
        }
        if (i == 0) {
            flow_graph[i].resize(num_total_child_schemas);
            for (auto j = 0; j < flow_graph[i].size(); j++) {
                flow_graph[i][j] = j;  // TODO
            }
        }
    }

    sfg.SetFlowGraph(flow_graph);
    sfgs.push_back(std::move(sfg));
}

void Planner::pClearSchemaFlowGraph()
{
    pipeline_operator_types.clear();
    num_schemas_of_childs.clear();
    pipeline_schemas.clear();
    pipeline_union_schema.clear();
}

void Planner::pInitializeSchemaFlowGraph()
{
    pipeline_operator_types.clear();
    num_schemas_of_childs.clear();
    pipeline_schemas.clear();
    pipeline_union_schema.clear();
    sfgs.clear();
    generate_sfg = false;
}

void Planner::pGenerateMappingInfo(vector<duckdb::idx_t> &scan_cols_id,
                                   duckdb::PropertyKeyID_vector *key_ids,
                                   vector<duckdb::LogicalType> &global_types,
                                   vector<duckdb::LogicalType> &local_types,
                                   vector<uint64_t> &projection_mapping,
                                   vector<uint64_t> &scan_projection_mapping)
{
    D_ASSERT(scan_cols_id.size() == global_types.size());

    for (int i = 0; i < scan_cols_id.size(); i++) {
        if (scan_cols_id[i] == 0) {
            // physical id column
            projection_mapping.push_back(i);
            scan_projection_mapping.push_back(0);
            local_types.push_back(duckdb::LogicalType::ID);
        } else {
            // TODO this is super inefficient -> we need to sort key_ids
            bool found = false;
            for (int j = 0; j < key_ids->size(); j++) {
                if (scan_cols_id[i] == (*key_ids)[j]) {
                    projection_mapping.push_back(i);
                    scan_projection_mapping.push_back(j + 1);
                    local_types.push_back(global_types[i]);
                    found = true;
                    break;
                }
            }
            if (!found) {
                projection_mapping.push_back(i);
                scan_projection_mapping.push_back(std::numeric_limits<uint64_t>::max());
                local_types.push_back(duckdb::LogicalType::SQLNULL);
            }
        }
    }
}


void Planner::pGenerateMappingInfo(vector<duckdb::idx_t> &scan_cols_id,
                                   duckdb::PropertyKeyID_vector *key_ids,
                                   vector<duckdb::LogicalType> &global_types,
                                   vector<duckdb::LogicalType> &local_types,
                                   vector<uint32_t> &union_inner_col_map,
                                   vector<uint32_t> &inner_col_map,
                                   vector<uint64_t> &projection_mapping,
                                   vector<uint64_t> &scan_projection_mapping,
                                   bool load_physical_id_col)
{
    D_ASSERT(scan_cols_id.size() == global_types.size());

    for (int i = 0; i < scan_cols_id.size(); i++) {
        // construct scan_projection_mapping, projection_mapping, scan_types
        if (scan_cols_id[i] == 0) {
            if (load_physical_id_col) {
                projection_mapping.push_back(i);
                scan_projection_mapping.push_back(0);
                local_types.push_back(duckdb::LogicalType::ID);
            }
        } else {
            bool found = false;
            for (int j = 0; j < key_ids->size(); j++) {
                if (scan_cols_id[i] == (*key_ids)[j]) {
                    projection_mapping.push_back(i);
                    scan_projection_mapping.push_back(j + 1);
                    local_types.push_back(global_types[i]);
                    found = true;
                    break;
                }
            }
            if (!found) continue;
        }

        // construct inner_col_map
        if (load_physical_id_col) {
            inner_col_map.push_back(union_inner_col_map[i]);
        }
        else if (!load_physical_id_col && scan_cols_id[i] != 0) {
            inner_col_map.push_back(union_inner_col_map[i - 1]);
        }
    }
}

void Planner::pBuildSchemaFlowGraphForSingleSchemaScan(duckdb::Schema &output_schema)
{    
    if (!restrict_generate_sfg_for_unionall) {
        generate_sfg = true;
        pipeline_operator_types.push_back(duckdb::OperatorType::UNARY);
        num_schemas_of_childs.push_back({1});
        pipeline_schemas.push_back({output_schema});
        pipeline_union_schema.push_back(output_schema);
    } else {
        other_source_schemas.push_back({output_schema});
    }
}

void Planner::pBuildSchemaFlowGraphForMultiSchemaScan(
    duckdb::Schema &global_schema, vector<duckdb::Schema>& local_schemas)
{
    if (!restrict_generate_sfg_for_unionall) {
        generate_sfg = true;
        pipeline_operator_types.push_back(duckdb::OperatorType::UNARY);
        num_schemas_of_childs.push_back({local_schemas.size()});
        pipeline_schemas.push_back(local_schemas);
        pipeline_union_schema.push_back(global_schema);
    }
    else {
        other_source_schemas.push_back(local_schemas);
    }
}

void Planner::pBuildSchemaFlowGraphForUnaryOperator(
    duckdb::Schema &output_schema)
{
    // Due to unified header implementation, we can fix the num schemas to 1
    if (!generate_sfg)
        return;
    pipeline_operator_types.push_back(duckdb::OperatorType::UNARY);
    num_schemas_of_childs.push_back({1});
    pipeline_union_schema.push_back(output_schema);
}

void Planner::pBuildSchemaFlowGraphForBinaryOperator(
    duckdb::Schema &output_schema, size_t num_rhs_schemas)
{
    if (!generate_sfg)
        return;
    vector<duckdb::Schema> prev_local_schemas = pipeline_schemas.back();
    auto &num_schemas_of_childs_prev = num_schemas_of_childs.back();
    duckdb::idx_t num_total_schemas_prev = 1;
    for (auto i = 0; i < num_schemas_of_childs_prev.size(); i++) {
        num_total_schemas_prev *= num_schemas_of_childs_prev[i];
    }
    pipeline_operator_types.push_back(duckdb::OperatorType::BINARY);
    num_schemas_of_childs.push_back({num_total_schemas_prev, num_rhs_schemas});
    pipeline_schemas.push_back(prev_local_schemas);
    pipeline_union_schema.push_back(output_schema);
}

bool Planner::pIsColumnarProjectionSimpleProject(CExpression *proj_expr)
{
    // check if all projetion expressions are CscalarIdent
    D_ASSERT(proj_expr->Pop()->Eopid() ==
             COperator::EOperatorId::EopPhysicalComputeScalarColumnar);
    D_ASSERT(proj_expr->Arity() == 2);  // 0 prev 1 projlist

    bool result = true;
    ULONG proj_size = proj_expr->operator[](1)->Arity();
    for (ULONG proj_idx = 0; proj_idx < proj_size; proj_idx++) {
        result = result &&
                 // list -> idx'th element -> ident
                 (proj_expr->operator[](1)
                      ->
                      operator[](proj_idx)
                      ->
                      operator[](0)
                      ->Pop()
                      ->Eopid() == COperator::EOperatorId::EopScalarIdent);
    }
    return result;
}

void Planner::pTranslatePredicateToJoinCondition(
    CExpression *pred, vector<duckdb::JoinCondition> &out_conds,
    CColRefArray *lhs_cols, CColRefArray *rhs_cols)
{
    // TODO what about OR condition in duckdb ?? -> IDK
    auto *op = pred->Pop();
    if (op->Eopid() == COperator::EOperatorId::EopScalarBoolOp) {
        CScalarBoolOp *boolop = (CScalarBoolOp *)op;
        if (boolop->Eboolop() == CScalarBoolOp::EBoolOperator::EboolopAnd) {
            D_ASSERT(pred->Arity() == 2);
            // Split predicates
            pTranslatePredicateToJoinCondition(pred->operator[](0), out_conds,
                                               lhs_cols, rhs_cols);
            pTranslatePredicateToJoinCondition(pred->operator[](1), out_conds,
                                               lhs_cols, rhs_cols);
        }
        else if (boolop->Eboolop() == CScalarBoolOp::EBoolOperator::EboolopOr) {
            D_ASSERT(false);
        }
        else if (boolop->Eboolop() ==
                 CScalarBoolOp::EBoolOperator::EboolopNot) {
            // NOT + EQUALS
            D_ASSERT(
                pred->operator[](0)->Pop()->Eopid() ==
                    COperator::EOperatorId::EopScalarCmp &&
                ((CScalarCmp *)(pred->operator[](0)->Pop()))->ParseCmpType() ==
                    IMDType::ECmpType::EcmptEq);
            duckdb::JoinCondition cond;
            unique_ptr<duckdb::Expression> lhs = pTransformScalarExpr(
                pred->operator[](0)->operator[](0), lhs_cols, rhs_cols);
            unique_ptr<duckdb::Expression> rhs = pTransformScalarExpr(
                pred->operator[](0)->operator[](1), lhs_cols, rhs_cols);
            if (lhs->return_type != rhs->return_type) {
                rhs = pGenScalarCast(move(rhs), lhs->return_type);
            }
            cond.left = move(lhs);
            cond.right = move(rhs);
            cond.comparison = duckdb::ExpressionType::COMPARE_NOTEQUAL;
            out_conds.push_back(move(cond));
        }
        else {
            D_ASSERT(false);
        }
    }
    else if (op->Eopid() == COperator::EOperatorId::EopScalarCmp) {
        CScalarCmp *cmpop = (CScalarCmp *)op;
        duckdb::JoinCondition cond;

        // Unwrap CScalarCast to find the underlying CScalarIdent for
        // determining which side (lhs/rhs) each predicate operand belongs to.
        auto pUnwrapToIdent = [](CExpression *expr) -> CScalarIdent * {
            while (expr->Pop()->Eopid() == COperator::EOperatorId::EopScalarCast) {
                D_ASSERT(expr->Arity() == 1);
                expr = expr->operator[](0);
            }
            D_ASSERT(expr->Pop()->Eopid() == COperator::EOperatorId::EopScalarIdent);
            return (CScalarIdent *)expr->Pop();
        };

        CScalarIdent *ident0 = pUnwrapToIdent(pred->operator[](0));
        bool is_left_col_included_in_lhs = lhs_cols->IndexOf(ident0->Pcr()) != gpos::ulong_max;
        D_ASSERT(is_left_col_included_in_lhs ||
                 rhs_cols->IndexOf(ident0->Pcr()) != gpos::ulong_max);
        unique_ptr<duckdb::Expression> lhs =
            is_left_col_included_in_lhs
                ? pTransformScalarExpr(pred->operator[](0), lhs_cols, rhs_cols)
                : pTransformScalarExpr(pred->operator[](1), lhs_cols, rhs_cols);
        unique_ptr<duckdb::Expression> rhs =
            !is_left_col_included_in_lhs
                ? pTransformScalarExpr(pred->operator[](0), lhs_cols, rhs_cols)
                : pTransformScalarExpr(pred->operator[](1), lhs_cols, rhs_cols);

        if (lhs->return_type != rhs->return_type) {
            rhs = pGenScalarCast(move(rhs), lhs->return_type);
        }
        cond.left = move(lhs);
        cond.right = move(rhs);
        if (cmpop->ParseCmpType() == IMDType::ECmpType::EcmptEq) {
            // EQUALS
            cond.comparison = duckdb::ExpressionType::COMPARE_EQUAL;
        }
        else if (cmpop->ParseCmpType() == IMDType::ECmpType::EcmptNEq) {
            // NOT EQUALS
            cond.comparison = duckdb::ExpressionType::COMPARE_NOTEQUAL;
        }
        else if (cmpop->ParseCmpType() == IMDType::ECmpType::EcmptLEq) {
            cond.comparison = duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO;
        }
        else if (cmpop->ParseCmpType() == IMDType::ECmpType::EcmptL) {
            cond.comparison = duckdb::ExpressionType::COMPARE_LESSTHAN;
        }
        else if (cmpop->ParseCmpType() == IMDType::ECmpType::EcmptGEq) {
            cond.comparison = duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO;
        }
        else if (cmpop->ParseCmpType() == IMDType::ECmpType::EcmptG) {
            cond.comparison = duckdb::ExpressionType::COMPARE_GREATERTHAN;
        }
        else {
            D_ASSERT(false);
        }
        out_conds.push_back(move(cond));
    }
    else {
        // Unsupported predicate type for join condition (e.g. list_contains)
        // Crash with diagnostic info
        D_ASSERT(false && "Unsupported join predicate operator type");
    }
    return;
}

bool Planner::pIsCartesianProduct(CExpression *expr)
{
    return expr->Pop()->Eopid() ==
               COperator::EOperatorId::EopPhysicalInnerNLJoin &&
           expr->operator[](2)->Pop()->Eopid() ==
               COperator::EOperatorId::EopScalarConst &&
           CUtils::FScalarConstTrue(expr->operator[](2));
}

bool Planner::pIsFilterPushdownAbleIntoScan(CExpression *selection_expr)
{

    D_ASSERT(selection_expr->Pop()->Eopid() ==
             COperator::EOperatorId::EopPhysicalFilter);
    CExpression *filter_expr = NULL;
    CExpression *filter_pred_expr = NULL;
    filter_expr = selection_expr;
    filter_pred_expr = filter_expr->operator[](1);

    auto ok = filter_pred_expr->Pop()->Eopid() ==
                  COperator::EOperatorId::EopScalarCmp &&
              (((CScalarCmp *)(filter_pred_expr->Pop()))->ParseCmpType() ==
                  IMDType::ECmpType::EcmptEq ||
                 ((CScalarCmp *)(filter_pred_expr->Pop()))->ParseCmpType() ==
                     IMDType::ECmpType::EcmptNEq ||
                 ((CScalarCmp *)(filter_pred_expr->Pop()))->ParseCmpType() ==
                     IMDType::ECmpType::EcmptL ||
                 ((CScalarCmp *)(filter_pred_expr->Pop()))->ParseCmpType() ==
                     IMDType::ECmpType::EcmptLEq ||
                 ((CScalarCmp *)(filter_pred_expr->Pop()))->ParseCmpType() ==
                     IMDType::ECmpType::EcmptG ||
                 ((CScalarCmp *)(filter_pred_expr->Pop()))->ParseCmpType() ==
                     IMDType::ECmpType::EcmptGEq)
              && filter_pred_expr->operator[](0)->Pop()->Eopid() ==
                     COperator::EOperatorId::EopScalarIdent &&
              filter_pred_expr->operator[](1)->Pop()->Eopid() ==
                  COperator::EOperatorId::EopScalarConst;

    return ok;
}

duckdb::OrderByNullType Planner::pTranslateNullType(
    COrderSpec::ENullTreatment ent)
{
    switch (ent) {
        case COrderSpec::ENullTreatment::EntAuto:
            return duckdb::OrderByNullType::ORDER_DEFAULT;
        case COrderSpec::ENullTreatment::EntFirst:
            return duckdb::OrderByNullType::NULLS_FIRST;
        case COrderSpec::ENullTreatment::EntLast:
            return duckdb::OrderByNullType::NULLS_LAST;
        case COrderSpec::ENullTreatment::EntSentinel:
            D_ASSERT(false);
    }
    return duckdb::OrderByNullType::ORDER_DEFAULT;
}

duckdb::JoinType Planner::pTranslateJoinType(COperator *op)
{

    switch (op->Eopid()) {
        case COperator::EOperatorId::EopPhysicalInnerNLJoin:
        case COperator::EOperatorId::EopPhysicalInnerIndexNLJoin:
        case COperator::EOperatorId::EopPhysicalInnerHashJoin:
        case COperator::EOperatorId::EopPhysicalInnerMergeJoin: {
            return duckdb::JoinType::INNER;
        }
        case COperator::EOperatorId::EopPhysicalLeftOuterNLJoin:
        case COperator::EOperatorId::EopPhysicalLeftOuterIndexNLJoin:
        case COperator::EOperatorId::EopPhysicalLeftOuterHashJoin:
        case COperator::EOperatorId::EopPhysicalRightOuterHashJoin: {
            return duckdb::JoinType::LEFT;
        }
        case COperator::EOperatorId::EopPhysicalLeftAntiSemiNLJoin:
        case COperator::EOperatorId::EopPhysicalLeftAntiSemiHashJoin:
        case COperator::EOperatorId::EopPhysicalCorrelatedLeftAntiSemiNLJoin: {
            return duckdb::JoinType::ANTI;
        }
        case COperator::EOperatorId::EopPhysicalLeftSemiNLJoin:
        case COperator::EOperatorId::EopPhysicalLeftSemiHashJoin:
        case COperator::EOperatorId::EopPhysicalCorrelatedLeftSemiNLJoin: {
            return duckdb::JoinType::SEMI;
        }
            // TODO where is FULL OUTER??????
    }
    D_ASSERT(false);
}

CExpression *Planner::pPredToDNF(CExpression *pred)
{
    CMemoryPool *mp = this->memory_pool;

    if (pred->Pop()->Eopid() == COperator::EOperatorId::EopScalarBoolOp) {
        CScalarBoolOp *boolop = (CScalarBoolOp *)pred->Pop();
        if (boolop->Eboolop() == CScalarBoolOp::EBoolOperator::EboolopAnd) {
            // Recursively convert children to DNF
            CExpression *leftDNF = pPredToDNF(pred->operator[](0));
            CExpression *rightDNF = pPredToDNF(pred->operator[](1));

            // Apply distributive law if one child is an OR
            CScalarBoolOp *left_op = (CScalarBoolOp *)leftDNF->Pop();
            CScalarBoolOp *right_op = (CScalarBoolOp *)rightDNF->Pop();
            if (left_op->Eopid() == COperator::EOperatorId::EopScalarBoolOp &&
                left_op->Eboolop() == CScalarBoolOp::EBoolOperator::EboolopOr) {
                // (A OR B) AND C => (A AND C) OR (B AND C)
                return pDistributeANDOverOR(leftDNF, rightDNF);
            }
            else if (right_op->Eopid() ==
                         COperator::EOperatorId::EopScalarBoolOp &&
                     right_op->Eboolop() ==
                         CScalarBoolOp::EBoolOperator::EboolopOr) {
                // A AND (B OR C) => (A AND B) OR (A AND C)
                return pDistributeANDOverOR(rightDNF, leftDNF);
            }
            else {
                // If neither child is an OR, just combine the DNF children with AND
                CExpressionArray *newDNF = GPOS_NEW(mp) CExpressionArray(mp);
                newDNF->Append(leftDNF);
                newDNF->Append(rightDNF);
                return CUtils::PexprScalarBoolOp(
                    mp, CScalarBoolOp::EBoolOperator::EboolopAnd, newDNF);
            }
        }
        else if (boolop->Eboolop() == CScalarBoolOp::EBoolOperator::EboolopOr) {
            // Recursively convert children to DNF
            CExpression *leftDNF = pPredToDNF(pred->operator[](0));
            CExpression *rightDNF = pPredToDNF(pred->operator[](1));
            CExpressionArray *newDNF = GPOS_NEW(mp) CExpressionArray(mp);
            newDNF->Append(leftDNF);
            newDNF->Append(rightDNF);

            // Combine the DNF children with OR
            return CUtils::PexprScalarBoolOp(
                mp, CScalarBoolOp::EBoolOperator::EboolopOr, newDNF);
        }
    }
    else {
        return pred;
    }
}
CExpression *Planner::pDistributeANDOverOR(CExpression *a, CExpression *b)
{
    CMemoryPool *mp = this->memory_pool;
    // Ensure that 'a' is the OR expression and 'b' is the one to distribute
    // If 'a' is not an OR expression, we should swap 'a' and 'b'
    if (!(a->Pop()->Eopid() == COperator::EOperatorId::EopScalarBoolOp &&
          ((CScalarBoolOp *)(a->Pop()))->Eboolop() ==
              CScalarBoolOp::EBoolOperator::EboolopOr)) {
        std::swap(a, b);
    }

    // Now 'a' is (B OR C) and 'b' is A in the (A AND (B OR C)) structure
    CExpression *left = a->operator[](0);   // B
    CExpression *right = a->operator[](1);  // C

    // Create (A AND B)
    CExpressionArray *left_array = GPOS_NEW(mp) CExpressionArray(mp);
    left_array->Append(b);
    left_array->Append(left);
    auto left_and = CUtils::PexprScalarBoolOp(
        mp, CScalarBoolOp::EBoolOperator::EboolopAnd, left_array);

    // Create (A AND C)
    CExpressionArray *right_array = GPOS_NEW(mp) CExpressionArray(mp);
    right_array->Append(b);
    right_array->Append(right);
    auto right_and = CUtils::PexprScalarBoolOp(
        mp, CScalarBoolOp::EBoolOperator::EboolopAnd, right_array);

    // Create ((A AND B) OR (A AND C))
    CExpressionArray *or_array = GPOS_NEW(mp) CExpressionArray(mp);
    or_array->Append(left_and);
    or_array->Append(right_and);
    return CUtils::PexprScalarBoolOp(
        mp, CScalarBoolOp::EBoolOperator::EboolopOr, or_array);
}

void Planner::pGetFilterAttrPosAndValue(CExpression *filter_pred_expr,
                                        gpos::ULONG &attr_pos,
                                        duckdb::Value &attr_value)
{
    CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
    CColRefTable *lhs_colref = (CColRefTable *)(col_factory->LookupColRef(
        ((CScalarIdent *)filter_pred_expr->operator[](0)->Pop())->Pcr()->Id()));
    gpos::INT lhs_attrnum = lhs_colref->AttrNum();
    attr_pos = lGetMDAccessor()
                   ->RetrieveRel(lhs_colref->GetMdidTable())
                   ->GetPosFromAttno(lhs_attrnum);
    CDatumGenericGPDB *datum =
        (CDatumGenericGPDB *)(((CScalarConst *)filter_pred_expr->operator[](1)
                                   ->Pop())
                                  ->GetDatum());
    attr_value = DatumSerDes::DeserializeOrcaByteArrayIntoDuckDBValue(
        CMDIdGPDB::CastMdid(datum->MDId())->Oid(), datum->TypeModifier(),
        datum->GetByteArrayValue(), (uint64_t)datum->Size());
}

void Planner::pGetFilterAttrPosAndValue(CExpression *filter_pred_expr,
                                        IMDId *mdid,
                                        gpos::ULONG &attr_pos,
                                        duckdb::Value &attr_value)
{
    D_ASSERT(filter_pred_expr != NULL);
    D_ASSERT(mdid != NULL);
    D_ASSERT(filter_pred_expr->Pop()->Eopid() == COperator::EOperatorId::EopScalarCmp);

    CMDIdGPDB *table_mdid = CMDIdGPDB::CastMdid(mdid);
    OID table_obj_id = table_mdid->Oid();
    duckdb::Catalog &cat_instance = context->db->GetCatalog();
    duckdb::PropertySchemaCatalogEntry *ps_cat =
        (duckdb::PropertySchemaCatalogEntry *)cat_instance.GetEntry(
            *context, DEFAULT_SCHEMA, table_obj_id);
    duckdb::PropertyKeyID_vector *key_ids = ps_cat->GetKeyIDs();

    // Obtain attr pos: resolve the property from the source graphlet's MD relation,
    // then find it in the target partition's MD relation by prop_id.
    attr_pos = 0;
    CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
    CColRefTable *lhs_colref = (CColRefTable *)(col_factory->LookupColRef(
        ((CScalarIdent *)filter_pred_expr->operator[](0)->Pop())->Pcr()->Id()));
    gpos::INT lhs_attrnum = lhs_colref->AttrNum();
    // Get prop_id from source graphlet's MD column
    const IMDRelation *src_rel = lGetMDAccessor()->RetrieveRel(lhs_colref->GetMdidTable());
    ULONG src_pos = src_rel->GetPosFromAttno(lhs_attrnum);
    ULONG prop_id = src_rel->GetMdCol(src_pos)->PropId();
    // Find that prop_id in target partition's MD relation
    const IMDRelation *tgt_rel = lGetMDAccessor()->RetrieveRel(table_mdid);
    for (ULONG j = 0; j < tgt_rel->ColumnCount(); j++) {
        if (tgt_rel->GetMdCol(j)->PropId() == prop_id) {
            attr_pos = j;
            break;
        }
    }

    // Obtain attr value
    CDatumGenericGPDB *datum =
        (CDatumGenericGPDB *)(((CScalarConst *)filter_pred_expr->operator[](1)
                                   ->Pop())
                                  ->GetDatum());
    attr_value = DatumSerDes::DeserializeOrcaByteArrayIntoDuckDBValue(
        CMDIdGPDB::CastMdid(datum->MDId())->Oid(), datum->TypeModifier(),
        datum->GetByteArrayValue(), (uint64_t)datum->Size());
}

void Planner::pConvertLocalFilterExprToUnionAllFilterExpr(
    unique_ptr<duckdb::Expression> &expr, CColRefArray *cols,
    vector<ULONG> unionall_output_original_col_ids)
{
    switch (expr->expression_class) {
        case duckdb::ExpressionClass::BOUND_BETWEEN: {
            auto between_expr = (duckdb::BoundBetweenExpression *)expr.get();
            pConvertLocalFilterExprToUnionAllFilterExpr(
                between_expr->input, cols, unionall_output_original_col_ids);
            pConvertLocalFilterExprToUnionAllFilterExpr(
                between_expr->lower, cols, unionall_output_original_col_ids);
            pConvertLocalFilterExprToUnionAllFilterExpr(
                between_expr->upper, cols, unionall_output_original_col_ids);
            break;
        }
        case duckdb::ExpressionClass::BOUND_REF: {
            auto bound_ref_expr =
                (duckdb::BoundReferenceExpression *)expr.get();
            CColRef *col = cols->operator[](bound_ref_expr->index);
            for (uint64_t i = 0; i < unionall_output_original_col_ids.size();
                 i++) {
                if (unionall_output_original_col_ids[i] == col->Id()) {
                    bound_ref_expr->index = i;
                    break;
                }
            }
            break;
        }
        case duckdb::ExpressionClass::BOUND_CASE: {
            auto bound_case_expr = (duckdb::BoundCaseExpression *)expr.get();
            for (auto &bound_case_check : bound_case_expr->case_checks) {
                pConvertLocalFilterExprToUnionAllFilterExpr(
                    bound_case_check.when_expr, cols,
                    unionall_output_original_col_ids);
                pConvertLocalFilterExprToUnionAllFilterExpr(
                    bound_case_check.then_expr, cols,
                    unionall_output_original_col_ids);
            }
            pConvertLocalFilterExprToUnionAllFilterExpr(
                bound_case_expr->else_expr, cols,
                unionall_output_original_col_ids);
            break;
        }
        case duckdb::ExpressionClass::BOUND_CAST: {
            auto bound_cast_expr = (duckdb::BoundCastExpression *)expr.get();
            pConvertLocalFilterExprToUnionAllFilterExpr(
                bound_cast_expr->child, cols, unionall_output_original_col_ids);
            break;
        }
        case duckdb::ExpressionClass::BOUND_COMPARISON: {
            auto bound_comparison_expr =
                (duckdb::BoundComparisonExpression *)expr.get();
            pConvertLocalFilterExprToUnionAllFilterExpr(
                bound_comparison_expr->left, cols,
                unionall_output_original_col_ids);
            pConvertLocalFilterExprToUnionAllFilterExpr(
                bound_comparison_expr->right, cols,
                unionall_output_original_col_ids);
            break;
        }
        case duckdb::ExpressionClass::BOUND_CONJUNCTION: {
            auto bound_conjunction_expr =
                (duckdb::BoundConjunctionExpression *)expr.get();
            for (auto &child : bound_conjunction_expr->children) {
                pConvertLocalFilterExprToUnionAllFilterExpr(
                    child, cols, unionall_output_original_col_ids);
            }
            break;
        }
        case duckdb::ExpressionClass::BOUND_CONSTANT:
            break;
        case duckdb::ExpressionClass::BOUND_FUNCTION: {
            auto bound_function_expr =
                (duckdb::BoundFunctionExpression *)expr.get();
            for (auto &child : bound_function_expr->children) {
                pConvertLocalFilterExprToUnionAllFilterExpr(
                    child, cols, unionall_output_original_col_ids);
            }
            break;
        }
        case duckdb::ExpressionClass::BOUND_OPERATOR: {
            auto bound_operator_expr =
                (duckdb::BoundOperatorExpression *)expr.get();
            for (auto &child : bound_operator_expr->children) {
                pConvertLocalFilterExprToUnionAllFilterExpr(
                    child, cols, unionall_output_original_col_ids);
            }
            break;
        }
        case duckdb::ExpressionClass::BOUND_PARAMETER:
            break;
        default:
            GPOS_ASSERT(false);
            throw InternalException(
                "Attempting to execute expression of unknown type!");
    }
}

void Planner::pShiftFilterPredInnerColumnIndices(
    unique_ptr<duckdb::Expression> &expr, size_t outer_size)
{
    switch (expr->expression_class) {
        case duckdb::ExpressionClass::BOUND_BETWEEN: {
            auto between_expr = (duckdb::BoundBetweenExpression *)expr.get();
            pShiftFilterPredInnerColumnIndices(between_expr->input, outer_size);
            pShiftFilterPredInnerColumnIndices(between_expr->lower, outer_size);
            pShiftFilterPredInnerColumnIndices(between_expr->upper, outer_size);
            break;
        }
        case duckdb::ExpressionClass::BOUND_REF: {
            /**
             * IdSeek will create temp chunk 
             * which is a concat of outer and inner chunk
             * e.g., oc1, oc2, ...., ic1, ic2, ....
             * Therefore, inner column index should be added by outer column size
            */
            auto bound_ref_expr =
                (duckdb::BoundReferenceExpression *)expr.get();
            if (bound_ref_expr->is_inner) {
                bound_ref_expr->index = bound_ref_expr->index + outer_size;
            }
            break;
        }
        case duckdb::ExpressionClass::BOUND_CASE: {
            auto bound_case_expr = (duckdb::BoundCaseExpression *)expr.get();
            for (auto &bound_case_check : bound_case_expr->case_checks) {
                pShiftFilterPredInnerColumnIndices(bound_case_check.when_expr,
                                                   outer_size);
                pShiftFilterPredInnerColumnIndices(bound_case_check.then_expr,
                                                   outer_size);
            }
            pShiftFilterPredInnerColumnIndices(bound_case_expr->else_expr,
                                               outer_size);
            break;
        }
        case duckdb::ExpressionClass::BOUND_CAST: {
            auto bound_cast_expr = (duckdb::BoundCastExpression *)expr.get();
            pShiftFilterPredInnerColumnIndices(bound_cast_expr->child,
                                               outer_size);
            break;
        }
        case duckdb::ExpressionClass::BOUND_COMPARISON: {
            auto bound_comparison_expr =
                (duckdb::BoundComparisonExpression *)expr.get();
            pShiftFilterPredInnerColumnIndices(bound_comparison_expr->left,
                                               outer_size);
            pShiftFilterPredInnerColumnIndices(bound_comparison_expr->right,
                                               outer_size);
            break;
        }
        case duckdb::ExpressionClass::BOUND_CONJUNCTION: {
            auto bound_conjunction_expr =
                (duckdb::BoundConjunctionExpression *)expr.get();
            for (auto &child : bound_conjunction_expr->children) {
                pShiftFilterPredInnerColumnIndices(child, outer_size);
            }
            break;
        }
        case duckdb::ExpressionClass::BOUND_CONSTANT:
            break;
        case duckdb::ExpressionClass::BOUND_FUNCTION: {
            auto bound_function_expr =
                (duckdb::BoundFunctionExpression *)expr.get();
            for (auto &child : bound_function_expr->children) {
                pShiftFilterPredInnerColumnIndices(child, outer_size);
            }
            break;
        }
        case duckdb::ExpressionClass::BOUND_OPERATOR: {
            auto bound_operator_expr =
                (duckdb::BoundOperatorExpression *)expr.get();
            for (auto &child : bound_operator_expr->children) {
                pShiftFilterPredInnerColumnIndices(child, outer_size);
            }
            break;
        }
        case duckdb::ExpressionClass::BOUND_PARAMETER:
            break;
        default:
            GPOS_ASSERT(false);
            throw InternalException(
                "Attempting to execute expression of unknown type!");
    }
}

void Planner::pAdjustBoundRefIndices(
    unique_ptr<duckdb::Expression> &expr, size_t threshold, int adjustment)
{
    // Recursively adjust BoundRef indices >= threshold by the given adjustment.
    // Used to remap filter-only column indices when scan_types order differs
    // from inner_required_cols order.
    switch (expr->expression_class) {
        case duckdb::ExpressionClass::BOUND_REF: {
            auto *ref = (duckdb::BoundReferenceExpression *)expr.get();
            if (ref->index >= threshold) {
                ref->index = (duckdb::idx_t)((int)ref->index + adjustment);
            }
            break;
        }
        case duckdb::ExpressionClass::BOUND_COMPARISON: {
            auto *cmp = (duckdb::BoundComparisonExpression *)expr.get();
            pAdjustBoundRefIndices(cmp->left, threshold, adjustment);
            pAdjustBoundRefIndices(cmp->right, threshold, adjustment);
            break;
        }
        case duckdb::ExpressionClass::BOUND_CONJUNCTION: {
            auto *conj = (duckdb::BoundConjunctionExpression *)expr.get();
            for (auto &child : conj->children) {
                pAdjustBoundRefIndices(child, threshold, adjustment);
            }
            break;
        }
        case duckdb::ExpressionClass::BOUND_FUNCTION: {
            auto *func = (duckdb::BoundFunctionExpression *)expr.get();
            for (auto &child : func->children) {
                pAdjustBoundRefIndices(child, threshold, adjustment);
            }
            break;
        }
        case duckdb::ExpressionClass::BOUND_CAST: {
            auto *cast = (duckdb::BoundCastExpression *)expr.get();
            pAdjustBoundRefIndices(cast->child, threshold, adjustment);
            break;
        }
        case duckdb::ExpressionClass::BOUND_OPERATOR: {
            auto *op = (duckdb::BoundOperatorExpression *)expr.get();
            for (auto &child : op->children) {
                pAdjustBoundRefIndices(child, threshold, adjustment);
            }
            break;
        }
        case duckdb::ExpressionClass::BOUND_CONSTANT:
        case duckdb::ExpressionClass::BOUND_PARAMETER:
            break;
        default:
            break;
    }
}

duckdb::CypherPhysicalOperatorGroups *
Planner::pBuildSchemaflowGraphForBinaryJoin(CExpression *plan_expr,
                                            duckdb::CypherPhysicalOperator *op,
                                            duckdb::Schema &output_schema,
                                            bool swap_children)
{
    /**
	 * Join is a binary operator, which needs two pipelines.
	 * If we need to generate schema flow graph, we need to create pipeline one-by-one.
	 * We first generate rhs pipeline and schema flow graph.
	 * We then clear the schema flow graph data structures.
	 * We finally generate lhs pipeline
	*/

    // For ROJ→LOJ conversion: swap_children=true means ORCA child[0] is now the
    // build (RHS) side and child[1] is the probe (LHS) side.
    ULONG rhs_idx = swap_children ? 0 : 1;
    ULONG lhs_idx = swap_children ? 1 : 0;

    // Step 1. rhs pipline
    duckdb::CypherPhysicalOperatorGroups *rhs_result =
        pTraverseTransformPhysicalPlan(plan_expr->PdrgPexpr()->operator[](rhs_idx));
    rhs_result->push_back(op);
    auto pipeline = new duckdb::CypherPipeline(*rhs_result);
    pipelines.push_back(pipeline);

    // Step 1. schema flow graph
    vector<duckdb::Schema> rhs_schemas;  // We need to change this.
    if (generate_sfg) {
        // Generate rhs schema flow graph
        duckdb::Schema prev_union_schema = pipeline_union_schema.back();
        pipeline_operator_types.push_back(duckdb::OperatorType::UNARY);
        num_schemas_of_childs.push_back({1});
        pipeline_union_schema.push_back(prev_union_schema);
        pGenerateSchemaFlowGraph(*rhs_result);
        pClearSchemaFlowGraph();  // Step 2
    }

    // Step 3. lhs pipeline
    duckdb::CypherPhysicalOperatorGroups *lhs_result =
        pTraverseTransformPhysicalPlan(plan_expr->PdrgPexpr()->operator[](lhs_idx));
    lhs_result->push_back(op);

    // Step 3. schema flow graph
    if (generate_sfg) {
        pipeline_operator_types.push_back(duckdb::OperatorType::BINARY);
        num_schemas_of_childs.push_back({1, 1});
        pipeline_union_schema.push_back(output_schema);
    }

    return lhs_result;
}

duckdb::LogicalType Planner::pGetColumnsDuckDBType(const CColRef *col)
{
    CMDIdGPDB *type_mdid = CMDIdGPDB::CastMdid(col->RetrieveType()->MDId());
    OID type_oid = type_mdid->Oid();
    INT type_mod = col->TypeModifier();
    return pConvertTypeOidToLogicalType(type_oid, type_mod);
}

void Planner::pGetColumnsDuckDBType(CColRefArray *columns,
                                    vector<duckdb::LogicalType> &output_types)
{
    for (ULONG col_idx = 0; col_idx < columns->Size(); col_idx++) {
        CColRef *col = (*columns)[col_idx];
        output_types.push_back(pGetColumnsDuckDBType(col));
    }
}

void Planner::pGetColumnsDuckDBType(CColRefArray *columns,
                                    vector<duckdb::LogicalType> &output_types,
                                    vector<duckdb::idx_t>& col_prop_ids)
{
    for (ULONG col_idx = 0; col_idx < columns->Size(); col_idx++) {
        CColRef *col = (*columns)[col_idx];
        CColRefTable *col_ref_table = (CColRefTable *)(*columns)[col_idx];
        col_prop_ids.push_back(col_ref_table->PropId());
        output_types.push_back(pGetColumnsDuckDBType(col));
    }
}

void Planner::pGetProjectionExprs(
    vector<duckdb::LogicalType> output_types, vector<duckdb::idx_t> &ref_idxs,
    vector<unique_ptr<duckdb::Expression>> &out_exprs)
{
    for (int i = 0; i < ref_idxs.size(); i++) {
        out_exprs.push_back(make_unique<duckdb::BoundReferenceExpression>(
            output_types[i], ref_idxs[i]));
    }
}

void Planner::pRemoveColumnsFromSchemas(vector<duckdb::Schema> &in_schemas,
                                        vector<duckdb::idx_t> &ref_idxs,
                                        vector<duckdb::Schema> &out_schemas)
{
    for (auto &schema : in_schemas) {
        duckdb::Schema new_schema;
        auto stored_types = schema.getStoredTypes();
        for (auto idx : ref_idxs) {
            new_schema.appendStoredTypes({stored_types[idx]});
        }
        out_schemas.push_back(new_schema);
    }
}

bool Planner::pIsColEdgeProperty(const CColRef *colref)
{
    const CColRefTable *colref_table = (const CColRefTable *)colref;
    const CName &col_name = colref_table->Name();
    wchar_t *full_col_name, *col_only_name, *first_token, *pt;
    full_col_name = new wchar_t[std::wcslen(col_name.Pstr()->GetBuffer()) + 1];
    std::wcscpy(full_col_name, col_name.Pstr()->GetBuffer());
    first_token = std::wcstok(full_col_name, L".", &pt);
    col_only_name = std::wcstok(NULL, L".", &pt);

    // Use column-only part after "." if present; otherwise use the full name (no dot).
    // Columns like "_id", "_sid", "_tid" may appear without a table prefix.
    const wchar_t *effective_name = (col_only_name != NULL) ? col_only_name : first_token;

    if (effective_name == NULL) {
        return true;  // empty name: conservatively treat as property
    }
    return (std::wcsncmp(effective_name, L"_sid", 4) != 0) &&
           (std::wcsncmp(effective_name, L"_tid", 4) != 0) &&
           (std::wcsncmp(effective_name, L"_id", 4) != 0);
}

void Planner::pGenerateCartesianProductSchema(
    vector<duckdb::Schema> &lhs_schemas, vector<duckdb::Schema> &rhs_schemas,
    vector<duckdb::Schema> &out_schemas)
{
    /**
    * TODO: This code assumes that the rhs is simply appended to the lhs.
    * If the output columns are shuffled, this code will not work. 
    */
    for (auto &lhs_schema : lhs_schemas) {
        for (auto &rhs_schema : rhs_schemas) {
            duckdb::Schema tmp_schema;
            tmp_schema.setStoredTypes(lhs_schema.getStoredTypes());
            tmp_schema.appendStoredTypes(rhs_schema.getStoredTypes());
            out_schemas.push_back(tmp_schema);
        }
    }
}

bool Planner::pIsJoinRhsOutputPhysicalIdOnly(CExpression *plan_expr)
{
    /* 
     * Example query: 
     * MATCH (m:Comment)-[roc:REPLY_OF_COMMENT]->(n:Comment) RETURN m.id AS messageId
     * 
     * The pattern:
     * CPhysicalInnerIndexNLJoin (plan_expr)
     * |--(some pattern)
     * |--CPhysicalSerialUnionAll (optional)
     * |  |--CPhysicalComputeScalarColumnar
     * |  |  |--CPhysicalIndexScan
     * |  |  |  |--CScalarCmp
     * |  |  |  |  |--CScalarIdent "n._id"
     * |  |  |  |  |--CScalarIdent "_tid"
     * |  |  |--CScalarProjectList
     * |  |  |  |--CScalarProjectElement "n._id"
     * |  |  |  |  |--CScalarIdent "n._id"
    */

    /* UNION ALL Case */
    vector<COperator::EOperatorId> p1 = vector<COperator::EOperatorId>(
        {COperator::EOperatorId::EopPhysicalSerialUnionAll,
         COperator::EOperatorId::EopPhysicalComputeScalarColumnar,
         COperator::EOperatorId::EopPhysicalIndexScan});

    if (pMatchExprPattern(plan_expr->operator[](1), p1, 0, true)) {
        CExpression *compute_scalar_expr =
            plan_expr->operator[](1)->operator[](0);
        CExpression *index_scan_expr = compute_scalar_expr->operator[](0);
        CExpression *cmp_expr = index_scan_expr->operator[](0);
        CExpression *project_list_expr = compute_scalar_expr->operator[](1);

        // Check if two ident's name contains w_id_col_name and w_tid_col_name
        if (cmp_expr->operator[](0)->Pop()->Eopid() ==
                COperator::EOperatorId::EopScalarIdent &&
            cmp_expr->operator[](1)->Pop()->Eopid() ==
                COperator::EOperatorId::EopScalarIdent) {
            CScalarIdent *lhs_ident =
                (CScalarIdent *)cmp_expr->operator[](0)->Pop();
            CScalarIdent *rhs_ident =
                (CScalarIdent *)cmp_expr->operator[](1)->Pop();

            if ((pCmpColName(lhs_ident->Pcr(), w_id_col_name) &&
                 pCmpColName(rhs_ident->Pcr(), w_tid_col_name)) ||
                (pCmpColName(lhs_ident->Pcr(), w_tid_col_name) &&
                 pCmpColName(rhs_ident->Pcr(), w_id_col_name))) {
                // Check project element is one, and is ._id
                if (project_list_expr->Arity() == 1 &&
                    project_list_expr->operator[](0)->Pop()->Eopid() ==
                        COperator::EOperatorId::EopScalarProjectElement) {
                    CScalarProjectElement *project_element =
                        (CScalarProjectElement *)project_list_expr
                            ->
                            operator[](0)
                            ->Pop();
                    if (pCmpColName(project_element->Pcr(), w_id_col_name)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /* No UNION ALL Case */
    vector<COperator::EOperatorId> p2 = vector<COperator::EOperatorId>(
        {COperator::EOperatorId::EopPhysicalComputeScalarColumnar,
         COperator::EOperatorId::EopPhysicalIndexScan});

    if (pMatchExprPattern(plan_expr->operator[](1), p2, 0, true)) {
        D_ASSERT(false);
    }

    return false;
}

bool Planner::pCmpColName(const CColRef *colref, const WCHAR *col_name)
{
    if (std::wcslen(colref->Name().Pstr()->GetBuffer()) <
        std::wcslen(col_name)) {
        return false;
    }
    return std::wcsncmp(colref->Name().Pstr()->GetBuffer() +
                            std::wcslen(colref->Name().Pstr()->GetBuffer()) -
                            std::wcslen(col_name),
                        col_name, std::wcslen(col_name)) == 0;
}

void Planner::pGetFilterOnlyInnerColsIdx(CExpression *expr,
                                         CColRefArray *inner_cols,
                                         CColRefArray *output_cols,
                                         vector<ULONG> &inner_cols_idx)
{
    switch (expr->Pop()->Eopid()) {
        case COperator::EopScalarIdent: {
            CScalarIdent *ident = (CScalarIdent *)expr->Pop();
            // check is filter only
            if (inner_cols->IndexOf(ident->Pcr()) != gpos::ulong_max &&
                output_cols->IndexOf(ident->Pcr()) == gpos::ulong_max) {
                inner_cols_idx.push_back(inner_cols->IndexOf(ident->Pcr()));
            }
            break;
        }
        case COperator::EopScalarConst:
        case COperator::EopScalarBoolOp:
        case COperator::EopScalarAggFunc:
        case COperator::EopScalarFunc:
        case COperator::EopScalarCmp:
        case COperator::EopScalarSwitch:
        case COperator::EopScalarNullTest: {
            for (ULONG child_idx = 0; child_idx < expr->Arity(); child_idx++) {
                pGetFilterOnlyInnerColsIdx(expr->operator[](child_idx),
                                           inner_cols, output_cols,
                                           inner_cols_idx);
            }
            break;
        }
        default:
            GPOS_ASSERT(false);  // NOT implemented yet
    }
}

void Planner::pGetFilterOnlyInnerColsIdx(CExpression *expr,
                                         CColRefSet *inner_cols,
                                         CColRefSet *output_cols,
                                         vector<const CColRef *> &filter_only_inner_cols)
{
    switch (expr->Pop()->Eopid()) {
        case COperator::EopScalarIdent: {
            CScalarIdent *ident = (CScalarIdent *)expr->Pop();
            // check is filter only
            if (inner_cols->FMember(ident->Pcr()) &&
                !(output_cols->FMember(ident->Pcr()))) {
                filter_only_inner_cols.push_back(ident->Pcr());
            }
            break;
        }
        case COperator::EopScalarConst:
        case COperator::EopScalarBoolOp:
        case COperator::EopScalarAggFunc:
        case COperator::EopScalarFunc:
        case COperator::EopScalarCmp:
        case COperator::EopScalarSwitch: {
            for (ULONG child_idx = 0; child_idx < expr->Arity(); child_idx++) {
                pGetFilterOnlyInnerColsIdx(expr->operator[](child_idx),
                                           inner_cols, output_cols,
                                           filter_only_inner_cols);
            }
            break;
        }
        default:
            GPOS_ASSERT(false);  // NOT implemented yet
    }
}

CExpression *Planner::pFindFilterExpr(CExpression *plan_expr)
{
    if (plan_expr->Pop()->Eopid() ==
        COperator::EOperatorId::EopPhysicalFilter) {
        return plan_expr;
    }
    for (ULONG child_idx = 0; child_idx < plan_expr->Arity(); child_idx++) {
        CExpression *result = pFindFilterExpr(plan_expr->operator[](child_idx));
        if (result != NULL) {
            return result;
        }
    }
    return NULL;
}

bool Planner::pIsFilterExist(CExpression *plan_expr)
{
    auto filter_expr = pFindFilterExpr(plan_expr);
    return filter_expr != NULL;
}

bool Planner::pIsEdgePropertyInFilter(CExpression *plan_expr)
{
    auto filter_expr = pFindFilterExpr(plan_expr);
    if (filter_expr == NULL) {
        return false;
    }

    bool is_edge_property_found = false;
    auto filter_pred_expr = filter_expr->operator[](1);

    vector<CExpression *> nodes_to_visit;
    nodes_to_visit.push_back(filter_pred_expr);
    while (!nodes_to_visit.empty()) {
        CExpression *current_node = nodes_to_visit.back();
        nodes_to_visit.pop_back();
        switch (current_node->Pop()->Eopid()) {
            case COperator::EOperatorId::EopScalarIdent: {
                CScalarIdent *ident = (CScalarIdent *)current_node->Pop();
                if (pIsColEdgeProperty(ident->Pcr())) {
                    is_edge_property_found = true;
                    break;
                }
                break;
            }
            default: {
                for (ULONG child_idx = 0; child_idx < current_node->Arity();
                     child_idx++) {
                    nodes_to_visit.push_back(
                        current_node->operator[](child_idx));
                }
                break;
            }
        }
        if (is_edge_property_found) {
            break;
        }
    }

    return is_edge_property_found;
}

bool Planner::pIsPropertyInCols(CColRefArray *cols)
{
    bool is_property_found = false;
    for (ULONG col_idx = 0; col_idx < cols->Size(); col_idx++) {
        if (pIsColEdgeProperty((*cols)[col_idx])) {
            is_property_found = true;
            break;
        }
    }
    return is_property_found;
}

bool Planner::pIsIDColInCols(CColRefArray *cols)
{
    bool is_edge_id_found = false;
    for (ULONG col_idx = 0; col_idx < cols->Size(); col_idx++) {
        CColRef *col = cols->operator[](col_idx);
        CColRefTable *colref_table = (CColRefTable *)col;
        if (colref_table->AttrNum() == -1) {
            is_edge_id_found = true;
            break;
        }
    }
    return is_edge_id_found;
}

CColRef *Planner::pGetIDColInCols(CColRefArray *cols)
{
    CColRef *id_col = NULL;
    for (ULONG col_idx = 0; col_idx < cols->Size(); col_idx++) {
        CColRef *col = cols->operator[](col_idx);
        CColRefTable *colref_table = (CColRefTable *)col;
        if (colref_table->AttrNum() == -1) {
            id_col = col;
            break;
        }
    }
    return id_col;
}

size_t Planner::pGetNumOuterSchemas()
{
    auto &num_schemas_of_childs_prev = num_schemas_of_childs.back();
    size_t num_outer_schemas = 1;
    for (auto i = 0; i < num_schemas_of_childs_prev.size(); i++) {
        num_outer_schemas *= num_schemas_of_childs_prev[i];
    }
    return num_outer_schemas;
}

CExpression *Planner::pFindIndexScanExpr(CExpression *plan_expr)
{
    if (plan_expr->Pop()->Eopid() ==
            COperator::EOperatorId::EopPhysicalIndexScan ||
        plan_expr->Pop()->Eopid() ==
            COperator::EOperatorId::EopPhysicalIndexOnlyScan) {
        return plan_expr;
    }
    for (ULONG child_idx = 0; child_idx < plan_expr->Arity(); child_idx++) {
        CExpression *result =
            pFindIndexScanExpr(plan_expr->operator[](child_idx));
        if (result != NULL) {
            return result;
        }
    }
    return NULL;
}

duckdb::idx_t Planner::pGetColIndexInPred(CExpression *pred,
                                          CColRefArray *colrefs)
{
    duckdb::idx_t idx = gpos::ulong_max;
    D_ASSERT(pred->Pop()->Eopid() == COperator::EOperatorId::EopScalarCmp);
    for (ULONG child_idx = 0; child_idx < pred->Arity(); child_idx++) {
        CExpression *child = pred->operator[](child_idx);
        if (child->Pop()->Eopid() == COperator::EOperatorId::EopScalarIdent) {
            CScalarIdent *ident = (CScalarIdent *)child->Pop();
            idx = colrefs->IndexOf(ident->Pcr());
            if (idx != gpos::ulong_max) {
                break;
            }
        }
    }
    return idx;
}

void Planner::pGetDuckDBTypesFromColRefs(CColRefArray *colrefs,
                                         vector<duckdb::LogicalType> &out_types)
{
    for (ULONG col_idx = 0; col_idx < colrefs->Size(); col_idx++) {
        CColRef *colref = colrefs->operator[](col_idx);
        CMDIdGPDB *type_mdid =
            CMDIdGPDB::CastMdid(colref->RetrieveType()->MDId());
        OID type_oid = type_mdid->Oid();
        INT type_mod = colref->TypeModifier();
        out_types.push_back(pConvertTypeOidToLogicalType(type_oid, type_mod));
    }
}

void Planner::pGetObjetIdsForColRefs(CColRefArray *cols,
                                     vector<uint64_t> &out_oids)
{
    // TODO: This function is quite weird, since only returns the first value
    // Check jhko's intended behavior
    for (ULONG col_idx = 0; col_idx < cols->Size(); col_idx++) {
        CColRef *colref = cols->operator[](col_idx);
        OID table_obj_id =
            CMDIdGPDB::CastMdid(((CColRefTable *)colref)->GetMdidTable())
                ->Oid();
        if (col_idx == 0) {
            out_oids.push_back((uint64_t)table_obj_id);
        }
    }
}

void Planner::pPushCartesianProductSchema(
    duckdb::Schema &out_schema, vector<duckdb::LogicalType> &rhs_types)
{
    duckdb::Schema rhs_schema;
    rhs_schema.setStoredTypes(rhs_types);
    vector<duckdb::Schema> prev_local_schemas = pipeline_schemas.back();
    pipeline_operator_types.push_back(duckdb::OperatorType::BINARY);
    num_schemas_of_childs.push_back({pGetNumOuterSchemas(), 1});
    vector<duckdb::Schema> out_schemas;
    vector<duckdb::Schema> rhs_schemas = {rhs_schema};
    pGenerateCartesianProductSchema(prev_local_schemas, rhs_schemas,
                                    out_schemas);
    pipeline_schemas.push_back(out_schemas);
    pipeline_union_schema.push_back(out_schema);
}

void Planner::pConstructColMapping(CColRefArray *in_cols,
                                   CColRefArray *out_cols,
                                   vector<uint32_t> &out_mapping)
{
    for (ULONG col_idx = 0; col_idx < in_cols->Size(); col_idx++) {
        CColRef *col = in_cols->operator[](col_idx);
        auto idx = out_cols->IndexOf(col);
        if (idx != gpos::ulong_max) {
            out_mapping.push_back(idx);
        }
        else {
            out_mapping.push_back(std::numeric_limits<uint32_t>::max());
        }
    }
}

void Planner::pAppendFilterOnlyCols(CExpression *filter_expr,
                                    CColRefArray *input_cols,
                                    CColRefArray *output_cols,
                                    CColRefArray *result_cols)
{
    D_ASSERT(filter_expr->Pop()->Eopid() ==
             COperator::EOperatorId::EopPhysicalFilter);
    vector<ULONG> filter_only_cols_idx;
    auto filter_pred_expr = filter_expr->operator[](1);
    pGetFilterOnlyInnerColsIdx(
        filter_pred_expr, input_cols /* all scanned cols */,
        output_cols /* output inner cols */, filter_only_cols_idx);
    for (auto col_idx : filter_only_cols_idx) {
        result_cols->Append(input_cols->operator[](col_idx));
    }
}

void Planner::pGetFilterDuckDBExprs(
    CExpression *filter_or_pred_expr, CColRefArray *outer_cols,
    CColRefArray *inner_cols, size_t index_shifting_size,
    vector<unique_ptr<duckdb::Expression>> &out_exprs)
{
    CExpression *filter_pred_expr;
    if (filter_or_pred_expr->Pop()->Eopid() ==
        COperator::EOperatorId::EopPhysicalFilter) {
        filter_pred_expr = filter_or_pred_expr->operator[](1);
    }
    else if (filter_or_pred_expr->Pop()->Eopid() ==
                 COperator::EOperatorId::EopScalarBoolOp ||
             filter_or_pred_expr->Pop()->Eopid() ==
                 COperator::EOperatorId::EopScalarCmp) {
        filter_pred_expr = filter_or_pred_expr;
    }
    unique_ptr<duckdb::Expression> filter_duckdb_expr;
    filter_duckdb_expr =
        pTransformScalarExpr(filter_pred_expr, outer_cols, inner_cols);
    if (index_shifting_size > 0)
        pShiftFilterPredInnerColumnIndices(filter_duckdb_expr,
                                           index_shifting_size);
    out_exprs.push_back(std::move(filter_duckdb_expr));
}

void Planner::pGetIdentIndices(unique_ptr<duckdb::Expression> &unique_expr,
                               vector<duckdb::idx_t> &out_idxs)
{
    auto expr = unique_expr.get();
    
	switch (expr->expression_class) {
    case duckdb::ExpressionClass::BOUND_BETWEEN:
    {
        auto bound_expr = (duckdb::BoundBetweenExpression *)expr;
        pGetIdentIndices(bound_expr->input, out_idxs);
        pGetIdentIndices(bound_expr->lower, out_idxs);
        pGetIdentIndices(bound_expr->upper, out_idxs);
        break;
    }
    case duckdb::ExpressionClass::BOUND_REF:
    {
        auto bound_ref_expr = (duckdb::BoundReferenceExpression *)expr;
        out_idxs.push_back(bound_ref_expr->index);
        break;
    }
    case duckdb::ExpressionClass::BOUND_CASE:
    {
        auto bound_case_expr = (duckdb::BoundCaseExpression *)expr;
        for (auto &bound_case_check : bound_case_expr->case_checks) {
            pGetIdentIndices(bound_case_check.when_expr, out_idxs);
            pGetIdentIndices(bound_case_check.then_expr, out_idxs);
        }
        pGetIdentIndices(bound_case_expr->else_expr, out_idxs);
        break;
    }
    case duckdb::ExpressionClass::BOUND_CAST:
    {
        auto bound_cast_expr = (duckdb::BoundCastExpression *)expr;
        pGetIdentIndices(bound_cast_expr->child, out_idxs);
        break;
    }
    case duckdb::ExpressionClass::BOUND_COMPARISON:
    {
        auto bound_cmp_expr = (duckdb::BoundComparisonExpression *)expr;
        pGetIdentIndices(bound_cmp_expr->left, out_idxs);
        pGetIdentIndices(bound_cmp_expr->right, out_idxs);
        break;
    }
    case duckdb::ExpressionClass::BOUND_CONJUNCTION:
    {
        auto bound_conj_expr = (duckdb::BoundConjunctionExpression *)expr;
        for (auto &child : bound_conj_expr->children) {
            pGetIdentIndices({child}, out_idxs);
        }
        break;
    }
    case duckdb::ExpressionClass::BOUND_CONSTANT:
        break;
    case duckdb::ExpressionClass::BOUND_FUNCTION:
    {
        auto bound_func_expr = (duckdb::BoundFunctionExpression *)expr;
        for (auto &child : bound_func_expr->children) {
            pGetIdentIndices(child, out_idxs);
        }
        break;
    }
    case duckdb::ExpressionClass::BOUND_OPERATOR:
    {
        auto bound_op_expr = (duckdb::BoundOperatorExpression *)expr;
        for (auto &child : bound_op_expr->children) {
            pGetIdentIndices(child, out_idxs);
        }
        break;
    }
    default:
        throw NotImplementedException("Attempting to execute expression of unknown type!");
    }
}

void Planner::pSeperatePropertyNonPropertyCols(CColRefSet *input_cols,
                                               CColRefSet *property_cols,
                                               CColRefSet *non_property_cols)
{
    CColRefSetIter crsi(*input_cols);
	while (crsi.Advance())
	{
        CColRef *col = crsi.Pcr();
        if (!pIsColEdgeProperty(col)) {
            non_property_cols->Include(col);
        }
        else {
            property_cols->Include(col);
        }
    }
}

void Planner::pGetProjectionExprsWithJoinCond(
    CExpression *scalar_cmp_expr,
    CColRefArray *input_cols, CColRefArray *output_cols,
    vector<duckdb::LogicalType> output_types,
    vector<unique_ptr<duckdb::Expression>> &out_exprs)
{
    for (ULONG col_idx = 0; col_idx < output_cols->Size(); col_idx++) {
        CColRef *col = (*output_cols)[col_idx];
        ULONG idx = input_cols->IndexOf(col);
        if (idx == gpos::ulong_max) { // join cond column
            D_ASSERT(scalar_cmp_expr->Pop()->Eopid() == COperator::EOperatorId::EopScalarCmp);
            for (uint32_t j = 0; j < scalar_cmp_expr->Arity();
                    j++) {
                CScalarIdent *sc_ident =
                    (CScalarIdent
                            *)(scalar_cmp_expr->operator[](j)->Pop());
                idx = input_cols->IndexOf(sc_ident->Pcr());
                if (idx != gpos::ulong_max) {
                    break;
                }
            }
        }
        D_ASSERT(idx != gpos::ulong_max);
        out_exprs.push_back(make_unique<duckdb::BoundReferenceExpression>(
            output_types[col_idx], (int)idx));
    }
}


void Planner::pGetProjectionExprs(
    CColRefArray *input_cols, CColRefArray *output_cols,
    vector<duckdb::LogicalType> output_types,
    vector<unique_ptr<duckdb::Expression>> &out_exprs)
{
    for (ULONG col_idx = 0; col_idx < output_cols->Size(); col_idx++) {
        CColRef *col = (*output_cols)[col_idx];
        ULONG idx = input_cols->IndexOf(col);
        D_ASSERT(idx != gpos::ulong_max);
        out_exprs.push_back(make_unique<duckdb::BoundReferenceExpression>(
            output_types[col_idx], (int)idx));
    }
}

bool Planner::pIsComplexPred(CExpression *pred_expr)
{
    // recursivley iterate over predicate and check
    auto *op = pred_expr->Pop();
    if (op->Eopid() == COperator::EOperatorId::EopScalarBoolOp) {
        CScalarBoolOp *boolop = (CScalarBoolOp *)op;
        if (boolop->Eboolop() == CScalarBoolOp::EBoolOperator::EboolopAnd) {
            D_ASSERT(pred_expr->Arity() == 2);
            return pIsComplexPred(pred_expr->operator[](0)) ||
                   pIsComplexPred(pred_expr->operator[](1));
        }
        else if (boolop->Eboolop() == CScalarBoolOp::EBoolOperator::EboolopOr) {
            return true;
        }
        else if (boolop->Eboolop() ==
                 CScalarBoolOp::EBoolOperator::EboolopNot) {
            // NOT(CMP) is simple, NOT(FUNC) is complex
            auto child_op = pred_expr->operator[](0)->Pop();
            return child_op->Eopid() != COperator::EOperatorId::EopScalarCmp;
        }
        else {
            D_ASSERT(false);
        }
    }
    else if (op->Eopid() == COperator::EOperatorId::EopScalarCmp) {
        return false;
    }
    else {
        // EopScalarFunc, EopScalarOp, etc. — treat as complex
        return true;
    }
}

void Planner::pAdustAggGroups(
    vector<unique_ptr<duckdb::Expression>> &agg_groups,
    vector<unique_ptr<duckdb::Expression>> &agg_exprs)
{
    for (ULONG agg_group_idx = 0; agg_group_idx < agg_groups.size();
         agg_group_idx++) {
        auto agg_group_expr =
            (duckdb::BoundReferenceExpression *)agg_groups[agg_group_idx].get();
        agg_group_expr->index = agg_group_idx;
    }
    ULONG accm_agg_expr_idx = 0;
    for (ULONG agg_expr_idx = 0; agg_expr_idx < agg_exprs.size();
         agg_expr_idx++) {
        auto agg_expr =
            (duckdb::BoundAggregateExpression *)agg_exprs[agg_expr_idx].get();
        for (ULONG agg_expr_child_idx = 0;
             agg_expr_child_idx < agg_expr->children.size();
             agg_expr_child_idx++) {
            auto bound_expr = (duckdb::BoundReferenceExpression *)agg_expr
                                  ->children[agg_expr_child_idx]
                                  .get();
            bound_expr->index = agg_groups.size() + accm_agg_expr_idx++;
        }
    }
}

void Planner::pUpdateProjAggExprs(
    CExpression *pexprScalarExpr,
    vector<unique_ptr<duckdb::Expression>> &agg_exprs,
    vector<unique_ptr<duckdb::Expression>> &agg_groups,
    vector<unique_ptr<duckdb::Expression>> &proj_exprs,
    vector<duckdb::LogicalType>& agg_types,
    vector<duckdb::LogicalType>& proj_types, CColRefArray *child_cols,
    bool &adjust_agg_groups_performed, bool &has_pre_projection)
{
    CExpression *aggargs_expr = pexprScalarExpr->operator[](0);
    if (aggargs_expr->operator[](0)->Pop()->Eopid() !=
        COperator::EopScalarIdent) {
        has_pre_projection = true;
        if (!adjust_agg_groups_performed) {
            pAdustAggGroups(agg_groups, agg_exprs);
            adjust_agg_groups_performed = true;
        }
        // aggregation have child in the last column
        proj_exprs.push_back(std::move(
            pTransformScalarExpr(aggargs_expr->operator[](0), child_cols)));
        agg_exprs.push_back(std::move(pTransformScalarAggFunc(
            pexprScalarExpr, child_cols, proj_exprs.back()->return_type,
            proj_exprs.size() - 1)));
    }
    else {
        proj_exprs.push_back(std::move(
            pTransformScalarExpr(aggargs_expr->operator[](0), child_cols)));
        if (has_pre_projection) {
            agg_exprs.push_back(std::move(pTransformScalarAggFunc(
                pexprScalarExpr, child_cols, proj_exprs.back()->return_type,
                proj_exprs.size() - 1)));
        }
        else {
            agg_exprs.push_back(
                std::move(pTransformScalarExpr(pexprScalarExpr, child_cols)));
        }
        proj_types.push_back(proj_exprs.back()->return_type);
        agg_types.push_back(agg_exprs.back()->return_type);
    }
    proj_types.push_back(proj_exprs.back()->return_type);
    agg_types.push_back(agg_exprs.back()->return_type);
}

bool Planner::pIsAdjIdxJoinInto(CExpression *scalar_expr, CColRefSet *outer_cols,
                       CColRefSet *inner_cols, CExpression *&adjidxjoin_into_expr)
{
    if (scalar_expr->Pop()->Eopid() == COperator::EOperatorId::EopScalarCmp) {
        CScalarCmp *cmp = (CScalarCmp *)scalar_expr->Pop();
        if (cmp->ParseCmpType() == IMDType::EcmptEq) {
            CExpression *left = scalar_expr->operator[](0);
            CExpression *right = scalar_expr->operator[](1);
            if (left->Pop()->Eopid() ==
                    COperator::EOperatorId::EopScalarIdent &&
                right->Pop()->Eopid() ==
                    COperator::EOperatorId::EopScalarIdent) {
                CScalarIdent *left_ident = (CScalarIdent *)left->Pop();
                CScalarIdent *right_ident = (CScalarIdent *)right->Pop();
                if (outer_cols->FMember(left_ident->Pcr()) &&
                    inner_cols->FMember(right_ident->Pcr())) {
                    adjidxjoin_into_expr = scalar_expr;
                    return true;
                }
                else if (outer_cols->FMember(right_ident->Pcr()) &&
                         inner_cols->FMember(left_ident->Pcr())) {
                    adjidxjoin_into_expr = scalar_expr;
                    return true;
                }
            }
        }
        return false;
    }
    else {
        for (ULONG child_idx = 0; child_idx < scalar_expr->Arity();
             child_idx++) {
            if (pIsAdjIdxJoinInto(scalar_expr->operator[](child_idx),
                                  outer_cols, inner_cols, adjidxjoin_into_expr)) {
                return true;
            }
        }
        return false;
    }
}

CExpression *Planner::reBuildFilterExpr(CExpression *filter_expr,
                                        CExpression *adjidxjoin_into_expr)
{
    if ((*filter_expr)[1] == adjidxjoin_into_expr) {
        return nullptr;
    }
    else {
        CExpression *new_scalar_expr = recursiveBuildFilterExpr((*filter_expr)[1], adjidxjoin_into_expr);
        if (new_scalar_expr != nullptr) {
            CExpression *new_filter_expr = GPOS_NEW(this->memory_pool) CExpression(
                this->memory_pool, filter_expr->Pop(), (*filter_expr)[0], new_scalar_expr);
            return new_filter_expr;
        } else {
            return nullptr;
        }
    }
}

CExpression *Planner::recursiveBuildFilterExpr(
    CExpression *scalar_expr, CExpression *adjidxjoin_into_expr)
{
    switch (scalar_expr->Pop()->Eopid()) {
		case COperator::EopScalarCmp: {
            if (scalar_expr == adjidxjoin_into_expr) {
                return nullptr;
            }
            else {
                CMemoryPool *mp = this->memory_pool;
                return GPOS_NEW(mp) CExpression(mp, scalar_expr->Pop(),
                                                scalar_expr->operator[](0),
                                                scalar_expr->operator[](1));
            }
            break;
        }
		case COperator::EopScalarBoolOp: {
            CMemoryPool *mp = this->memory_pool;
            ULONG num_valid_children = 0;
            CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
            for (ULONG child_idx = 0; child_idx < scalar_expr->Arity();
                    child_idx++) {
                CExpression *result = recursiveBuildFilterExpr(
                    scalar_expr->operator[](child_idx), adjidxjoin_into_expr);
                if (result != nullptr) {
                    num_valid_children++;
                    pdrgpexpr->Append(result);
                }
            }
            if (num_valid_children == 0) {
                return nullptr;
            }
            else if (num_valid_children == 1) {
                return pdrgpexpr->operator[](0);
            }
            else {
                return GPOS_NEW(mp) CExpression(mp, scalar_expr->Pop(),
                                                pdrgpexpr);
            }
            break;
        }
		default:
			GPOS_ASSERT(false); // NOT implemented yet
	}
}

duckdb::AdjIdxIdIdxs Planner::pGetAdjIdxIdIdxs(CColRefArray *inner_cols, IMDIndex::EmdindexType index_type) {
    duckdb::idx_t edge_id_idx = -1;
    duckdb::idx_t src_id_idx = -1;
    duckdb::idx_t tgt_id_idx = -1;

    for (ULONG col_idx = 0; col_idx < inner_cols->Size(); col_idx++) {
        CColRef *colref = inner_cols->operator[](col_idx);
        CColRefTable *colref_table = (CColRefTable *)colref;
        const CName &col_name = colref_table->Name();
        wchar_t *full_col_name, *col_only_name, *first_token, *pt;
        full_col_name = new wchar_t[std::wcslen(col_name.Pstr()->GetBuffer()) + 1];
        std::wcscpy(full_col_name, col_name.Pstr()->GetBuffer());
        first_token = std::wcstok(full_col_name, L".", &pt);
        col_only_name = std::wcstok(NULL, L".", &pt);

        // Use column-only part after "." if present; otherwise use the full name.
        const wchar_t *effective_name = (col_only_name != NULL) ? col_only_name : first_token;

        if (effective_name != NULL) {
            if (std::wcsncmp(effective_name, L"_id", 4) == 0) {
                edge_id_idx = col_idx;
            }
            else if (std::wcsncmp(effective_name, L"_sid", 4) == 0) {
                src_id_idx = col_idx;
            }
            else if (std::wcsncmp(effective_name, L"_tid", 4) == 0) {
                tgt_id_idx = col_idx;
            }
        }
    }

    if (index_type == gpmd::IMDIndex::EmdindFwdAdjlist) {
        return {edge_id_idx, src_id_idx, tgt_id_idx};
    }
    else {
        return {edge_id_idx, tgt_id_idx, src_id_idx};
    }
}

OID Planner::pGetTableOidFromScanExpr(CExpression *scan_expr)
{
     if (scan_expr->Pop()->Eopid() == COperator::EOperatorId::EopPhysicalIndexScan) {
        auto *idxscan_op = (CPhysicalIndexScan *)scan_expr->Pop();
        return CMDIdGPDB::CastMdid(idxscan_op->Ptabdesc()->MDId())->Oid();
    }
    else if (scan_expr->Pop()->Eopid() == COperator::EOperatorId::EopPhysicalIndexOnlyScan) {
        auto *idxonlyscan_op = (CPhysicalIndexOnlyScan *)scan_expr->Pop();
        return CMDIdGPDB::CastMdid(idxonlyscan_op->Ptabdesc()->MDId())->Oid();
    }
    else if (scan_expr->Pop()->Eopid() == COperator::EOperatorId::EopPhysicalTableScan) {
        auto *scan_op = (CPhysicalTableScan *)scan_expr->Pop();
        return CMDIdGPDB::CastMdid(scan_op->Ptabdesc()->MDId())->Oid();
    }
    else {
        D_ASSERT(false);
    }
}

bool Planner::pConstructColumnInfosRegardingFilter(
    CExpression *projection_expr, vector<ULONG> &output_original_colref_ids,
    vector<duckdb::idx_t> &non_filter_only_column_idxs)
{
    auto *mp = this->memory_pool;
    bool has_filter_only_column = false;

    CExpression *proj_list_expr = projection_expr->PdrgPexpr()->operator[](1);
    CColRefArray *proj_output_cols =
        projection_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);

    for (int j = 0; j < proj_list_expr->PdrgPexpr()->Size(); j++) {
        CExpression *proj_elem = proj_list_expr->PdrgPexpr()->operator[](j);
        CScalarProjectElement *proj_elem_op =
            (CScalarProjectElement *)proj_elem->Pop();
        auto scalarident_pattern = vector<COperator::EOperatorId>(
            {COperator::EOperatorId::EopScalarProjectElement,
             COperator::EOperatorId::EopScalarIdent});

        if (pMatchExprPattern(proj_elem, scalarident_pattern)) {
            CScalarIdent *ident_op =
                (CScalarIdent *)proj_elem->PdrgPexpr()->operator[](0)->Pop();
            output_original_colref_ids.push_back(ident_op->Pcr()->Id());

            if (proj_output_cols->IndexOf(proj_elem_op->Pcr()) !=
                gpos::ulong_max) {
                non_filter_only_column_idxs.push_back(j);
            }
            else {
                has_filter_only_column = true;
            }
        }
        else {
            output_original_colref_ids.push_back(
                std::numeric_limits<ULONG>::max());
            non_filter_only_column_idxs.push_back(j);
        }
    }

    return has_filter_only_column;
}

void Planner::pConstructFilterColPosVals(
    CExpression *projection_expr, vector<int64_t> &pred_attr_poss, vector<duckdb::Value>& literal_vals
)
{
    auto *mp = this->memory_pool;
    CExpression *proj_list_expr = projection_expr->PdrgPexpr()->operator[](1);
    CColRefArray *proj_output_cols =
        projection_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);

    // Obtain scan and filter expression
    CExpression *filter_expr = pFindFilterExpr(projection_expr);
    D_ASSERT(filter_expr != NULL);
    CExpression *filter_pred_expr = filter_expr->operator[](1);
    CExpression *scan_expr = filter_expr->operator[](0);

    // Obtain scan operator for table descriptor
    CTableDescriptor *tab_desc = ((CPhysicalTableScan *)scan_expr->Pop())->Ptabdesc();
    if (!tab_desc->IsInstanceDescriptor()) {
        gpos::ULONG pred_attr_pos;
        duckdb::Value literal_val;
        pGetFilterAttrPosAndValue(filter_pred_expr, pred_attr_pos,
                                    literal_val);
        pred_attr_poss.push_back(pred_attr_pos);
        literal_vals.push_back(move(literal_val));
    }
    else {
        IMdIdArray *mdid_array = tab_desc->GetTableIdsInGroup();
        for (int i = 0; i < mdid_array->Size(); i++) {
            gpos::ULONG pred_attr_pos;
            duckdb::Value literal_val;
            pGetFilterAttrPosAndValue(filter_pred_expr, 
                                        (*mdid_array)[i],
                                        pred_attr_pos,
                                        literal_val);
            pred_attr_poss.push_back(pred_attr_pos);
            literal_vals.push_back(move(literal_val));
        }
    }
}

bool Planner::pFindOperandsColIdxs(CExpression *expr, CColRefArray *cols, duckdb::idx_t &out_idx) {
    for (uint32_t i = 0; i < expr->Arity(); i++) {
        CExpression *scalar_expr = expr->operator[](i);
        if (scalar_expr->Pop()->Eopid() == COperator::EOperatorId::EopScalarIdent) {
            CScalarIdent *sc_ident = (CScalarIdent *)(scalar_expr->Pop());
            duckdb::idx_t idx = cols->IndexOf(sc_ident->Pcr());
            if (idx != gpos::ulong_max) {
                out_idx = idx;
                return true;
            }
        }
    }
    return false;
}

string Planner::pResolvePartitionName(duckdb::idx_t graphlet_oid) {
    auto &cat = context->db->GetCatalog();
    auto *ps = dynamic_cast<duckdb::PropertySchemaCatalogEntry *>(
        cat.GetEntry(*context, DEFAULT_SCHEMA, graphlet_oid, true));
    if (!ps) return "";
    auto *part = dynamic_cast<duckdb::PartitionCatalogEntry *>(
        cat.GetEntry(*context, DEFAULT_SCHEMA, ps->partition_oid, true));
    if (!part) return "";

    string name = part->name;
    bool is_edge = (name.substr(0, 6) == "epart_");
    // Strip "vpart_" / "epart_" prefix
    if (name.size() > 6 && (name.substr(0, 6) == "vpart_" || is_edge))
        name = name.substr(6);

    if (is_edge) {
        // Edge partition name format: "EDGETYPE@SrcLabel@DstLabel"
        // Extract just the edge type (before first '@')
        auto at_pos = name.find('@');
        if (at_pos != string::npos) {
            name = name.substr(0, at_pos);
        }
    }
    return name;
}

// ============================================================
// UNWIND / Unnest
// ============================================================
duckdb::CypherPhysicalOperatorGroups *Planner::pTransformEopUnnest(
    CExpression *plan_expr)
{
    CMemoryPool *mp = this->memory_pool;

    CPhysicalUnnest *pop = CPhysicalUnnest::PopConvert(plan_expr->Pop());
    CColRef *pcrOutput = pop->PcrOutput();

    // Check if scalar child is a constant list (standalone UNWIND [1,2,3]).
    // In that case, skip the child ConstTableGet and directly produce rows.
    CExpression *pexprScalar = (*plan_expr)[1];  // project list
    D_ASSERT(pexprScalar->Arity() > 0);
    CExpression *pexprProjElem = (*pexprScalar)[0];  // project element
    D_ASSERT(pexprProjElem->Arity() > 0);
    CExpression *pexprListExpr = (*pexprProjElem)[0]; // the list expression

    // Standalone UNWIND with constant/function list expression:
    // Check if child is ConstTableGet (single empty row source).
    CExpression *pexprChild = (*plan_expr)[0];
    if (pexprChild->Pop()->Eopid() == COperator::EopPhysicalConstTableGet) {
        // Evaluate the scalar list expression to get a DuckDB Value
        CColRefArray *empty_cols = GPOS_NEW(mp) CColRefArray(mp);
        auto scalar_result = pTransformScalarExpr(pexprListExpr, empty_cols);
        empty_cols->Release();

        // The scalar expression should be a constant or function that
        // produces a LIST value.  Extract children.
        auto elem_type = pGetColumnsDuckDBType(pcrOutput);
        duckdb::Schema scan_schema;
        scan_schema.setStoredTypes({elem_type});

        vector<vector<duckdb::Value>> scan_rows;
        if (scalar_result && scalar_result->GetExpressionClass() ==
                duckdb::ExpressionClass::BOUND_CONSTANT) {
            auto &cv = static_cast<duckdb::BoundConstantExpression &>(*scalar_result);
            if (cv.value.type().id() == duckdb::LogicalTypeId::LIST) {
                auto &children = duckdb::ListValue::GetChildren(cv.value);
                for (auto &child : children) {
                    scan_rows.push_back({child});
                }
            }
        }
        // If the scalar is a function (e.g., list_value(1,2,3)), extract
        // each child constant as an individual row.
        if (scan_rows.empty() && scalar_result &&
            scalar_result->GetExpressionClass() == duckdb::ExpressionClass::BOUND_FUNCTION) {
            auto &fn = static_cast<duckdb::BoundFunctionExpression &>(*scalar_result);
            for (idx_t ci = 0; ci < fn.children.size(); ci++) {
                if (fn.children[ci]->GetExpressionClass() ==
                    duckdb::ExpressionClass::BOUND_CONSTANT) {
                    auto &cc = static_cast<duckdb::BoundConstantExpression &>(
                        *fn.children[ci]);
                    scan_rows.push_back({cc.value});
                }
            }
        }

        auto *op = new duckdb::PhysicalConstScan(scan_schema, std::move(scan_rows));
        auto *result = new duckdb::CypherPhysicalOperatorGroups();
        result->push_back(op);
        pBuildSchemaFlowGraphForSingleSchemaScan(scan_schema);
        return result;
    }

    // Non-constant list — process child and use PhysicalUnwind
    duckdb::CypherPhysicalOperatorGroups *result =
        pTraverseTransformPhysicalPlan(plan_expr->PdrgPexpr()->operator[](0));
    CExpression *pexprListIdent = (*pexprProjElem)[0];  // scalar ident

    // Find the list column's position in the child output.
    // The CPhysicalUnnest output column replaces the list column in the
    // schema flow.  Look up which position in the ORCA output columns
    // corresponds to the output colref of the unnest operator.
    CColRefArray *output_cols =
        plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CColRefArray *outer_cols =
        (*plan_expr)[0]->Prpp()->PcrsRequired()->Pdrgpcr(mp);

    idx_t list_col_idx = 0;
    if (pexprListIdent->Pop()->Eopid() == COperator::EopScalarIdent) {
        CScalarIdent *psi = CScalarIdent::PopConvert(pexprListIdent->Pop());
        ULONG idx = outer_cols->IndexOf(psi->Pcr());
        if (idx != gpos::ulong_max) {
            list_col_idx = idx;
        }
    }

    // Build output schema from the ORCA output columns.
    // The unnest output has the same columns as the child, PLUS the
    // unnest output column.  But in the schema flow graph, we need
    // to use the child's types and add/replace the list column.
    duckdb::CypherPhysicalOperator *last_op = result->back();
    auto &child_types = last_op->GetTypes();

    // Build output types: same as child but replace LIST type at col_idx
    // with its element type (since PhysicalUnwind expands list → elements).
    vector<duckdb::LogicalType> out_types = child_types;
    if (list_col_idx < out_types.size()) {
        auto &lt = out_types[list_col_idx];
        if (lt.id() == duckdb::LogicalTypeId::LIST) {
            out_types[list_col_idx] = duckdb::ListType::GetChildType(lt);
        }
    }
    duckdb::Schema tmp_schema;
    tmp_schema.setStoredTypes(out_types);

    duckdb::CypherPhysicalOperator *op =
        new duckdb::PhysicalUnwind(tmp_schema, list_col_idx);
    result->push_back(op);

    pBuildSchemaFlowGraphForUnaryOperator(tmp_schema);

    return result;
}

}  // namespace turbolynx
