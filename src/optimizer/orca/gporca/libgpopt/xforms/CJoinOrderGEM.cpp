//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CJoinOrderGEM.cpp
//
//	@doc:
//		Implementation of dynamic programming-based join order generation
//---------------------------------------------------------------------------

#include "gpopt/xforms/CJoinOrderGEM.h"

#include "gpos/base.h"
#include "gpos/common/CBitSet.h"
#include "gpos/common/CBitSetIter.h"
#include "gpos/common/clibwrapper.h"
#include "gpos/error/CAutoTrace.h"

#include "gpopt/base/CDrvdPropScalar.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/exception.h"
#include "gpopt/operators/CLogicalInnerJoin.h"
#include "gpopt/operators/CLogicalLeftOuterJoin.h"
#include "gpopt/operators/CNormalizer.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/base/CColRefTable.h"
#include "gpopt/operators/CLogicalGet.h"
#include "gpopt/operators/CLogicalUnionAll.h"
#include <cstdlib>  // [DEMO PROBE — uncommitted] getenv
#include <cstdio>   // [DEMO PROBE] fprintf
#include <algorithm>  // [DEMO PROBE] sort
#include <vector>     // [DEMO PROBE]
#include <utility>    // [DEMO PROBE] pair
#include <string>
#include <unordered_map>
#include "naucrates/md/CMDIdGPDB.h"  // [DEMO PROBE] Oid from mdid
#include "optimizer/orca/gpopt/tbgppdbwrappers.hpp"  // [DEMO PROBE] GetRelation
#include "optimizer/orca/gpopt/translate/CTranslatorTBGPPToDXL.hpp"  // AddVirtualEdgeTable

using namespace gpopt;

#define SPLIT_TIME_LIMIT 10
#define COMPONENT_TIME_LIMIT 100

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderGEM::SComponentPair::SComponentPair
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJoinOrderGEM::SComponentPair::SComponentPair(CBitSet *pbsFst, CBitSet *pbsSnd)
	: m_pbsFst(pbsFst), m_pbsSnd(pbsSnd)
{
	GPOS_ASSERT(NULL != pbsFst);
	GPOS_ASSERT(NULL != pbsSnd);
	GPOS_ASSERT(pbsFst->IsDisjoint(pbsSnd));
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderGEM::SComponentPair::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CJoinOrderGEM::SComponentPair::HashValue(const SComponentPair *pcomppair)
{
	GPOS_ASSERT(NULL != pcomppair);

	return CombineHashes(pcomppair->m_pbsFst->HashValue(),
						 pcomppair->m_pbsSnd->HashValue());
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderGEM::SComponentPair::Equals
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
BOOL
CJoinOrderGEM::SComponentPair::Equals(const SComponentPair *pcomppairFst,
									 const SComponentPair *pcomppairSnd)
{
	GPOS_ASSERT(NULL != pcomppairFst);
	GPOS_ASSERT(NULL != pcomppairSnd);

	return pcomppairFst->m_pbsFst->Equals(pcomppairSnd->m_pbsFst) &&
		   pcomppairFst->m_pbsSnd->Equals(pcomppairSnd->m_pbsSnd);
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderGEM::SComponentPair::~SComponentPair
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CJoinOrderGEM::SComponentPair::~SComponentPair()
{
	m_pbsFst->Release();
	m_pbsSnd->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderGEM::CJoinOrderGEM
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJoinOrderGEM::CJoinOrderGEM(CMemoryPool *mp,
						   CExpressionArray *pdrgpexprComponents,
						   CExpressionArray *pdrgpexprConjuncts)
	: CJoinOrder(mp, pdrgpexprComponents, pdrgpexprConjuncts,
				 false /* m_include_loj_childs */)
{
	m_phmcomplink = GPOS_NEW(mp) ComponentPairToExpressionMap(mp);
	m_phmbsexpr = GPOS_NEW(mp) BitSetToExpressionMap(mp);
	m_phmexprcost = GPOS_NEW(mp) ExpressionToCostMap(mp);
	m_pdrgpexprTopKOrders = GPOS_NEW(mp) CExpressionArray(mp);
	m_pexprDummy = GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp));

#ifdef GPOS_DEBUG
	for (ULONG ul = 0; ul < m_ulComps; ul++)
	{
		GPOS_ASSERT(NULL != m_rgpcomp[ul]->m_pexpr->Pstats() &&
					"stats were not derived on input component");
	}
#endif	// GPOS_DEBUG
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderGEM::~CJoinOrderGEM
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CJoinOrderGEM::~CJoinOrderGEM()
{
#ifdef GPOS_DEBUG
	// in optimized build, we flush-down memory pools without leak checking,
	// we can save time in optimized build by skipping all de-allocations here,
	// we still have all de-llocations enabled in debug-build to detect any possible leaks
	m_phmcomplink->Release();
	m_phmbsexpr->Release();
	m_phmexprcost->Release();
	m_pdrgpexprTopKOrders->Release();
	m_pexprDummy->Release();
#endif	// GPOS_DEBUG
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderGEM::PexprPred
//
//	@doc:
//		Extract predicate joining the two given sets
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderGEM::PexprPred(CBitSet *pbsFst, CBitSet *pbsSnd)
{
	GPOS_ASSERT(NULL != pbsFst);
	GPOS_ASSERT(NULL != pbsSnd);

	if (!pbsFst->IsDisjoint(pbsSnd) || 0 == pbsFst->Size() ||
		0 == pbsSnd->Size())
	{
		// components must be non-empty and disjoint
		return NULL;
	}

	CExpression *pexprPred = NULL;
	SComponentPair *pcomppair = NULL;

	// lookup link map
	for (ULONG ul = 0; ul < 2; ul++)
	{
		pbsFst->AddRef();
		pbsSnd->AddRef();
		pcomppair = GPOS_NEW(m_mp) SComponentPair(pbsFst, pbsSnd);
		pexprPred = m_phmcomplink->Find(pcomppair);
		if (NULL != pexprPred)
		{
			pcomppair->Release();
			if (m_pexprDummy == pexprPred)
			{
				return NULL;
			}
			return pexprPred;
		}

		// try again after swapping sets
		if (0 == ul)
		{
			pcomppair->Release();
			std::swap(pbsFst, pbsSnd);
		}
	}

	// could not find link in the map, construct it from edge set
	pexprPred = PexprBuildPred(pbsFst, pbsSnd);
	if (NULL == pexprPred)
	{
		m_pexprDummy->AddRef();
		pexprPred = m_pexprDummy;
	}

	// store predicate in link map
#ifdef GPOS_DEBUG
	BOOL fInserted =
#endif	// GPOS_DEBUG
		m_phmcomplink->Insert(pcomppair, pexprPred);
	GPOS_ASSERT(fInserted);

	if (m_pexprDummy != pexprPred)
	{
		return pexprPred;
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderGEM::DeriveStats
//
//	@doc:
//		Derive stats on given expression
//
//---------------------------------------------------------------------------
void
CJoinOrderGEM::DeriveStats(CExpression *pexpr)
{
	GPOS_ASSERT(NULL != pexpr);

	if (m_pexprDummy != pexpr && NULL == pexpr->Pstats())
	{
		CExpressionHandle exprhdl(m_mp);
		exprhdl.Attach(pexpr);
		exprhdl.DeriveStats(m_mp, m_mp, NULL /*prprel*/, NULL /*stats_ctxt*/);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderGEM::DCost
//
//	@doc:
//		Primitive costing of join expressions;
//		cost of a join expression is the summation of the costs of its
//		children plus its local cost;
//		cost of a leaf expression is the estimated number of rows
//
//---------------------------------------------------------------------------
CDouble
CJoinOrderGEM::DCost(CExpression *pexpr)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pexpr);

	CDouble *pd = m_phmexprcost->Find(pexpr);
	if (NULL != pd)
	{
		// stop recursion if cost was already cashed
		return *pd;
	}

	CDouble dCost(0.0);
	const ULONG arity = pexpr->Arity();
	if (0 == arity)
	{
		if (NULL == pexpr->Pstats())
		{
			GPOS_RAISE(
				CException::ExmaInvalid, CException::ExmiAssert,
				GPOS_WSZ_LIT("stats were not derived on an input component"));
		}

		// leaf operator, use its estimated number of rows as cost
		// dCost = CDouble(pexpr->Pstats()->Rows());
		dCost = 0.0;
	}
	else
	{
		// inner join operator, sum-up cost of its children
		DOUBLE rgdRows[2] = {0.0, 0.0};
		for (ULONG ul = 0; ul < arity - 1; ul++)
		{
			CExpression *pexprChild = (*pexpr)[ul];

			// call function recursively to find child cost
			dCost = dCost + DCost(pexprChild);
			DeriveStats(pexprChild);
			rgdRows[ul] = pexprChild->Pstats()->Rows().Get();
		}

		// add inner join local cost
		dCost = dCost + (rgdRows[0] + rgdRows[1]);
	}

	return dCost;
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderGEM::PexprBuildPred
//
//	@doc:
//		Build predicate connecting the two given sets
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderGEM::PexprBuildPred(CBitSet *pbsFst, CBitSet *pbsSnd)
{
	// collect edges connecting the given sets
	CBitSet *pbsEdges = GPOS_NEW(m_mp) CBitSet(m_mp);
	CBitSet *pbs = GPOS_NEW(m_mp) CBitSet(m_mp, *pbsFst);
	pbs->Union(pbsSnd);

	for (ULONG ul = 0; ul < m_ulEdges; ul++)
	{
		SEdge *pedge = m_rgpedge[ul];
		if (pbs->ContainsAll(pedge->m_pbs) &&
			!pbsFst->IsDisjoint(pedge->m_pbs) &&
			!pbsSnd->IsDisjoint(pedge->m_pbs))
		{
#ifdef GPOS_DEBUG
			BOOL fSet =
#endif	// GPOS_DEBUG
				pbsEdges->ExchangeSet(ul);
			GPOS_ASSERT(!fSet);
		}
	}
	pbs->Release();

	CExpression *pexprPred = NULL;
	if (0 < pbsEdges->Size())
	{
		CExpressionArray *pdrgpexpr = GPOS_NEW(m_mp) CExpressionArray(m_mp);
		CBitSetIter bsi(*pbsEdges);
		while (bsi.Advance())
		{
			ULONG ul = bsi.Bit();
			SEdge *pedge = m_rgpedge[ul];
			pedge->m_pexpr->AddRef();
			pdrgpexpr->Append(pedge->m_pexpr);
		}

		pexprPred = CPredicateUtils::PexprConjunction(m_mp, pdrgpexpr);
	}

	pbsEdges->Release();
	return pexprPred;
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderGEM::lCreateTableDescForRel
//
//	@doc:
//		Create table descriptor for relation
//
//---------------------------------------------------------------------------
CTableDescriptor *CJoinOrderGEM::CreateTableDescForVirtualTable(
    CTableDescriptor *ptabdesc, IMdIdArray *pdrgmdid)
{
	CTableDescriptor *new_ptabdesc;
	CMDAccessor *mda = COptCtxt::PoctxtFromTLS()->Pmda();

	if (pdrgmdid->Size() > 1) {
		IMDId *mdid = mda->AddVirtualTable(ptabdesc->MDId(), pdrgmdid);

		new_ptabdesc = GPOS_NEW(m_mp) CTableDescriptor(
			m_mp,
			mdid,
			ptabdesc->Name(),
			ptabdesc->ConvertHashToRandom(),
			IMDRelation::EreldistrMasterOnly,
			IMDRelation::ErelstorageHeap,
			0
		);

		new_ptabdesc->SetInstanceDescriptor(true);
		for (ULONG ul = 0; ul < pdrgmdid->Size(); ul++) {
			new_ptabdesc->AddTableInTheGroup((*pdrgmdid)[ul]);
		}

		// Get column descriptors from original table descriptor
		CColumnDescriptorArray *org_pdrgpcoldesc = ptabdesc->OrgPdrgpcoldesc();
		CColumnDescriptorArray *pdrgpcoldesc = ptabdesc->Pdrgpcoldesc();
		GPOS_ASSERT(NULL != org_pdrgpcoldesc);
		GPOS_ASSERT(NULL != pdrgpcoldesc);

		// Add each column descriptor to the new table descriptor
		for (ULONG ul = 0; ul < org_pdrgpcoldesc->Size(); ul++)
		{
			CColumnDescriptor *pcoldesc = (*org_pdrgpcoldesc)[ul];
			pcoldesc->AddRef();
			new_ptabdesc->AddColumn(pcoldesc);
		}

		new_ptabdesc->SetPdrgpcoldesc(pdrgpcoldesc);
	} else {
		GPOS_ASSERT(pdrgmdid->Size() == 1);

		// don't need to create new table descriptor. get the existing table ID
		IMDId *mdid = (*pdrgmdid)[0];
		new_ptabdesc = GPOS_NEW(m_mp) CTableDescriptor(
			m_mp,
			mdid,
			ptabdesc->Name(),
			ptabdesc->ConvertHashToRandom(),
			IMDRelation::EreldistrMasterOnly,
			IMDRelation::ErelstorageHeap,
			0
		);

		// Get column descriptors from original table descriptor
		CColumnDescriptorArray *org_pdrgpcoldesc = ptabdesc->OrgPdrgpcoldesc();
		CColumnDescriptorArray *pdrgpcoldesc = ptabdesc->Pdrgpcoldesc();
		GPOS_ASSERT(NULL != org_pdrgpcoldesc);
		GPOS_ASSERT(NULL != pdrgpcoldesc);

		// Add each column descriptor to the new table descriptor
		for (ULONG ul = 0; ul < org_pdrgpcoldesc->Size(); ul++)
		{
			CColumnDescriptor *pcoldesc = (*org_pdrgpcoldesc)[ul];
			pcoldesc->AddRef();
			new_ptabdesc->AddColumn(pcoldesc);
		}

		new_ptabdesc->SetPdrgpcoldesc(pdrgpcoldesc);
	}

	new_ptabdesc->AddRef();
	return new_ptabdesc;
}

void CJoinOrderGEM::SplitUnionAll(CExpression *pexpr, ULONG ulTarget,
                                           SComponent **splitted_components,
                                           BOOL is_first_time)
{
    GPOS_ASSERT(NULL != pexpr);

	// If first time, initialize split state
    if (is_first_time) {
        m_ulSplitIndex = 0;
    }
    else {
        // Move to next split configuration
        m_ulSplitIndex++;
    }

    CExpression *scan_expr = FindLogicalGetExpr(pexpr);
	GPOS_ASSERT(scan_expr != NULL);

	// Get table descriptor from LogicalGet
	CTableDescriptor *ptabdesc = CLogicalGet::PopConvert(scan_expr->Pop())->Ptabdesc();

	// Get table ids from table descriptor
	IMdIdArray *pimdidarray = ptabdesc->GetTableIdsInGroup();
	
	// Get number of tables in the virtual table
	ULONG ulTables = pimdidarray->Size();
	GPOS_ASSERT(ulTables > 1);
	
	// Create arrays to hold table descriptors for each group
	IMdIdArray *pdrgmdidFirst = GPOS_NEW(m_mp) IMdIdArray(m_mp);
	IMdIdArray *pdrgmdidSecond = GPOS_NEW(m_mp) IMdIdArray(m_mp);
	
	// Split table IDs into two groups (query-aware: separate graphlets by which
	// of the split target's incident edge types dominates them).
	std::vector<std::string> query_etypes = QueryEdgeTypesForTarget(ulTarget);
	SplitGraphlets(pimdidarray, ulTables, pdrgmdidFirst, pdrgmdidSecond,
				   m_ulSplitIndex, query_etypes);
	
	// Create new virtual tables for each group
    CTableDescriptor *ptabdescFirst =
        CreateTableDescForVirtualTable(ptabdesc, pdrgmdidFirst);
    
    CTableDescriptor *ptabdescSecond =
        CreateTableDescForVirtualTable(ptabdesc, pdrgmdidSecond);
	
    // Get output columns from original LogicalGet
    CLogicalGet *plogicalget = CLogicalGet::PopConvert(scan_expr->Pop());
    CColRefArray *pdrgpcrOutput = plogicalget->PdrgpcrOutput();

    std::wstring w_alias1 = L"VirtualTable1";
	std::wstring w_alias2 = L"VirtualTable2";
	CWStringConst strAlias1(w_alias1.c_str());
	CWStringConst strAlias2(w_alias2.c_str());

	pdrgpcrOutput->AddRef();
    CExpression *pexprFirstVirtual = GPOS_NEW(m_mp) CExpression(
        m_mp, GPOS_NEW(m_mp) CLogicalGet(
                  m_mp, GPOS_NEW(m_mp) CName(m_mp, CName(&strAlias1)),
                  ptabdescFirst, pdrgpcrOutput));

	pdrgpcrOutput->AddRef();
    CExpression *pexprSecondVirtual = GPOS_NEW(m_mp) CExpression(
        m_mp, GPOS_NEW(m_mp) CLogicalGet(
                  m_mp, GPOS_NEW(m_mp) CName(m_mp, CName(&strAlias2)),
                  ptabdescSecond, pdrgpcrOutput));

    // Create new expressions by replacing the original LogicalGet with the new virtual table LogicalGets
    CExpression *pexprFirstComponent =
        PexprCopyWithNewChildren(m_mp, pexpr, scan_expr, pexprFirstVirtual);
    CExpression *pexprSecondComponent =
        PexprCopyWithNewChildren(m_mp, pexpr, scan_expr, pexprSecondVirtual);

	
    splitted_components[0] = GPOS_NEW(m_mp) SComponent(m_mp, pexprFirstComponent);
    splitted_components[1] =
        GPOS_NEW(m_mp) SComponent(m_mp, pexprSecondComponent);

	splitted_components[0]->m_pbs->Union(m_rgpcomp[ulTarget]->m_pbs);
    splitted_components[0]->m_edge_set->Union(m_rgpcomp[ulTarget]->m_edge_set);
    splitted_components[1]->m_pbs->Union(m_rgpcomp[ulTarget]->m_pbs);
    splitted_components[1]->m_edge_set->Union(m_rgpcomp[ulTarget]->m_edge_set);

	pdrgmdidFirst->Release();
    pdrgmdidSecond->Release();
    ptabdescFirst->Release();
    ptabdescSecond->Release();
    pexprFirstVirtual->Release();
    pexprSecondVirtual->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderGEM::PexprCopyWithNewChildren
//
//	@doc:
//		Copy expression tree and replace scan_expr with new_expr
//
//---------------------------------------------------------------------------
CExpression *CJoinOrderGEM::PexprCopyWithNewChildren(
    CMemoryPool *mp, CExpression *pexpr, CExpression *target_expr,
    CExpression *new_expr)
{
    GPOS_ASSERT(NULL != pexpr);
    GPOS_ASSERT(NULL != target_expr);
    GPOS_ASSERT(NULL != new_expr);

    // If current expression matches scan_expr, return new_expr
    if (pexpr == target_expr) {
        new_expr->AddRef();
        return new_expr;
    }

    // Copy operator
    COperator *pop = pexpr->Pop();
    pop->AddRef();

    // Recursively process children
    CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
    const ULONG ulArity = pexpr->Arity();

    for (ULONG ul = 0; ul < ulArity; ul++) {
        CExpression *pexprChild = (*pexpr)[ul];
        CExpression *pexprNewChild =
            PexprCopyWithNewChildren(mp, pexprChild, target_expr, new_expr);
        pdrgpexpr->Append(pexprNewChild);
    }

    return GPOS_NEW(mp) CExpression(mp, pop, pdrgpexpr);
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderGEM::PexprSwapEdgeToVirtual
//
//	@doc:
//		Rebuild an edge component's expression with a per-branch virtual edge Get
//		(row-count estimate = dFanout). This is the memo-side counterpart to the
//		xform-time GemBranchEdgeFanout: giving each UnionAll branch a distinct edge
//		relation mdid whose RetrieveRelStats reports the branch's own forward edge
//		count lets ORCA's generic (histogram-less) join cost diverge per branch, so
//		the memo keeps each branch's divergent join order and prefers the split over
//		a single washed tree. The virtual edge is created under the original edge's
//		partition (inheriting its forward-adjlist index), so the physical scan and
//		execution are unchanged — only the estimate differs.
//---------------------------------------------------------------------------
CExpression *
CJoinOrderGEM::PexprSwapEdgeToVirtual(CExpression *pexprEdgeComp, CDouble dFanout)
{
	if (dFanout < CDouble(0.0)) return NULL;

	CExpression *pexprGet = FindLogicalGetExpr(pexprEdgeComp);
	if (NULL == pexprGet) return NULL;

	CLogicalGet *plget = CLogicalGet::PopConvert(pexprGet->Pop());
	CTableDescriptor *ptabdesc = plget->Ptabdesc();

	// Only swap edge relations (eps_<type> Gets); leave node components shared.
	OID oid = CMDIdGPDB::CastMdid(ptabdesc->MDId())->Oid();
	duckdb::PropertySchemaCatalogEntry *rel = duckdb::GetRelation(oid);
	if (NULL == rel || rel->name.compare(0, 4, "eps_") != 0) return NULL;

	// Create the per-branch virtual edge relation. Row count is the branch's
	// forward edge fan-out (>= 1 so an empty branch still scans/estimates sanely).
	ULLONG rows = (ULLONG) std::max(1.0, dFanout.Get());
	IMDId *vedge_mdid =
		CTranslatorTBGPPToDXL::AddVirtualEdgeTable(m_mp, ptabdesc->MDId(), rows);

	// Build the virtual edge table descriptor: its own mdid is the per-branch
	// virtual edge (drives the STATS the memo reads), while the group holds the
	// ORIGINAL edge relation (drives EXECUTION — the physical scan resolves the
	// real edge extents through the group, exactly like the anchor virtual table
	// resolves its member graphlets). So execution is unchanged; only the row
	// estimate becomes per-branch.
	CTableDescriptor *vptabdesc = GPOS_NEW(m_mp) CTableDescriptor(
		m_mp, vedge_mdid, ptabdesc->Name(), ptabdesc->ConvertHashToRandom(),
		IMDRelation::EreldistrMasterOnly, IMDRelation::ErelstorageHeap, 0);
	vptabdesc->SetInstanceDescriptor(true);
	IMDId *orig_edge_mdid = ptabdesc->MDId();
	orig_edge_mdid->AddRef();
	vptabdesc->AddTableInTheGroup(orig_edge_mdid);
	// Copy column descriptors from the original edge (mirrors
	// CreateTableDescForVirtualTable).
	CColumnDescriptorArray *org_pdrgpcoldesc = ptabdesc->OrgPdrgpcoldesc();
	CColumnDescriptorArray *pdrgpcoldesc = ptabdesc->Pdrgpcoldesc();
	for (ULONG ul = 0; ul < org_pdrgpcoldesc->Size(); ul++) {
		CColumnDescriptor *pcoldesc = (*org_pdrgpcoldesc)[ul];
		pcoldesc->AddRef();
		vptabdesc->AddColumn(pcoldesc);
	}
	vptabdesc->SetPdrgpcoldesc(pdrgpcoldesc);
	vptabdesc->AddRef();

	// New Get over the virtual edge, reusing the original edge's output colrefs so
	// the join predicates still bind (mirrors the anchor split in SplitUnionAll).
	CColRefArray *pdrgpcrOutput = plget->PdrgpcrOutput();
	pdrgpcrOutput->AddRef();
	std::wstring w_alias = L"VirtualEdge";
	CWStringConst strAlias(w_alias.c_str());
	CExpression *pexprNewGet = GPOS_NEW(m_mp) CExpression(
		m_mp, GPOS_NEW(m_mp) CLogicalGet(
				  m_mp, GPOS_NEW(m_mp) CName(m_mp, CName(&strAlias)), vptabdesc,
				  pdrgpcrOutput));

	CExpression *pexprNew =
		PexprCopyWithNewChildren(m_mp, pexprEdgeComp, pexprGet, pexprNewGet);
	// Derive stats now: the greedy left-deep builder reads each leaf component's
	// Pstats row count, and this also resolves the virtual edge mdid eagerly.
	DeriveStats(pexprNew);
	return pexprNew;
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderGEM::SplitGraphlets
//
//	@doc:
//		Split graphlets into two groups
//
//---------------------------------------------------------------------------
// Defined below; forward-declared so SplitGraphlets can use the cached
// per-graphlet forward edge counts.
static const std::unordered_map<std::string, double> &GraphletFwdMap(OID g_oid);
static double GraphletFwdEdgeCount(OID g_oid, const std::string &edge_type);
static void CollectLogicalGets(CExpression *pexpr, std::vector<CLogicalGet *> &out);

// Collect the forward edge-type internal names of query edges incident to the
// split-target component. Each such edge's neighbour component is the edge-node
// Get on an "eps_<edge-type>" relation, so we read that name and strip the prefix.
std::vector<std::string>
CJoinOrderGEM::QueryEdgeTypesForTarget(ULONG ulTarget)
{
	static const std::string eps_prefix = "eps_";
	std::vector<std::string> out;
	for (ULONG ulEdge = 0; ulEdge < m_ulEdges; ulEdge++)
	{
		SEdge *pedge = m_rgpedge[ulEdge];
		// endpoints of this edge
		CBitSetIter bsi(*pedge->m_pbs);
		BOOL fTouches = false;
		ULONG ulOther = gpos::ulong_max;
		while (bsi.Advance())
		{
			ULONG b = bsi.Bit();
			if (b == ulTarget) fTouches = true;
			else ulOther = b;
		}
		if (!fTouches || ulOther == gpos::ulong_max) continue;
		std::vector<CLogicalGet *> gets;
		CollectLogicalGets(m_rgpcomp[ulOther]->m_pexpr, gets);
		for (CLogicalGet *pg : gets)
		{
			OID oid = CMDIdGPDB::CastMdid(pg->Ptabdesc()->MDId())->Oid();
			duckdb::PropertySchemaCatalogEntry *rel = duckdb::GetRelation(oid);
			if (NULL != rel &&
				rel->name.compare(0, eps_prefix.size(), eps_prefix) == 0)
			{
				std::string t = rel->name.substr(eps_prefix.size());
				bool seen = false;
				for (const auto &s : out) if (s == t) { seen = true; break; }
				if (!seen) out.push_back(t);
				break;
			}
		}
	}
	return out;
}

void CJoinOrderGEM::SplitGraphlets(IMdIdArray *pimdidarray,
                                            ULONG ulTables,
                                            IMdIdArray *pdrgmdidFirst,
                                            IMdIdArray *pdrgmdidSecond,
											ULONG ulSplitIndex,
											const std::vector<std::string> &query_etypes)
{
    // Split the graphlet set into two BALANCED halves. A grossly unbalanced
    // split (e.g. 2 vs the rest) makes one UnionAll branch process tiny,
    // under-filled chunks; a downstream operator then emits stale rows from the
    // unfilled tail of those chunks, over-counting on cyclic (triangle) joins.
    // A balanced split keeps both branches' chunks well filled and avoids it.
    // ulTables > 1 here, so ulTables/2 is always in [1, ulTables-1].
    ULONG ulFirstGroupSize = ulTables / 2;

    // [DEMO PROBE — uncommitted] TLX_GEM_INTERLEAVE=1 distributes graphlets to
    // the two groups by parity instead of contiguous first-N/last-N. Tests
    // whether a degenerate (one empty) split is why the physical UnionAll
    // collapses: interleaving spreads matching graphlets across both branches.
    bool interleave = (NULL != getenv("TLX_GEM_INTERLEAVE"));

    // [DEMO PROBE — uncommitted] TLX_GEM_RATIO overrides the first-group SIZE as
    // a fraction of ulTables, to sweep split ratios and see if any lets the
    // physical UnionAll survive.
    const char *ratio_env = getenv("TLX_GEM_RATIO");
    if (NULL != ratio_env) {
        double r = std::atof(ratio_env);
        if (r > 0.0 && r < 1.0) {
            ulFirstGroupSize = (ULONG)(ulTables * r);
            if (ulFirstGroupSize == 0) ulFirstGroupSize = 1;
            if (ulFirstGroupSize >= ulTables) ulFirstGroupSize = ulTables - 1;
        }
    }

    // CONTENT-AWARE split (TLX_GEM_BYSIZE): group graphlets by node count so the
    // densest go to group 1 and the sparsest to group 2, making the two UnionAll
    // branches heterogeneous so each branch's GOO picks a join order tuned to its
    // subset. NOTE: this currently exposes a cyclic-split disjointness bug (a
    // triangle tuple whose split node is referenced by both the path edge and
    // the closing edge can match in BOTH branches → over-count), so it is opt-in
    // until that is fixed; the default balanced index-order split is correct.
    if (NULL != getenv("TLX_GEM_BYSIZE")) {
        std::vector<std::pair<uint64_t, ULONG>> byrows;  // (rows, index)
        byrows.reserve(ulTables);
        for (ULONG ul = 0; ul < ulTables; ul++) {
            uint64_t rows = 0;
            IMDId *pmdid = (*pimdidarray)[ul];
            OID oid = CMDIdGPDB::CastMdid(pmdid)->Oid();
            duckdb::PropertySchemaCatalogEntry *rel = duckdb::GetRelation(oid);
            if (NULL != rel) rows = rel->GetNumberOfRowsApproximately();
            byrows.push_back({rows, ul});
        }
        std::sort(byrows.begin(), byrows.end(),
                  [](const std::pair<uint64_t, ULONG> &a,
                     const std::pair<uint64_t, ULONG> &b) { return a.first > b.first; });
        for (ULONG k = 0; k < ulTables; k++) {
            IMDId *pmdid = (*pimdidarray)[byrows[k].second];
            pmdid->AddRef();
            if (k < ulFirstGroupSize) pdrgmdidFirst->Append(pmdid);
            else pdrgmdidSecond->Append(pmdid);
        }
        return;
    }

    // Content-aware split by edge-degree profile (default). Group the graphlets
    // so that each UnionAll branch is homogeneous in edge fan-out: pick the edge
    // type whose forward count varies most across graphlets, then sort graphlets
    // by that count and split at ulTables/2. Graphlets with opposite degree
    // profiles (e.g. e1-heavy vs e3-heavy) land in different branches, while the
    // group sizes stay balanced so no branch processes under-filled chunks. This
    // is what makes the per-branch join-order cardinalities differ. Falls through
    // to the index-order split when no forward edge counts are available.
    if (NULL == getenv("TLX_BALANCED_SPLIT")) {
        // Query-aware split. Order the graphlets by the DIFFERENCE of the two
        // query edges' forward counts (count[e0] - count[e1]), so that graphlets
        // dominated by e0 land in one branch and those dominated by e1 in the
        // other. Each branch is then homogeneous in which edge explodes, so its
        // GOO picks the join order (selective edge first) that fits its subset —
        // and the two branches diverge, beating any single unified order. When the
        // query edge types aren't resolvable, fall back to the widest-fan-out edge
        // across the graphlets. Per-graphlet counts are cached and the sorted order
        // is memoized per (graphlet-set, edge-pair) signature.
        static std::unordered_map<size_t, std::vector<ULONG>> s_split_order_cache;
        std::vector<OID> oids(ulTables);
        size_t sig = ulTables * 1099511628211ull;
        for (ULONG ul = 0; ul < ulTables; ul++) {
            oids[ul] = CMDIdGPDB::CastMdid((*pimdidarray)[ul])->Oid();
            sig = (sig ^ (size_t) oids[ul]) * 1099511628211ull;
        }
        for (const std::string &et : query_etypes)
            sig = (sig ^ std::hash<std::string>{}(et)) * 1099511628211ull;
        auto cit = s_split_order_cache.find(sig);
        if (cit == s_split_order_cache.end()) {
            std::vector<double> key(ulTables, 0.0);
            bool have_key = false;
            if (query_etypes.size() >= 2) {
                // Pick the two most explosive query edges (highest total fan-out
                // across graphlets) as the split axis, then order graphlets by the
                // difference of their counts so each side is dominated by one edge.
                std::string e0, e1; double t0 = -1.0, t1 = -1.0;
                for (const std::string &et : query_etypes) {
                    double tot = 0.0;
                    for (ULONG ul = 0; ul < ulTables; ul++)
                        tot += GraphletFwdEdgeCount(oids[ul], et);
                    if (tot > t0) { t1 = t0; e1 = e0; t0 = tot; e0 = et; }
                    else if (tot > t1) { t1 = tot; e1 = et; }
                }
                for (ULONG ul = 0; ul < ulTables; ul++) {
                    const auto &m = GraphletFwdMap(oids[ul]);
                    double c0 = 0.0, c1 = 0.0;
                    auto j0 = m.find(e0); if (j0 != m.end()) c0 = j0->second;
                    auto j1 = m.find(e1); if (j1 != m.end()) c1 = j1->second;
                    key[ul] = c0 - c1;
                    if (c0 != c1) have_key = true;
                }
            }
            if (!have_key) {
                // fallback: widest-fan-out forward edge type across these graphlets
                std::unordered_map<std::string, double> etype_max;
                for (ULONG ul = 0; ul < ulTables; ul++)
                    for (const auto &kv : GraphletFwdMap(oids[ul])) {
                        double &mx = etype_max[kv.first];
                        if (kv.second > mx) mx = kv.second;
                    }
                std::string best_etype; double best_max = 0.0;
                for (const auto &kv : etype_max)
                    if (kv.second > best_max) { best_max = kv.second; best_etype = kv.first; }
                if (!best_etype.empty() && best_max > 0.0) {
                    for (ULONG ul = 0; ul < ulTables; ul++)
                        key[ul] = GraphletFwdEdgeCount(oids[ul], best_etype);
                    have_key = true;
                }
            }
            std::vector<ULONG> order;
            if (have_key) {
                order.resize(ulTables);
                for (ULONG ul = 0; ul < ulTables; ul++) order[ul] = ul;
                std::stable_sort(order.begin(), order.end(),
                    [&](ULONG a, ULONG b) { return key[a] > key[b]; });
            }
            cit = s_split_order_cache.emplace(sig, std::move(order)).first;
        }
        const std::vector<ULONG> &order = cit->second;
        if (!order.empty()) {
            for (ULONG k = 0; k < ulTables; k++) {
                IMDId *pmdid = (*pimdidarray)[order[k]];
                pmdid->AddRef();
                if (k < ulFirstGroupSize) pdrgmdidFirst->Append(pmdid);
                else pdrgmdidSecond->Append(pmdid);
            }
            return;
        }
    }

    // Split tables between the two groups
    for (ULONG ul = ulSplitIndex; ul < ulTables + ulSplitIndex; ul++) {
		ULONG ulIndex = ul % ulTables;
        IMDId *pmdid = (*pimdidarray)[ulIndex];
        pmdid->AddRef();

        bool toFirst = interleave ? (ulIndex % 2 == 0)
                                  : (ul - ulSplitIndex < ulFirstGroupSize);
        if (toFirst) {
            pdrgmdidFirst->Append(pmdid);
        }
        else {
            pdrgmdidSecond->Append(pmdid);
        }
    }
}

CExpression *CJoinOrderGEM::ProcessUnionAllComponents(CDouble &dCost)
{
    CExpression *pexprResultUnionAll = NULL;
	CDouble dTotalTime = 0;

    // Iterate through components
    for (ULONG ul = 0; ul < m_ulComps; ul++) {
		CTimerUser timerComponent;
		timerComponent.Restart();

        // Get component
        SComponent *pcomp = m_rgpcomp[ul];
        if (NULL == pcomp) {
            continue;
        }

		CExpression *pexpr = FindLogicalGetExpr(pcomp->m_pexpr);
		if (pexpr == NULL) {
			continue;
		}

        // Get table descriptor
        CTableDescriptor *ptabdesc =
            CLogicalGet::PopConvert(pexpr->Pop())->Ptabdesc();
        if (NULL == ptabdesc) {
            continue;
        }

        // Check if table descriptor is instance descriptor
        if (!ptabdesc->IsInstanceDescriptor()) {
            continue;
        }
        
		CExpression *pexprGOO = 
			BuildQueryGraphAndRunGOO(pcomp->m_pexpr, ul, dCost);
		
		if (pexprGOO == NULL) {
			continue;
		}

		CDouble dCostGOO = DCost(pexprGOO);

		// Update best plan if cost is lower
		if (dCostGOO < dCost) {
			if (pexprResultUnionAll != NULL) {
				pexprResultUnionAll->Release();
			}
			pexprResultUnionAll = pexprGOO;
			dCost = dCostGOO;
		}
		else {
			pexprGOO->Release();
		}

        // CDouble dComponentTime(timerComponent.ElapsedUS() /
        //                        CDouble(GPOS_USEC_IN_MSEC));
        // dTotalTime = CDouble(dTotalTime.Get() + dComponentTime.Get());
        // if (dTotalTime.Get() > COMPONENT_TIME_LIMIT) {
        //     break;
        // }
    }
	
	return pexprResultUnionAll;
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderGEM::PexprExpand
//
//	@doc:
//		Create join order
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderGEM::PexprExpand()
{
	// Calculate edge selectivity
	CalcEdgeSelectivity(&m_pdrgdSelectivity);

	// Run GOO for original query graph
    CExpression *pexprResult =
        RunGOO(m_ulComps, m_rgpcomp, m_ulEdges, m_rgpedge, m_pdrgdSelectivity);

    // compute cost for pexprResult
	CDouble dCost = DCost(pexprResult);

    // Split one by one
    // [DEMO PROBE — uncommitted] TLX_GEM_NOSPLIT=1 skips the UnionAll-split
    // search so PexprExpand returns the plain un-split GOO plan. Lets us
    // isolate the split's runtime contribution (GEM-split vs GEM-nosplit).
    CExpression *pexprResultUnionAll =
        (NULL != getenv("TLX_GEM_NOSPLIT")) ? NULL : ProcessUnionAllComponents(dCost);

    if (NULL != pexprResultUnionAll)
	{
		pexprResultUnionAll->AddRef();
		return pexprResultUnionAll;
	} else {
		return pexprResult;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderGEM::CalcEdgeSelectivity
//
//	@doc:
//		Calculate selectivity for an edge
//
//---------------------------------------------------------------------------
void
CJoinOrderGEM::CalcEdgeSelectivity(CDoubleArray **pdrgdSelectivity)
{
	*pdrgdSelectivity = GPOS_NEW(m_mp) CDoubleArray(m_mp);

	for (ULONG ulEdge = 0; ulEdge < m_ulEdges; ulEdge++)
	{
		SEdge *pedge = m_rgpedge[ulEdge];

		// Get components connected by this edge
		CBitSetIter bsi(*pedge->m_pbs);
		(void) bsi.Advance();
		ULONG ulFirst = bsi.Bit();
		(void) bsi.Advance();
		ULONG ulSecond = bsi.Bit();

		SComponent *comp1 = m_rgpcomp[ulFirst];
		SComponent *comp2 = m_rgpcomp[ulSecond];

		GPOS_ASSERT(IsValidJoinCombination(comp1, comp2));

		CJoinOrder::SComponent *compTemp = PcompCombine(comp1, comp2);

		GPOS_ASSERT(!CUtils::FCrossJoin(compTemp->m_pexpr));
		
		DeriveStats(compTemp->m_pexpr);
		CDouble dRows = compTemp->m_pexpr->Pstats()->Rows();

		// Get row counts for components
		CDouble dRowsFirst = m_rgpcomp[ulFirst]->m_pexpr->Pstats()->Rows();
		CDouble dRowsSecond = m_rgpcomp[ulSecond]->m_pexpr->Pstats()->Rows();

		// [DEMO PROBE — uncommitted] dump every logical Get in the combined
		// expression with its table relation oid + output columns, to see whether
		// the edge partition (edge type) is recoverable and how to match it to the
		// _sid/_tid column referenced by this edge's predicate.
		if (NULL != getenv("TLX_GET_TRACE")) {
			std::fprintf(stderr, "[EDGE %u GETS]", ulEdge);
			CExpressionArray *stack = GPOS_NEW(m_mp) CExpressionArray(m_mp);
			stack->Append(compTemp->m_pexpr);
			compTemp->m_pexpr->AddRef();
			while (stack->Size() > 0) {
				CExpression *cur = (*stack)[stack->Size() - 1];
				stack->RemoveLast();
				COperator *pop = cur->Pop();
				if (COperator::EopLogicalGet == pop->Eopid()) {
					CLogicalGet *pget = CLogicalGet::PopConvert(pop);
					CTableDescriptor *ptabdesc = pget->Ptabdesc();
					OID rel_oid = ptabdesc ? CMDIdGPDB::CastMdid(ptabdesc->MDId())->Oid() : 0;
					duckdb::PropertySchemaCatalogEntry *rel =
						rel_oid ? duckdb::GetRelation(rel_oid) : nullptr;
					std::fprintf(stderr, " Get{oid=%u name=%s part=%llu rows=%llu cols=[",
								 (unsigned) rel_oid,
								 rel ? rel->name.c_str() : "?",
								 rel ? (unsigned long long) rel->GetPartitionOID() : 0ull,
								 rel ? (unsigned long long) rel->GetNumberOfRowsApproximately() : 0ull);
					CColRefArray *pdrgpcr = pget->PdrgpcrOutput();
					for (ULONG k = 0; pdrgpcr && k < pdrgpcr->Size(); k++)
						std::fprintf(stderr, "%u ", (*pdrgpcr)[k]->Id());
					std::fprintf(stderr, "]}");
				}
				for (ULONG c = 0; c < cur->Arity(); c++) {
					(*cur)[c]->AddRef();
					stack->Append((*cur)[c]);
				}
				cur->Release();
			}
			stack->Release();
			std::fprintf(stderr, "\n");
		}

		// [DEMO PROBE — uncommitted] inspect edge predicate to see how edge type
		// (adjidx) is encoded, and the component tables (graphlets).
		if (NULL != getenv("TLX_EDGE_TRACE")) {
			CColRefSet *used = pedge->m_pexpr->DeriveUsedColumns();
			std::fprintf(stderr, "[EDGE %u] comps={%u,%u} predRoot=%d usedcols=%u :",
						 ulEdge, ulFirst, ulSecond,
						 (int) pedge->m_pexpr->Pop()->Eopid(), used->Size());
			CColRefSetIter it(*used);
			while (it.Advance()) {
				CColRef *c = it.Pcr();
				const CWStringConst *w = c->Name().Pstr();
				std::fprintf(stderr, " col%u(%ls)", c->Id(), w->GetBuffer());
				if (CColRef::EcrtTable == c->Ecrt()) {
					CColRefTable *ct = CColRefTable::PcrConvert(c);
					std::fprintf(stderr, "{prop=%u,node=%u,attno=%d,src=%u}",
								 c->PropId(), c->NodeId(),
								 (int) ct->AttrNum(), ct->UlSourceOpId());
				}
			}
			std::fprintf(stderr, "\n");
		}

		// Get predicate selectivity from stats
		CDouble dSelectivity = dRows / (dRowsFirst * dRowsSecond);

		// Clamp selectivity to valid range [0,1]
		dSelectivity = std::min(CDouble(1.0),
									std::max(CDouble(0.0), dSelectivity));

		(*pdrgdSelectivity)->Append(GPOS_NEW(m_mp) CDouble(dSelectivity));

		compTemp->Release();
	}
}

// Process-wide cache of per-graphlet forward edge counts, keyed by the
// graphlet's relation oid → (edge-type name → total forward edges). The counts
// are fixed for a loaded workspace, so caching avoids re-reading the catalog and
// re-scanning the adjacency-key list on every per-branch selectivity call (which
// happens for every edge × branch × split trial, and would otherwise be O(members
// × adjkeys) per call — prohibitive at thousands of graphlets).
static std::unordered_map<OID, std::unordered_map<std::string, double>>
	g_tlx_graphlet_fwd_cache;

static const std::unordered_map<std::string, double> &
GraphletFwdMap(OID g_oid)
{
	auto it = g_tlx_graphlet_fwd_cache.find(g_oid);
	if (it == g_tlx_graphlet_fwd_cache.end())
	{
		std::unordered_map<std::string, double> m;
		duckdb::PropertySchemaCatalogEntry *g = duckdb::GetRelation(g_oid);
		if (NULL != g)
		{
			const auto &names = g->GetAdjListNames();
			const auto &kinds = g->GetAdjListTypes();
			for (ULONG k = 0; k < names.size(); k++)
				if (kinds[k] == duckdb::LogicalTypeId::FORWARD_ADJLIST)
					m[names[k]] += (double) g->GetAdjListEdgeCount(k);
		}
		it = g_tlx_graphlet_fwd_cache.emplace(g_oid, std::move(m)).first;
	}
	return it->second;
}

static double
GraphletFwdEdgeCount(OID g_oid, const std::string &edge_type)
{
	const std::unordered_map<std::string, double> &m = GraphletFwdMap(g_oid);
	auto jt = m.find(edge_type);
	return jt == m.end() ? 0.0 : jt->second;
}

// Recursively collect all logical Get operators reachable from an expression.
static void
CollectLogicalGets(CExpression *pexpr, std::vector<CLogicalGet *> &out)
{
	if (NULL == pexpr) return;
	if (COperator::EopLogicalGet == pexpr->Pop()->Eopid())
		out.push_back(CLogicalGet::PopConvert(pexpr->Pop()));
	for (ULONG i = 0; i < pexpr->Arity(); i++)
		CollectLogicalGets((*pexpr)[i], out);
}

// Per-graphlet edge fan-out for a split (virtual-table) component.
//
// pexprVT       : the split component's expression (its Get carries the member
//                 graphlet mdids for this UnionAll branch).
// pexprCombined : the component joined with its neighbour through one edge; its
//                 edge Get (name "eps_<edge-type>") identifies the traversed
//                 edge type.
//
// Returns the total number of forward adjacency edges leaving this branch's
// graphlets through that edge type, i.e. the branch-specific join fan-out.
// Returns a negative value when the edge type or graphlets cannot be resolved,
// in which case the caller keeps the generic stats-based estimate.
CDouble
CJoinOrderGEM::GemBranchEdgeFanout(CExpression *pexprVT, CExpression *pexprCombined)
{
	static const std::string eps_prefix = "eps_";

	// 1) Resolve the traversed edge type from the eps_ Get in the combined expr.
	std::vector<CLogicalGet *> combined_gets;
	CollectLogicalGets(pexprCombined, combined_gets);
	std::string internal_name;
	for (CLogicalGet *pget : combined_gets)
	{
		OID oid = CMDIdGPDB::CastMdid(pget->Ptabdesc()->MDId())->Oid();
		duckdb::PropertySchemaCatalogEntry *rel = duckdb::GetRelation(oid);
		if (NULL != rel && rel->name.compare(0, eps_prefix.size(), eps_prefix) == 0)
		{
			internal_name = rel->name.substr(eps_prefix.size());
			break;
		}
	}
	if (internal_name.empty()) return CDouble(-1.0);

	// 2) Recover the branch's member graphlets from the split component's Get.
	// A multi-graphlet branch carries them in GetTableIdsInGroup(); a
	// single-graphlet branch has an empty group and its descriptor mdid IS the
	// graphlet, so fall back to that.
	std::vector<CLogicalGet *> vt_gets;
	CollectLogicalGets(pexprVT, vt_gets);
	std::vector<OID> member_oids;
	for (CLogicalGet *pget : vt_gets)
	{
		OID oid = CMDIdGPDB::CastMdid(pget->Ptabdesc()->MDId())->Oid();
		duckdb::PropertySchemaCatalogEntry *rel = duckdb::GetRelation(oid);
		// Skip edge Gets; the vertex Get carries the graphlet group.
		if (NULL != rel && rel->name.compare(0, eps_prefix.size(), eps_prefix) == 0)
			continue;
		IMdIdArray *members = pget->Ptabdesc()->GetTableIdsInGroup();
		if (NULL != members && members->Size() > 0)
			for (ULONG i = 0; i < members->Size(); i++)
				member_oids.push_back(CMDIdGPDB::CastMdid((*members)[i])->Oid());
		else
			member_oids.push_back(oid);
		break;
	}
	if (member_oids.empty()) return CDouble(-1.0);

	// 3) Sum each member graphlet's forward edge count for this edge type,
	// using the process-wide cache to avoid repeated catalog scans.
	double total = 0.0;
	for (OID g_oid : member_oids)
		total += GraphletFwdEdgeCount(g_oid, internal_name);
	return CDouble(total);
}

void CJoinOrderGEM::UpdateEdgeSelectivity(
    ULONG ulTarget, CDoubleArray *pdrgdSelectivity, ULONG ul,
    SComponent **splitted_components)
{
    if (pdrgdSelectivity == NULL) {
		pdrgdSelectivity = GPOS_NEW(m_mp) CDoubleArray(m_mp);
	}
	
	for (ULONG ulEdge = 0; ulEdge < m_ulEdges; ulEdge++)
	{
		SEdge *pedge = m_rgpedge[ulEdge];

		// Get components connected by this edge
		CBitSetIter bsi(*pedge->m_pbs);
		(void) bsi.Advance();
		ULONG ulFirst = bsi.Bit();
		(void) bsi.Advance();
		ULONG ulSecond = bsi.Bit();

		if (ulFirst != ulTarget && ulSecond != ulTarget) {
			continue;
		}

		SComponent *comp1, *comp2;
		if (ulFirst == ulTarget) {
			comp1 = splitted_components[ul];
			comp2 = m_rgpcomp[ulSecond];
		}
		else {
			comp1 = m_rgpcomp[ulFirst];
			comp2 = splitted_components[ul];
		}

		GPOS_ASSERT(IsValidJoinCombination(comp1, comp2));

		CJoinOrder::SComponent *compTemp = PcompCombine(comp1, comp2);

		GPOS_ASSERT(!CUtils::FCrossJoin(compTemp->m_pexpr));
		
		DeriveStats(compTemp->m_pexpr);
		CDouble dRows = compTemp->m_pexpr->Pstats()->Rows();

		// Get row counts for components
		CDouble dRowsFirst = comp1->m_pexpr->Pstats()->Rows();
		CDouble dRowsSecond = comp2->m_pexpr->Pstats()->Rows();

		// Branch-aware fan-out: when this branch (the split virtual table) is the
		// source of the edge, replace the schema-agnostic join estimate with the
		// actual number of edges leaving this branch's graphlets through the
		// traversed edge type. This lets heterogeneous graphlet groups receive
		// distinct per-edge cardinalities, so each UnionAll branch's GOO can pick
		// the join order matching its own edge-degree profile.
		SComponent *pcompVT =
			(ulFirst == ulTarget) ? comp1 : comp2;
		CDouble dFanout(-1.0);
		if (NULL == getenv("TLX_NO_FANOUT")) {
			dFanout = GemBranchEdgeFanout(pcompVT->m_pexpr, compTemp->m_pexpr);
			if (dFanout >= CDouble(0.0))
				dRows = dFanout;
		}
		if (NULL != getenv("TLX_GEM_TRACE"))
			std::fprintf(stderr, "[SEL] branch=%u edge=%u fanout=%.0f dRows=%.0f "
						 "first=%.0f second=%.0f\n", ul, ulEdge, dFanout.Get(),
						 dRows.Get(), dRowsFirst.Get(), dRowsSecond.Get());

		// Get predicate selectivity from stats
		CDouble dSelectivity = dRows / (dRowsFirst * dRowsSecond);

		// Clamp selectivity to valid range [0,1]
		dSelectivity = std::min(CDouble(1.0),
									std::max(CDouble(0.0), dSelectivity));

		pdrgdSelectivity->Replace(ulEdge, GPOS_NEW(m_mp) CDouble(dSelectivity));

		compTemp->Release();
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderGEM::Find
//
//	@doc:
//		Find set representative with path compression
//
//---------------------------------------------------------------------------
ULONG
CJoinOrderGEM::Find(ULONG *parent, ULONG x)
{
	if (parent[x] != x)
	{
		parent[x] = Find(parent, parent[x]); // Path compression
	}
	return parent[x];
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderGEM::Union
//
//	@doc:
//		Union two sets by rank
//
//---------------------------------------------------------------------------
ULONG
CJoinOrderGEM::Union(ULONG *parent, ULONG *rank, ULONG x, ULONG y)
{
	ULONG px = Find(parent, x);
	ULONG py = Find(parent, y);
	ULONG root;

	if (rank[px] < rank[py])
	{
		parent[px] = py;
		root = py;
	}
	else if (rank[px] > rank[py])
	{
		parent[py] = px;
		root = px;
	}
	else
	{
		parent[py] = px;
		rank[px]++;
		root = px;
	}

	return root;
}

CExpression *CJoinOrderGEM::PexprBuildLeftDeepTree(ULONG ulComps, SComponent **rgpcomp,
							SComponent **splitted_components, ULONG ulTarget,ULONG ulSplit,
							double *out_peak_cost)
{
    if (out_peak_cost) *out_peak_cost = 0.0;
    if (0 == ulComps)
    {
        return NULL;
    }
    
    CExpressionArray *pdrgpexprLeaves = GPOS_NEW(m_mp) CExpressionArray(m_mp);
    if (splitted_components != NULL) {
        for (ULONG ul = 0; ul < ulComps; ul++) {
            SComponent *comp;
            if (ul == ulTarget) {
                comp = splitted_components[ulSplit];
            } else {
                comp = rgpcomp[ul];
            }
            comp->m_pexpr->AddRef();
            pdrgpexprLeaves->Append(comp->m_pexpr);
        }
    } else {
        for (ULONG ul = 0; ul < ulComps; ul++) {
            SComponent *comp = rgpcomp[ul];
            comp->m_pexpr->AddRef();
            pdrgpexprLeaves->Append(comp->m_pexpr);
        }
    }

    if (1 == ulComps)
    {
        CExpression *pexprResult = (*pdrgpexprLeaves)[0];
        pexprResult->AddRef();
        pdrgpexprLeaves->Release();
        return pexprResult;
    }

    // Greedy left-deep join ORDER driven by the per-branch selectivity, so each
    // UnionAll branch follows the cheapest order for its own graphlet subset
    // (giving divergent per-branch orders) while keeping a correct left-deep tree
    // built with the proper connecting predicates. A bushy RunGOO tree used
    // directly as the plan drops rows during execution; ordering a left-deep tree
    // instead keeps correctness AND the divergence.
    std::vector<double> comp_size(ulComps, 1.0);
    for (ULONG ul = 0; ul < ulComps; ul++) {
        SComponent *comp = (splitted_components != NULL && ul == ulTarget)
                               ? splitted_components[ulSplit] : rgpcomp[ul];
        comp_size[ul] = comp->m_pexpr->Pstats()->Rows().Get();
    }
    std::vector<ULONG> join_order;
    join_order.reserve(ulComps);
    // Default: fixed component-index order — the only order the physical IdSeek
    // pipeline executes correctly (reordered join trees currently drop matching
    // rows via the order-sensitive inner-IdSeek path, an execution bug). The
    // greedy selectivity-driven order (TLX_DIVERGE_ORDER) produces per-branch
    // divergence but is unsafe until that execution bug is fixed.
    if (NULL == getenv("TLX_DIVERGE_ORDER")) {
        for (ULONG ul = 0; ul < ulComps; ul++) join_order.push_back(ul);
    } else {
    std::vector<bool> in_tree(ulComps, false);
    CBitSet *pbsAcc = GPOS_NEW(m_mp) CBitSet(m_mp);
    double acc_size = 1.0;
    // Seed with the split-target (anchor) component so the branch's virtual table
    // stays the SCAN BASE: the graphlet-subset split is enforced by the base scan
    // + inner-IdSeek drop, which only works when the vtbl is scanned, not reached
    // by adjacency traversal. Only the ORDER in which the anchor's edges are then
    // joined varies per branch — that is where the divergence lives.
    {
        ULONG seed = (splitted_components != NULL) ? ulTarget : 0;
        if (splitted_components == NULL) {
            for (ULONG ul = 1; ul < ulComps; ul++)
                if (comp_size[ul] < comp_size[seed]) seed = ul;
        }
        join_order.push_back(seed);
        in_tree[seed] = true;
        (void) pbsAcc->ExchangeSet(seed);
        acc_size = comp_size[seed];
    }
    while (join_order.size() < ulComps) {
        ULONG best = gpos::ulong_max;
        double best_cost = 0.0;
        bool best_connected = false;
        for (ULONG c = 0; c < ulComps; c++) {
            if (in_tree[c]) continue;
            // is c connected to the accumulated set by any edge?
            bool connected = false;
            ULONG conn_edge = gpos::ulong_max;
            for (ULONG e = 0; e < m_ulEdges; e++) {
                CBitSet *pbsE = m_rgpedge[e]->m_pbs;
                if (pbsE->Get(c) && !pbsAcc->IsDisjoint(pbsE)) {
                    connected = true; conn_edge = e; break;
                }
            }
            double cost = acc_size * comp_size[c];
            // Per-branch edge fan-out: weight the expansion by the branch-specific
            // edge selectivity that UpdateEdgeSelectivity just computed (via
            // GemBranchEdgeFanout). Without this the greedy uses only the generic
            // Pstats row counts, which the empty-histogram join fallback washes to
            // the same value for every branch -> both branches pick the same order.
            // Applying the per-branch selectivity makes an e1-heavy graphlet group
            // and an e3-heavy group order their edges differently (divergence).
            // Weight the expansion by the branch-specific edge selectivity that
            // UpdateEdgeSelectivity computed (via GemBranchEdgeFanout). Guard: a
            // non-anchor edge (e.g. the b->c hop in a triangle) whose anchor fan-out
            // is ~0 gets a near-zero selectivity that would drive its cost to ~0 and
            // make the greedy join it first (a degenerate cross join that explodes).
            // The resulting intermediate can never be < 1 row, so if the weighted
            // cost underflows below 1 we treat it as a non-anchor edge and keep the
            // generic (cross-join) cost, which keeps it OUT of the front of the order.
            if (connected && conn_edge != gpos::ulong_max &&
                m_pdrgdSelectivity != NULL &&
                conn_edge < m_pdrgdSelectivity->Size()) {
                double sel = (*m_pdrgdSelectivity)[conn_edge]->Get();
                double weighted = cost * sel;
                if (sel > 0.0 && weighted >= 1.0) cost = weighted;
            }
            // prefer connected components (avoid early cross joins); among the same
            // connectivity class pick the smallest resulting size.
            bool take = (best == gpos::ulong_max) ||
                        (connected && !best_connected) ||
                        (connected == best_connected && cost < best_cost);
            if (take) { best = c; best_cost = cost; best_connected = connected; }
        }
        join_order.push_back(best);
        in_tree[best] = true;
        (void) pbsAcc->ExchangeSet(best);
        acc_size = best_cost > 0.0 ? best_cost : acc_size;
        // Track the PEAK intermediate along this branch's order. The split-trial
        // selection (BuildQueryGraphAndRunGOO) uses this instead of the washed
        // DCost, so the trial whose per-branch orders keep the peak small (the
        // divergent one) is actually chosen.
        if (out_peak_cost && best_cost > *out_peak_cost) *out_peak_cost = best_cost;
    }
    pbsAcc->Release();
    }

    CExpression *pexprLeftDeep = (*pdrgpexprLeaves)[join_order[0]];
    pexprLeftDeep->AddRef();

    CBitSet *pbsLeft = GPOS_NEW(m_mp) CBitSet(m_mp);
    (void) pbsLeft->ExchangeSet(join_order[0]);

	for (ULONG oi = 1; oi < ulComps; oi++)
    {
        ULONG ul = join_order[oi];
        CExpression *pexprRight = (*pdrgpexprLeaves)[ul];
        pexprRight->AddRef();

        CBitSet *pbsRight = GPOS_NEW(m_mp) CBitSet(m_mp);
        (void) pbsRight->ExchangeSet(ul);

        CExpression *pexprPred = PexprPred(pbsLeft, pbsRight);
        pbsRight->Release();

        if (NULL == pexprPred)
        {
            pexprPred = CUtils::PexprScalarConstBool(m_mp, true /*value*/);
        }
        else
        {
            // PexprPred returns a reference owned by the m_phmcomplink link
            // map, not a fresh one. PexprLogicalJoin consumes a ref, so AddRef
            // here (as CJoinOrderDP::PexprJoin does) — otherwise the map's
            // cached predicate is freed when the join tree is released and the
            // next lookup of the same component pair is a use-after-free.
            pexprPred->AddRef();
        }

        CExpression *pexprNewJoin = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(
            m_mp, pexprLeftDeep, pexprRight, pexprPred);
        
        pexprLeftDeep = pexprNewJoin;
        
        (void) pbsLeft->ExchangeSet(ul);
    }

    pbsLeft->Release();
    pdrgpexprLeaves->Release(); 

    return pexprLeftDeep;
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderGEM::GOO
//
//	@doc:
//		Greedy Operator Ordering (GOO) algorithm implementation using UnionFind
//
//---------------------------------------------------------------------------
CExpression *CJoinOrderGEM::RunGOO(ULONG ulComps, SComponent **rgpcomp,
                                            ULONG ulEdges, SEdge **rgpedge,
                                            CDoubleArray *pdrgdSelectivity,
											SComponent **splitted_components,
											ULONG ulTarget, ULONG ulSplit)
{
    // Initialize arrays for GOO algorithm
	CDoubleArray *size = GPOS_NEW(m_mp) CDoubleArray(m_mp);
	CExpressionArray *tree = GPOS_NEW(m_mp) CExpressionArray(m_mp);
	
	// Initialize UnionFind data structure
	ULONG *parent = GPOS_NEW_ARRAY(m_mp, ULONG, ulComps);
	ULONG *rank = GPOS_NEW_ARRAY(m_mp, ULONG, ulComps);
	
	// Initialize sizes, trees and UnionFind sets
	if (splitted_components != NULL) {
		for (ULONG ul = 0; ul < ulComps; ul++) {
			SComponent *comp;
			if (ul == ulTarget) {
				comp = splitted_components[ulSplit];
			} else {
				comp = rgpcomp[ul];
			}

			CDouble rows = comp->m_pexpr->Pstats()->Rows();
			size->Append(GPOS_NEW(m_mp) CDouble(rows.Get()));
			comp->m_pexpr->AddRef();
			tree->Append(comp->m_pexpr);

			// Initialize each element as its own set
			parent[ul] = ul;
			rank[ul] = 0;
		}
	} else {
		for (ULONG ul = 0; ul < ulComps; ul++)
		{
			SComponent *comp = rgpcomp[ul];

			CDouble rows = comp->m_pexpr->Pstats()->Rows();
			size->Append(GPOS_NEW(m_mp) CDouble(rows.Get()));
			comp->m_pexpr->AddRef();
			tree->Append(comp->m_pexpr);
			
			// Initialize each element as its own set
			parent[ul] = ul;
			rank[ul] = 0;
		}
	}

    // Main GOO loop
    ULONG numSets = ulComps;
    while (numSets > 1) {
        // Find minimum cost join pair
        CDouble dMinCost(0.0);
        ULONG ulMinI = 0;
        ULONG ulMinJ = 0;
        BOOL fFirst = true;

        // Iterate through all edges to find minimum cost join
        for (ULONG ulEdge = 0; ulEdge < ulEdges; ulEdge++) {
            SEdge *pedge = rgpedge[ulEdge];
            // Get components connected by this edge
            CBitSetIter bsi(*pedge->m_pbs);
            (void)bsi.Advance();
            ULONG ulFirst = bsi.Bit();
            (void)bsi.Advance();
            ULONG ulSecond = bsi.Bit();

            // Skip if vertices already in same set
            ULONG ulFirstRoot = Find(parent, ulFirst);
            ULONG ulSecondRoot = Find(parent, ulSecond);
            if (ulFirstRoot == ulSecondRoot) {
                continue;
            }

            // Use pre-calculated edge selectivity
            CDouble selectivity = (*pdrgdSelectivity)[ulEdge]->Get();

            // Calculate join size
            CDouble dCost = (*size)[ulFirstRoot]->Get() *
                            (*size)[ulSecondRoot]->Get() * selectivity.Get();

            if (fFirst || dCost < dMinCost) {
                dMinCost = dCost;
				if ((*size)[ulFirstRoot]->Get() > (*size)[ulSecondRoot]->Get()) {
					ulMinI = ulFirstRoot;
					ulMinJ = ulSecondRoot;
				} else {
					ulMinI = ulSecondRoot;
					ulMinJ = ulFirstRoot;
				}
                fFirst = false;
            }
        }

        // Create join between selected tables
		CBitSet *pbsFirst = GPOS_NEW(m_mp) CBitSet(m_mp);
		CBitSet *pbsSecond = GPOS_NEW(m_mp) CBitSet(m_mp);
		
		// Add all elements that have ulMinI as root to pbsFirst
		for (ULONG ul = 0; ul < ulComps; ul++) {
			if (Find(parent, ul) == ulMinI) {
				(void) pbsFirst->ExchangeSet(ul);
			}
		}
		
		// Add all elements that have ulMinJ as root to pbsSecond  
		for (ULONG ul = 0; ul < ulComps; ul++) {
			if (Find(parent, ul) == ulMinJ) {
				(void) pbsSecond->ExchangeSet(ul);
			}
		}
		
		CExpression *pexprPred = PexprPred(pbsFirst, pbsSecond);
		pbsFirst->Release();
		pbsSecond->Release();
		if (NULL == pexprPred)
		{
			pexprPred = CUtils::PexprScalarConstBool(m_mp, true /*value*/);
		}
		else
		{
			// PexprPred returns a reference owned by the m_phmcomplink link
			// map, not a fresh one. PexprLogicalJoin consumes a ref, so AddRef
			// here (as CJoinOrderDP::PexprJoin does) — otherwise the map's
			// cached predicate is freed when the join tree is released and the
			// next lookup of the same component pair is a use-after-free.
			pexprPred->AddRef();
		}

		// Get expressions from tree array and increase their reference counts
		CExpression *pexprLeft = (*tree)[ulMinJ];
		CExpression *pexprRight = (*tree)[ulMinI]; 
		pexprLeft->AddRef();
		pexprRight->AddRef();

		// Create join expression using the referenced expressions
		CExpression *pexprJoin = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(
			m_mp, pexprLeft, pexprRight, pexprPred);
		
		// Union the sets
		ULONG new_root = Union(parent, rank, ulMinI, ulMinJ);
		numSets--;

		// // Update size and tree arrays
		GPOS_ASSERT(new_root == ulMinI || new_root == ulMinJ);
		size->Replace(new_root, GPOS_NEW(m_mp) CDouble(dMinCost.Get()));
		tree->Replace(new_root, pexprJoin);
		
		ULONG ulOldRoot = (new_root == ulMinI) ? ulMinJ : ulMinI;
        tree->Replace(ulOldRoot, NULL);
    }

    // Get final join tree (root will be at index of set representative)
	ULONG rootIndex = Find(parent, 0);
	CExpression *pexprResult = (*tree)[rootIndex];
	pexprResult->AddRef();

	// Cleanup
	size->Release();
	GPOS_DELETE_ARRAY(parent);
	GPOS_DELETE_ARRAY(rank);

	return pexprResult;
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderGEM::BuildQueryGraphAndRunGOO
//
//	@doc:
//		Build query graph and run GOO
//---------------------------------------------------------------------------
CExpression *
CJoinOrderGEM::BuildQueryGraphAndRunGOO(CExpression *pexpr, ULONG ulTarget, CDouble dBestCost)
{
    // pexpr = logicalget expression
    // Try different union all splits
    ULONG num_query_graphs = 2;
    SComponent **splitted_components = NULL;
    splitted_components = GPOS_NEW_ARRAY(m_mp, SComponent *, num_query_graphs);

	CExpression *pexprResult = NULL;
    // [DEMO PROBE — uncommitted] TLX_GEM_FORCESPLIT keeps the best (lowest-cost)
    // split even if the un-split GOO plan is estimated cheaper. Lets us measure
    // whether the split PLAN actually executes faster when the cost model would
    // not pick it (i.e. is the slowness a cost-model limitation or real?).
    if (NULL != getenv("TLX_GEM_FORCESPLIT")) dBestCost = CDouble(1.0e18);
    const ULONG ulMaxTrySplit = 10;
	CDouble dTotalTime = 0;
    for (ULONG ulTrySplit = 0; ulTrySplit < ulMaxTrySplit; ulTrySplit++) {
		CTimerUser timerSplit;
		timerSplit.Restart();
        SplitUnionAll(pexpr, ulTarget, splitted_components, ulTrySplit == 0);

		CExpressionArray *pexprArray = GPOS_NEW(m_mp) CExpressionArray(m_mp);
		CColRefArray *pdrgpcrOutput = GPOS_NEW(m_mp) CColRefArray(m_mp);
    	CColRef2dArray *pdrgdrgpcrInput = GPOS_NEW(m_mp) CColRef2dArray(m_mp);
        CDouble total_cost = 0;
        for (ULONG ul = 0; ul < num_query_graphs; ul++) {
            UpdateEdgeSelectivity(ulTarget, m_pdrgdSelectivity, ul,
                                  splitted_components);

            // // Run GOO on current split
            CExpression *pexprGOO = RunGOO(m_ulComps, m_rgpcomp, m_ulEdges,
                                           m_rgpedge, m_pdrgdSelectivity,
										   splitted_components, ulTarget,
										   ul);
			// pexprGOO (bushy RunGOO tree) is discarded: the actual branch plan is
			// the left-deep tree below, and the split trial must be SELECTED on the
			// cost of the plan we will actually run — not the bushy tree, whose
			// order can differ (RunGOO may pick a good bushy order while the
			// left-deep greedy picks another, so costing the bushy tree selects a
			// trial whose real left-deep plan explodes). Cost the left-deep result.
			pexprGOO->Release();

			// Per-branch edge stats: swap each edge component's Get for a virtual
			// edge relation whose row estimate is THIS branch's forward edge count
			// (GemBranchEdgeFanout). Built into the branch's left-deep tree, these
			// distinct edge mdids let ORCA's memo see divergent per-branch edge
			// cardinalities, so it keeps each branch's divergent order and prefers
			// the split over a single washed tree. Gated behind TLX_DIVERGE_ORDER
			// (the same opt-in as the divergent greedy order). The swapped
			// SComponents/expressions are pool-reclaimed; only the pointer array is
			// explicitly freed.
			SComponent **branch_rgpcomp = m_rgpcomp;
			SComponent **branch_owned = NULL;
			if (NULL != getenv("TLX_DIVERGE_ORDER")) {
				branch_owned =
					GPOS_NEW_ARRAY(m_mp, SComponent *, m_ulComps);
				for (ULONG c = 0; c < m_ulComps; c++) {
					branch_owned[c] = m_rgpcomp[c];
					if (c == ulTarget)
						continue;  // anchor comes from splitted_components
					CDouble dF = GemBranchEdgeFanout(
						splitted_components[ul]->m_pexpr,
						m_rgpcomp[c]->m_pexpr);
					CExpression *pexprSwap = PexprSwapEdgeToVirtual(
						m_rgpcomp[c]->m_pexpr, dF);
					if (NULL != pexprSwap) {
						m_rgpcomp[c]->m_pbs->AddRef();
						m_rgpcomp[c]->m_edge_set->AddRef();
						branch_owned[c] = GPOS_NEW(m_mp) SComponent(
							pexprSwap, m_rgpcomp[c]->m_pbs,
							m_rgpcomp[c]->m_edge_set);
					}
				}
				branch_rgpcomp = branch_owned;
			}

			double branch_peak = 0.0;
			CExpression *pexprLeftDeepResult = PexprBuildLeftDeepTree(
				m_ulComps, branch_rgpcomp, splitted_components, ulTarget, ul,
				&branch_peak);
			if (NULL != branch_owned) GPOS_DELETE_ARRAY(branch_owned);
			DeriveStats(pexprLeftDeepResult);
			// Select the split trial on the greedy's per-branch PEAK-intermediate
			// estimate (selectivity-aware, not washed by the empty-histogram join
			// fallback the way DCost is) when the divergent greedy ran; otherwise
			// fall back to DCost. This makes the trial whose per-branch orders keep
			// the peak small (the divergent one) win.
			CDouble dCostGOO = (branch_peak > 0.0)
				? CDouble(branch_peak)
				: (DCost(pexprLeftDeepResult) * 0.5);
			total_cost = total_cost + dCostGOO;
			pexprArray->Append(pexprLeftDeepResult);
            // [DEMO PROBE — uncommitted] per-branch estimated rows: is VT2 empty?
            if (NULL != getenv("TLX_GEM_TRACE")) {
                DeriveStats(pexprLeftDeepResult);
                // Dump the branch's join order: the edge types (eps_ Gets) in
                // left-deep traversal order = the order they are joined.
                std::vector<CLogicalGet *> order_gets;
                CollectLogicalGets(pexprLeftDeepResult, order_gets);
                std::string order;
                for (CLogicalGet *pg : order_gets) {
                    OID o = CMDIdGPDB::CastMdid(pg->Ptabdesc()->MDId())->Oid();
                    duckdb::PropertySchemaCatalogEntry *r = duckdb::GetRelation(o);
                    if (r && r->name.compare(0, 4, "eps_") == 0) {
                        if (!order.empty()) order += " -> ";
                        order += r->name.substr(4);
                    }
                }
                std::fprintf(stderr, "[GEM] trial=%u branch=%u rows=%.1f cost=%.3f order=[%s]\n",
                             ulTrySplit, ul,
                             pexprLeftDeepResult->Pstats()->Rows().Get(),
                             dCostGOO.Get(), order.c_str());
            }
            pdrgdrgpcrInput->Append(
                pexprLeftDeepResult->DeriveOutputColumns()->Pdrgpcr(m_mp));
        }

        if (total_cost < dBestCost) {
			if (NULL != pexprResult) pexprResult->Release();
			dBestCost = total_cost;
			pdrgpcrOutput->AppendArray(
                (*pexprArray)[0]->DeriveOutputColumns()->Pdrgpcr(m_mp));
			pdrgpcrOutput->AddRef();
			pdrgdrgpcrInput->AddRef();
            pexprResult = GPOS_NEW(m_mp) CExpression(
                m_mp,
                GPOS_NEW(m_mp)
                    CLogicalUnionAll(m_mp, pdrgpcrOutput, pdrgdrgpcrInput),
                pexprArray);
        }
			
		// CDouble dSplitTime(timerSplit.ElapsedUS() / CDouble(GPOS_USEC_IN_MSEC));
		// dTotalTime = CDouble(dTotalTime.Get() + dSplitTime.Get());
		
		// if (dTotalTime.Get() > SPLIT_TIME_LIMIT) {
		// 	break;
		// }
    }
	GPOS_DELETE_ARRAY(splitted_components);

    return pexprResult;
}

CExpression *CJoinOrderGEM::FindLogicalGetExpr(CExpression *pexpr) {
	// Base case: if expression is NULL, return NULL
	if (NULL == pexpr)
	{
		return NULL;
	}

	// Check if current node is LogicalGet
	if (COperator::EopLogicalGet == pexpr->Pop()->Eopid())
	{
		return pexpr;
	}

	// Recursively check children
	const ULONG ulArity = pexpr->Arity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexprChild = FindLogicalGetExpr((*pexpr)[ul]);
		if (NULL != pexprChild)
		{
			return pexprChild;
		}
	}

	// No LogicalGet found in this subtree
	return NULL;
}

CExpression *CJoinOrderGEM::PushJoinBelowUnionAll(CExpression *pexpr) const
{
	CMemoryPool *mp = this->m_mp;

	// extract components
	CExpression *pexprLeft = (*pexpr)[0];
	CExpression *pexprRight = (*pexpr)[1];
	CExpression *pexprScalar = (*pexpr)[2];

	CExpression *pexprUnionAll, *pexprOther;

	// This is used to preserve the join order, which we leave
	// for other xforms to optimize
	BOOL isLeftChildUnion;

	if (COperator::EopLogicalUnionAll == pexprLeft->Pop()->Eopid())
	{
		pexprUnionAll = pexprLeft;
		pexprOther = pexprRight;
		isLeftChildUnion = true;
	}
	else
	{
		pexprUnionAll = pexprRight;
		pexprOther = pexprLeft;
		isLeftChildUnion = false;
	}

	CLogicalUnionAll *popUnionAll =
		CLogicalUnionAll::PopConvert(pexprUnionAll->Pop());
	CColRef2dArray *union_input_columns = popUnionAll->PdrgpdrgpcrInput();

	if (!popUnionAll->CanPushJoinBelowUnionAll()) 
	{
		return pexpr;
	}

	// used for alternative union all expression
	CColRef2dArray *input_columns = GPOS_NEW(mp) CColRef2dArray(mp);
	CExpressionArray *join_array = GPOS_NEW(mp) CExpressionArray(mp);

	CColRefArray *other_colref_array =
		pexprOther->DeriveOutputColumns()->Pdrgpcr(mp);
	CColRefArray *colref_array_from = GPOS_NEW(mp) CColRefArray(mp);

	// Iterate through all union all children
	const ULONG arity = pexprUnionAll->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprChild = (*pexprUnionAll)[ul];
		CColRefArray *child_colref_array = (*union_input_columns)[ul];

		CExpression *pexprLeftChild, *pexprRightChild, *pexprRemappedScalar,
			*pexprRemappedOther, *join_expr;

        if (ul == 0)
        {
            // The 1st child is special
            // The join table and condition can be readily used
            // and doesn't require remapping
            pexprRemappedScalar = pexprScalar;
            pexprRemappedOther = pexprOther;
            pexprRemappedScalar->AddRef();
            pexprRemappedOther->AddRef();
            // We append the output columns from the 1st union all child,
            // and from the other table, and use them as the source
            // of column remapping
            colref_array_from->AppendArray(child_colref_array);
            colref_array_from->AppendArray(other_colref_array);
            input_columns->Append(colref_array_from);
        }
        else
        {
            CColRefArray *colref_array_to = GPOS_NEW(mp) CColRefArray(mp);
            // We append the output columns from the 2nd (and onward)
            // union all child, and a copy of the other table's output
            // columns, and use them as the destination of column
            // remapping
            colref_array_to->AppendArray(child_colref_array);
            colref_array_to->AppendArray(other_colref_array);
            input_columns->Append(colref_array_to);
            UlongToColRefMap *colref_mapping =
                CUtils::PhmulcrMapping(mp, colref_array_from, colref_array_to);
            // Create a copy of the join condition with remapped columns,
            // and a copy of the other expression with remapped columns
            pexprRemappedScalar = pexprScalar->PexprCopyWithRemappedColumns(
                mp, colref_mapping, true /*must_exist*/);
            pexprRemappedOther = pexprOther;
            pexprRemappedOther->AddRef();
            colref_mapping->Release();
        }

		// Preserve the join order
		if (isLeftChildUnion)
		{
			pexprLeftChild = pexprChild;
			pexprRightChild = pexprRemappedOther;
			pexprLeftChild->AddRef();
		}
		else
		{
			pexprLeftChild = pexprRemappedOther;
			pexprRightChild = pexprChild;
			pexprRightChild->AddRef();
		}


		BOOL isOuterJoin =
			pexpr->Pop()->Eopid() == COperator::EopLogicalLeftOuterJoin;
		if (isOuterJoin)
		{
			join_expr = CUtils::PexprLogicalJoin<CLogicalLeftOuterJoin>(
				mp, pexprLeftChild, pexprRightChild, pexprRemappedScalar);
		}
		else
		{
			join_expr = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(
				mp, pexprLeftChild, pexprRightChild, pexprRemappedScalar);
		}
		join_array->Append(join_expr);
	}
	other_colref_array->Release();

	CColRefArray *output_columns = pexpr->DeriveOutputColumns()->Pdrgpcr(mp);
	CExpression *pexprAlt = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CLogicalUnionAll(mp, output_columns, input_columns),
		join_array);

	return pexprAlt;
}

FORCE_GENERATE_DBGSTR(gpopt::CJoinOrderGEM);

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderGEM::OsPrint
//
//	@doc:
//		Print created join order
//
//---------------------------------------------------------------------------
IOstream &
CJoinOrderGEM::OsPrint(IOstream &os) const
{
	return os;
}
