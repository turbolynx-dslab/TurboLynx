#ifndef GPOPT_CJoinOrderGEM_H
#define GPOPT_CJoinOrderGEM_H

#include <string>
#include <vector>

#include "gpos/base.h"
#include "gpos/common/CBitSet.h"
#include "gpos/common/CHashMap.h"
#include "gpos/common/DbgPrintMixin.h"
#include "gpos/io/IOstream.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/xforms/CJoinOrder.h"


namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CJoinOrderGEM
//
//	@doc:
//		Helper class for creating join orders using dynamic programming
//
//---------------------------------------------------------------------------
class CJoinOrderGEM : public CJoinOrder, public gpos::DbgPrintMixin<CJoinOrderGEM>
{
private:
	//---------------------------------------------------------------------------
	//	@struct:
	//		SComponentPair
	//
	//	@doc:
	//		Struct to capture a pair of components
	//
	//---------------------------------------------------------------------------
	struct SComponentPair : public CRefCount
	{
		// first component
		CBitSet *m_pbsFst;

		// second component
		CBitSet *m_pbsSnd;

		// ctor
		SComponentPair(CBitSet *pbsFst, CBitSet *pbsSnd);

		// dtor
		~SComponentPair();

		// hashing function
		static ULONG HashValue(const SComponentPair *pcomppair);

		// equality function
		static BOOL Equals(const SComponentPair *pcomppairFst,
						   const SComponentPair *pcomppairSnd);
	};

	// hashing function
	static ULONG
	UlHashBitSet(const CBitSet *pbs)
	{
		GPOS_ASSERT(NULL != pbs);

		return pbs->HashValue();
	}

	// equality function
	static BOOL
	FEqualBitSet(const CBitSet *pbsFst, const CBitSet *pbsSnd)
	{
		GPOS_ASSERT(NULL != pbsFst);
		GPOS_ASSERT(NULL != pbsSnd);

		return pbsFst->Equals(pbsSnd);
	}

	// hash map from component to best join order
	typedef CHashMap<CBitSet, CExpression, UlHashBitSet, FEqualBitSet,
					 CleanupRelease<CBitSet>, CleanupRelease<CExpression> >
		BitSetToExpressionMap;

	// hash map from component pair to connecting edges
	typedef CHashMap<SComponentPair, CExpression, SComponentPair::HashValue,
					 SComponentPair::Equals, CleanupRelease<SComponentPair>,
					 CleanupRelease<CExpression> >
		ComponentPairToExpressionMap;

	// hash map from expression to cost of best join order
	typedef CHashMap<CExpression, CDouble, CExpression::HashValue,
					 CUtils::Equals, CleanupRelease<CExpression>,
					 CleanupDelete<CDouble> >
		ExpressionToCostMap;

	// lookup table for links
	ComponentPairToExpressionMap *m_phmcomplink;

	// dynamic programming table
	BitSetToExpressionMap *m_phmbsexpr;

	// map of expressions to its cost
	ExpressionToCostMap *m_phmexprcost;

	// array of top-k join expression
	CExpressionArray *m_pdrgpexprTopKOrders;

	// dummy expression to used for non-joinable components
	CExpression *m_pexprDummy;

	// array to store selectivity values for edges
	CDoubleArray *m_pdrgdSelectivity;

	// variables to maintain split state
	ULONG m_ulSplitIndex;
	CBitSetArray *m_pdrgpbsSplits;

	// build expression linking given components
	CExpression *PexprBuildPred(CBitSet *pbsFst, CBitSet *pbsSnd);

	// extract predicate joining the two given sets
	CExpression *PexprPred(CBitSet *pbsFst, CBitSet *pbsSnd);

	// compute cost of given join expression
	CDouble DCost(CExpression *pexpr);

	// derive stats on given expression
	virtual void DeriveStats(CExpression *pexpr);

    void CalcEdgeSelectivity(CDoubleArray **pdrgdSelectivity);

    void UpdateEdgeSelectivity(ULONG ulTarget, CDoubleArray *pdrgdSelectivity,
                               ULONG ul, SComponent **splitted_components);

    // Branch-specific forward edge fan-out for a split virtual-table component
    // (per-graphlet edge cardinality). Returns a negative value when the edge
    // type or member graphlets cannot be resolved.
    CDouble GemBranchEdgeFanout(CExpression *pexprVT, CExpression *pexprCombined);

	CExpression *PexprBuildLeftDeepTree(ULONG ulComps, SComponent **rgpcomp,
							SComponent **splitted_components, ULONG ulTarget,ULONG ulSplit,
							double *out_peak_cost = nullptr);

    CExpression *RunGOO(ULONG ulComps, SComponent **rgpcomp, ULONG ulEdges,
                        SEdge **rgpedge, CDoubleArray *pdrgdSelectivity,
                        SComponent **splitted_components = NULL,
                        ULONG ulTarget = 0, ULONG ulSplit = 0);

    CExpression *BuildQueryGraphAndRunGOO(CExpression *pexpr, ULONG ul, CDouble dBestCost);

    ULONG Find(ULONG *parent, ULONG x);

    ULONG Union(ULONG *parent, ULONG *rank, ULONG x, ULONG y);

    CExpression *ProcessUnionAllComponents(CDouble &dCost);

    CTableDescriptor *CreateTableDescForVirtualTable(CTableDescriptor *ptabdesc,
                                                     IMdIdArray *pdrgmdid);

    void SplitUnionAll(CExpression *pexpr, ULONG ulTarget,
                       SComponent **splitted_components, BOOL is_first_time);

    CExpression *PexprCopyWithNewChildren(CMemoryPool *mp, CExpression *pexpr,
                                          CExpression *target_expr,
                                          CExpression *new_expr);

    // For a split branch, rebuild an edge component's expression with a per-branch
    // virtual edge Get whose row-count estimate is dFanout (the branch's forward
    // edge count). Returns NULL when the component is not an eps_ edge Get or the
    // fan-out is unresolved, so the caller keeps the shared component. Only the
    // stats differ; the physical scan/index are unchanged.
    CExpression *PexprSwapEdgeToVirtual(CExpression *pexprEdgeComp,
                                        CDouble dFanout);

    void SplitGraphlets(IMdIdArray *pimdidarray, ULONG ulTables,
                        IMdIdArray *pdrgmdidFirst, IMdIdArray *pdrgmdidSecond,
						ULONG ulSplitIndex,
						const std::vector<std::string> &query_etypes);

    // Collect the forward edge-type internal names of query edges incident to the
    // split-target component (used to make SplitGraphlets query-aware).
    std::vector<std::string> QueryEdgeTypesForTarget(ULONG ulTarget);

    CExpression *FindLogicalGetExpr(CExpression *pexpr);

	CExpression *PushJoinBelowUnionAll(CExpression *pexpr) const;

   public:
	// ctor
	CJoinOrderGEM(CMemoryPool *mp, CExpressionArray *pdrgpexprComponents,
				 CExpressionArray *pdrgpexprConjuncts);

	// dtor
	virtual ~CJoinOrderGEM();

	// main handler
	virtual CExpression *PexprExpand();

	// best join orders
	CExpressionArray *
	PdrgpexprTopK() const
	{
		return m_pdrgpexprTopKOrders;
	}

	// print function
	virtual IOstream &OsPrint(IOstream &) const;

	virtual CXform::EXformId
	EOriginXForm() const
	{
		return CXform::ExfExpandNAryJoinGEM;
	}


};	// class CJoinOrderGEM

}  // namespace gpopt

#endif	// !GPOPT_CJoinOrderGEM_H

// EOF
