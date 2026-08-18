//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformExpandNAryJoinDP.cpp
//
//	@doc:
//		Implementation of n-ary join expansion using dynamic programming
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformExpandNAryJoinGEM.h"

#include <cstdlib>  // [DEMO PROBE — uncommitted] getenv
#include <cstdio>   // [DEMO PROBE — uncommitted] fprintf
#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/engine/CHint.h"
#include "gpopt/operators/CLogicalNAryJoin.h"
#include "gpopt/operators/CNormalizer.h"
#include "gpopt/operators/CPatternMultiLeaf.h"
#include "gpopt/operators/CPatternMultiTree.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpopt/xforms/CJoinOrderGEM.h"
#include "gpopt/xforms/CXformUtils.h"



using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformExpandNAryJoinDP::CXformExpandNAryJoinDP
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformExpandNAryJoinGEM::CXformExpandNAryJoinGEM(
    CMemoryPool *mp)
    : CXformExploration(
          // pattern
          GPOS_NEW(mp) CExpression(
              mp, GPOS_NEW(mp) CLogicalNAryJoin(mp),
              GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternMultiTree(mp)),
              GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternTree(mp))))
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformExpandNAryJoinDP::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformExpandNAryJoinGEM::Exfp(CExpressionHandle &exprhdl) const
{
	// COptimizerConfig *optimizer_config =
	// 	COptCtxt::PoctxtFromTLS()->GetOptimizerConfig();
	// const CHint *phint = optimizer_config->GetHint();

	// const ULONG arity = exprhdl.Arity();

	// since the last child of the join operator is a scalar child
	// defining the join predicate, ignore it.
	// const ULONG ulRelChild = arity - 1;

	// if (ulRelChild > phint->UlJoinOrderDPLimit())
	// {
	// 	return CXform::ExfpNone;
	// }

	return CXformUtils::ExfpExpandJoinOrder(exprhdl, this);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformExpandNAryJoinDP::Transform
//
//	@doc:
//		Actual transformation of n-ary join to cluster of inner joins using
//		dynamic programming
//
//---------------------------------------------------------------------------
void
CXformExpandNAryJoinGEM::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
								  CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(NULL != pxfres);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	const ULONG arity = pexpr->Arity();
	GPOS_ASSERT(arity >= 3);

	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	for (ULONG ul = 0; ul < arity - 1; ul++)
	{
		CExpression *pexprChild = (*pexpr)[ul];
		pexprChild->AddRef();
		pdrgpexpr->Append(pexprChild);
	}

	CExpression *pexprScalar = (*pexpr)[arity - 1];
	CExpressionArray *pdrgpexprPreds =
		CPredicateUtils::PdrgpexprConjuncts(mp, pexprScalar);

	// create join order using dynamic programming
	CJoinOrderGEM jodp(mp, pdrgpexpr, pdrgpexprPreds);
	CExpression *pexprResult = jodp.PexprExpand();

	if (NULL != pexprResult)
	{
		// normalize resulting expression
		CExpression *pexprNormalized =
			CNormalizer::PexprNormalize(mp, pexprResult);
		pexprResult->Release();
		pxfres->Add(pexprNormalized);

		// [DEMO PROBE — uncommitted] Log whether the normalized GEM result is
		// still a UnionAll after normalization (else the normalizer dissolved it).
		if (NULL != getenv("TLX_GEM_TRACE"))
		{
			std::fprintf(stderr, "[GEM] normalized root Eopid=%d (UnionAll=%d)\n",
						 (int) pexprNormalized->Pop()->Eopid(),
						 (int) (COperator::EopLogicalUnionAll ==
								pexprNormalized->Pop()->Eopid()));
			// Compare output columns of the GEM UnionAll vs the original NAry
			// join group: if they differ, the UnionAll is NOT a valid equivalent
			// alternative for the group and can never be selected at the root.
			CColRefSet *pcrsJoin = pexpr->DeriveOutputColumns();
			CColRefSet *pcrsGem = pexprNormalized->DeriveOutputColumns();
			std::fprintf(stderr,
						 "[GEM] out-cols: join.size=%u gem.size=%u equal=%d "
						 "gem_subset_of_join=%d join_subset_of_gem=%d\n",
						 pcrsJoin->Size(), pcrsGem->Size(),
						 (int) pcrsJoin->Equals(pcrsGem),
						 (int) pcrsJoin->ContainsAll(pcrsGem),
						 (int) pcrsGem->ContainsAll(pcrsJoin));
		}

		// [DEMO PROBE — uncommitted] TLX_GEM_ONLY_UNION=1 suppresses adding the
		// non-union TopK GOO join orders, leaving ONLY the UnionAll candidate in
		// the plan space. Lets us tell cost-based pruning (UnionAll then survives
		// to the physical plan) from normalizer/implementation dissolution (it
		// still vanishes even with no competitor).
		if (NULL == getenv("TLX_GEM_ONLY_UNION"))
		{
			const ULONG UlTopKJoinOrders = jodp.PdrgpexprTopK()->Size();
			for (ULONG ul = 0; ul < UlTopKJoinOrders; ul++)
			{
				CExpression *pexprJoinOrder = (*jodp.PdrgpexprTopK())[ul];
				if (pexprJoinOrder != pexprResult)
				{
					// We should consider normalizing this expression before inserting it, as we do for pexprResult
					pexprJoinOrder->AddRef();
					pxfres->Add(pexprJoinOrder);
				}
			}
		}
	}
}

// EOF
