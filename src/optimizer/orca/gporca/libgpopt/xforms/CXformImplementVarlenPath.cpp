//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2026 TurboLynx
//
//	@filename:
//		CXformImplementVarlenPath.cpp
//
//	@doc:
//		Implementation of VarlenPath xform.
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformImplementVarlenPath.h"

#include "gpos/base.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalVarlenPath.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CPhysicalVarlenPath.h"

using namespace gpopt;


CXformImplementVarlenPath::CXformImplementVarlenPath(CMemoryPool *mp)
	:  // pattern: child[0] = relational subtree, child[1] = ScalarProjectList
	  CXformImplementation(
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalVarlenPath(mp),
			  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),
			  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternTree(mp))))
{
}

CXform::EXformPromise
CXformImplementVarlenPath::Exfp(CExpressionHandle &exprhdl) const
{
	(void) exprhdl;
	return CXform::ExfpHigh;
}

void
CXformImplementVarlenPath::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
									 CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	CLogicalVarlenPath *popVP = CLogicalVarlenPath::PopConvert(pexpr->Pop());
	CExpression *pexprRelational = (*pexpr)[0];
	CExpression *pexprScalar = (*pexpr)[1];

	pexprRelational->AddRef();
	pexprScalar->AddRef();

	CExpression *pexprPhysical = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CPhysicalVarlenPath(mp, popVP->PnameAlias(),
											 popVP->PcrPath()),
		pexprRelational, pexprScalar);

	pxfres->Add(pexprPhysical);
}


// EOF
