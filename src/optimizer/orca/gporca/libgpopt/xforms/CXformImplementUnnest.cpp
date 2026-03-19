#include "gpopt/xforms/CXformImplementUnnest.h"

#include "gpos/base.h"
#include "gpopt/operators/CLogicalUnnest.h"
#include "gpopt/operators/CPhysicalUnnest.h"
#include "gpopt/operators/CPatternLeaf.h"

using namespace gpopt;

CXformImplementUnnest::CXformImplementUnnest(CMemoryPool *mp)
    : CXformImplementation(GPOS_NEW(mp) CExpression(
          mp, GPOS_NEW(mp) CLogicalUnnest(mp),
          GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),   // relational child
          GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))))  // scalar child
{
}

void
CXformImplementUnnest::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
                                  CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	// Extract the output column ref from the logical operator
	CLogicalUnnest *popLogical = CLogicalUnnest::PopConvert(pexpr->Pop());
	CColRef *pcrOutput = popLogical->PcrOutput();

	// Extract children
	CExpression *pexprRelational = (*pexpr)[0];
	CExpression *pexprScalar = (*pexpr)[1];
	pexprRelational->AddRef();
	pexprScalar->AddRef();

	// Create physical operator
	CExpression *pexprPhysical = GPOS_NEW(mp) CExpression(
	    mp, GPOS_NEW(mp) CPhysicalUnnest(mp, pcrOutput),
	    pexprRelational, pexprScalar);

	pxfres->Add(pexprPhysical);
}
