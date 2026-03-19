#include "gpopt/operators/CLogicalUnnest.h"

#include "gpos/base.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CKeyCollection.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/xforms/CXformUtils.h"

using namespace gpopt;

CLogicalUnnest::CLogicalUnnest(CMemoryPool *mp)
    : CLogicalUnary(mp), m_pcrOutput(NULL)
{
}

CLogicalUnnest::CLogicalUnnest(CMemoryPool *mp, CColRef *pcrOutput)
    : CLogicalUnary(mp), m_pcrOutput(pcrOutput)
{
	GPOS_ASSERT(NULL != pcrOutput);
}

CColRefSet *
CLogicalUnnest::DeriveOutputColumns(CMemoryPool *mp, CExpressionHandle &exprhdl)
{
	// Output = child columns + the new unnest column
	CColRefSet *pcrs = exprhdl.DeriveOutputColumns(0);
	pcrs->AddRef();
	CColRefSet *pcrsOutput = GPOS_NEW(mp) CColRefSet(mp, *pcrs);
	pcrs->Release();
	if (m_pcrOutput)
	{
		pcrsOutput->Include(m_pcrOutput);
	}
	return pcrsOutput;
}

CKeyCollection *
CLogicalUnnest::DeriveKeyCollection(CMemoryPool *, CExpressionHandle &) const
{
	// UNWIND destroys keys (1→N expansion)
	return NULL;
}

CMaxCard
CLogicalUnnest::DeriveMaxCard(CMemoryPool *, CExpressionHandle &) const
{
	// Unbounded: UNWIND can multiply rows
	return CMaxCard();
}

CPropConstraint *
CLogicalUnnest::DerivePropertyConstraint(CMemoryPool *mp,
                                          CExpressionHandle &exprhdl) const
{
	return PpcDeriveConstraintPassThru(exprhdl, 0 /* ulChild */);
}

CXformSet *
CLogicalUnnest::PxfsCandidates(CMemoryPool *mp) const
{
	CXformSet *xform_set = GPOS_NEW(mp) CXformSet(mp);
	(void) xform_set->ExchangeSet(CXform::ExfImplementUnnest);
	return xform_set;
}

IStatistics *
CLogicalUnnest::PstatsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl,
                              IStatisticsArray *) const
{
	// Conservative: pass through child stats (multiply factor unknown)
	return PstatsDeriveProject(mp, exprhdl);
}

BOOL
CLogicalUnnest::Matches(COperator *pop) const
{
	if (Eopid() != pop->Eopid())
		return false;
	CLogicalUnnest *popUnnest = PopConvert(pop);
	if (m_pcrOutput == NULL && popUnnest->m_pcrOutput == NULL)
		return true;
	if (m_pcrOutput == NULL || popUnnest->m_pcrOutput == NULL)
		return false;
	return m_pcrOutput->Id() == popUnnest->m_pcrOutput->Id();
}

ULONG
CLogicalUnnest::HashValue() const
{
	ULONG id = m_pcrOutput ? m_pcrOutput->Id() : 0;
	return gpos::CombineHashes(COperator::HashValue(), gpos::HashValue(&id));
}

COperator *
CLogicalUnnest::PopCopyWithRemappedColumns(CMemoryPool *mp,
    UlongToColRefMap *colref_mapping, BOOL must_exist)
{
	CColRef *pcrNew = m_pcrOutput;
	if (m_pcrOutput)
	{
		ULONG id = m_pcrOutput->Id();
		CColRef *pcrMapped = colref_mapping->Find(&id);
		if (pcrMapped)
			pcrNew = pcrMapped;
		else if (must_exist)
			GPOS_ASSERT(!"CLogicalUnnest: column not found in remapping");
	}
	return GPOS_NEW(mp) CLogicalUnnest(mp, pcrNew);
}
