// CPhysicalUnnest — physical unnest (UNWIND) operator.
// Based on CPhysicalComputeScalarColumnar with simplified property handling.
// Key difference: UNWIND destroys ordering (1→N expansion).

#include "gpopt/operators/CPhysicalUnnest.h"

#include "gpos/base.h"
#include "gpopt/base/CDistributionSpecAny.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CExpressionHandle.h"

using namespace gpopt;

CPhysicalUnnest::CPhysicalUnnest(CMemoryPool *mp)
    : CPhysical(mp), m_pcrOutput(NULL)
{
	SetDistrRequests(1);
}

CPhysicalUnnest::CPhysicalUnnest(CMemoryPool *mp, CColRef *pcrOutput)
    : CPhysical(mp), m_pcrOutput(pcrOutput)
{
	SetDistrRequests(1);
}

CPhysicalUnnest::~CPhysicalUnnest() {}

BOOL
CPhysicalUnnest::Matches(COperator *pop) const
{
	if (Eopid() != pop->Eopid()) return false;
	CPhysicalUnnest *popUnnest = PopConvert(pop);
	if (!m_pcrOutput && !popUnnest->m_pcrOutput) return true;
	if (!m_pcrOutput || !popUnnest->m_pcrOutput) return false;
	return m_pcrOutput->Id() == popUnnest->m_pcrOutput->Id();
}

// ---- Required Plan Properties ----

CColRefSet *
CPhysicalUnnest::PcrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
    CColRefSet *pcrsRequired, ULONG child_index, CDrvdPropArray *, ULONG)
{
	GPOS_ASSERT(0 == child_index);
	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp, *pcrsRequired);
	// Remove the output column (produced by us, not required from child)
	if (m_pcrOutput) pcrs->Exclude(m_pcrOutput);
	CColRefSet *pcrsChildReqd =
	    PcrsChildReqd(mp, exprhdl, pcrs, child_index, 1 /*ulScalarIndex*/);
	pcrs->Release();
	return pcrsChildReqd;
}

COrderSpec *
CPhysicalUnnest::PosRequired(CMemoryPool *mp, CExpressionHandle &,
    COrderSpec *, ULONG, CDrvdPropArray *, ULONG) const
{
	// UNWIND destroys ordering — don't pass through any order requirement
	return GPOS_NEW(mp) COrderSpec(mp);
}

CDistributionSpec *
CPhysicalUnnest::PdsRequired(CMemoryPool *mp, CExpressionHandle &,
    CDistributionSpec *, ULONG, CDrvdPropArray *, ULONG) const
{
	return GPOS_NEW(mp) CDistributionSpecAny(this->Eopid());
}

CRewindabilitySpec *
CPhysicalUnnest::PrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
    CRewindabilitySpec *prsRequired, ULONG child_index,
    CDrvdPropArray *, ULONG) const
{
	GPOS_ASSERT(0 == child_index);
	return PrsPassThru(mp, exprhdl, prsRequired, child_index);
}

CPartitionPropagationSpec *
CPhysicalUnnest::PppsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
    CPartitionPropagationSpec *pppsRequired, ULONG child_index,
    CDrvdPropArray *, ULONG)
{
	GPOS_ASSERT(0 == child_index);
	return CPhysical::PppsRequiredPushThru(mp, exprhdl, pppsRequired, child_index);
}

CCTEReq *
CPhysicalUnnest::PcteRequired(CMemoryPool *, CExpressionHandle &,
    CCTEReq *pcter, ULONG, CDrvdPropArray *, ULONG) const
{
	return PcterPushThru(pcter);
}

BOOL
CPhysicalUnnest::FProvidesReqdCols(CExpressionHandle &exprhdl,
    CColRefSet *pcrsRequired, ULONG) const
{
	GPOS_ASSERT(NULL != pcrsRequired);
	CColRefSet *pcrs = GPOS_NEW(m_mp) CColRefSet(m_mp);
	// Child output columns + defined columns from scalar child
	pcrs->Union(exprhdl.DeriveOutputColumns(0));
	pcrs->Union(exprhdl.DeriveDefinedColumns(1));
	// Plus our output column
	if (m_pcrOutput) pcrs->Include(m_pcrOutput);
	BOOL fProvides = pcrs->ContainsAll(pcrsRequired);
	pcrs->Release();
	return fProvides;
}

// ---- Derived Plan Properties ----

COrderSpec *
CPhysicalUnnest::PosDerive(CMemoryPool *mp, CExpressionHandle &) const
{
	// UNWIND destroys order
	return GPOS_NEW(mp) COrderSpec(mp);
}

CDistributionSpec *
CPhysicalUnnest::PdsDerive(CMemoryPool *, CExpressionHandle &exprhdl) const
{
	CDistributionSpec *pds = exprhdl.Pdpplan(0)->Pds();
	pds->AddRef();
	return pds;
}

CRewindabilitySpec *
CPhysicalUnnest::PrsDerive(CMemoryPool *mp, CExpressionHandle &) const
{
	// UNWIND is not rewindable
	return GPOS_NEW(mp) CRewindabilitySpec(
	    CRewindabilitySpec::ErtRescannable, CRewindabilitySpec::EmhtNoMotion);
}

// ---- Enforced Properties ----

CEnfdProp::EPropEnforcingType
CPhysicalUnnest::EpetOrder(CExpressionHandle &, const CEnfdOrder *) const
{
	// Sort must go on top of UNWIND
	return CEnfdProp::EpetRequired;
}

CEnfdProp::EPropEnforcingType
CPhysicalUnnest::EpetRewindability(CExpressionHandle &,
    const CEnfdRewindability *) const
{
	return CEnfdProp::EpetRequired;
}
