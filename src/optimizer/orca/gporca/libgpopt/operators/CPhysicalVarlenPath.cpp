//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2026 TurboLynx
//
//	@filename:
//		CPhysicalVarlenPath.cpp
//
//	@doc:
//		Implementation of physical VarlenPath operator.
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalVarlenPath.h"

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/base/CDistributionSpecAny.h"


using namespace gpopt;


CPhysicalVarlenPath::CPhysicalVarlenPath(CMemoryPool *mp)
	: CPhysical(mp),
	  m_pnameAlias(NULL),
	  m_path_col_ref(NULL)
{
	GPOS_ASSERT(NULL != mp);
}

CPhysicalVarlenPath::CPhysicalVarlenPath(CMemoryPool *mp, const CName *pnameAlias,
										 CColRef *path_col_ref)
	: CPhysical(mp),
	  m_pnameAlias(pnameAlias),
	  m_path_col_ref(path_col_ref)
{
}

CPhysicalVarlenPath::~CPhysicalVarlenPath()
{
}

BOOL
CPhysicalVarlenPath::Matches(COperator *pop) const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}
	return true;
}

CColRefSet *
CPhysicalVarlenPath::PcrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
								  CColRefSet *pcrsRequired, ULONG child_index,
								  CDrvdPropArray *,	 // pdrgpdpCtxt
								  ULONG				 // ulOptReq
)
{
	GPOS_ASSERT(0 == child_index);

	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
	pcrs->Union(pcrsRequired);

	CColRefSet *prcsOutput = PcrsChildReqd(mp, exprhdl, pcrsRequired, child_index, 1);
	pcrs->Release();

	return prcsOutput;
}

COrderSpec *
CPhysicalVarlenPath::PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
								 COrderSpec *posRequired, ULONG child_index,
								 CDrvdPropArray *,	// pdrgpdpCtxt
								 ULONG				// ulOptReq
) const
{
	GPOS_ASSERT(0 == child_index);

	return PosPassThru(mp, exprhdl, posRequired, child_index);
}

CDistributionSpec *
CPhysicalVarlenPath::PdsRequired(CMemoryPool *mp, CExpressionHandle &,
								 CDistributionSpec *, ULONG, CDrvdPropArray *,
								 ULONG) const
{
	return GPOS_NEW(mp) CDistributionSpecAny(this->Eopid());
}

CRewindabilitySpec *
CPhysicalVarlenPath::PrsRequired(CMemoryPool *mp,
								 CExpressionHandle &,	// exprhdl
								 CRewindabilitySpec *,	// prsRequired
								 ULONG,					// child_index
								 CDrvdPropArray *,		// pdrgpdpCtxt
								 ULONG					// ulOptReq
) const
{
	GPOS_ASSERT(NULL != mp);

	return GPOS_NEW(mp) CRewindabilitySpec(CRewindabilitySpec::ErtNone,
										   CRewindabilitySpec::EmhtNoMotion);
}

CPartitionPropagationSpec *
CPhysicalVarlenPath::PppsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
								  CPartitionPropagationSpec *pppsRequired_unused,
								  ULONG
#ifdef GPOS_DEBUG
									  child_index
#endif
								  ,
								  CDrvdPropArray *,	 // pdrgpdpCtxt
								  ULONG				 // ulOptReq
)
{
	GPOS_ASSERT(0 == child_index);
	GPOS_ASSERT(NULL != pppsRequired_unused);

	return CPhysical::PppsRequiredPushThruUnresolvedUnary(
		mp, exprhdl, pppsRequired_unused, CPhysical::EppcProhibited, NULL);
}

CCTEReq *
CPhysicalVarlenPath::PcteRequired(CMemoryPool *mp_unused,
								  CExpressionHandle &,	// exprhdl
								  CCTEReq *pcter,
								  ULONG
#ifdef GPOS_DEBUG
									  child_index
#endif
								  ,
								  CDrvdPropArray *,	 // pdrgpdpCtxt
								  ULONG				 // ulOptReq
) const
{
	(void)mp_unused;
	GPOS_ASSERT(0 == child_index);
	return PcterPushThru(pcter);
}

BOOL
CPhysicalVarlenPath::FProvidesReqdCols(CExpressionHandle &exprhdl,
									   CColRefSet *pcrsRequired,
									   ULONG  // ulOptReq
) const
{
	CColRefSet *pcrs = GPOS_NEW(m_mp) CColRefSet(m_mp);
	pcrs->Union(exprhdl.DeriveOutputColumns(0));
	pcrs->Union(exprhdl.DeriveDefinedColumns(1));

	BOOL fProvidesCols = pcrs->ContainsAll(pcrsRequired);
	pcrs->Release();

	return fProvidesCols;
}

COrderSpec *
CPhysicalVarlenPath::PosDerive(CMemoryPool *,  // mp
							   CExpressionHandle &exprhdl) const
{
	return PosDerivePassThruOuter(exprhdl);
}

CDistributionSpec *
CPhysicalVarlenPath::PdsDerive(CMemoryPool *,  // mp
							   CExpressionHandle &exprhdl) const
{
	return PdsDerivePassThruOuter(exprhdl);
}

CRewindabilitySpec *
CPhysicalVarlenPath::PrsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const
{
	return PrsDerivePassThruOuter(mp, exprhdl);
}

CPartIndexMap *
CPhysicalVarlenPath::PpimDerive(CMemoryPool *, CExpressionHandle &exprhdl,
								CDrvdPropCtxt *) const
{
	return PpimPassThruOuter(exprhdl);
}

CPartFilterMap *
CPhysicalVarlenPath::PpfmDerive(CMemoryPool *, CExpressionHandle &exprhdl) const
{
	return PpfmPassThruOuter(exprhdl);
}

CEnfdProp::EPropEnforcingType
CPhysicalVarlenPath::EpetOrder(CExpressionHandle &,	 // exprhdl
							   const CEnfdOrder *peo) const
{
	GPOS_ASSERT(NULL != peo);
	GPOS_ASSERT(!peo->PosRequired()->IsEmpty());

	return CEnfdProp::EpetRequired;
}

CEnfdProp::EPropEnforcingType
CPhysicalVarlenPath::EpetRewindability(CExpressionHandle &,		  // exprhdl
									   const CEnfdRewindability * // per
) const
{
	return CEnfdProp::EpetOptional;
}

IOstream &
CPhysicalVarlenPath::OsPrint(IOstream &os) const
{
	os << SzId() << "( ";
	if (NULL != m_path_col_ref)
	{
		os << " Path Col: [";
		m_path_col_ref->OsPrint(os);
		os << "]";
	}
	os << " )";
	return os;
}



// EOF
