//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2026 TurboLynx
//
//	@filename:
//		CLogicalVarlenPath.cpp
//
//	@doc:
//		Implementation of logical VarlenPath operator.
//---------------------------------------------------------------------------

#include "gpopt/operators/CLogicalVarlenPath.h"

#include "gpos/base.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/xforms/CXformUtils.h"
#include "naucrates/statistics/CStatistics.h"

using namespace gpopt;

CLogicalVarlenPath::CLogicalVarlenPath(CMemoryPool *mp)
	: CLogicalUnary(mp),
	  m_pnameAlias(NULL),
	  m_path_col_ref(NULL)
{
}

CLogicalVarlenPath::CLogicalVarlenPath(CMemoryPool *mp, const CName *pnameAlias,
									   CColRef *path_col_ref)
	: CLogicalUnary(mp),
	  m_pnameAlias(pnameAlias),
	  m_path_col_ref(path_col_ref)
{
	GPOS_ASSERT(NULL != path_col_ref);
}

CLogicalVarlenPath::~CLogicalVarlenPath()
{
}

CColRefSet *
CLogicalVarlenPath::DeriveOutputColumns(CMemoryPool *mp, CExpressionHandle &exprhdl)
{
	GPOS_ASSERT(2 == exprhdl.Arity());

	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);

	pcrs->Include(exprhdl.DeriveOutputColumns(0));
	pcrs->Union(exprhdl.DeriveDefinedColumns(1));

	return pcrs;
}

CColRefSet *
CLogicalVarlenPath::DeriveOuterReferences(CMemoryPool *mp,
										  CExpressionHandle &exprhdl)
{
	return CLogical::DeriveOuterReferences(mp, exprhdl);
}

CColRefSet *
CLogicalVarlenPath::DeriveNotNullColumns(CMemoryPool *mp,
										 CExpressionHandle &exprhdl) const
{
	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
	pcrs->Include(exprhdl.DeriveNotNullColumns(0));
	return pcrs;
}

CPropConstraint *
CLogicalVarlenPath::DerivePropertyConstraint(CMemoryPool *mp,
											 CExpressionHandle &exprhdl) const
{
	return PpcDeriveConstraintPassThru(exprhdl, 0 /* ulChild */);
}

CColRefSet *
CLogicalVarlenPath::PcrsStat(CMemoryPool *mp, CExpressionHandle &exprhdl,
							 CColRefSet *pcrsInput, ULONG child_index) const
{
	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
	pcrs->Union(exprhdl.DeriveUsedColumns(1));

	CColRefSet *pcrsRequired =
		PcrsReqdChildStats(mp, exprhdl, pcrsInput, pcrs, child_index);
	pcrs->Release();

	return pcrsRequired;
}

IStatistics *
CLogicalVarlenPath::PstatsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl,
								 IStatisticsArray *	 // not used
) const
{
	return PstatsDeriveDummy(mp, exprhdl, CStatistics::DefaultRelationRows);
}

BOOL
CLogicalVarlenPath::Matches(COperator *pop) const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}

	CLogicalVarlenPath *popVP = reinterpret_cast<CLogicalVarlenPath *>(pop);
	return CColRef::Equals(m_path_col_ref, popVP->PcrPath());
}

CXformSet *
CLogicalVarlenPath::PxfsCandidates(CMemoryPool *mp) const
{
	CXformSet *xform_set = GPOS_NEW(mp) CXformSet(mp);
	(void) xform_set->ExchangeSet(CXform::ExfImplementVarlenPath);
	return xform_set;
}

IOstream &
CLogicalVarlenPath::OsPrint(IOstream &os) const
{
	os << SzId() << " (";
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
