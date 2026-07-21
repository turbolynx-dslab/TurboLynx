//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CLogicalUnary.cpp
//
//	@doc:
//		Implementation of logical unary operators
//---------------------------------------------------------------------------

#include "gpopt/operators/CLogicalUnary.h"

#include "gpos/base.h"

#include "gpopt/operators/CScalarIdent.h"
#include "gpopt/operators/CScalarProjectElement.h"
#include "gpopt/xforms/CXformUtils.h"
#include "naucrates/statistics/CProjectStatsProcessor.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnary::Matches
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CLogicalUnary::Matches(COperator *pop) const
{
	return (pop->Eopid() == Eopid());
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnary::Esp
//
//	@doc:
//		Promise level for stat derivation
//
//---------------------------------------------------------------------------
CLogical::EStatPromise
CLogicalUnary::Esp(CExpressionHandle &exprhdl) const
{
	// low promise for stat derivation if scalar predicate has subqueries, or logical
	// expression has outer-refs or is part of an Apply expression
	if (exprhdl.DeriveHasSubquery(1) || exprhdl.HasOuterRefs() ||
		(NULL != exprhdl.Pgexpr() &&
		 CXformUtils::FGenerateApply(exprhdl.Pgexpr()->ExfidOrigin())))
	{
		return EspLow;
	}

	return EspHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnary::PstatsDeriveProject
//
//	@doc:
//		Derive statistics for projection operators
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalUnary::PstatsDeriveProject(CMemoryPool *mp, CExpressionHandle &exprhdl,
								   UlongToIDatumMap *phmuldatum) const
{
	GPOS_ASSERT(Esp(exprhdl) > EspNone);
	IStatistics *child_stats = exprhdl.Pstats(0);
	CReqdPropRelational *prprel =
		CReqdPropRelational::GetReqdRelationalProps(exprhdl.Prp());
	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp, *prprel->PcrsStat());
	pcrs->Include(exprhdl.DeriveOutputColumns());
	ULongPtrArray *colids = GPOS_NEW(mp) ULongPtrArray(mp);
	pcrs->ExtractColIds(mp, colids);

	// map identity project elements (colid -> source colid) so the source
	// column's histogram survives projections that drop pass-through columns
	UlongToUlongMap *ident_source_map = GPOS_NEW(mp) UlongToUlongMap(mp);
	CExpression *pexprPrList = exprhdl.PexprScalarRepChild(1 /*child_index*/);
	if (NULL != pexprPrList)
	{
		const ULONG arity = pexprPrList->Arity();
		for (ULONG ul = 0; ul < arity; ul++)
		{
			CExpression *pexprPrElem = (*pexprPrList)[ul];
			if (1 != pexprPrElem->Arity())
			{
				continue;
			}
			CExpression *pexprScalar = (*pexprPrElem)[0];
			if (COperator::EopScalarIdent == pexprScalar->Pop()->Eopid())
			{
				CColRef *colref = CScalarProjectElement::PopConvert(
									  pexprPrElem->Pop())
									  ->Pcr();
				const CColRef *src_colref =
					CScalarIdent::PopConvert(pexprScalar->Pop())->Pcr();
				ident_source_map->Insert(GPOS_NEW(mp) ULONG(colref->Id()),
										 GPOS_NEW(mp) ULONG(src_colref->Id()));
			}
		}
	}

	IStatistics *stats = CProjectStatsProcessor::CalcProjStats(
		mp, dynamic_cast<CStatistics *>(child_stats), colids, phmuldatum,
		ident_source_map);

	// clean up
	ident_source_map->Release();
	pcrs->Release();
	colids->Release();

	return stats;
}

// EOF
