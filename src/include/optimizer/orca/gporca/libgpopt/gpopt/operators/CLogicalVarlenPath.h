//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2026 TurboLynx
//
//	@filename:
//		CLogicalVarlenPath.h
//
//	@doc:
//		Logical wrap that requests path-column materialization on a plain
//		variable-length MATCH path = (a)-[:R*..]->(b) subtree.
//
//		Mirrors CLogicalShortestPath's two-child shape — child[0] is the
//		PathJoin subtree, child[1] is a CScalarProjectList that defines
//		the path colref — so the path column's origin is properly
//		established in ORCA's column lifecycle. The placeholder is later
//		filled at runtime by PhysicalVarlenAdjIdxJoin.
//---------------------------------------------------------------------------
#ifndef GPOS_CLogicalVarlenPath_H
#define GPOS_CLogicalVarlenPath_H

#include "gpos/base.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalUnary.h"

namespace gpopt
{
class CLogicalVarlenPath : public CLogicalUnary
{
private:
	CLogicalVarlenPath(const CLogicalVarlenPath &);

public:
	// ctor for xform pattern
	explicit CLogicalVarlenPath(CMemoryPool *mp);

	// ctor
	CLogicalVarlenPath(CMemoryPool *mp, CColRef *path_col_ref);

	virtual ~CLogicalVarlenPath();

	virtual EOperatorId
	Eopid() const
	{
		return EopLogicalVarlenPath;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CLogicalVarlenPath";
	}

	virtual BOOL Matches(COperator *pop) const;

	CColRef *
	PcrPath() const
	{
		return m_path_col_ref;
	}

	//-------------------------------------------------------------------------------------
	// Derived Relational Properties
	//-------------------------------------------------------------------------------------

	virtual CColRefSet *DeriveOutputColumns(CMemoryPool *, CExpressionHandle &);

	virtual CColRefSet *DeriveOuterReferences(CMemoryPool *mp,
											  CExpressionHandle &exprhdl);

	virtual CColRefSet *DeriveNotNullColumns(CMemoryPool *mp,
											 CExpressionHandle &exprhdl) const;

	virtual CPropConstraint *DerivePropertyConstraint(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const;

	//-------------------------------------------------------------------------------------
	// Required Relational Properties
	//-------------------------------------------------------------------------------------

	virtual CColRefSet *PcrsStat(CMemoryPool *mp, CExpressionHandle &exprhdl,
								 CColRefSet *pcrsInput,
								 ULONG child_index) const;

	//-------------------------------------------------------------------------------------
	// Derived Stats
	//-------------------------------------------------------------------------------------

	virtual IStatistics *PstatsDerive(CMemoryPool *mp,
									  CExpressionHandle &exprhdl,
									  IStatisticsArray *stats_ctxt) const;

	CXformSet *PxfsCandidates(CMemoryPool *mp) const;

	static CLogicalVarlenPath *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(EopLogicalVarlenPath == pop->Eopid());

		return reinterpret_cast<CLogicalVarlenPath *>(pop);
	}

	virtual IOstream &OsPrint(IOstream &os) const;

private:
	// the materialized path colref; defined by child[1]'s ProjectList
	CColRef *m_path_col_ref;

};	// class CLogicalVarlenPath

}  // namespace gpopt


#endif	// !GPOS_CLogicalVarlenPath_H

// EOF
