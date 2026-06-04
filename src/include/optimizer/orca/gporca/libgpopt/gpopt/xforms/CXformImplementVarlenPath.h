//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2026 TurboLynx
//
//	@filename:
//		CXformImplementVarlenPath.h
//
//	@doc:
//		Transform CLogicalVarlenPath → CPhysicalVarlenPath.
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformImplementVarlenPath_H
#define GPOPT_CXformImplementVarlenPath_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{
using namespace gpos;

class CXformImplementVarlenPath : public CXformImplementation
{
private:
	CXformImplementVarlenPath(const CXformImplementVarlenPath &);

public:
	explicit CXformImplementVarlenPath(CMemoryPool *mp);

	virtual ~CXformImplementVarlenPath()
	{
	}

	virtual EXformId
	Exfid() const
	{
		return ExfImplementVarlenPath;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformImplementVarlenPath";
	}

	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	void Transform(CXformContext *, CXformResult *, CExpression *) const;

};	// class CXformImplementVarlenPath

}  // namespace gpopt

#endif	// !CXformImplementVarlenPath

// EOF
