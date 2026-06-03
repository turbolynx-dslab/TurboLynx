//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2026 TurboLynx
//
//	@filename:
//		CPhysicalVarlenPath.h
//
//	@doc:
//		Physical wrap that materializes a path column on a plain
//		variable-length MATCH path = (a)-[:R*..]->(b) subtree.
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalVarlenPath_H
#define GPOPT_CPhysicalVarlenPath_H

#include "gpos/base.h"

#include "gpopt/base/COrderSpec.h"
#include "gpopt/operators/CPhysical.h"

namespace gpopt
{
class CPhysicalVarlenPath : public CPhysical
{
private:
	CPhysicalVarlenPath(const CPhysicalVarlenPath &);

public:
	CPhysicalVarlenPath(CMemoryPool *mp);

	CPhysicalVarlenPath(CMemoryPool *mp, const CName *pnameAlias,
						CColRef *path_col_ref);

	virtual ~CPhysicalVarlenPath();

	virtual EOperatorId
	Eopid() const
	{
		return EopPhysicalVarlenPath;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CPhysicalVarlenPath";
	}

	virtual BOOL Matches(COperator *pop) const;

	virtual BOOL
	FInputOrderSensitive() const
	{
		return true;
	}

	const CName *
	PnameAlias() const
	{
		return m_pnameAlias;
	}

	CColRef *
	PcrPath() const
	{
		return m_path_col_ref;
	}

	//-------------------------------------------------------------------------------------
	// Required Plan Properties
	//-------------------------------------------------------------------------------------

	virtual CColRefSet *PcrsRequired(
		CMemoryPool *mp, CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
		ULONG child_index, CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq);

	virtual CCTEReq *PcteRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
								  CCTEReq *pcter, ULONG child_index,
								  CDrvdPropArray *pdrgpdpCtxt,
								  ULONG ulOptReq) const;

	virtual COrderSpec *PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
									COrderSpec *posRequired, ULONG child_index,
									CDrvdPropArray *pdrgpdpCtxt,
									ULONG ulOptReq) const;

	virtual CDistributionSpec *PdsRequired(CMemoryPool *mp,
										   CExpressionHandle &exprhdl,
										   CDistributionSpec *pdsRequired,
										   ULONG child_index,
										   CDrvdPropArray *pdrgpdpCtxt,
										   ULONG ulOptReq) const;

	virtual CRewindabilitySpec *PrsRequired(CMemoryPool *mp,
											CExpressionHandle &exprhdl,
											CRewindabilitySpec *prsRequired,
											ULONG child_index,
											CDrvdPropArray *pdrgpdpCtxt,
											ULONG ulOptReq) const;

	virtual CPartitionPropagationSpec *PppsRequired(
		CMemoryPool *mp, CExpressionHandle &exprhdl,
		CPartitionPropagationSpec *pppsRequired, ULONG child_index,
		CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq);

	virtual BOOL FProvidesReqdCols(CExpressionHandle &exprhdl,
								   CColRefSet *pcrsRequired,
								   ULONG ulOptReq) const;

	//-------------------------------------------------------------------------------------
	// Derived Plan Properties
	//-------------------------------------------------------------------------------------

	virtual COrderSpec *PosDerive(CMemoryPool *mp,
								  CExpressionHandle &exprhdl) const;

	virtual CDistributionSpec *PdsDerive(CMemoryPool *mp,
										 CExpressionHandle &exprhdl) const;

	virtual CRewindabilitySpec *PrsDerive(CMemoryPool *mp,
										  CExpressionHandle &exprhdl) const;

	virtual CPartIndexMap *PpimDerive(CMemoryPool *,
									  CExpressionHandle &exprhdl,
									  CDrvdPropCtxt *) const;

	virtual CPartFilterMap *PpfmDerive(CMemoryPool *,
									   CExpressionHandle &exprhdl) const;

	//-------------------------------------------------------------------------------------
	// Enforced Properties
	//-------------------------------------------------------------------------------------

	virtual CEnfdProp::EPropEnforcingType EpetOrder(
		CExpressionHandle &exprhdl, const CEnfdOrder *peo) const;

	virtual CEnfdProp::EPropEnforcingType EpetRewindability(
		CExpressionHandle &,
		const CEnfdRewindability *) const;

	virtual BOOL
	FPassThruStats() const
	{
		return false;
	}

	virtual IOstream &OsPrint(IOstream &) const;

	static CPhysicalVarlenPath *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(EopPhysicalVarlenPath == pop->Eopid());

		return dynamic_cast<CPhysicalVarlenPath *>(pop);
	}

private:
	const CName *m_pnameAlias;

	CColRef *m_path_col_ref;

};	// class CPhysicalVarlenPath

}  // namespace gpopt

#endif	// !GPOPT_CPhysicalVarlenPath_H

// EOF
