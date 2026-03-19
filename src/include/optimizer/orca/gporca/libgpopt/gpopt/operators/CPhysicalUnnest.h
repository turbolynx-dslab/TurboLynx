#ifndef GPOPT_CPhysicalUnnest_H
#define GPOPT_CPhysicalUnnest_H

#include "gpos/base.h"
#include "gpopt/operators/CPhysical.h"

namespace gpopt
{

// CPhysicalUnnest — physical unnest (UNWIND) operator.
// Mirrors CPhysicalComputeScalarColumnar: unary, pass-through properties,
// but destroys ordering (1→N row expansion).
class CPhysicalUnnest : public CPhysical
{
private:
	CPhysicalUnnest(const CPhysicalUnnest &);
	CColRef *m_pcrOutput;

public:
	explicit CPhysicalUnnest(CMemoryPool *mp);
	CPhysicalUnnest(CMemoryPool *mp, CColRef *pcrOutput);
	virtual ~CPhysicalUnnest();

	EOperatorId Eopid() const override { return EopPhysicalUnnest; }
	const CHAR *SzId() const override { return "CPhysicalUnnest"; }
	CColRef *PcrOutput() const { return m_pcrOutput; }

	BOOL Matches(COperator *) const override;
	BOOL FInputOrderSensitive() const override { return true; }

	// required plan properties
	CColRefSet *PcrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
	    CColRefSet *pcrsRequired, ULONG child_index,
	    CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq) override;
	CCTEReq *PcteRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
	    CCTEReq *pcter, ULONG child_index, CDrvdPropArray *pdrgpdpCtxt,
	    ULONG ulOptReq) const override;
	COrderSpec *PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
	    COrderSpec *posRequired, ULONG child_index,
	    CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq) const override;
	CDistributionSpec *PdsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
	    CDistributionSpec *pdsRequired, ULONG child_index,
	    CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq) const override;
	CRewindabilitySpec *PrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
	    CRewindabilitySpec *prsRequired, ULONG child_index,
	    CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq) const override;
	CPartitionPropagationSpec *PppsRequired(CMemoryPool *mp,
	    CExpressionHandle &exprhdl, CPartitionPropagationSpec *pppsRequired,
	    ULONG child_index, CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq) override;
	BOOL FProvidesReqdCols(CExpressionHandle &exprhdl,
	    CColRefSet *pcrsRequired, ULONG ulOptReq) const override;

	// derived plan properties
	COrderSpec *PosDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const override;
	CDistributionSpec *PdsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const override;
	CRewindabilitySpec *PrsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const override;
	CPartIndexMap *PpimDerive(CMemoryPool *, CExpressionHandle &exprhdl,
	    CDrvdPropCtxt *) const override
	{ return PpimPassThruOuter(exprhdl); }
	CPartFilterMap *PpfmDerive(CMemoryPool *, CExpressionHandle &exprhdl) const override
	{ return PpfmPassThruOuter(exprhdl); }

	// enforced properties
	CEnfdProp::EPropEnforcingType EpetOrder(CExpressionHandle &exprhdl,
	    const CEnfdOrder *peo) const override;
	CEnfdProp::EPropEnforcingType EpetRewindability(CExpressionHandle &,
	    const CEnfdRewindability *) const override;

	BOOL FPassThruStats() const override { return false; }

	static CPhysicalUnnest *PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(EopPhysicalUnnest == pop->Eopid());
		return dynamic_cast<CPhysicalUnnest *>(pop);
	}
};

}  // namespace gpopt

#endif  // !GPOPT_CPhysicalUnnest_H
