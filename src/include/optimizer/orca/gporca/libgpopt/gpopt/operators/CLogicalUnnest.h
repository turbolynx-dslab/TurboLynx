#ifndef GPOS_CLogicalUnnest_H
#define GPOS_CLogicalUnnest_H

#include "gpos/base.h"
#include "gpopt/operators/CLogicalUnary.h"

namespace gpopt
{

// CLogicalUnnest — UNWIND / unnest operator.
// Takes a relational child (or single-row source) and a scalar list
// expression, producing one row per list element.
// Arity: 2 children — [0] relational, [1] scalar (the list expression).
class CLogicalUnnest : public CLogicalUnary
{
private:
	CLogicalUnnest(const CLogicalUnnest &);

	CColRef *m_pcrOutput;  // the new column produced by unnesting

public:
	explicit CLogicalUnnest(CMemoryPool *mp);
	CLogicalUnnest(CMemoryPool *mp, CColRef *pcrOutput);
	virtual ~CLogicalUnnest() {}

	EOperatorId Eopid() const override { return EopLogicalUnnest; }
	const CHAR *SzId() const override { return "CLogicalUnnest"; }

	CColRef *PcrOutput() const { return m_pcrOutput; }

	// derived relational properties
	CColRefSet *DeriveOutputColumns(CMemoryPool *mp, CExpressionHandle &exprhdl) override;
	CKeyCollection *DeriveKeyCollection(CMemoryPool *mp, CExpressionHandle &exprhdl) const override;
	CMaxCard DeriveMaxCard(CMemoryPool *mp, CExpressionHandle &exprhdl) const override;
	CPropConstraint *DerivePropertyConstraint(CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

	// transformations
	CXformSet *PxfsCandidates(CMemoryPool *mp) const override;

	// statistics
	IStatistics *PstatsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl,
	                          IStatisticsArray *stats_ctxt) const override;

	// match / hash
	BOOL Matches(COperator *pop) const override;
	ULONG HashValue() const override;

	// column remapping
	COperator *PopCopyWithRemappedColumns(CMemoryPool *mp,
	    UlongToColRefMap *colref_mapping, BOOL must_exist) override;

	static CLogicalUnnest *PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(EopLogicalUnnest == pop->Eopid());
		return dynamic_cast<CLogicalUnnest *>(pop);
	}
};

}  // namespace gpopt

#endif  // !GPOS_CLogicalUnnest_H
