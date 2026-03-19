#ifndef GPOPT_CXformImplementUnnest_H
#define GPOPT_CXformImplementUnnest_H

#include "gpos/base.h"
#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{

class CXformImplementUnnest : public CXformImplementation
{
private:
	CXformImplementUnnest(const CXformImplementUnnest &);

public:
	explicit CXformImplementUnnest(CMemoryPool *mp);
	virtual ~CXformImplementUnnest() {}

	EXformId Exfid() const override { return ExfImplementUnnest; }
	const CHAR *SzId() const override { return "CXformImplementUnnest"; }

	EXformPromise Exfp(CExpressionHandle &) const override
	{ return CXform::ExfpHigh; }

	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
	               CExpression *pexpr) const override;
};

}  // namespace gpopt

#endif  // !GPOPT_CXformImplementUnnest_H
