//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CAutoSuspendAbort.h
//
//	@doc:
//		Auto object for suspending and resuming task cancellation
//---------------------------------------------------------------------------
#ifndef GPOS_CAutoSuspendAbort_H
#define GPOS_CAutoSuspendAbort_H

#include "gpos/base.h"
#include "gpos/common/CStackObject.h"

namespace gpos
{
class CTask;

//---------------------------------------------------------------------------
//	@class:
//		CAutoSuspendAbort
//
//	@doc:
//		Auto object for suspending and resuming task cancellation
//
//---------------------------------------------------------------------------
class CAutoSuspendAbort : public CStackObject
{
private:
	// pointer to task in current execution context
	CTask *m_task;

public:
	// ctor - suspends CFA
	CAutoSuspendAbort();

	// dtor - resumes CFA
	virtual ~CAutoSuspendAbort();

	// Detach from the task — destructor becomes a no-op.  Used by
	// ~CAutoTaskProxy after DestroyAll() has freed the very task this
	// guard was suspending; without this the dtor would dereference a
	// dangling pointer (issue #232).  The suspend/resume counter on a
	// freed task is moot.
	void Forget() { m_task = NULL; }

};	// class CAutoSuspendAbort

}  // namespace gpos

#endif	// GPOS_CAutoSuspendAbort_H


// EOF
