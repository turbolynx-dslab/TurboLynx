/*-------------------------------------------------------------------------
 *
 * assert.c
 *	  Assert code.
 *
 * Portions Copyright (c) 2005-2009, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/error/assert.c
 *
 * NOTE
 *	  This should eventually work with elog()
 *
 *-------------------------------------------------------------------------
 */
#include "optimizer/orca/postgres.h"

// #include "libpq/pqsignal.h"
// #include "cdb/cdbvars.h"                /* currentSliceId */

#include <unistd.h>

/*
 * ExceptionalCondition - Handles the failure of an Assert()
 *
 * On WASM, we cannot call abort() (it triggers an uncatchable 'unreachable'
 * trap). Instead, print the message and return — the caller will propagate
 * the error through ORCA's CException mechanism.
 */
void
ExceptionalCondition(const char *conditionName,
					 const char *errorType,
					 const char *fileName,
					 int lineNumber)
{
	fprintf(stderr,
			"ASSERTION FAILED: %s(\"%s\", File: \"%s\", Line: %d)\n",
			errorType ? errorType : "Unknown",
			conditionName ? conditionName : "?",
			fileName ? fileName : "?",
			lineNumber);
	fflush(stderr);

#ifdef TURBOLYNX_WASM
	/* On WASM, avoid abort — it is not catchable. */
	return;
#else

#ifdef SLEEP_ON_ASSERT
	sleep(1000000);
#endif

	abort();
#endif
}
