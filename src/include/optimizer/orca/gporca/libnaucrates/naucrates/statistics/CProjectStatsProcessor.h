//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal, Inc.
//
//	@filename:
//		CProjectStatsProcessor.h
//
//	@doc:
//		Compute statistics for project operation
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CProjectStatsProcessor_H
#define GPNAUCRATES_CProjectStatsProcessor_H

#include "gpopt/optimizer/COptimizerConfig.h"
#include "naucrates/statistics/CStatistics.h"

namespace gpnaucrates
{
class CProjectStatsProcessor
{
public:
	// project; ident_source_map optionally maps a projected colid to the colid
	// of the source column when the project element is a pure column identity,
	// so the source column's histogram can be carried over
	static CStatistics *CalcProjStats(CMemoryPool *mp,
									  const CStatistics *input_stats,
									  ULongPtrArray *projection_colids,
									  UlongToIDatumMap *datum_map,
									  UlongToUlongMap *ident_source_map = NULL);
};
}  // namespace gpnaucrates

#endif	// !GPNAUCRATES_CProjectStatsProcessor_H

// EOF
