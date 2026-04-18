//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/physical_operator/physical_top.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "common/typedef.hpp"

#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "execution/execution_context.hpp"

#include <atomic>

namespace duckdb {

class PhysicalTop: public CypherPhysicalOperator {

public:
	PhysicalTop(Schema& sch, idx_t limit, idx_t offset)
		: CypherPhysicalOperator(PhysicalOperatorType::TOP, sch), limit(limit), offset(offset), shared_count(0), shared_skipped(0)  {
	}
	PhysicalTop(Schema& sch, idx_t limit)
		: CypherPhysicalOperator(PhysicalOperatorType::TOP, sch), limit(limit), offset(0), shared_count(0), shared_skipped(0) {}

public:
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	OperatorResultType Execute(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &state) const override;

	std::string ParamsToString() const override;
	std::string ToString() const override;

private:
	idx_t offset;
	idx_t limit;
	//! Shared atomic counter for parallel LIMIT (correctness across threads)
	mutable std::atomic<idx_t> shared_count;
	//! Shared atomic counter for SKIP offset
	mutable std::atomic<idx_t> shared_skipped;
};

}
