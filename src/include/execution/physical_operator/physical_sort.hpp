//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/physical_operator/physical_sort.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/typedef.hpp"
#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "main/client_context.hpp"
#include "common/sort/sort.hpp"

#include "common/types/chunk_collection.hpp"
#include "parallel/pipeline.hpp"
#include "planner/bound_query_node.hpp"


namespace duckdb {

class PhysicalSort: public CypherPhysicalOperator {

public:

	PhysicalSort(Schema& sch, vector<BoundOrderByNode> orders);
	~PhysicalSort();

public:

	// sink
	virtual SinkResultType Sink(ExecutionContext &context, DataChunk &input, LocalSinkState &lstate) const;
	virtual SinkResultType Sink(ExecutionContext &context, GlobalSinkState &gstate,
	                            LocalSinkState &lstate, DataChunk &input) const override;
	virtual unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	virtual void Combine(ExecutionContext& context, LocalSinkState& lstate) const;
	void Combine(ExecutionContext &context, GlobalSinkState &gstate,
	             LocalSinkState &lstate) const override;
	SinkFinalizeType Finalize(ExecutionContext &context,
	                          GlobalSinkState &gstate) const override;
	bool IsSink() const override { return true; }
	bool ParallelSink() const override { return true; }
	DataChunk &GetLastSinkedData(LocalSinkState &lstate) const override;
	//! Transfer finalized global sort state into local for downstream
	void TransferGlobalToLocal(GlobalSinkState &gstate, LocalSinkState &lstate) const;

	// source
	void GetData(ExecutionContext &context, DataChunk &chunk, LocalSourceState &lstate, LocalSinkState &sink_state) const;
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context) const override;
	bool IsSource() const override { return true; }
	bool IsSourceDataRemaining(LocalSourceState &lstate, LocalSinkState &sink_state) const override;

	std::string ParamsToString() const override;
	std::string ToString() const override;

	// operator parameters
	vector<BoundOrderByNode> orders;
};
	
}
