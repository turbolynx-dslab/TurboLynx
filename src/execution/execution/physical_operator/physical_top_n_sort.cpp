//===----------------------------------------------------------------------===//
//                         DuckDB
//
// src/execution/execution/physical_operator/physical_top_n_sort.cpp
//
//
//===----------------------------------------------------------------------===//

#include "common/typedef.hpp"

#include "execution/physical_operator/physical_top_n_sort.hpp"
#include "execution/top_n_sort.hpp"

#include "execution/expression_executor.hpp"

#include <string>

#include "common/allocator.hpp"
#include "common/output_util.hpp"
#include "icecream.hpp"

namespace duckdb {


PhysicalTopNSort::PhysicalTopNSort(Schema& sch, vector<BoundOrderByNode> orders, idx_t limit, idx_t offset)
	: CypherPhysicalOperator(PhysicalOperatorType::TOP_N_SORT, sch), orders(move(orders)), limit(limit), offset(offset)
{ }

PhysicalTopNSort::~PhysicalTopNSort() { };

class TopNSortGlobalSinkState : public GlobalSinkState {
public:
	TopNSortGlobalSinkState(ClientContext &context, const vector<LogicalType> &payload_types,
	                        const vector<BoundOrderByNode> &orders, idx_t limit, idx_t offset)
	    : heap(context, payload_types, orders, limit, offset) {}
	TopNHeap heap;
	mutex heap_lock;
};

class TopNSortSinkState : public LocalSinkState {
public:
	explicit TopNSortSinkState(ClientContext &context, const vector<LogicalType> &payload_types,
	             const vector<BoundOrderByNode> &orders, idx_t limit, idx_t offset)
	    : heap(context, payload_types, orders, limit, offset) {
	}

	TopNHeap heap;
};

unique_ptr<LocalSinkState> PhysicalTopNSort::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<TopNSortSinkState>(*(context.client), types, orders, limit, offset);
}

unique_ptr<GlobalSinkState> PhysicalTopNSort::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<TopNSortGlobalSinkState>(context, types, orders, limit, offset);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType PhysicalTopNSort::Sink(ExecutionContext &context, DataChunk &input, LocalSinkState &lstate) const {
	// append to the local sink state
	auto &sink = (TopNSortSinkState &)lstate;
	sink.heap.Sink(input);
	sink.heap.Reduce();
	return SinkResultType::NEED_MORE_INPUT;
}

DataChunk &PhysicalTopNSort::GetLastSinkedData(LocalSinkState &lstate) const {
	auto &sink_state = (TopNSortSinkState &)lstate;
	return sink_state.heap.sort_chunk;
}

//===--------------------------------------------------------------------===//
// Combine
//===--------------------------------------------------------------------===//
void PhysicalTopNSort::Combine(ExecutionContext &context, LocalSinkState &lstate_p) const {
	// auto &gstate = (TopNGlobalState &)state;
	auto &lstate = (TopNSortSinkState &)lstate_p;
	// directly call finalize for heap
	lstate.heap.Finalize();
}

SinkResultType PhysicalTopNSort::Sink(ExecutionContext &context,
                                      GlobalSinkState &gstate,
                                      LocalSinkState &lstate,
                                      DataChunk &input) const {
	// Parallel Sink: each thread fills its own local heap
	auto &sink = (TopNSortSinkState &)lstate;
	sink.heap.Sink(input);
	sink.heap.Reduce();
	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalTopNSort::Combine(ExecutionContext &context, GlobalSinkState &gstate,
                                LocalSinkState &lstate) const {
	auto &lsink = (TopNSortSinkState &)lstate;
	auto &gsink = (TopNSortGlobalSinkState &)gstate;
	// Merge thread-local heap into global heap (mutex-protected)
	lock_guard<mutex> guard(gsink.heap_lock);
	gsink.heap.Combine(lsink.heap);
}

SinkFinalizeType PhysicalTopNSort::Finalize(ExecutionContext &context,
                                             GlobalSinkState &gstate) const {
	auto &gsink = (TopNSortGlobalSinkState &)gstate;
	gsink.heap.Finalize();
	return SinkFinalizeType::READY;
}

void PhysicalTopNSort::TransferGlobalToLocal(GlobalSinkState &gstate,
                                              LocalSinkState &lstate) const {
	// Replace local heap's sort_state with the finalized global one
	// by using TopNSortState::Move (which moves the underlying sort blocks).
	auto &lsink = (TopNSortSinkState &)lstate;
	auto &gsink = (TopNSortGlobalSinkState &)gstate;
	lsink.heap.sort_state.Move(gsink.heap.sort_state);
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class TopNSortSourceState : public LocalSourceState {
public:
	TopNScanState state;
	bool initialized = false;
	bool is_finished = false;
};

unique_ptr<LocalSourceState> PhysicalTopNSort::GetLocalSourceState(ExecutionContext &context) const {
	return make_unique<TopNSortSourceState>();
}

void PhysicalTopNSort::GetData(ExecutionContext &context, DataChunk &chunk, LocalSourceState &lstate, LocalSinkState &sink_state) const{
	if (limit == 0) {
		return;
	}
	auto &state = (TopNSortSourceState &)lstate;
	auto &gstate = (TopNSortSinkState &)sink_state;

	if (!state.initialized) {
		gstate.heap.InitializeScan(state.state, true);
		state.initialized = true;
	}
	gstate.heap.Scan(state.state, chunk);

	if (chunk.size() == 0) {
		state.is_finished = true;
		return;
	}

	chunk.SetSchemaIdx(0); // TODO always 0? after sort, schemas are unified
}


bool PhysicalTopNSort::IsSourceDataRemaining(LocalSourceState &lstate, LocalSinkState &sink_state) const {
	auto &state = (TopNSortSourceState &)lstate;
	return !state.is_finished;
}


std::string PhysicalTopNSort::ParamsToString() const {
	string params = "topnsort-param: ";
	for (auto & order : orders) {
		params += order.ToString();
		params += " / ";
	}
	return params;
}

std::string PhysicalTopNSort::ToString() const {
	return "TopNSort";
}


}