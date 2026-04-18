//===----------------------------------------------------------------------===//
//                         DuckDB
//
// src/execution/execution/physical_operator/physical_top.cpp
//
//
//===----------------------------------------------------------------------===//

#include "execution/physical_operator/physical_top.hpp"

#include "common/typedef.hpp"
#include "icecream.hpp"

#include <string>
#include "common/allocator.hpp"

namespace duckdb {

class TopState : public OperatorState {
public:
	explicit TopState(): current_offset((idx_t)0) {}
public:
	idx_t current_offset;
};

unique_ptr<OperatorState> PhysicalTop::GetOperatorState(ExecutionContext &context) const {
	return make_unique<TopState>();
}

OperatorResultType PhysicalTop::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const {
	idx_t input_size = input.size();

	// Phase 1: SKIP — discard the first `offset` rows.
	idx_t skip_start = 0;
	if (offset > 0) {
		idx_t already_skipped = shared_skipped.load(std::memory_order_relaxed);
		if (already_skipped < offset) {
			idx_t can_skip = std::min(input_size, offset - already_skipped);
			shared_skipped.fetch_add(can_skip, std::memory_order_relaxed);
			skip_start = can_skip;
			if (skip_start >= input_size) {
				return OperatorResultType::NEED_MORE_INPUT;
			}
		}
	}

	idx_t remaining = input_size - skip_start;

	// Phase 2: LIMIT — take up to `limit` rows from the remaining.
	idx_t prev = shared_count.fetch_add(remaining, std::memory_order_relaxed);
	if (prev >= limit) {
		shared_count.fetch_sub(remaining, std::memory_order_relaxed);
		return OperatorResultType::FINISHED;
	}

	idx_t take = remaining;
	if (prev + remaining > limit) {
		take = limit - prev;
		shared_count.fetch_sub(remaining - take, std::memory_order_relaxed);
	}

	if (skip_start == 0 && take == input_size) {
		chunk.Reference(input);
	} else {
		SelectionVector sel(STANDARD_VECTOR_SIZE);
		for (idx_t i = 0; i < take; i++) {
			sel.set_index(i, skip_start + i);
		}
		chunk.Slice(input, sel, take);
	}
	return OperatorResultType::NEED_MORE_INPUT;
}

std::string PhysicalTop::ParamsToString() const {
	return "top-param";
}

std::string PhysicalTop::ToString() const {
	return "Top";
}

}