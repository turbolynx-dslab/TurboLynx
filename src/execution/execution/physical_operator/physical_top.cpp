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
	// Atomic reservation: claim a slice of the LIMIT for this chunk.
	// Multiple threads may call concurrently — fetch_add ensures correctness.
	idx_t input_size = input.size();
	idx_t prev = shared_count.fetch_add(input_size, std::memory_order_relaxed);
	if (prev >= limit) {
		// Already at limit — return our reservation and signal done
		shared_count.fetch_sub(input_size, std::memory_order_relaxed);
		return OperatorResultType::FINISHED;
	}

	idx_t take = input_size;
	if (prev + input_size > limit) {
		// Partial: only take limit - prev rows
		take = limit - prev;
		// Return the unused portion
		shared_count.fetch_sub(input_size - take, std::memory_order_relaxed);
	}

	if (take == input_size) {
		chunk.Reference(input);
	} else {
		SelectionVector sel(STANDARD_VECTOR_SIZE);
		for (idx_t i = 0; i < take; i++) {
			sel.set_index(i, i);
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