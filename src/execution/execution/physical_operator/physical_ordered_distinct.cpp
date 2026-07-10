#include "execution/physical_operator/physical_ordered_distinct.hpp"

#include <string>
#include <unordered_set>

#include "common/types/selection_vector.hpp"
#include "common/types/value.hpp"

namespace duckdb {

class OrderedDistinctState : public OperatorState {
public:
	OrderedDistinctState() : sel(STANDARD_VECTOR_SIZE) {}

	SelectionVector sel;
	std::unordered_set<std::string> seen;
};

unique_ptr<OperatorState> PhysicalOrderedDistinct::GetOperatorState(
    ExecutionContext &context) const {
	return make_unique<OrderedDistinctState>();
}

OperatorResultType PhysicalOrderedDistinct::Execute(ExecutionContext &context,
                                                    DataChunk &input,
                                                    DataChunk &chunk,
                                                    OperatorState &lstate) const {
	auto &state = (OrderedDistinctState &)lstate;

	idx_t result_count = 0;
	std::string key;
	for (idx_t row = 0; row < input.size(); row++) {
		key.clear();
		for (auto col : key_input_idxs) {
			Value v = input.GetValue(col, row);
			if (v.IsNull()) {
				key += "\x01N";
			} else {
				key += v.ToString();
			}
			key += '\x1f';
		}
		if (state.seen.insert(key).second) {
			state.sel.set_index(result_count++, row);
		}
	}

	for (idx_t out = 0; out < output_input_idxs.size(); out++) {
		chunk.data[out].Slice(input.data[output_input_idxs[out]], state.sel,
		                      result_count);
	}
	chunk.SetCardinality(result_count);
	return OperatorResultType::NEED_MORE_INPUT;
}

std::string PhysicalOrderedDistinct::ParamsToString() const {
	std::string keys;
	for (auto k : key_input_idxs) {
		if (!keys.empty()) keys += ",";
		keys += std::to_string(k);
	}
	return "ordered-distinct keys: [" + keys + "]";
}

std::string PhysicalOrderedDistinct::ToString() const {
	return "OrderedDistinct";
}

}  // namespace duckdb
