#pragma once
#include "common/typedef.hpp"

#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "execution/execution_context.hpp"

namespace duckdb {

//! Streaming duplicate elimination that preserves the input row order.
//! Used to lower a pure-distinct GbAgg sitting above a Sort: a hash
//! aggregate would discard the sort order, changing which rows a
//! subsequent LIMIT keeps.
class PhysicalOrderedDistinct : public CypherPhysicalOperator {
public:
	PhysicalOrderedDistinct(Schema &sch, vector<uint32_t> key_input_idxs,
	                        vector<uint32_t> output_input_idxs)
	    : CypherPhysicalOperator(PhysicalOperatorType::ORDERED_DISTINCT, sch),
	      key_input_idxs(std::move(key_input_idxs)),
	      output_input_idxs(std::move(output_input_idxs)) {}
	~PhysicalOrderedDistinct() {}

	unique_ptr<OperatorState> GetOperatorState(
	    ExecutionContext &context) const override;
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input,
	                           DataChunk &chunk,
	                           OperatorState &state) const override;

	std::string ParamsToString() const override;
	std::string ToString() const override;

	//! Input column positions forming the distinct key.
	vector<uint32_t> key_input_idxs;
	//! Input column position feeding each output column.
	vector<uint32_t> output_input_idxs;
};

}  // namespace duckdb
