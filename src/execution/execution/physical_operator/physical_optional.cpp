#include "execution/physical_operator/physical_optional.hpp"

namespace duckdb {

class OptionalOperatorState : public OperatorState {};

unique_ptr<OperatorState> PhysicalOptional::GetOperatorState(
    ExecutionContext &context) const {
    return make_unique<OptionalOperatorState>();
}

OperatorResultType PhysicalOptional::Execute(ExecutionContext &context,
                                              DataChunk &input,
                                              DataChunk &chunk,
                                              OperatorState &state) const {
    if (input.size() > 0) {
        has_emitted_any_ = true;
        chunk.Reference(input);
    } else {
        chunk.SetCardinality(0);
    }
    return OperatorResultType::NEED_MORE_INPUT;
}

OperatorResultType PhysicalOptional::FinalExecute(ExecutionContext &context,
                                                   DataChunk &chunk,
                                                   OperatorState &state) const {
    if (has_emitted_any_ || null_row_emitted_) {
        chunk.SetCardinality(0);
        return OperatorResultType::FINISHED;
    }
    null_row_emitted_ = true;
    chunk.SetCardinality(1);
    for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
        chunk.data[c].SetVectorType(VectorType::CONSTANT_VECTOR);
        ConstantVector::SetNull(chunk.data[c], true);
    }
    return OperatorResultType::FINISHED;
}

}  // namespace duckdb
