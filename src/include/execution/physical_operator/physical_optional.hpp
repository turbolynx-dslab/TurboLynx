#pragma once

#include "common/typedef.hpp"
#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "execution/execution_context.hpp"

namespace duckdb {

// Cypher OPTIONAL MATCH "atomic existence" operator (#203, #204).
//
// Wraps a child plan and guarantees ≥1 output row. If the child produces
// any rows they are forwarded unchanged in streaming fashion. If the child
// produces zero rows, exactly one row of NULLs (with the child's schema)
// is emitted via FinalExecute after the upstream source EOS. Mirrors
// Neo4j's `+Optional` operator that sits above the MATCH plan for a
// standalone OPTIONAL MATCH clause.
//
// Implemented as a piped operator (not a pipeline break) — relies on the
// FinalExecute hook in CypherPhysicalOperator to emit the synthetic NULL
// row exactly once at EOS, so the matched-row path remains pass-through.
class PhysicalOptional : public CypherPhysicalOperator {
public:
    explicit PhysicalOptional(Schema &sch)
        : CypherPhysicalOperator(PhysicalOperatorType::OPTIONAL, sch) {}
    ~PhysicalOptional() override {}

    unique_ptr<OperatorState> GetOperatorState(
        ExecutionContext &context) const override;
    OperatorResultType Execute(ExecutionContext &context, DataChunk &input,
                                DataChunk &chunk,
                                OperatorState &state) const override;
    OperatorResultType FinalExecute(ExecutionContext &context, DataChunk &chunk,
                                     OperatorState &state) const override;

    std::string ToString() const override { return "Optional"; }

private:
    // has_emitted must survive ReinitializePipeline, which destroys
    // local_operator_states between source-group advances. Tracking it on
    // the operator (single-threaded use only) keeps the OPTIONAL "at
    // least one row" guarantee atomic across the whole pipeline.
    mutable bool has_emitted_any_ = false;
    mutable bool null_row_emitted_ = false;
};

}  // namespace duckdb
