#pragma once

#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "common/types/value.hpp"
#include <vector>

namespace duckdb {

// PhysicalConstScan — emits a fixed set of pre-computed rows.
// Used as a source operator for standalone UNWIND, ConstTableGet, etc.
class PhysicalConstScan : public CypherPhysicalOperator {
public:
    PhysicalConstScan(Schema &sch, vector<vector<Value>> rows);
    ~PhysicalConstScan() override = default;

    // Source interface (leaf source — no sink)
    unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context) const override;
    void GetData(ExecutionContext &context, DataChunk &chunk,
                 LocalSourceState &lstate) const override;
    bool IsSourceDataRemaining(LocalSourceState &lstate) const override;
    bool IsSource() const override { return true; }

    // Non-source interface (unused but required)
    unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
    OperatorResultType Execute(ExecutionContext &context, DataChunk &input,
                               DataChunk &chunk, OperatorState &state) const override;

    std::string ParamsToString() const override { return "const-scan"; }
    std::string ToString() const override { return "ConstScan"; }

    vector<vector<Value>> rows;
};

}  // namespace duckdb
