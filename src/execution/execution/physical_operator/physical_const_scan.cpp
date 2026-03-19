#include "execution/physical_operator/physical_const_scan.hpp"

namespace duckdb {

PhysicalConstScan::PhysicalConstScan(Schema &sch, vector<vector<Value>> rows)
    : CypherPhysicalOperator(PhysicalOperatorType::CONST_SCAN, sch),
      rows(std::move(rows)) {}

// ---- Source interface ----

class ConstScanSourceState : public LocalSourceState {
public:
    idx_t offset = 0;
};

unique_ptr<LocalSourceState> PhysicalConstScan::GetLocalSourceState(ExecutionContext &) const {
    return make_unique<ConstScanSourceState>();
}

void PhysicalConstScan::GetData(ExecutionContext &, DataChunk &chunk,
                                 LocalSourceState &lstate) const {
    auto &state = (ConstScanSourceState &)lstate;

    idx_t count = 0;
    while (state.offset < rows.size() && count < STANDARD_VECTOR_SIZE) {
        auto &row = rows[state.offset];
        for (idx_t col = 0; col < chunk.ColumnCount() && col < row.size(); col++) {
            chunk.SetValue(col, count, row[col]);
        }
        state.offset++;
        count++;
    }
    chunk.SetCardinality(count);
}

bool PhysicalConstScan::IsSourceDataRemaining(LocalSourceState &lstate) const {
    auto &state = (ConstScanSourceState &)lstate;
    return state.offset < rows.size();
}

// ---- Non-source interface (unused) ----

unique_ptr<OperatorState> PhysicalConstScan::GetOperatorState(ExecutionContext &) const {
    return make_unique<OperatorState>();
}

OperatorResultType PhysicalConstScan::Execute(ExecutionContext &, DataChunk &,
                                               DataChunk &, OperatorState &) const {
    return OperatorResultType::NEED_MORE_INPUT;
}

}  // namespace duckdb
