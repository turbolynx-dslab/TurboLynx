//===----------------------------------------------------------------------===//
//                         DuckDB
//
// src/execution/execution/physical_operator/physical_const_scan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "execution/physical_operator/physical_const_scan.hpp"

#include <atomic>

namespace duckdb {

PhysicalConstScan::PhysicalConstScan(Schema &sch, vector<vector<Value>> rows)
    : CypherPhysicalOperator(PhysicalOperatorType::CONST_SCAN, sch),
      rows(std::move(rows)) {}

// ---- Source interface ----

class ConstScanSourceState : public LocalSourceState {
public:
    idx_t offset = 0;
    bool finished = false;
};

class ConstScanGlobalSourceState : public GlobalSourceState {
public:
    explicit ConstScanGlobalSourceState(idx_t total_rows) : total(total_rows), next_offset(0) {}
    idx_t MaxThreads() override {
        return std::max((idx_t)1, (total + STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE);
    }
    idx_t total;
    std::atomic<idx_t> next_offset;
};

unique_ptr<LocalSourceState> PhysicalConstScan::GetLocalSourceState(ExecutionContext &) const {
    return make_unique<ConstScanSourceState>();
}

unique_ptr<GlobalSourceState> PhysicalConstScan::GetGlobalSourceState(ClientContext &) const {
    return make_unique<ConstScanGlobalSourceState>(rows.size());
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

void PhysicalConstScan::GetData(ExecutionContext &context, DataChunk &chunk,
                                 GlobalSourceState &gstate,
                                 LocalSourceState &lstate) const {
    auto &state = (ConstScanSourceState &)lstate;
    auto &global = (ConstScanGlobalSourceState &)gstate;

    // Atomically reserve a chunk-sized batch
    idx_t my_start = global.next_offset.fetch_add(STANDARD_VECTOR_SIZE,
                                                   std::memory_order_relaxed);
    if (my_start >= rows.size()) {
        chunk.SetCardinality(0);
        state.finished = true;
        return;
    }
    idx_t my_end = std::min(my_start + (idx_t)STANDARD_VECTOR_SIZE, (idx_t)rows.size());

    idx_t count = 0;
    for (idx_t i = my_start; i < my_end; i++) {
        auto &row = rows[i];
        for (idx_t col = 0; col < chunk.ColumnCount() && col < row.size(); col++) {
            chunk.SetValue(col, count, row[col]);
        }
        count++;
    }
    chunk.SetCardinality(count);
    if (my_end >= rows.size()) {
        state.finished = true;
    }
}

bool PhysicalConstScan::IsSourceDataRemaining(LocalSourceState &lstate) const {
    auto &state = (ConstScanSourceState &)lstate;
    return state.offset < rows.size();
}

bool PhysicalConstScan::IsSourceDataRemaining(GlobalSourceState &gstate,
                                               LocalSourceState &lstate) const {
    auto &state = (ConstScanSourceState &)lstate;
    return !state.finished;
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
