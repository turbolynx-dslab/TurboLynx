#include "execution/physical_operator/physical_sort.hpp"

#include "planner/expression.hpp"
// #include "planner/bound_result_modifier.hpp"
#include "common/sort/sort.hpp"
#include "execution/expression_executor.hpp"
#include "main/client_context.hpp"
#include "storage/buffer_manager.hpp"

namespace duckdb {

PhysicalSort::PhysicalSort(Schema &sch, vector<BoundOrderByNode> orders_p)
    : CypherPhysicalOperator(PhysicalOperatorType::SORT, sch),
      orders(std::move(orders_p))
{}

PhysicalSort::~PhysicalSort() {}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class SortGlobalSinkState : public GlobalSinkState {
   public:
    SortGlobalSinkState(BufferManager &buffer_manager,
                        const PhysicalSort &order, RowLayout &payload_layout)
        : global_sort_state(buffer_manager, order.orders, payload_layout) {}
    //! Shared global sort state (workers AddLocalState in Combine)
    GlobalSortState global_sort_state;
};

class SortSinkState : public LocalSinkState {
   public:
    SortSinkState(BufferManager &buffer_manager, const PhysicalSort &order,
                  RowLayout &payload_layout)
        : global_sort_state(buffer_manager, order.orders, payload_layout)
    {}
    //! Per-thread global sort state (used in single-thread fallback)
    GlobalSortState global_sort_state;
    //! The local sort state
    LocalSortState local_sort_state;
    //! Local copy of the sorting expression executor
    ExpressionExecutor executor;
    //! Holds a vector of incoming sorting columns
    DataChunk sort;
};

unique_ptr<LocalSinkState> PhysicalSort::GetLocalSinkState(
    ExecutionContext &context) const
{
    // create global sort state
    RowLayout payload_layout;
    payload_layout.Initialize(types);
    auto result = make_unique<SortSinkState>(
        BufferManager::GetBufferManager(*context.client), *this,
        payload_layout);

    // create local sort state
    vector<LogicalType> sort_types;
    for (auto &order : orders) {
        sort_types.push_back(order.expression->return_type);
        result->executor.AddExpression(*order.expression);
    }
    result->sort.Initialize(sort_types);
    return move(result);
}

unique_ptr<GlobalSinkState> PhysicalSort::GetGlobalSinkState(
    ClientContext &context) const
{
    RowLayout payload_layout;
    payload_layout.Initialize(types);
    return make_unique<SortGlobalSinkState>(
        BufferManager::GetBufferManager(context), *this, payload_layout);
}

SinkResultType PhysicalSort::Sink(ExecutionContext &context, DataChunk &input,
                                  LocalSinkState &lstate) const
{
    auto &state = (SortSinkState &)lstate;

    auto &local_sort_state = state.local_sort_state;
    auto &global_sort_state = state.global_sort_state;

    // Initialize local state (if necessary)
    if (!local_sort_state.initialized) {
        local_sort_state.Initialize(
            global_sort_state,
            BufferManager::GetBufferManager(*context.client));
    }

    // Obtain sorting columns
    auto &sort = state.sort;
    sort.Reset();
    state.executor.Execute(input, sort);

    // Sink the data into the local sort state
    local_sort_state.SinkChunk(sort, input);

    // When sorting data reaches a certain size, we sort it
    // TODO currently, we operate as in-memory mode
    // if (local_sort_state.SizeInBytes() >= gstate.memory_per_thread) {
    // 	local_sort_state.Sort(global_sort_state, true);
    // }
    return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalSort::Combine(ExecutionContext &context,
                           LocalSinkState &lstate) const
{
    auto &state = (SortSinkState &)lstate;
    state.global_sort_state.AddLocalState(state.local_sort_state);
}

SinkResultType PhysicalSort::Sink(ExecutionContext &context,
                                  GlobalSinkState &gstate,
                                  LocalSinkState &lstate,
                                  DataChunk &input) const
{
    // Parallel Sink: build into per-thread local sort state.
    // Reuse the per-thread global_sort_state for layout/buffer init.
    auto &state = (SortSinkState &)lstate;

    auto &local_sort_state = state.local_sort_state;
    auto &shared_global = ((SortGlobalSinkState &)gstate).global_sort_state;

    if (!local_sort_state.initialized) {
        local_sort_state.Initialize(
            shared_global,
            BufferManager::GetBufferManager(*context.client));
    }

    auto &sort = state.sort;
    sort.Reset();
    state.executor.Execute(input, sort);
    local_sort_state.SinkChunk(sort, input);

    return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalSort::Combine(ExecutionContext &context, GlobalSinkState &gstate,
                           LocalSinkState &lstate) const
{
    auto &state = (SortSinkState &)lstate;
    auto &gsink = (SortGlobalSinkState &)gstate;
    // GlobalSortState::AddLocalState is internally locked
    gsink.global_sort_state.AddLocalState(state.local_sort_state);
}

SinkFinalizeType PhysicalSort::Finalize(ExecutionContext &context,
                                         GlobalSinkState &gstate) const
{
    auto &gsink = (SortGlobalSinkState &)gstate;
    auto &gss = gsink.global_sort_state;
    if (gss.sorted_blocks.empty()) {
        return SinkFinalizeType::READY;
    }
    gss.PrepareMergePhase();
    while (gss.sorted_blocks.size() > 1) {
        gss.InitializeMergeRound();
        MergeSorter merge_sorter(gss, BufferManager::GetBufferManager(*(context.client)));
        merge_sorter.PerformInMergeRound();
        gss.CompleteMergeRound();
    }
    return SinkFinalizeType::READY;
}

void PhysicalSort::TransferGlobalToLocal(GlobalSinkState &gstate,
                                          LocalSinkState &lstate) const
{
    auto &state = (SortSinkState &)lstate;
    auto &gsink = (SortGlobalSinkState &)gstate;
    // Move shared sorted blocks into local state's global_sort_state
    // so downstream PhysicalSort::GetData reads them as usual.
    state.global_sort_state.sorted_blocks =
        std::move(gsink.global_sort_state.sorted_blocks);
    state.global_sort_state.heap_blocks =
        std::move(gsink.global_sort_state.heap_blocks);
    state.global_sort_state.pinned_blocks =
        std::move(gsink.global_sort_state.pinned_blocks);
    state.global_sort_state.external = gsink.global_sort_state.external;
    state.global_sort_state.block_capacity =
        gsink.global_sort_state.block_capacity;
}

DataChunk &PhysicalSort::GetLastSinkedData(LocalSinkState &lstate) const
{
    auto &sink_state = (SortSinkState &)lstate;
    return sink_state.sort;
}

class PhysicalSortOperatorState : public LocalSourceState {
   public:
    //! Payload scanner
    unique_ptr<PayloadScanner> scanner;
    bool is_finished = false;
};

unique_ptr<LocalSourceState> PhysicalSort::GetLocalSourceState(
    ExecutionContext &context) const
{
    return make_unique<PhysicalSortOperatorState>();
}

void PhysicalSort::GetData(ExecutionContext &context, DataChunk &chunk,
                           LocalSourceState &lstate,
                           LocalSinkState &sink_state) const
{
    auto &state = (PhysicalSortOperatorState &)lstate;

    if (!state.scanner) {
        // Initialize scanner (if not yet initialized)
        auto &sstate = (SortSinkState &)sink_state;
        auto &global_sort_state = sstate.global_sort_state;
        if (global_sort_state.sorted_blocks.empty()) {
            state.is_finished = true;
            return;
        }
        state.scanner = make_unique<PayloadScanner>(
            *global_sort_state.sorted_blocks[0]->payload_data,
            global_sort_state);
    }

    // Sort always union all schema
    chunk.SetSchemaIdx(0);

    // If the scanner is finished, we are done
    if (state.scanner->Remaining() == 0) {
        state.is_finished = true;
    }
    else {
        state.is_finished = false;  
        state.scanner->Scan(chunk);
    }
}

bool PhysicalSort::IsSourceDataRemaining(LocalSourceState &lstate,
                                         LocalSinkState &sink_state) const
{
    auto &state = (PhysicalSortOperatorState &)lstate;
    return !state.is_finished;
}

std::string PhysicalSort::ParamsToString() const
{
    string params = "sort-param: ";
	for (auto & order : orders) {
		params += order.ToString();
		params += " / ";
	}
	return params;
}

std::string PhysicalSort::ToString() const
{
    return "Sort";
}

}  // namespace duckdb