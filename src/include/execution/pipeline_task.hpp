#pragma once

#include "parallel/task.hpp"
#include "execution/cypher_pipeline.hpp"
#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "execution/physical_operator/physical_operator.hpp"
#include "execution/execution_context.hpp"
#include "parallel/thread_context.hpp"
#include "common/types/data_chunk.hpp"

#include <map>
#include <stack>

namespace duckdb {

class ClientContext;
class CypherPipelineExecutor;

//! PipelineTask executes a single pipeline segment on one thread.
//! Multiple PipelineTasks can run in parallel on the same pipeline,
//! each with its own LocalState instances.
class PipelineTask : public ExecutorTask {
public:
    PipelineTask(CypherPipeline &pipeline_p, ExecutionContext &context_p,
                 GlobalSourceState &global_source_p,
                 GlobalSinkState &global_sink_p,
                 std::map<CypherPhysicalOperator *, CypherPipelineExecutor *> &deps_p,
                 LocalSinkState *child_sink_state_p = nullptr);

    TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override;

    //! Transfer ownership of local sink state to caller (for downstream pipeline compatibility)
    unique_ptr<LocalSinkState> TakeLocalSinkState() { return std::move(local_sink_state); }

private:
    //! Process a single chunk through the pipeline operators and sink it
    OperatorResultType ProcessChunk(DataChunk &source_chunk);

    CypherPipeline &pipeline;
    ExecutionContext &exec_context;
    GlobalSourceState &global_source;
    GlobalSinkState &global_sink;
    //! When the source is a non-leaf operator (e.g. HashAgg-as-source) the
    //! previous pipeline executor's local_sink_state is the bridge that
    //! exposes the finalized sink data to this pipeline. nullptr for leaf
    //! sources (Scan, etc.).
    LocalSinkState *child_sink_state;

    //! Dependent executors — operators that need another pipeline's sink state
    std::map<CypherPhysicalOperator *, CypherPipelineExecutor *> &deps;

    //! Thread-local context for profiling
    ThreadContext thread_context;

    //! Per-thread local states
    unique_ptr<LocalSourceState> local_source_state;
    unique_ptr<LocalSinkState> local_sink_state;
    vector<unique_ptr<OperatorState>> local_operator_states;

    //! Intermediate output chunks for each operator
    vector<unique_ptr<DataChunk>> intermediate_chunks;

    //! Stack of operator indices that have pending HAVE_MORE_OUTPUT.
    //! Mirrors CypherPipelineExecutor::in_process_operators for parallel pipe drain.
    std::stack<idx_t> in_process_operators;
};

} // namespace duckdb
