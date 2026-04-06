#pragma once

#include "parallel/task.hpp"
#include "execution/cypher_pipeline.hpp"
#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "execution/physical_operator/physical_operator.hpp"
#include "execution/execution_context.hpp"
#include "parallel/thread_context.hpp"
#include "common/types/data_chunk.hpp"

#include <map>

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
                 std::map<CypherPhysicalOperator *, CypherPipelineExecutor *> &deps_p);

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
};

} // namespace duckdb
