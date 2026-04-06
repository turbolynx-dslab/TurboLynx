#include "execution/pipeline_task.hpp"
#include "execution/cypher_pipeline_executor.hpp"
#include "main/client_context.hpp"
#include "execution/schema_flow_graph.hpp"

namespace duckdb {

PipelineTask::PipelineTask(CypherPipeline &pipeline_p,
                           ExecutionContext &context_p,
                           GlobalSourceState &global_source_p,
                           GlobalSinkState &global_sink_p,
                           std::map<CypherPhysicalOperator *, CypherPipelineExecutor *> &deps_p)
    : ExecutorTask(*context_p.client),
      pipeline(pipeline_p),
      exec_context(context_p),
      global_source(global_source_p),
      global_sink(global_sink_p),
      deps(deps_p),
      thread_context(*context_p.client)
{
    // Initialize per-thread local states
    local_source_state = pipeline.GetSource()->GetLocalSourceState(exec_context);
    local_sink_state = pipeline.GetSink()->GetLocalSinkState(exec_context);

    for (auto op : pipeline.GetOperators()) {
        local_operator_states.push_back(op->GetOperatorState(exec_context));
    }

    // Initialize intermediate chunks for each operator (source + middle ops, excluding sink)
    for (int i = 0; i < pipeline.pipelineLength - 1; i++) {
        auto chunk = make_unique<DataChunk>();
        chunk->Initialize(pipeline.GetIdxOperator(i)->GetTypes());
        chunk->SetSchemaIdx(0);
        intermediate_chunks.push_back(std::move(chunk));
    }
}

TaskExecutionResult PipelineTask::ExecuteTask(TaskExecutionMode mode)
{
    // Main loop: fetch chunks from source until exhausted
    while (true) {
        auto &source_chunk = *intermediate_chunks[0];
        source_chunk.Reset();

        // Fetch from source using global state for parallel work distribution
        pipeline.GetSource()->GetData(exec_context, source_chunk,
                                      global_source, *local_source_state);

        bool source_exhausted = !pipeline.GetSource()->IsSourceDataRemaining(
            global_source, *local_source_state);

        if (source_chunk.size() > 0) {
            auto result = ProcessChunk(source_chunk);
            if (result == OperatorResultType::FINISHED) {
                break;
            }
        }

        if (source_exhausted) {
            break;
        }

        if (mode == TaskExecutionMode::PROCESS_PARTIAL) {
            return TaskExecutionResult::TASK_NOT_FINISHED;
        }
    }

    // Note: Combine is NOT called here — the executor handles it after all tasks complete.
    // This allows the executor to control the combine order and state transfer.

    return TaskExecutionResult::TASK_FINISHED;
}

OperatorResultType PipelineTask::ProcessChunk(DataChunk &source_chunk)
{
    auto operators = pipeline.GetOperators();

    if (operators.empty()) {
        // No intermediate operators — sink directly from source
        pipeline.GetSink()->Sink(exec_context, global_sink,
                                 *local_sink_state, source_chunk);
        return OperatorResultType::NEED_MORE_INPUT;
    }

    // Push through intermediate operators
    DataChunk *current_input = &source_chunk;

    for (idx_t op_idx = 0; op_idx < operators.size(); op_idx++) {
        auto &output_chunk = *intermediate_chunks[op_idx + 1];
        output_chunk.Reset();

        OperatorResultType result;
        // Operators that are also sinks (e.g., IdSeek referencing another pipeline's
        // build side) need the dependent pipeline's sink state.
        auto dep_it = deps.find(operators[op_idx]);
        if (operators[op_idx]->IsSink() && dep_it != deps.end()) {
            result = operators[op_idx]->Execute(
                exec_context, *current_input, output_chunk,
                *local_operator_states[op_idx],
                dep_it->second->GetSinkState());
        } else {
            result = operators[op_idx]->Execute(
                exec_context, *current_input, output_chunk,
                *local_operator_states[op_idx]);
        }

        if (result == OperatorResultType::FINISHED) {
            return OperatorResultType::FINISHED;
        }

        current_input = &output_chunk;

        // Handle HAVE_MORE_OUTPUT by draining the operator
        while (result == OperatorResultType::HAVE_MORE_OUTPUT) {
            // Sink the current output
            if (current_input->size() > 0) {
                pipeline.GetSink()->Sink(exec_context, global_sink,
                                         *local_sink_state, *current_input);
            }

            output_chunk.Reset();
            if (operators[op_idx]->IsSink() && dep_it != deps.end()) {
                result = operators[op_idx]->Execute(
                    exec_context, *current_input, output_chunk,
                    *local_operator_states[op_idx],
                    dep_it->second->GetSinkState());
            } else {
                result = operators[op_idx]->Execute(
                    exec_context, *current_input, output_chunk,
                    *local_operator_states[op_idx]);
            }

            if (result == OperatorResultType::FINISHED) {
                return OperatorResultType::FINISHED;
            }
            current_input = &output_chunk;
        }
    }

    // Sink the final output
    if (current_input->size() > 0) {
        pipeline.GetSink()->Sink(exec_context, global_sink,
                                 *local_sink_state, *current_input);
    }

    return OperatorResultType::NEED_MORE_INPUT;
}

} // namespace duckdb
