#include "execution/pipeline_task.hpp"
#include "execution/cypher_pipeline_executor.hpp"
#include "main/client_context.hpp"
#include "execution/schema_flow_graph.hpp"

namespace duckdb {

PipelineTask::PipelineTask(CypherPipeline &pipeline_p,
                           ExecutionContext &context_p,
                           GlobalSourceState &global_source_p,
                           GlobalSinkState &global_sink_p,
                           std::map<CypherPhysicalOperator *, CypherPipelineExecutor *> &deps_p,
                           LocalSinkState *child_sink_state_p)
    : ExecutorTask(*context_p.client),
      pipeline(pipeline_p),
      exec_context(context_p),
      global_source(global_source_p),
      global_sink(global_sink_p),
      child_sink_state(child_sink_state_p),
      deps(deps_p),
      thread_context(*context_p.client)
{
    // Initialize per-thread local states. When the source is a non-leaf
    // operator (i.e. has a child pipeline as its data feed), it may need a
    // parallel-shaped LocalSourceState distinct from the sequential one.
    if (child_sink_state != nullptr) {
        local_source_state = pipeline.GetSource()->GetLocalSourceStateParallel(exec_context);
    } else {
        local_source_state = pipeline.GetSource()->GetLocalSourceState(exec_context);
    }
    local_sink_state = pipeline.GetSink()->GetLocalSinkState(exec_context);

    for (auto op : pipeline.GetOperators()) {
        local_operator_states.push_back(op->GetOperatorState(exec_context));
    }

    // Initialize intermediate chunks for each operator (source + middle ops, excluding sink)
    // Use the operator's InitializeOutputChunks override so operators that
    // emit empty-type chunks (e.g. PhysicalIdSeek for EXISTS-decorrelated
    // subqueries that ORCA pruned to zero output columns) can substitute a
    // dummy column. The sequential CypherPipelineExecutor uses the same hook.
    for (int i = 0; i < pipeline.pipelineLength - 1; i++) {
        auto *op = pipeline.GetIdxOperator(i);
        std::vector<unique_ptr<DataChunk>> tmp;
        op->InitializeOutputChunks(tmp, op->schema, 0);
        D_ASSERT(!tmp.empty());
        tmp.front()->SetSchemaIdx(0);
        intermediate_chunks.push_back(std::move(tmp.front()));
    }
}

TaskExecutionResult PipelineTask::ExecuteTask(TaskExecutionMode mode)
{
    // Main loop: fetch chunks from source until exhausted
    while (true) {
        auto &source_chunk = *intermediate_chunks[0];
        source_chunk.Reset();

        // Fetch from source using global state for parallel work distribution.
        // Non-leaf sources (e.g. HashAgg-as-source) additionally need the
        // bridged sink state from the previous pipeline.
        bool source_exhausted;
        if (child_sink_state != nullptr) {
            pipeline.GetSource()->GetData(exec_context, source_chunk,
                                          global_source, *local_source_state,
                                          *child_sink_state);
            source_exhausted = !pipeline.GetSource()->IsSourceDataRemaining(
                global_source, *local_source_state, *child_sink_state);
        } else {
            pipeline.GetSource()->GetData(exec_context, source_chunk,
                                          global_source, *local_source_state);
            source_exhausted = !pipeline.GetSource()->IsSourceDataRemaining(
                global_source, *local_source_state);
        }

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
        if (source_chunk.size() > 0) {
            pipeline.GetSink()->Sink(exec_context, global_sink,
                                     *local_sink_state, source_chunk);
        }
        return OperatorResultType::NEED_MORE_INPUT;
    }

    // Drain the entire pipe for this source chunk, including any operators
    // that produce HAVE_MORE_OUTPUT. Modeled after sequential ExecutePipe:
    // when an operator returns HMO, push its index; when sink reached, restart
    // the pipe from the top of the stack (= deepest pending op) so subsequent
    // operators see the freshly-drained output of the upstream HMO operator.
    do {
        idx_t start_idx = 0;
        if (!in_process_operators.empty()) {
            start_idx = in_process_operators.top();
            in_process_operators.pop();
        }

        // intermediate_chunks[op_idx] holds op_idx's input (= upstream's output,
        // or the source chunk when op_idx == 0). It is not overwritten between
        // executions of op_idx itself, so re-running the popped op uses the same
        // input it saw the first time.
        DataChunk *prev_output = intermediate_chunks[start_idx].get();

        for (idx_t op_idx = start_idx; op_idx < operators.size(); op_idx++) {
            auto &output_chunk = *intermediate_chunks[op_idx + 1];
            output_chunk.Reset();

            OperatorResultType result;
            // Operators that are also sinks (e.g., IdSeek referencing another
            // pipeline's build side) need the dependent pipeline's sink state.
            auto dep_it = deps.find(operators[op_idx]);
            if (operators[op_idx]->IsSink() && dep_it != deps.end()) {
                result = operators[op_idx]->Execute(
                    exec_context, *prev_output, output_chunk,
                    *local_operator_states[op_idx],
                    dep_it->second->GetSinkState());
            } else {
                result = operators[op_idx]->Execute(
                    exec_context, *prev_output, output_chunk,
                    *local_operator_states[op_idx]);
            }

            if (result == OperatorResultType::FINISHED) {
                while (!in_process_operators.empty()) in_process_operators.pop();
                return OperatorResultType::FINISHED;
            }

            if (result == OperatorResultType::HAVE_MORE_OUTPUT) {
                in_process_operators.push(op_idx);
            }

            prev_output = &output_chunk;

            // If this op produced no rows, downstream ops have nothing to do.
            // Skip to sink (which will be a no-op for empty chunks) and let the
            // outer do/while restart from the next pending HMO operator.
            if (prev_output->size() == 0) {
                break;
            }
        }

        // Reached sink (or short-circuited on empty intermediate output)
        if (prev_output->size() > 0) {
            pipeline.GetSink()->Sink(exec_context, global_sink,
                                     *local_sink_state, *prev_output);
        }
    } while (!in_process_operators.empty());

    return OperatorResultType::NEED_MORE_INPUT;
}

} // namespace duckdb
