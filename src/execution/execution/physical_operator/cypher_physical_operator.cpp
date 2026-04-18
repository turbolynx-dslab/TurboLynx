#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "common/enums/operator_result_type.hpp"
#include "common/exception.hpp"
#include "common/types/schemaless_data_chunk.hpp"
#include "execution/execution_context.hpp"
#include "main/client_context.hpp"

namespace duckdb {
}
namespace turbolynx {
}
namespace duckdb {
    using namespace turbolynx;
}
namespace turbolynx {
using namespace duckdb;
idx_t CypherPhysicalOperator::operator_version = 0;

void CypherPhysicalOperator::GetData(ExecutionContext &context,
                                     DataChunk &chunk,
                                     LocalSourceState &lstate) const
{
    throw InternalException("Calling GetData on a node that is not a source!");
}
void CypherPhysicalOperator::GetData(ExecutionContext &context,
                                     DataChunk &chunk,
                                     GlobalSourceState &gstate,
                                     LocalSourceState &lstate) const
{
    // Default: delegate to non-parallel GetData
    GetData(context, chunk, lstate);
}

void CypherPhysicalOperator::GetData(ExecutionContext &context,
                                     DataChunk &chunk, LocalSourceState &lstate,
                                     LocalSinkState &sink_state) const
{
    throw InternalException("Calling GetData on a node that is not a source!");
}

void CypherPhysicalOperator::GetData(ExecutionContext &context,
                                     DataChunk &chunk,
                                     GlobalSourceState &gstate,
                                     LocalSourceState &lstate,
                                     LocalSinkState &child_sink_state) const
{
    // Default: delegate to non-parallel non-leaf source GetData
    GetData(context, chunk, lstate, child_sink_state);
}

bool CypherPhysicalOperator::IsSourceDataRemaining(
    GlobalSourceState &gstate, LocalSourceState &lstate,
    LocalSinkState &child_sink_state) const
{
    return IsSourceDataRemaining(lstate, child_sink_state);
}

bool CypherPhysicalOperator::IsSourceDataRemaining(
    GlobalSourceState &gstate, LocalSourceState &lstate) const
{
    // Default: delegate to non-parallel version
    return IsSourceDataRemaining(lstate);
}

unique_ptr<LocalSourceState> CypherPhysicalOperator::GetLocalSourceState(
    ExecutionContext &context) const
{
    return make_unique<LocalSourceState>();
}

unique_ptr<LocalSourceState> CypherPhysicalOperator::GetLocalSourceStateParallel(
    ExecutionContext &context) const
{
    return GetLocalSourceState(context);
}

unique_ptr<GlobalSourceState> CypherPhysicalOperator::GetGlobalSourceState(
    ClientContext &context) const
{
    return make_unique<GlobalSourceState>();
}

SinkResultType CypherPhysicalOperator::Sink(ExecutionContext &context,
                                            DataChunk &input,
                                            LocalSinkState &lstate) const
{
    throw InternalException("Calling Sink on a node that is not a sink!");
}

SinkResultType CypherPhysicalOperator::Sink(ExecutionContext &context,
                                            GlobalSinkState &gstate,
                                            LocalSinkState &lstate,
                                            DataChunk &input) const
{
    // Default: delegate to non-parallel Sink
    return Sink(context, input, lstate);
}

unique_ptr<LocalSinkState> CypherPhysicalOperator::GetLocalSinkState(
    ExecutionContext &context) const
{
    return make_unique<LocalSinkState>();
}

unique_ptr<GlobalSinkState> CypherPhysicalOperator::GetGlobalSinkState(
    ClientContext &context) const
{
    return make_unique<GlobalSinkState>();
}

bool CypherPhysicalOperator::IsSourceDataRemaining(
    LocalSourceState &lstate) const
{
    throw InternalException(
        "Calling IsSourceDataRemaining on a node that is not a source!");
}

bool CypherPhysicalOperator::IsSourceDataRemaining(
    LocalSourceState &lstate, LocalSinkState &sink_state) const
{
    throw InternalException(
        "Calling IsSourceDataRemaining on a node that is not a source!");
}

void CypherPhysicalOperator::Combine(ExecutionContext &context,
                                     LocalSinkState &lstate) const
{
    // nothing
}

void CypherPhysicalOperator::Combine(ExecutionContext &context,
                                     GlobalSinkState &gstate,
                                     LocalSinkState &lstate) const
{
    // Default: delegate to non-parallel Combine
    Combine(context, lstate);
}

SinkFinalizeType CypherPhysicalOperator::Finalize(ExecutionContext &context,
                                                   GlobalSinkState &gstate) const
{
    return SinkFinalizeType::READY;
}

OperatorResultType CypherPhysicalOperator::Execute(ExecutionContext &context,
                                                   DataChunk &input,
                                                   DataChunk &chunk,
                                                   OperatorState &state) const
{
    throw InternalException(
        "Calling Execute on a node that is not an operator!");
}

OperatorResultType CypherPhysicalOperator::Execute(
    ExecutionContext &context, DataChunk &input,
    vector<unique_ptr<DataChunk>> &chunks, OperatorState &state,
    idx_t &output_chunk_idx) const
{
    throw InternalException(
        "Calling Execute on a node that is not an operator!");
}

OperatorResultType CypherPhysicalOperator::Execute(
    ExecutionContext &context, DataChunk &input, DataChunk &chunk,
    OperatorState &state, LocalSinkState &sink_state) const
{
    throw InternalException(
        "Calling Execute on a node that is not an operator!");
}

unique_ptr<OperatorState> CypherPhysicalOperator::GetOperatorState(
    ExecutionContext &context) const
{
    return make_unique<OperatorState>();
}

DataChunk &CypherPhysicalOperator::GetLastSinkedData(
    LocalSinkState &lstate) const
{
    throw InternalException(
        "Calling GetLastSinkedData on a node that is not a sink!");
}

void CypherPhysicalOperator::InitializeOutputChunk(DataChunk &output_chunk)
{
    // nothing
}

void CypherPhysicalOperator::InitializeOutputChunks(
    std::vector<unique_ptr<DataChunk>> &output_chunks, Schema &output_schema,
    idx_t idx)
{
    // auto opOutputChunk = std::make_unique<SchemalessDataChunk>();
    auto opOutputChunk = std::make_unique<DataChunk>();
    opOutputChunk->Initialize(output_schema.getStoredTypes());
    output_chunks.push_back(std::move(opOutputChunk));
}

const vector<LogicalType> &CypherPhysicalOperator::GetTypes()
{
    return types;
}

} // namespace turbolynx