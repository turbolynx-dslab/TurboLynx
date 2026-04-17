#include "execution/physical_operator/physical_produce_results.hpp"
#include "common/types/rowcol_type.hpp"
#include "common/types/chunk_collection.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "main/client_context.hpp"

#include <algorithm>
#include <cassert>
#include <mutex>

#include "icecream.hpp"

namespace duckdb {

//! Global state for ProduceResults — collects results from multiple threads
class ProduceResultsGlobalState : public GlobalSinkState {
public:
    mutex result_lock;
    vector<shared_ptr<DataChunk>> all_result_chunks;
};

class ProduceResultsState : public LocalSinkState {
   public:
    explicit ProduceResultsState(std::vector<uint64_t> projection_mapping, std::vector<std::vector<uint64_t>> projection_mappings)
        : projection_mapping(projection_mapping), projection_mappings(projection_mappings)
    {
        const size_t INITIAL_SIZE = 10000;
        resultChunks.reserve(INITIAL_SIZE);
        emptyChunk = make_shared<DataChunk>();
        size_t num_columns = projection_mapping.size() != 0 ? 
            projection_mapping.size() : projection_mappings[0].size();
        vector<LogicalType> any_types(num_columns, LogicalTypeId::SQLNULL);
        emptyChunk->InitializeEmpty(any_types);
    }

    ~ProduceResultsState() override
    {
        for (auto &chunk : resultChunks) {
            chunk.reset();
        }
    }

   public:
    vector<shared_ptr<DataChunk>> resultChunks;
    shared_ptr<DataChunk> emptyChunk;

    std::vector<uint64_t> projection_mapping;
    std::vector<std::vector<uint64_t>> projection_mappings;
};

unique_ptr<LocalSinkState> PhysicalProduceResults::GetLocalSinkState(
    ExecutionContext &context) const
{
    return make_unique<ProduceResultsState>(projection_mapping, projection_mappings);
}

unique_ptr<GlobalSinkState> PhysicalProduceResults::GetGlobalSinkState(
    ClientContext &context) const
{
    return make_unique<ProduceResultsGlobalState>();
}

SinkResultType PhysicalProduceResults::Sink(ExecutionContext &context,
                                            GlobalSinkState &gstate,
                                            LocalSinkState &lstate,
                                            DataChunk &input) const
{
    // Delegate to the regular Sink (which stores in thread-local state)
    return Sink(context, input, lstate);
}

SinkResultType PhysicalProduceResults::Sink(ExecutionContext &context,
                                            DataChunk &input,
                                            LocalSinkState &lstate) const
{
    auto &state = (ProduceResultsState &)lstate;

    // jhha: sugar logic for single column projection speed up
    if (input.IsAllInvalid()) {
        state.emptyChunk->IncreaseSize(input.size());
        return SinkResultType::NEED_MORE_INPUT;
    }

    auto copyChunk = make_shared<DataChunk>();
    if (projection_mapping.size() == 0 &&
        projection_mappings.size() == 0) {  // projection mapping not provided
        copyChunk->Initialize(types, input, projection_mappings, input.size());
        input.Copy(*copyChunk, 0);
    }
    else {  // projection mapping provided
        if (projection_mapping.size() != 0) {
            copyChunk->Initialize(types, input, projection_mappings, input.size());
            for (idx_t idx = 0; idx < copyChunk->ColumnCount(); idx++) {
                VectorOperations::Copy(input.data[projection_mapping[idx]],
                                       copyChunk->data[idx], input.size(), 0,
                                       0);
            }
        }
        else if (projection_mappings.size() != 0) {
            bool copy_rowstore = false;
            vector<bool> column_has_rowvec;
            vector<vector<uint32_t>> rowstore_idx_list_per_depth;
            IdentifyRowVectors(input, types.size(),
                            column_has_rowvec, rowstore_idx_list_per_depth);
            copyChunk->Initialize(types, input,
                                projection_mappings, column_has_rowvec,
                                input.size());
            // idx_t schema_idx = input.GetSchemaIdx();
            for (idx_t idx = 0; idx < copyChunk->ColumnCount(); idx++) {
                if (projection_mappings[0][idx] ==
                    std::numeric_limits<uint8_t>::max()) {
                    copyChunk->data[idx].SetIsValid(false);
                    continue;
                }
                if (column_has_rowvec[idx]) {
                    copy_rowstore = true;
                    continue;
                } else {
                    VectorOperations::Copy(
                        input.data[projection_mappings[0][idx]],
                        copyChunk->data[idx], input.size(), 0, 0);
                }
            }
            if (copy_rowstore) {
                CopyRowVectors(input, *copyChunk, column_has_rowvec,
                            rowstore_idx_list_per_depth);
            }
            copyChunk->SetSchemaIdx(0);
        }
        else {
            D_ASSERT(false);
        }
        copyChunk->SetCardinality(input.size());
    }
    state.resultChunks.push_back(move(copyChunk));

    return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalProduceResults::Combine(ExecutionContext &context,
                                     LocalSinkState &lstate) const
{
    auto &state = (ProduceResultsState &)lstate;
    if (state.emptyChunk->size() > 0) state.resultChunks.push_back(state.emptyChunk);
    context.query_results = &(((ProduceResultsState &)lstate).resultChunks);
    // context.query_results = ((ProduceResultsState &)lstate).resultCollection.ChunksUnsafe();
    // printf("num_nulls = %ld\n", num_nulls);
    return;
}

void PhysicalProduceResults::Combine(ExecutionContext &context,
                                     GlobalSinkState &gstate,
                                     LocalSinkState &lstate) const
{
    auto &state = (ProduceResultsState &)lstate;
    auto &global_state = (ProduceResultsGlobalState &)gstate;

    if (state.emptyChunk->size() > 0) {
        state.resultChunks.push_back(state.emptyChunk);
    }

    // Merge thread-local results into global state (thread-safe)
    lock_guard<mutex> lock(global_state.result_lock);
    for (auto &chunk : state.resultChunks) {
        global_state.all_result_chunks.push_back(std::move(chunk));
    }
    state.resultChunks.clear();
}

SinkFinalizeType PhysicalProduceResults::Finalize(ExecutionContext &context,
                                                   GlobalSinkState &gstate) const
{
    auto &global_state = (ProduceResultsGlobalState &)gstate;
    // Set query results to the global combined results
    context.query_results = &global_state.all_result_chunks;
    return SinkFinalizeType::READY;
}

DataChunk &PhysicalProduceResults::GetLastSinkedData(
    LocalSinkState &lstate) const
{
    auto &state = (ProduceResultsState &)lstate;
    return *state.resultChunks.back();
    // return state.resultCollection.GetChunk(state.resultCollection.ChunkCount() - 1);
}

void PhysicalProduceResults::IdentifyRowVectors(
    DataChunk &input, idx_t num_columns, vector<bool> &column_has_rowvec,
    vector<vector<uint32_t>> &rowstore_idx_list_per_depth) const
{
    column_has_rowvec.reserve(num_columns);
    rowstore_idx_list_per_depth.reserve(3);
    for (idx_t idx = 0; idx < num_columns; idx++) {
        int depth = 0;
        bool has_rowvec = IdentifyIfBaseVectorTypeIsRowVector(
            input.data[projection_mappings[0][idx]], depth);
        if (has_rowvec) {
            if (rowstore_idx_list_per_depth.size() <= depth) {
                rowstore_idx_list_per_depth.resize(depth + 1);
            }
            rowstore_idx_list_per_depth[depth].push_back(idx);
        }
        column_has_rowvec.push_back(has_rowvec);
    }
}

bool PhysicalProduceResults::IdentifyIfBaseVectorTypeIsRowVector(
    const Vector &vector, int &depth) const
{
    switch (vector.GetVectorType()) {
        case VectorType::DICTIONARY_VECTOR: {
            // dictionary: continue into child with selection vector
            auto &child = DictionaryVector::Child(vector);
            return IdentifyIfBaseVectorTypeIsRowVector(child, ++depth);
        }
        case VectorType::CONSTANT_VECTOR: {
            return false;
        }
        case VectorType::FLAT_VECTOR: {
            return false;
        }
        // case VectorType::SEQUENCE_VECTOR: {
        // 	break;
        // }
        case VectorType::ROW_VECTOR: {
            return true;
        }
        default:
            throw NotImplementedException(
                "FIXME: unimplemented vector type for "
                "IdentifyIfBaseVectorTypeIsRowVector");
    }
}

void PhysicalProduceResults::CopyRowVectors(
    DataChunk &input, DataChunk &output, vector<bool> &column_has_rowvec,
    vector<vector<uint32_t>> &rowstore_idx_list_per_depth) const
{
    for (idx_t depth = 0; depth < rowstore_idx_list_per_depth.size(); depth++) {
        if (rowstore_idx_list_per_depth[depth].size() == 0) {
            continue;
        }
        vector<uint32_t> &rowstore_idx_list =
            rowstore_idx_list_per_depth[depth];
        D_ASSERT(rowstore_idx_list.size() > 0);
        output.InitializeRowColumn(rowstore_idx_list, input.size());
        SelectionVector *sel = nullptr;
        Vector &rowcol = output.data[rowstore_idx_list[0]];
        Vector &input_rowcol = GetRowVectorAtSpecificDepth(
            input.data[projection_mappings[0][rowstore_idx_list[0]]], depth, 0,
            input.size(), sel);
        rowcol_t *rowcol_arr = (rowcol_t *)rowcol.GetData();
        rowcol_t *input_rowcol_arr = (rowcol_t *)input_rowcol.GetData();

        if (sel == nullptr) {
            for (idx_t i = 0; i < input.size(); i++) {
                rowcol_arr[i] = input_rowcol_arr[i];
            }
        }
        else {
            for (idx_t i = 0; i < input.size(); i++) {
                auto source_idx = sel->get_index(i);
                rowcol_arr[i] = input_rowcol_arr[source_idx];
            }
        }

        output.AssignRowMajorStore(rowstore_idx_list,
                                   input_rowcol.GetRowMajorStoreBufferPtr());
        for (idx_t i = 0; i < rowstore_idx_list.size(); i++) {
            output.data[rowstore_idx_list[i]].SetRowColIdx(
                input.data[projection_mappings[0][rowstore_idx_list[i]]]
                    .GetRowColIdx());
        }
    }
}

Vector &PhysicalProduceResults::GetRowVectorAtSpecificDepth(
    Vector &input, int depth, int current_depth, idx_t source_count,
    SelectionVector *&target_sel) const
{
    if (current_depth == depth) {
        D_ASSERT(input.GetVectorType() == VectorType::ROW_VECTOR);
        return input;
    }
    else {
        D_ASSERT(input.GetVectorType() == VectorType::DICTIONARY_VECTOR);
        auto &child = DictionaryVector::Child(input);
        auto &dict_sel = DictionaryVector::SelVector(input);
        // merge the selection vectors and verify the child
        return GetRowVectorAtSpecificDepth(child, dict_sel, depth,
                                           current_depth + 1, source_count,
                                           target_sel);
    }
}

Vector &PhysicalProduceResults::GetRowVectorAtSpecificDepth(
    Vector &input, const SelectionVector &sel_p, int depth, int current_depth,
    idx_t source_count, SelectionVector *&target_sel) const
{
    if (current_depth == depth) {
        D_ASSERT(input.GetVectorType() == VectorType::ROW_VECTOR);
        target_sel = new SelectionVector(sel_p);
        return input;
    }
    else {
        const SelectionVector *sel = &sel_p;
        D_ASSERT(input.GetVectorType() == VectorType::DICTIONARY_VECTOR);
        auto &child = DictionaryVector::Child(input);
        auto &dict_sel = DictionaryVector::SelVector(input);
        // merge the selection vectors and verify the child
        auto new_buffer = dict_sel.Slice(*sel, source_count);
        SelectionVector merged_sel(new_buffer);
        return GetRowVectorAtSpecificDepth(child, merged_sel, depth,
                                           current_depth + 1, source_count,
                                           target_sel);
    }
}


void PhysicalProduceResults::DetermineResultTypes() 
{
    vector<LogicalType> new_types;
    if (projection_mapping.size() == 0 && projection_mappings.size() == 0) {
        new_types = types;
    }
    else {
        if (projection_mapping.size() != 0) {
            for (auto &target : projection_mapping) {
                new_types.push_back(types[target]);
            }
        }
        else if (projection_mappings.size() != 0) {
            idx_t schema_idx = 0;
            for (auto &target : projection_mappings[schema_idx]) {
                new_types.push_back(types[target]);
            }
        }
        else {
            D_ASSERT(false);
        }
    }
    types = new_types;
}

std::string PhysicalProduceResults::ParamsToString() const
{
    return "getresults-param";
}

std::string PhysicalProduceResults::ToString() const
{
    return "ProduceResults";
}

}  // namespace duckdb
