//===----------------------------------------------------------------------===//
//                         DuckDB
//
// src/execution/execution/physical_operator/physical_hash_aggregate.cpp
//
//
//===----------------------------------------------------------------------===//

#include "execution/physical_operator/physical_hash_aggregate.hpp"

#include "catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "execution/aggregate_hashtable.hpp"
#include "execution/partitionable_hashtable.hpp"
#include "main/client_context.hpp"
#include "parallel/pipeline.hpp"
#include "parallel/task_scheduler.hpp"
#include "planner/expression/bound_aggregate_expression.hpp"
#include "planner/expression/bound_constant_expression.hpp"
#include "parallel/event.hpp"
#include "common/atomic.hpp"

#include "execution/execution_context.hpp"

#include "planner/expression/bound_reference_expression.hpp"

#include "execution/physical_operator/physical_operator.hpp"

#include "icecream.hpp"

namespace duckdb {

PhysicalHashAggregate::PhysicalHashAggregate(
    Schema &sch, vector<uint64_t> &output_projection_mapping,
    vector<unique_ptr<Expression>> expressions,
    vector<uint32_t> &grouping_key_idxs_p)
    : PhysicalHashAggregate(sch, output_projection_mapping, move(expressions),
                            {}, grouping_key_idxs_p)
{}
PhysicalHashAggregate::PhysicalHashAggregate(
    Schema &sch, vector<uint64_t> &output_projection_mapping,
    vector<unique_ptr<Expression>> expressions,
    vector<unique_ptr<Expression>> groups_p,
    vector<uint32_t> &grouping_key_idxs_p)
    : PhysicalHashAggregate(sch, output_projection_mapping, move(expressions),
                            move(groups_p), {}, {}, grouping_key_idxs_p)
{}
PhysicalHashAggregate::PhysicalHashAggregate(
    Schema &sch, vector<uint64_t> &output_projection_mapping,
    vector<unique_ptr<Expression>> expressions,
    vector<unique_ptr<Expression>> groups_p,
    vector<GroupingSet> grouping_sets_p,
    vector<vector<idx_t>> grouping_functions_p,
    vector<uint32_t> &grouping_key_idxs_p)
    : CypherPhysicalOperator(PhysicalOperatorType::HASH_AGGREGATE, sch),
      groups(move(groups_p)),
      grouping_sets(move(grouping_sets_p)),
      grouping_functions(move(grouping_functions_p)),
      all_combinable(true),
      any_distinct(false),
      output_projection_mapping(output_projection_mapping),
      grouping_key_idxs(move(grouping_key_idxs_p))
{
    // TODO no support for custom grouping sets and grouping functions
    D_ASSERT(grouping_sets.size() == 0);
    D_ASSERT(grouping_functions.size() == 0);

    // get a list of all aggregates to be computed
    for (auto &expr : groups) {
        group_types.push_back(expr->return_type);
    }
    if (grouping_sets.empty()) {
        GroupingSet set;
        for (idx_t i = 0; i < group_types.size(); i++) {
            set.insert(i);
        }
        grouping_sets.push_back(move(set));
    }

    vector<LogicalType> payload_types_filters;
    for (auto &expr : expressions) {
        D_ASSERT(expr->expression_class == ExpressionClass::BOUND_AGGREGATE);
        D_ASSERT(expr->IsAggregate());
        auto &aggr = (BoundAggregateExpression &)*expr;
        bindings.push_back(&aggr);

        if (aggr.distinct) {
            any_distinct = true;
        }

        aggregate_return_types.push_back(aggr.return_type);
        for (auto &child : aggr.children) {
            payload_types.push_back(child->return_type);
        }
        if (aggr.filter) {
            payload_types_filters.push_back(aggr.filter->return_type);
        }
        if (!aggr.function.combine) {
            all_combinable = false;
        }
        aggregates.push_back(move(expr));
    }

    for (const auto &pay_filters : payload_types_filters) {
        payload_types.push_back(pay_filters);
    }

    // filter_indexes must be pre-built, not lazily instantiated in parallel...
    idx_t aggregate_input_idx = 0;
    for (auto &aggregate : aggregates) {
        auto &aggr = (BoundAggregateExpression &)*aggregate;
        aggregate_input_idx += aggr.children.size();
    }
    for (auto &aggregate : aggregates) {
        auto &aggr = (BoundAggregateExpression &)*aggregate;
        if (aggr.filter) {
            auto &bound_ref_expr = (BoundReferenceExpression &)*aggr.filter;
            auto it = filter_indexes.find(aggr.filter.get());
            if (it == filter_indexes.end()) {
                filter_indexes[aggr.filter.get()] = bound_ref_expr.index;
                bound_ref_expr.index = aggregate_input_idx++;
            }
            else {
                ++aggregate_input_idx;
            }
        }
    }

    for (auto &grouping_set : grouping_sets) {
        radix_tables.emplace_back(grouping_set, *this);
    }
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class HashAggregateGlobalSinkState : public GlobalSinkState {
public:
	HashAggregateGlobalSinkState(const PhysicalHashAggregate &op, ClientContext &context) {
		radix_states.reserve(op.radix_tables.size());
		for (auto &rt : op.radix_tables) {
			radix_states.push_back(rt.GetGlobalSinkState(context));
		}
	}

	vector<unique_ptr<GlobalSinkState>> radix_states;
};

class HashAggregateLocalSinkState : public LocalSinkState {
public:
	HashAggregateLocalSinkState(const PhysicalHashAggregate &op, ExecutionContext &context) {
		if (!op.payload_types.empty()) {
			aggregate_input_chunk.InitializeEmpty(op.payload_types);
		}

		local_radix_states.reserve(op.radix_tables.size());
		for (auto &rt : op.radix_tables) {
			local_radix_states.push_back(rt.GetLocalSinkState(context));
		}

		// Per-thread global states (used by single-thread path for backward compat)
		global_radix_states.reserve(op.radix_tables.size());
		for (auto &rt : op.radix_tables) {
			global_radix_states.push_back(rt.GetGlobalSinkState(*(context.client)));
		}
	}

	DataChunk aggregate_input_chunk;

	vector<unique_ptr<LocalSinkState>> local_radix_states;
	vector<unique_ptr<GlobalSinkState>> global_radix_states;
};

// void PhysicalHashAggregate::SetMultiScan(GlobalSinkState &state) {
// 	auto &gstate = (HashAggregateGlobalState &)state;
// 	for (auto &radix_state : gstate.radix_states) {
// 		RadixPartitionedHashTable::SetMultiScan(*radix_state);
// 	}
// }

unique_ptr<GlobalSinkState> PhysicalHashAggregate::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<HashAggregateGlobalSinkState>(*this, context);
}

unique_ptr<LocalSinkState> PhysicalHashAggregate::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<HashAggregateLocalSinkState>(*this, context);
}

SinkResultType PhysicalHashAggregate::Sink(ExecutionContext &context, DataChunk &input, LocalSinkState &lstate) const {
	auto &llstate = (HashAggregateLocalSinkState &)lstate;

	DataChunk &aggregate_input_chunk = llstate.aggregate_input_chunk;

	idx_t aggregate_input_idx = 0;
	for (auto &aggregate : aggregates) {
		auto &aggr = (BoundAggregateExpression &)*aggregate;
		for (auto &child_expr : aggr.children) {
			D_ASSERT(child_expr->type == ExpressionType::BOUND_REF);
			auto &bound_ref_expr = (BoundReferenceExpression &)*child_expr;
			aggregate_input_chunk.data[aggregate_input_idx++].Reference(input.data[bound_ref_expr.index]);
		}
	}
	for (auto &aggregate : aggregates) {
		auto &aggr = (BoundAggregateExpression &)*aggregate;
		if (aggr.filter) {
			auto it = filter_indexes.find(aggr.filter.get());
			D_ASSERT(it != filter_indexes.end());
			aggregate_input_chunk.data[aggregate_input_idx++].Reference(input.data[it->second]);
		}
	}

	aggregate_input_chunk.SetCardinality(input.size());
	aggregate_input_chunk.Verify();

    for (idx_t i = 0; i < radix_tables.size(); i++) {
        radix_tables[i].Sink(context, *llstate.global_radix_states[i],
                             *llstate.local_radix_states[i], input,
                             aggregate_input_chunk);
    }

    num_loops++;

	return SinkResultType::NEED_MORE_INPUT;
}

SinkResultType PhysicalHashAggregate::Sink(ExecutionContext &context,
                                           GlobalSinkState &gstate,
                                           LocalSinkState &lstate,
                                           DataChunk &input) const {
	auto &llstate = (HashAggregateLocalSinkState &)lstate;
	auto &ggstate = (HashAggregateGlobalSinkState &)gstate;

	DataChunk &aggregate_input_chunk = llstate.aggregate_input_chunk;

	idx_t aggregate_input_idx = 0;
	for (auto &aggregate : aggregates) {
		auto &aggr = (BoundAggregateExpression &)*aggregate;
		for (auto &child_expr : aggr.children) {
			D_ASSERT(child_expr->type == ExpressionType::BOUND_REF);
			auto &bound_ref_expr = (BoundReferenceExpression &)*child_expr;
			aggregate_input_chunk.data[aggregate_input_idx++].Reference(input.data[bound_ref_expr.index]);
		}
	}
	for (auto &aggregate : aggregates) {
		auto &aggr = (BoundAggregateExpression &)*aggregate;
		if (aggr.filter) {
			auto it = filter_indexes.find(aggr.filter.get());
			D_ASSERT(it != filter_indexes.end());
			aggregate_input_chunk.data[aggregate_input_idx++].Reference(input.data[it->second]);
		}
	}

	aggregate_input_chunk.SetCardinality(input.size());
	aggregate_input_chunk.Verify();

	// Use shared global radix states from GlobalSinkState
	for (idx_t i = 0; i < radix_tables.size(); i++) {
		radix_tables[i].Sink(context, *ggstate.radix_states[i],
		                     *llstate.local_radix_states[i], input,
		                     aggregate_input_chunk);
	}

	num_loops++;
	return SinkResultType::NEED_MORE_INPUT;
}

DataChunk &PhysicalHashAggregate::GetLastSinkedData(LocalSinkState &lstate) const {
	auto &llstate = (HashAggregateLocalSinkState &)lstate;
	return llstate.aggregate_input_chunk;
}

class HashAggregateFinalizeEvent : public Event {
public:
	HashAggregateFinalizeEvent(const PhysicalHashAggregate &op_p, HashAggregateLocalSinkState &lstate_p, ClientContext& context)
	    : Event(*(context.executor)), context(context), op(op_p), lstate(lstate_p) {
	}

	const PhysicalHashAggregate &op;
	HashAggregateLocalSinkState &lstate;
	ClientContext& context;
	// Pipeline *pipeline;

public:
	void Schedule() override {
		vector<unique_ptr<Task>> tasks;
		for (idx_t i = 0; i < op.radix_tables.size(); i++) {
			op.radix_tables[i].ScheduleTasks(*(context.executor), shared_from_this(), *lstate.global_radix_states[i], tasks);
		}
		D_ASSERT(!tasks.empty());
		SetTasks(move(tasks));
	}
};

void PhysicalHashAggregate::Combine(ExecutionContext &context, LocalSinkState &lstate) const {
	
	//
	// combine content
	//
	// auto &gstate = (HashAggregateGlobalState &)state;
	auto &llstate = (HashAggregateLocalSinkState &)lstate;

	for (idx_t i = 0; i < radix_tables.size(); i++) {
		radix_tables[i].Combine(context, *llstate.global_radix_states[i], *llstate.local_radix_states[i]);
	}

	//
	// finalize content
	//
	// TODO new pipelinefinishevent (which is already being executed at this point)

	bool any_partitioned = false;
	for (idx_t i = 0; i < llstate.global_radix_states.size(); i++) {
		bool is_partitioned = radix_tables[i].Finalize(*(context.client), *llstate.global_radix_states[i]);
		if (is_partitioned) {
			any_partitioned = true;
		}
	}
	if (any_partitioned) {
		D_ASSERT(false && "JHKO this logic encuntererd. plz add to execute child event when this encountered");
		//auto new_event = make_shared<HashAggregateFinalizeEvent>(*this, llstate, *(context.client));
		// make child event for pipelinefinish event
		//event.InsertEvent(move(new_event));
	}
	// finishevent
	// TODO start event
	// here
	// new_event.start?

}

void PhysicalHashAggregate::Combine(ExecutionContext &context,
                                    GlobalSinkState &gstate,
                                    LocalSinkState &lstate) const {
	auto &llstate = (HashAggregateLocalSinkState &)lstate;
	auto &ggstate = (HashAggregateGlobalSinkState &)gstate;

	// Combine local radix states into the shared global radix states
	for (idx_t i = 0; i < radix_tables.size(); i++) {
		radix_tables[i].Combine(context, *ggstate.radix_states[i], *llstate.local_radix_states[i]);
	}
}

SinkFinalizeType PhysicalHashAggregate::Finalize(ExecutionContext &context,
                                                  GlobalSinkState &gstate) const {
	auto &ggstate = (HashAggregateGlobalSinkState &)gstate;

	for (idx_t i = 0; i < ggstate.radix_states.size(); i++) {
		radix_tables[i].Finalize(*(context.client), *ggstate.radix_states[i]);
	}

	return SinkFinalizeType::READY;
}

void PhysicalHashAggregate::TransferGlobalToLocal(GlobalSinkState &gstate,
                                                   LocalSinkState &lstate) const {
	auto &ggstate = (HashAggregateGlobalSinkState &)gstate;
	auto &llstate = (HashAggregateLocalSinkState &)lstate;
	// Move the finalized shared radix states into local state so downstream
	// GetData can read from local_sink_state.global_radix_states as usual.
	llstate.global_radix_states = std::move(ggstate.radix_states);
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class HashAggregateLocalSourceState : public LocalSourceState {
public:
	explicit HashAggregateLocalSourceState(const PhysicalHashAggregate &op) : scan_index(0) {
		for (auto &rt : op.radix_tables) {
			radix_states.push_back(rt.GetGlobalSourceState());
		}
	}

	idx_t scan_index;
	vector<unique_ptr<GlobalSourceState>> radix_states;
};

unique_ptr<LocalSourceState> PhysicalHashAggregate::GetLocalSourceState(ExecutionContext &context) const {
	return make_unique<HashAggregateLocalSourceState>(*this);
}

void PhysicalHashAggregate::GetData(ExecutionContext &context, DataChunk &chunk, LocalSourceState &lstate, LocalSinkState &sink_state) const {
	auto &sstate = (HashAggregateLocalSinkState &)sink_state;
	auto &state = (HashAggregateLocalSourceState &)lstate;

	/* We assume after aggregation, schema is unified */
	chunk.SetSchemaIdx(0);
	while (state.scan_index < state.radix_states.size()) {
		auto prev_chunk_card = chunk.size();
		radix_tables[state.scan_index].GetData(context, chunk, *sstate.global_radix_states[state.scan_index],
		                                       *state.radix_states[state.scan_index], output_projection_mapping);
		auto new_chunk_card = chunk.size() - prev_chunk_card;
		if (new_chunk_card != 0) {
			return;
		}

		state.scan_index++;
	}

}

bool PhysicalHashAggregate::IsSourceDataRemaining(LocalSourceState &lstate, LocalSinkState &sink_state) const {
	auto &state = (HashAggregateLocalSourceState &)lstate;
	return state.scan_index < state.radix_states.size();
}

//===--------------------------------------------------------------------===//
// Parallel source path
//===--------------------------------------------------------------------===//
//
// Multiple PipelineTasks read concurrently from a finalized HashAggregate.
// The shared GlobalSourceState holds an atomic per-radix-table partition
// claim cursor. Each thread atomically claims one finalized HT partition,
// scans it linearly into its own per-thread scratch chunk, then claims the
// next. The bridged sink state (childs[0]->local_sink_state) provides
// access to the finalized hash tables that the previous pipeline produced.

class HashAggregateParallelGlobalSourceState : public GlobalSourceState {
public:
	explicit HashAggregateParallelGlobalSourceState(const PhysicalHashAggregate &op)
	    : next_ht_idx(op.radix_tables.size()),
	      empty_emitted(op.radix_tables.size()),
	      max_threads(1)
	{
		for (idx_t i = 0; i < op.radix_tables.size(); i++) {
			next_ht_idx[i].store(0);
			empty_emitted[i].store(false);
		}
	}

	idx_t MaxThreads() override { return max_threads; }
	void SetMaxThreads(idx_t v) { max_threads = v == 0 ? 1 : v; }

	// One claim cursor per radix table; threads atomically fetch_add to claim
	// the next finalized HT partition within that table.
	std::vector<std::atomic<idx_t>> next_ht_idx;
	// One per radix table; for the empty-grouping-set + no-data special case
	// we must emit a single synthetic null row across all threads.
	std::vector<std::atomic<bool>> empty_emitted;
	idx_t max_threads;
};

class HashAggregateParallelLocalSourceState : public LocalSourceState {
public:
	explicit HashAggregateParallelLocalSourceState(const PhysicalHashAggregate &op) {
		scan_chunks.reserve(op.radix_tables.size());
		for (idx_t i = 0; i < op.radix_tables.size(); i++) {
			auto &rt = op.radix_tables[i];
			vector<LogicalType> scan_types = rt.group_types;
			for (auto &t : op.aggregate_return_types) {
				scan_types.push_back(t);
			}
			auto chunk = make_unique<DataChunk>();
			chunk->Initialize(scan_types);
			scan_chunks.push_back(std::move(chunk));
		}
		cur_table = 0;
		cur_ht = DConstants::INVALID_INDEX;
		cur_pos = 0;
	}

	// Per-thread scratch (one per radix table — types differ across tables).
	vector<unique_ptr<DataChunk>> scan_chunks;
	// Currently-claimed (table, partition, position-within-partition).
	idx_t cur_table;
	idx_t cur_ht;
	idx_t cur_pos;
};

unique_ptr<GlobalSourceState> PhysicalHashAggregate::GetGlobalSourceState(ClientContext &context) const {
	auto state = make_unique<HashAggregateParallelGlobalSourceState>(*this);
	// MaxThreads is bounded by the total number of partitions across all
	// radix tables; the executor takes the min with the user's thread budget.
	// We don't know finalized_hts.size() until Finalize, so optimistically
	// expose hardware_concurrency — the executor caps anyway.
	idx_t hw = (idx_t)std::thread::hardware_concurrency();
	if (hw == 0) hw = 1;
	state->SetMaxThreads(hw);
	return state;
}

unique_ptr<LocalSourceState> PhysicalHashAggregate::GetLocalSourceStateParallel(ExecutionContext &context) const {
	return make_unique<HashAggregateParallelLocalSourceState>(*this);
}

//! Project a single scratch row-block into the operator output chunk.
//! Mirrors the projection at the bottom of RadixPartitionedHashTable::GetData.
static void ProjectFromScratch(const PhysicalHashAggregate &op, idx_t table_idx,
                               DataChunk &scratch, DataChunk &chunk, idx_t found) {
	auto &rt = op.radix_tables[table_idx];
	chunk.SetCardinality(found);
	idx_t num_unused_grouping_cols = 0;
	for (idx_t i = 0; i < op.output_projection_mapping.size(); i++) {
		if (op.output_projection_mapping[i] != std::numeric_limits<uint32_t>::max()) {
			chunk.data[op.output_projection_mapping[i]].Reference(scratch.data[i]);
		} else {
			num_unused_grouping_cols++;
		}
	}
	for (auto null_group : rt.null_groups) {
		chunk.data[null_group].SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(chunk.data[null_group], true);
	}
	idx_t skip_offset = op.groups.size() - num_unused_grouping_cols;
	for (idx_t col_idx = 0; col_idx < op.aggregates.size(); col_idx++) {
		chunk.data[skip_offset + col_idx].Reference(scratch.data[rt.group_types.size() + col_idx]);
	}
	D_ASSERT(op.grouping_functions.size() == rt.grouping_values.size());
	for (idx_t i = 0; i < op.grouping_functions.size(); i++) {
		chunk.data[skip_offset + op.aggregates.size() + i].Reference(rt.grouping_values[i]);
	}
}

//! Emit the synthetic single-row result for an empty-grouping-set HashAgg
//! against an empty input (e.g. SELECT COUNT(*) FROM empty_table). Mirrors
//! the special case at radix_partitioned_hashtable.cpp lines 357-378.
static void EmitEmptyAggregateRow(const PhysicalHashAggregate &op, idx_t table_idx,
                                  DataChunk &chunk) {
	auto &rt = op.radix_tables[table_idx];
	D_ASSERT(chunk.ColumnCount() == rt.null_groups.size() + op.aggregates.size());
	chunk.SetCardinality(1);
	for (auto null_group : rt.null_groups) {
		chunk.data[null_group].SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(chunk.data[null_group], true);
	}
	for (idx_t i = 0; i < op.aggregates.size(); i++) {
		D_ASSERT(op.aggregates[i]->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
		auto &aggr = (BoundAggregateExpression &)*op.aggregates[i];
		auto aggr_state = unique_ptr<data_t[]>(new data_t[aggr.function.state_size()]);
		aggr.function.initialize(aggr_state.get());
		Vector state_vector(Value::POINTER((uintptr_t)aggr_state.get()));
		aggr.function.finalize(state_vector, aggr.bind_info.get(),
		                       chunk.data[rt.null_groups.size() + i], 1, 0);
		if (aggr.function.destructor) {
			aggr.function.destructor(state_vector, 1);
		}
	}
}

void PhysicalHashAggregate::GetData(ExecutionContext &context, DataChunk &chunk,
                                    GlobalSourceState &gss, LocalSourceState &lss,
                                    LocalSinkState &child_sink_state) const {
	auto &gstate = (HashAggregateParallelGlobalSourceState &)gss;
	auto &lstate = (HashAggregateParallelLocalSourceState &)lss;
	auto &sstate = (HashAggregateLocalSinkState &)child_sink_state;

	chunk.SetSchemaIdx(0);

	while (lstate.cur_table < radix_tables.size()) {
		auto &rt = radix_tables[lstate.cur_table];
		auto &rt_sink_state = *sstate.global_radix_states[lstate.cur_table];
		bool rt_is_empty = rt.IsEmpty(rt_sink_state);

		// Empty-input + empty-grouping-set: emit one synthetic row exactly once.
		if (rt_is_empty && rt.grouping_set.empty()) {
			bool expected = false;
			if (gstate.empty_emitted[lstate.cur_table].compare_exchange_strong(expected, true)) {
				EmitEmptyAggregateRow(*this, lstate.cur_table, chunk);
				lstate.cur_table++;
				return;
			}
			lstate.cur_table++;
			continue;
		}
		if (rt_is_empty) {
			lstate.cur_table++;
			continue;
		}

		// Claim the next partition for this table if we don't already hold one.
		if (lstate.cur_ht == DConstants::INVALID_INDEX) {
			idx_t claimed = gstate.next_ht_idx[lstate.cur_table].fetch_add(1);
			if (claimed >= rt.GetFinalizedPartitionCount(rt_sink_state)) {
				lstate.cur_table++;
				continue;
			}
			lstate.cur_ht = claimed;
			lstate.cur_pos = 0;
		}

		// Scan the currently-claimed partition into per-thread scratch.
		auto &scratch = *lstate.scan_chunks[lstate.cur_table];
		scratch.Reset();
		idx_t found = rt.ScanFinalizedPartition(rt_sink_state, lstate.cur_ht,
		                                         lstate.cur_pos, scratch);

		if (found == 0) {
			// Done with this partition; loop and claim the next one.
			lstate.cur_ht = DConstants::INVALID_INDEX;
			continue;
		}

		ProjectFromScratch(*this, lstate.cur_table, scratch, chunk, found);
		return;
	}
	// All tables exhausted — leave chunk empty so caller's IsSourceDataRemaining
	// returns false on the next check.
}

bool PhysicalHashAggregate::IsSourceDataRemaining(GlobalSourceState &gss, LocalSourceState &lss,
                                                  LocalSinkState &child_sink_state) const {
	auto &lstate = (HashAggregateParallelLocalSourceState &)lss;
	return lstate.cur_table < radix_tables.size();
}

string PhysicalHashAggregate::ParamsToString() const {
	string params = "hashagg-params / groups: ";
	for (auto &group : groups) {
		params += group->ToString() + ", ";
	}
	params += " / aggregates: ";
	for (auto &aggr : aggregates) {
		params += aggr->ToString() + ", ";
	}
	return params;
}
std::string PhysicalHashAggregate::ToString() const {
	return "HashAggregate";
}

}