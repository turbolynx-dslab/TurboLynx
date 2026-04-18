//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "execution/schema_flow_graph.hpp"
#include "main/client_context.hpp"
#include "parallel/thread_context.hpp"

namespace duckdb {

class ExecutionContext {
public:
	// ExecutionContext(duckdb::ClientContext *client_p, duckdb::ThreadContext &thread_p) : client(client_p), thread(thread_p) {
	// }
	ExecutionContext(duckdb::ClientContext *client_p) : client(client_p) {
		// NOTE; thread should be manually set after calling constructor
	}

	//! The client-global context; caution needs to be taken when used in parallel situations
	duckdb::ClientContext *client;
	//! The thread-local context for this execution
	duckdb::ThreadContext *thread;
	//! The schema flow graph for this execution
	turbolynx::SchemaFlowGraph *sfg;
	//! The intermediate schema infos for this execution
	vector<Schema> *schema_infos;
	
	//! if this is the last pipeline, store pointer to query results here
	vector<shared_ptr<DataChunk>> *query_results;
	
};

} // namespace duckdb

namespace turbolynx {
using ExecutionContext = duckdb::ExecutionContext;
}
