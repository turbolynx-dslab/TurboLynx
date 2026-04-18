//===----------------------------------------------------------------------===//
//                         DuckDB
//
// src/main/client_context.cpp
//
//
//===----------------------------------------------------------------------===//

#include "main/client_context.hpp"

#include "main/database.hpp"
#include "main/client_data.hpp"
#include "main/query_profiler.hpp"

#include "common/types/value.hpp"

#include <memory>
using namespace std;

namespace duckdb {

ClientContext::ClientContext(shared_ptr<DatabaseInstance> database)
    : db(move(database)),
      client_data(make_unique<ClientData>(*this)),
	  graph_storage_wrapper(make_unique<iTbgppGraphStorageWrapper>(*this)),
	  executor(make_unique<Executor>(*this)),
	  profiler(make_shared<QueryProfiler>(*this)) {
}

ClientContext::~ClientContext() {
	if (Exception::UncaughtException()) {
		return;
	}
}

unique_ptr<ClientContextLock> ClientContext::LockContext() {
	return make_unique<ClientContextLock>(context_lock);
}

Executor &ClientContext::GetExecutor() {
	return *executor;
}

void ClientContext::EnableProfiling() {
	auto lock = LockContext();
	auto &config = ClientConfig::GetConfig(*this);
	config.enable_profiler = true;
}

void ClientContext::DisableProfiling() {
	auto lock = LockContext();
	auto &config = ClientConfig::GetConfig(*this);
	config.enable_profiler = false;
}

idx_t ClientContext::GetNewPhyiscalOpId() {
	auto lock = LockContext();
	return ClientData::Get(*this).physical_op_counter++;
}

} // namespace duckdb
