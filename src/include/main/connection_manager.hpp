//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/connection_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <mutex>

namespace duckdb {

class ClientContext;

class ConnectionManager {
public:
	// Register a new connection; returns the assigned conn_id
	int64_t Register(std::shared_ptr<ClientContext> client) {
		std::lock_guard<std::mutex> lk(lock_);
		int64_t id = next_id_++;
		connections_[id] = client;
		return id;
	}

	void Unregister(int64_t conn_id) {
		std::lock_guard<std::mutex> lk(lock_);
		connections_.erase(conn_id);
	}

	std::shared_ptr<ClientContext> Get(int64_t conn_id) const {
		std::lock_guard<std::mutex> lk(lock_);
		auto it = connections_.find(conn_id);
		if (it == connections_.end()) return nullptr;
		return it->second.lock();
	}

	bool Empty() const {
		std::lock_guard<std::mutex> lk(lock_);
		for (auto &kv : connections_) {
			if (!kv.second.expired()) return false;
		}
		return true;
	}

private:
	mutable std::mutex lock_;
	std::map<int64_t, std::weak_ptr<ClientContext>> connections_;
	std::atomic<int64_t> next_id_{0};
};

} // namespace duckdb
