//===----------------------------------------------------------------------===//
//                         DuckDB
//
// src/main/database.cpp
//
//
//===----------------------------------------------------------------------===//

#include "main/database.hpp"

#include "catalog/catalog.hpp"
#include "catalog/catalog_wrapper.hpp"
#include "main/client_context.hpp"
#include "common/logger.hpp"
#include "parser/parsed_data/create_schema_info.hpp"
#include "parser/parsed_data/create_graph_info.hpp"
#include "common/constants.hpp"

#ifndef DUCKDB_NO_THREADS
#include "common/thread.hpp"
#endif

#include <filesystem>

namespace duckdb {

DatabaseInstance::DatabaseInstance() {
}

DatabaseInstance::~DatabaseInstance() {
	if (Exception::UncaughtException()) {
		return;
	}

	try {
	} catch (...) {
	}
}

BufferManager &BufferManager::GetBufferManager(DatabaseInstance &db) {
	return *db.GetStorageManager().buffer_manager;
}

BlockManager &BlockManager::GetBlockManager(DatabaseInstance &db) {
	return *db.GetStorageManager().block_manager;
}

BlockManager &BlockManager::GetBlockManager(ClientContext &context) {
	return BlockManager::GetBlockManager(DatabaseInstance::GetDatabase(context));
}

DatabaseInstance &DatabaseInstance::GetDatabase(ClientContext &context) {
	return *context.db;
}

StorageManager &StorageManager::GetStorageManager(DatabaseInstance &db) {
	return db.GetStorageManager();
}

Catalog &Catalog::GetCatalog(DatabaseInstance &db) {
	return db.GetCatalog();
}

ClientConfig &ClientConfig::GetConfig(ClientContext &context) {
	return context.config;
}

void DatabaseInstance::Initialize(const char *path) {
	storage =
	    make_unique<StorageManager>(*this, path ? string(path) : string(), false);

	spdlog::trace("StorageManager initialized");

	// Check if an existing catalog exists on disk
	string catalog_path = path ? (string(path) + "/catalog_version") : "";
	bool has_existing_catalog = !catalog_path.empty() && std::filesystem::exists(catalog_path);

	if (has_existing_catalog) {
		// Load existing catalog
		catalog = make_unique<Catalog>(*this);
		vector<vector<string>> object_names; // empty - no SHM iteration needed
		catalog->LoadCatalog(object_names, path);
	} else {
		// Create new catalog
		catalog = make_unique<Catalog>(*this);

		// Set the catalog path so that SaveCatalog() works on a fresh DB
		if (path) {
			catalog->catalog_path_ = string(path);
		}

		// Create default schema and initialize functions
		std::shared_ptr<ClientContext> client =
			std::make_shared<ClientContext>(shared_from_this());
		CreateSchemaInfo schema_info;
		catalog->CreateSchema(*client.get(), &schema_info);

		// Initialize built-in functions
		BuiltinFunctions builtin(*client.get(), *catalog, false);
		builtin.Initialize();

		// Seed a default empty graph entry so the Planner and other
		// consumers (shell, .tables, .schema) can attach to a fresh
		// workspace without crashing. The graph stays empty until the
		// user imports data or runs write queries via the C API.
		CreateGraphInfo graph_info(DEFAULT_SCHEMA, DEFAULT_GRAPH);
		graph_info.temporary = false;
		catalog->CreateGraph(*client.get(), &graph_info);
	}

	catalog_wrapper = make_unique<CatalogWrapper>(*this);
	storage->Initialize();
}

DuckDB::DuckDB(DatabaseInstance &instance_p) : instance(instance_p.shared_from_this()) {
}

DuckDB::DuckDB(const char *path) : instance(make_shared<DatabaseInstance>()) {
	instance->Initialize(path);
}

DuckDB::~DuckDB() {
	instance.reset();
}

StorageManager &DatabaseInstance::GetStorageManager() {
	return *storage;
}

Catalog &DatabaseInstance::GetCatalog() {
	return *catalog;
}

CatalogWrapper &DatabaseInstance::GetCatalogWrapper() {
	return *catalog_wrapper;
}

FileSystem &DatabaseInstance::GetFileSystem() {
	return DEFAULT_LOCAL_FILE_SYSTEM;
}

FileSystem &DuckDB::GetFileSystem() {
	return instance->GetFileSystem();
}

} // namespace duckdb
