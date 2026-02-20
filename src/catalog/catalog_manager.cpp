#include "catalog/catalog_manager.hpp"
#include "catalog/catalog_entry/list.hpp"
#include "common/logger.hpp"

namespace duckdb {

std::string CatalogManager::shm_directory_ = "";

void CatalogManager::CreateOrOpenCatalog(std::string shm_directory) {
  SetupLogger();
  spdlog::info("CatalogManager initialized (single-process embedded mode)");
  shm_directory_ = shm_directory;
}

void CatalogManager::CloseCatalog() {
  spdlog::info("Close CatalogManager done");
}

} // namespace duckdb
