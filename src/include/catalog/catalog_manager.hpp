//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#ifndef CATALOG_SERVER_H
#define CATALOG_SERVER_H

#include <mutex>
#include <string>
#include <sys/types.h>
#include <unordered_set>

namespace duckdb {

class CatalogManager {
public:
  static void CreateOrOpenCatalog(std::string shm_directory);
  static void CloseCatalog();

private:
  static std::string shm_directory_;
};

} // namespace duckdb

#endif // CATALOG_SERVER_H
