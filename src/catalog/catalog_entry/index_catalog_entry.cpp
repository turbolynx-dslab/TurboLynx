//===----------------------------------------------------------------------===//
//                         DuckDB
//
// src/catalog/catalog_entry/index_catalog_entry.cpp
//
//
//===----------------------------------------------------------------------===//

#include "catalog/catalog_entry/index_catalog_entry.hpp"
#include "catalog/catalog_serializer.hpp"
#include "main/client_context.hpp"

namespace duckdb {

IndexCatalogEntry::IndexCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateIndexInfo *info)
    : StandardEntry(CatalogType::INDEX_ENTRY, schema, catalog, info->index_name), index(nullptr), index_type(info->index_type),
	pid(info->partition_oid), psid(info->propertyschema_oid), adj_col_idx(info->adj_col_idx) {
	for (idx_t i = 0; i < info->column_ids.size(); i++)
		index_key_columns.push_back(info->column_ids[i]);
}

IndexCatalogEntry::~IndexCatalogEntry() {
	// remove the associated index from the info
	// if (!info || !index) {
	// 	return;
	// }
	// info->indexes.RemoveIndex(index);
}

string IndexCatalogEntry::ToSQL() {
}

idx_t IndexCatalogEntry::GetPartitionID() {
	return pid;
}

idx_t IndexCatalogEntry::GetPropertySchemaID() {
	return psid;
}

int64_t_vector *IndexCatalogEntry::GetIndexKeyColumns() {
	return &index_key_columns;
}

IndexType IndexCatalogEntry::GetIndexType() {
	return index_type;
}

idx_t IndexCatalogEntry::GetAdjColIdx() {
	return adj_col_idx;
}

void IndexCatalogEntry::Serialize(CatalogSerializer &ser, ClientContext &ctx) const {
	ser.Write(static_cast<uint8_t>(index_type));
	ser.Write(static_cast<uint64_t>(pid));
	ser.Write(static_cast<uint64_t>(psid));
	ser.Write(static_cast<uint64_t>(adj_col_idx));
	ser.WriteVector<int64_t>(index_key_columns);
}

void IndexCatalogEntry::Deserialize(CatalogDeserializer &des, ClientContext &ctx) {
	index_type  = static_cast<IndexType>(des.ReadU8());
	pid         = des.ReadU64();
	psid        = des.ReadU64();
	adj_col_idx = des.ReadU64();
	index_key_columns = des.ReadVector<int64_t>();
}

} // namespace duckdb
