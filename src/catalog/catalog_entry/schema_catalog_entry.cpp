#include "catalog/catalog_entry/schema_catalog_entry.hpp"
#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "catalog/catalog_entry/collate_catalog_entry.hpp"
#include "catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "catalog/catalog_entry/index_catalog_entry.hpp"
#include "catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "catalog/catalog_entry/pragma_function_catalog_entry.hpp"
#include "catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "catalog/catalog_entry/sequence_catalog_entry.hpp"
#include "catalog/catalog_entry/table_catalog_entry.hpp"
#include "catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "catalog/catalog_entry/type_catalog_entry.hpp"
#include "catalog/catalog_entry/graph_catalog_entry.hpp"
#include "catalog/catalog_entry/partition_catalog_entry.hpp"
#include "catalog/catalog_entry/property_schema_catalog_entry.hpp"
#include "catalog/catalog_entry/extent_catalog_entry.hpp"
#include "catalog/catalog_entry/chunkdefinition_catalog_entry.hpp"
#include "catalog/catalog_entry/table_macro_catalog_entry.hpp"
#include "catalog/default/default_functions.hpp"

#include "common/exception.hpp"
#include "parser/parsed_data/alter_table_info.hpp"
#include "parser/parsed_data/create_graph_info.hpp"
#include "parser/parsed_data/create_partition_info.hpp"
#include "parser/parsed_data/create_property_schema_info.hpp"
#include "parser/parsed_data/create_extent_info.hpp"
#include "parser/parsed_data/create_chunkdefinition_info.hpp"
#include "parser/parsed_data/create_index_info.hpp"
#include "parser/parsed_data/create_scalar_function_info.hpp"
#include "parser/parsed_data/create_schema_info.hpp"
#include "parser/parsed_data/drop_info.hpp"
#include "main/client_context.hpp"

#include <algorithm>
#include <sstream>
#include <iostream>
#include "icecream.hpp"

namespace duckdb {

SchemaCatalogEntry::SchemaCatalogEntry(Catalog *catalog, string name_p, bool internal)
    : CatalogEntry(CatalogType::SCHEMA_ENTRY, catalog, move(name_p)),
		graphs(*catalog),
		partitions(*catalog),
		propertyschemas(*catalog),
		extents(*catalog),
		chunkdefinitions(*catalog),
		indexes(*catalog) {
	this->internal = internal;
}

CatalogEntry *SchemaCatalogEntry::AddEntry(ClientContext &context, StandardEntry *entry,
                                           OnCreateConflict on_conflict, unordered_set<CatalogEntry *> dependencies) {
	auto entry_name = entry->name;
	auto entry_type = entry->type;
	auto result = entry;

	auto &set = GetCatalogSet(entry_type);

	if (name != TEMP_SCHEMA) {
		dependencies.insert(this);
	} else {
		entry->temporary = true;
	}
	if (on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		auto old_entry = set.GetEntry(context, entry_name);
		if (old_entry) {
			if (old_entry->type != entry_type) {
				throw CatalogException("Existing object %s is of type %s, trying to replace with type %s", entry_name,
				                       CatalogTypeToString(old_entry->type), CatalogTypeToString(entry_type));
			}
			(void)set.DropEntry(context, entry_name, false);
		}
	}
	if (!catalog->loading_ && on_conflict != OnCreateConflict::REPLACE_ON_CONFLICT) {
		auto existing_entry = set.GetEntry(context, entry_name);
		if (existing_entry) {
			if (on_conflict == OnCreateConflict::ERROR_ON_CONFLICT) {
				throw CatalogException("%s with name \"%s\" already exists!",
				                       CatalogTypeToString(entry_type), entry_name);
			}
			return nullptr;
		}
	}
	if (!set.CreateEntry(context, entry_name, move(entry), dependencies)) {
		if (on_conflict == OnCreateConflict::ERROR_ON_CONFLICT) {
			throw CatalogException("%s with name \"%s\" already exists!", CatalogTypeToString(entry_type), entry_name);
		} else {
			return nullptr;
		}
	}
	// Use insert_or_assign so that a properly-steered entry (e.g., a Partition
	// loaded at OID N) wins over an unsteered ChunkDef that happened to land at
	// the same OID during a previous ExtentCatalogEntry::Deserialize call.
	oid_to_catalog_entry_array.insert_or_assign(result->GetOid(), (void *)result);
	return result;
}

CatalogEntry *SchemaCatalogEntry::AddEntry(ClientContext &context, StandardEntry *entry,
                                           OnCreateConflict on_conflict) {
	unordered_set<CatalogEntry *> dependencies;
	return AddEntry(context, move(entry), on_conflict, dependencies);
}

bool SchemaCatalogEntry::AddEntryInternal(ClientContext &context, CatalogEntry *entry, string &entry_name, CatalogType &entry_type,
                                           OnCreateConflict on_conflict, unordered_set<CatalogEntry *> dependencies) {
	auto &set = GetCatalogSet(entry_type);

	if (name != TEMP_SCHEMA) {
		dependencies.insert(this);
	} else {
		entry->temporary = true;
	}
	if (on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		auto old_entry = set.GetEntry(context, entry_name);
		if (old_entry) {
			if (old_entry->type != entry_type) {
				throw CatalogException("Existing object %s is of type %s, trying to replace with type %s", entry_name,
				                       CatalogTypeToString(old_entry->type), CatalogTypeToString(entry_type));
			}
			(void)set.DropEntry(context, entry_name, false);
		}
	}
	if (!catalog->loading_ && on_conflict != OnCreateConflict::REPLACE_ON_CONFLICT) {
		auto existing_entry = set.GetEntry(context, entry_name);
		if (existing_entry) {
			if (on_conflict == OnCreateConflict::ERROR_ON_CONFLICT) {
				throw CatalogException("%s with name \"%s\" already exists!",
				                       CatalogTypeToString(entry_type), entry_name);
			}
			return false;
		}
	}

	if (!set.CreateEntry(context, entry_name, move(entry), dependencies)) {
		if (on_conflict == OnCreateConflict::ERROR_ON_CONFLICT) {
			throw CatalogException("%s with name \"%s\" already exists!", CatalogTypeToString(entry_type), entry_name);
		} else {
			return false;
		}
	}
	return true;
}

CatalogEntry *SchemaCatalogEntry::CreateGraph(ClientContext &context, CreateGraphInfo *info) {
	unordered_set<CatalogEntry *> dependencies;
	auto graph = new GraphCatalogEntry(catalog, this, info);
	return AddEntry(context, move(graph), info->on_conflict, dependencies);
}

CatalogEntry *SchemaCatalogEntry::CreatePartition(ClientContext &context, CreatePartitionInfo *info) {
	unordered_set<CatalogEntry *> dependencies;
	auto partition = new PartitionCatalogEntry(catalog, this, info);
	return AddEntry(context, move(partition), info->on_conflict, dependencies);
}

CatalogEntry *SchemaCatalogEntry::CreatePropertySchema(ClientContext &context, CreatePropertySchemaInfo *info) {
	unordered_set<CatalogEntry *> dependencies;
	auto propertyschema = new PropertySchemaCatalogEntry(catalog, this, info);
	return AddEntry(context, move(propertyschema), info->on_conflict, dependencies);
}

CatalogEntry *SchemaCatalogEntry::CreateExtent(ClientContext &context, CreateExtentInfo *info) {
	unordered_set<CatalogEntry *> dependencies;
	auto extent = new ExtentCatalogEntry(catalog, this, info);
	return AddEntry(context, move(extent), info->on_conflict, dependencies);
}

CatalogEntry *SchemaCatalogEntry::CreateChunkDefinition(ClientContext &context, CreateChunkDefinitionInfo *info) {
	unordered_set<CatalogEntry *> dependencies;
	auto chunkdefinition = new ChunkDefinitionCatalogEntry(catalog, this, info);
	return AddEntry(context, move(chunkdefinition), info->on_conflict, dependencies);
}

CatalogEntry *SchemaCatalogEntry::CreateIndex(ClientContext &context, CreateIndexInfo *info) {
	unordered_set<CatalogEntry *> dependencies;
	auto index = new IndexCatalogEntry(catalog, this, info);
	return AddEntry(context, move(index), info->on_conflict, dependencies);
}

CatalogEntry *SchemaCatalogEntry::CreateFunction(ClientContext &context, CreateFunctionInfo *info) {
	StandardEntry *function;
	switch (info->type) {
	case CatalogType::SCALAR_FUNCTION_ENTRY:
		function = new ScalarFunctionCatalogEntry(catalog, this,
		                                          (CreateScalarFunctionInfo *)info);
		break;
	case CatalogType::MACRO_ENTRY:
		D_ASSERT(false);
		break;
	case CatalogType::TABLE_MACRO_ENTRY:
		D_ASSERT(false);
		break;
	case CatalogType::AGGREGATE_FUNCTION_ENTRY:
		D_ASSERT(info->type == CatalogType::AGGREGATE_FUNCTION_ENTRY);
		function = new AggregateFunctionCatalogEntry(catalog, this,
		                                             (CreateAggregateFunctionInfo *)info);
		break;
	default:
		throw InternalException("Unknown function type \"%s\"", CatalogTypeToString(info->type));
	}
	return AddEntry(context, move(function), info->on_conflict);
}

CatalogEntry *SchemaCatalogEntry::AddFunction(ClientContext &context, CreateFunctionInfo *info) {
	D_ASSERT(false);
	auto entry = GetCatalogSet(info->type).GetEntry(context, info->name);
	if (!entry) {
		return CreateFunction(context, info);
	}

	info->on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;
	return CreateFunction(context, info);
}

void SchemaCatalogEntry::DropEntry(ClientContext &context, DropInfo *info) {
	auto &set = GetCatalogSet(info->type);

	auto existing_entry = set.GetEntry(context, info->name);
	if (!existing_entry) {
		if (!info->if_exists) {
			throw CatalogException("%s with name \"%s\" does not exist!", CatalogTypeToString(info->type), info->name);
		}
		return;
	}
	if (existing_entry->type != info->type) {
		throw CatalogException("Existing object %s is of type %s, trying to replace with type %s", info->name,
		                       CatalogTypeToString(existing_entry->type), CatalogTypeToString(info->type));
	}

	if (!set.DropEntry(context, info->name, info->cascade)) {
		throw InternalException("Could not drop element because of an internal error");
	}
}

void SchemaCatalogEntry::Alter(ClientContext &context, AlterInfo *info) {
	D_ASSERT(false);
}

void SchemaCatalogEntry::Scan(ClientContext &context, CatalogType type,
                              const std::function<void(CatalogEntry *)> &callback) {
	auto &set = GetCatalogSet(type);
	set.Scan(context, callback);
}

void SchemaCatalogEntry::Scan(CatalogType type, const std::function<void(CatalogEntry *)> &callback) {
	auto &set = GetCatalogSet(type);
	set.Scan(callback);
}

void SchemaCatalogEntry::Serialize(Serializer &serializer) {
	D_ASSERT(false);
}

unique_ptr<CreateSchemaInfo> SchemaCatalogEntry::Deserialize(Deserializer &source) {
	D_ASSERT(false);
}

void SchemaCatalogEntry::LoadCatalogSet(Catalog* new_catalog) {
	this->catalog = new_catalog;
	// In single-process mode, catalog sets are already in-memory.
	// Nothing to load from shared memory.
}

string SchemaCatalogEntry::ToSQL() {
	std::stringstream ss;
	ss << "CREATE SCHEMA " << name << ";";
	return ss.str();
}

CatalogEntry *SchemaCatalogEntry::GetCatalogEntryFromOid(idx_t oid) {
	auto it = oid_to_catalog_entry_array.find(oid);
	if (it == oid_to_catalog_entry_array.end()) {
		return nullptr;
	}
	return (CatalogEntry *)it->second;
}

CatalogSet &SchemaCatalogEntry::GetCatalogSet(CatalogType type) {
	switch (type) {
	case CatalogType::GRAPH_ENTRY:
		return graphs;
	case CatalogType::PARTITION_ENTRY:
		return partitions;
	case CatalogType::PROPERTY_SCHEMA_ENTRY:
		return propertyschemas;
	case CatalogType::EXTENT_ENTRY:
		return extents;
	case CatalogType::CHUNKDEFINITION_ENTRY:
		return chunkdefinitions;
	case CatalogType::INDEX_ENTRY:
		return indexes;
	case CatalogType::AGGREGATE_FUNCTION_ENTRY:
	case CatalogType::SCALAR_FUNCTION_ENTRY:
	case CatalogType::MACRO_ENTRY:
		throw InternalException("Function catalog is not supported in the schema anymore");
	default:
		throw InternalException("Unsupported catalog type in schema");
	}
}

} // namespace duckdb
