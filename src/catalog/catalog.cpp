#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/list.hpp"
#include "common/logger.hpp"
#include "catalog/catalog_serializer.hpp"
#include "catalog/catalog_set.hpp"
#include "catalog/default/default_schemas.hpp"
#include "catalog/dependency_manager.hpp"

#include "common/exception.hpp"
#include "main/client_context.hpp"
#include "main/client_data.hpp"
#include "main/database.hpp"

#include "parser/parsed_data/alter_table_info.hpp"
#include "parser/parsed_data/create_graph_info.hpp"
#include "parser/parsed_data/create_partition_info.hpp"
#include "parser/parsed_data/create_property_schema_info.hpp"
#include "parser/parsed_data/create_extent_info.hpp"
#include "parser/parsed_data/create_chunkdefinition_info.hpp"
#include "parser/parsed_data/create_aggregate_function_info.hpp"
#include "parser/parsed_data/create_scalar_function_info.hpp"
#include "parser/parsed_data/create_index_info.hpp"
#include "parser/parsed_data/create_schema_info.hpp"
#include "parser/parsed_data/drop_info.hpp"

#include <algorithm>
#include <execinfo.h>
#include <iostream>
#include <filesystem>
#include <fstream>

#ifdef ENABLE_SANITIZER_FLAG
#include <sanitizer/lsan_interface.h>
#endif

#include "icecream.hpp"
namespace duckdb {

string SimilarCatalogEntry::GetQualifiedName() const {
	D_ASSERT(Found());

	return schema->name + "." + name;
}

Catalog::Catalog(DatabaseInstance &db)
    : db(db), dependency_manager(make_unique<DependencyManager>(*this)) {
	catalog_version = 1;
	functions = make_unique<CatalogSet>(*this);
}

Catalog::~Catalog() {
}

void Catalog::LoadCatalog(vector<vector<string>> &object_names, string path) {
	catalog_path_ = path;

	schemas = make_unique<CatalogSet>(*this, make_unique<DefaultSchemaGenerator>(*this));

	// Create SchemaCatalogEntry
	unordered_set<CatalogEntry *> dependencies;
	auto entry = new SchemaCatalogEntry(this, DEFAULT_SCHEMA, false);

	std::shared_ptr<ClientContext> client =
		std::make_shared<ClientContext>(db.shared_from_this());
	if (!schemas->CreateEntry(*client.get(), DEFAULT_SCHEMA, move(entry), dependencies)) {
		throw CatalogException("Schema with name main already exists!");
	}

	// initialize default functions
	BuiltinFunctions builtin(*client.get(), *this, true);
	builtin.Initialize();

	if (std::filesystem::exists(path + "/catalog_version")) {
		string catalog_version_str;
		std::ifstream ifs;
		ifs.open(path + "/catalog_version", std::fstream::in);
        if (ifs.is_open()) {
            ifs.seekg(-2, std::ios_base::end);

            bool keepLooping = true;
            while (keepLooping) {
                char ch;
                ifs.get(ch);

                if ((int)ifs.tellg() <= 1) {
                    ifs.seekg(0);
                    keepLooping = false;
                }
                else if (ch == '\n') {
                    keepLooping = false;
                }
                else {
                    ifs.seekg(-2, std::ios_base::cur);
                }
            }

            std::getline(ifs, catalog_version_str);
            ifs.close();
        }

        catalog_version = std::stoll(catalog_version_str);
		spdlog::info("catalog_version: {}", catalog_version.load());

#ifdef ENABLE_SANITIZER_FLAG
		__lsan_disable();
#endif
		ofs = new std::ofstream();
		ofs->open(path + "/catalog_version", std::ofstream::out | std::ofstream::ate);
		*ofs << std::to_string(catalog_version) + "\n" << std::flush;
	} else {
#ifdef ENABLE_SANITIZER_FLAG
		__lsan_disable();
#endif
		catalog_version = 10000000; // temporary..
		ofs = new std::ofstream();
		ofs->open(path + "/catalog_version", std::ofstream::out | std::ofstream::trunc);
		*ofs << std::to_string(catalog_version) + "\n" << std::flush;
	}
#ifdef ENABLE_SANITIZER_FLAG
	__lsan_enable();
#endif

	// -------------------------------------------------------------------
	// Restore catalog entries from catalog.bin (if it exists)
	// -------------------------------------------------------------------
	const string bin_path = path + "/catalog.bin";
	if (!std::filesystem::exists(bin_path)) {
		// Fresh DB: seed a default empty graph entry so downstream
		// components (Planner, shell) can attach without crashing.
		// The graph stays empty until the user imports data or runs
		// write queries via the C API.
		CreateGraphInfo gi;
		gi.schema    = DEFAULT_SCHEMA;
		gi.graph     = DEFAULT_GRAPH;
		gi.temporary = false;
		CreateGraph(*client.get(), &gi);
		return;
	}

	CatalogDeserializer des(bin_path);

	// Header: magic(4) + format_version(1) + saved_catalog_version(8) + entry_count(4)
	uint32_t magic = des.ReadU32();
	if (magic != 0x53363243u) { // "S62C"
		throw std::runtime_error("catalog.bin: invalid magic header");
	}
	uint8_t  fmt_ver        = des.ReadU8();
	catalog_format_version_ = fmt_ver;
	uint64_t saved_cv       = des.ReadU64();
	uint32_t entry_count    = des.ReadU32();

	auto *schema_entry = GetSchema(*client.get(), DEFAULT_SCHEMA);

	// Suppress catalog_version file writes during replay
	loading_ = true;

	for (uint32_t i = 0; i < entry_count; i++) {
		uint8_t  type_byte = des.ReadU8();
		uint64_t saved_oid = des.ReadU64();
		string   entry_name = des.ReadString();

		// Steer catalog_version so the next Create*() call gets exactly saved_oid
		catalog_version.store(saved_oid);

		CatalogType ctype = static_cast<CatalogType>(type_byte);
		switch (ctype) {
		case CatalogType::GRAPH_ENTRY: {
			CreateGraphInfo gi;
			gi.schema    = DEFAULT_SCHEMA;
			gi.graph     = entry_name;
			gi.temporary = false;
			auto *e = (GraphCatalogEntry *)CreateGraph(*client.get(), schema_entry, &gi);
			e->Deserialize(des, *client.get());
			break;
		}
		case CatalogType::PARTITION_ENTRY: {
			CreatePartitionInfo pi;
			pi.schema    = DEFAULT_SCHEMA;
			pi.partition = entry_name;
			pi.pid       = 0; // overwritten by Deserialize
			pi.temporary = false;
			auto *e = (PartitionCatalogEntry *)CreatePartition(*client.get(), schema_entry, &pi);
			e->Deserialize(des, *client.get());
			break;
		}
		case CatalogType::PROPERTY_SCHEMA_ENTRY: {
			CreatePropertySchemaInfo psi;
			psi.schema         = DEFAULT_SCHEMA;
			psi.propertyschema = entry_name;
			psi.pid            = 0; // overwritten by Deserialize
			psi.partition_oid  = 0; // overwritten by Deserialize
			psi.temporary      = false;
			auto *e = (PropertySchemaCatalogEntry *)CreatePropertySchema(*client.get(), schema_entry, &psi);
			e->Deserialize(des, *client.get());
			break;
		}
		case CatalogType::INDEX_ENTRY: {
			CreateIndexInfo ii;
			ii.schema            = DEFAULT_SCHEMA;
			ii.index_name        = entry_name;
			ii.index_type        = IndexType::PHYSICAL_ID; // overwritten by Deserialize
			ii.partition_oid     = 0;                      // overwritten by Deserialize
			ii.propertyschema_oid = 0;                     // overwritten by Deserialize
			ii.adj_col_idx       = 0;                      // overwritten by Deserialize
			auto *e = (IndexCatalogEntry *)CreateIndex(*client.get(), schema_entry, &ii);
			e->Deserialize(des, *client.get());
			break;
		}
		case CatalogType::EXTENT_ENTRY: {
			CreateExtentInfo ei;
			ei.schema               = DEFAULT_SCHEMA;
			ei.extent               = entry_name;
			ei.temporary            = false;
			ei.eid                  = 0;
			ei.pid                  = 0;
			ei.ps_oid               = 0;
			ei.extent_type          = ExtentType::EXTENT;
			ei.num_tuples_in_extent = 0;
			auto *e = (ExtentCatalogEntry *)CreateExtent(*client.get(), schema_entry, &ei);
			e->Deserialize(des, *client.get()); // also recreates ChunkDef entries
			break;
		}
		default:
			throw std::runtime_error("catalog.bin: unknown entry type " +
			                         std::to_string(type_byte));
		}
	}

	loading_ = false;

	bool repaired_partition_refs = false;
	schema_entry->Scan(CatalogType::PARTITION_ENTRY, [&](CatalogEntry *entry) {
		auto *part = static_cast<PartitionCatalogEntry *>(entry);
		auto *ps_ids = part->GetPropertySchemaIDs();
		if (ps_ids) {
			auto old_size = ps_ids->size();
			ps_ids->erase(std::remove_if(ps_ids->begin(), ps_ids->end(),
			                             [&](idx_t ps_oid) {
				                             auto *ps_entry =
				                                 schema_entry->GetCatalogEntryFromOid(ps_oid);
				                             return !ps_entry ||
				                                    ps_entry->type != CatalogType::PROPERTY_SCHEMA_ENTRY;
			                             }),
			              ps_ids->end());
			repaired_partition_refs |= (ps_ids->size() != old_size);
		}

		auto *ps_index = part->GetPropertySchemaIndex();
		if (ps_index) {
			for (auto it = ps_index->begin(); it != ps_index->end();) {
				auto &pairs = it->second;
				auto old_size = pairs.size();
				pairs.erase(std::remove_if(pairs.begin(), pairs.end(),
				                           [&](const auto &pair) {
					                           auto *ps_entry =
					                               schema_entry->GetCatalogEntryFromOid(pair.first);
					                           return !ps_entry ||
					                                  ps_entry->type != CatalogType::PROPERTY_SCHEMA_ENTRY;
				                           }),
				            pairs.end());
				repaired_partition_refs |= (pairs.size() != old_size);
				if (pairs.empty()) {
					it = ps_index->erase(it);
				} else {
					++it;
				}
			}
		}
	});

	if (repaired_partition_refs) {
		spdlog::warn("catalog: repaired dangling partition property-schema references");
		SaveCatalog();
	}

	// Restore catalog_version to the value at save time (skip over any ChunkDef gaps)
	catalog_version.store(saved_cv);
	if (ofs) {
		*ofs << std::to_string(catalog_version) + "\n" << std::flush;
	}
	spdlog::info("catalog: restored {} entries from catalog.bin", entry_count);
}

Catalog &Catalog::GetCatalog(ClientContext &context) {
	return context.db->GetCatalog();
}

CatalogEntry *Catalog::CreateGraph(ClientContext &context, CreateGraphInfo *info) {
	auto schema = GetSchema(context, info->schema);
	return CreateGraph(context, schema, info);
}

CatalogEntry *Catalog::CreateGraph(ClientContext &context, SchemaCatalogEntry *schema, CreateGraphInfo *info) {
	return schema->CreateGraph(context, info);
}

CatalogEntry *Catalog::CreatePartition(ClientContext &context, CreatePartitionInfo *info) {
	auto schema = GetSchema(context, info->schema);
	return CreatePartition(context, schema, info);
}

CatalogEntry *Catalog::CreatePartition(ClientContext &context, SchemaCatalogEntry *schema, CreatePartitionInfo *info) {
	return schema->CreatePartition(context, info);
}

CatalogEntry *Catalog::CreatePropertySchema(ClientContext &context, CreatePropertySchemaInfo *info) {
	auto schema = GetSchema(context, info->schema);
	return CreatePropertySchema(context, schema, info);
}

CatalogEntry *Catalog::CreatePropertySchema(ClientContext &context, SchemaCatalogEntry *schema, CreatePropertySchemaInfo *info) {
	return schema->CreatePropertySchema(context, info);
}

CatalogEntry *Catalog::CreateExtent(ClientContext &context, CreateExtentInfo *info) {
	auto schema = GetSchema(context, info->schema);
	return CreateExtent(context, schema, info);
}

CatalogEntry *Catalog::CreateExtent(ClientContext &context, SchemaCatalogEntry *schema, CreateExtentInfo *info) {
	return schema->CreateExtent(context, info);
}

CatalogEntry *Catalog::CreateChunkDefinition(ClientContext &context, CreateChunkDefinitionInfo *info) {
	auto schema = GetSchema(context, info->schema);
	return CreateChunkDefinition(context, schema, info);
}

CatalogEntry *Catalog::CreateChunkDefinition(ClientContext &context, SchemaCatalogEntry *schema, CreateChunkDefinitionInfo *info) {
	return schema->CreateChunkDefinition(context, info);
}

CatalogEntry *Catalog::CreateFunction(ClientContext &context, CreateFunctionInfo *info) {
	auto schema = GetSchema(context, info->schema);
	return CreateFunction(context, schema, info);
}

CatalogEntry *Catalog::CreateFunction(ClientContext &context, SchemaCatalogEntry *schema, CreateFunctionInfo *info) {
    StandardEntry *function;
#ifdef ENABLE_SANITIZER_FLAG
	__lsan_disable();
#endif
    switch (info->type) {
        case CatalogType::SCALAR_FUNCTION_ENTRY:
            function = new ScalarFunctionCatalogEntry(
                this, nullptr, (CreateScalarFunctionInfo *)info);
            break;
        case CatalogType::MACRO_ENTRY:
            D_ASSERT(false);
            break;

        case CatalogType::TABLE_MACRO_ENTRY:
            D_ASSERT(false);
            break;
        case CatalogType::AGGREGATE_FUNCTION_ENTRY:
            D_ASSERT(info->type == CatalogType::AGGREGATE_FUNCTION_ENTRY);
            function = new AggregateFunctionCatalogEntry(
                this, nullptr, (CreateAggregateFunctionInfo *)info);
            break;
        default:
            throw InternalException("Unknown function type \"%s\"",
                                    CatalogTypeToString(info->type));
    }
#ifdef ENABLE_SANITIZER_FLAG
	__lsan_enable();
#endif
	unordered_set<CatalogEntry *> dependencies;
	auto entry_name = function->name;
	auto entry_type = function->type;
	auto result = function;

	// first find the set for this entry
	auto &set = *functions;

	if (info->on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		// CREATE OR REPLACE: first try to drop the entry
		auto old_entry = set.GetEntry(context, entry_name);
		if (old_entry) {
			if (old_entry->type != entry_type) {
				throw CatalogException("Existing object %s is of type %s, trying to replace with type %s", entry_name,
				                       CatalogTypeToString(old_entry->type), CatalogTypeToString(entry_type));
			}
			(void)set.DropEntry(context, entry_name, false);
		}
	}
	// now try to add the entry
	if (!set.CreateEntry(context, entry_name, move(function), dependencies)) {
		// entry already exists!
		if (info->on_conflict == OnCreateConflict::ERROR_ON_CONFLICT) {
			throw CatalogException("%s with name \"%s\" already exists!", CatalogTypeToString(entry_type), entry_name);
		} else {
			return nullptr;
		}
	}
	oid_to_catalog_entry_array.insert({result->GetOid(), (void *)result});
	return result;
}

CatalogEntry *Catalog::CreateIndex(ClientContext &context, CreateIndexInfo *info) {
	auto schema = GetSchema(context, info->schema);
	return CreateIndex(context, schema, info);
}

CatalogEntry *Catalog::CreateIndex(ClientContext &context, SchemaCatalogEntry *schema, CreateIndexInfo *info) {
	return schema->CreateIndex(context, info);
}

CatalogEntry *Catalog::CreateSchema(ClientContext &context, CreateSchemaInfo *info) {
	D_ASSERT(!info->schema.empty());
	if (info->schema == TEMP_SCHEMA) {
		throw CatalogException("Cannot create built-in schema \"%s\"", info->schema);
	}

	if (!schemas) {
		schemas = make_unique<CatalogSet>(*this, make_unique<DefaultSchemaGenerator>(*this));
	}

	unordered_set<CatalogEntry *> dependencies;
	auto entry = new SchemaCatalogEntry(this, info->schema, info->internal);
	auto result = (CatalogEntry*) entry;

	if (!schemas->CreateEntry(context, info->schema, move(entry), dependencies)) {
		if (info->on_conflict == OnCreateConflict::ERROR_ON_CONFLICT) {
			throw CatalogException("Schema with name %s already exists!", info->schema);
		} else {
			D_ASSERT(info->on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT);
		}
		return nullptr;
	}
	return result;
}

void Catalog::DropSchema(ClientContext &context, DropInfo *info) {
	D_ASSERT(!info->name.empty());
	ModifyCatalog();
	if (!schemas->DropEntry(context, info->name, info->cascade)) {
		if (!info->if_exists) {
			throw CatalogException("Schema with name \"%s\" does not exist!", info->name);
		}
	}
}

void Catalog::DropEntry(ClientContext &context, DropInfo *info) {
	ModifyCatalog();
	if (info->type == CatalogType::SCHEMA_ENTRY) {
		// DROP SCHEMA
		DropSchema(context, info);
		return;
	}

	auto lookup = LookupEntry(context, info->type, info->schema, info->name, info->if_exists);
	if (!lookup.Found()) {
		return;
	}

	lookup.schema->DropEntry(context, info);
}

CatalogEntry *Catalog::AddFunction(ClientContext &context, CreateFunctionInfo *info) {
	D_ASSERT(false);
	auto schema = GetSchema(context, info->schema);
	return AddFunction(context, schema, info);
}

CatalogEntry *Catalog::AddFunction(ClientContext &context, SchemaCatalogEntry *schema, CreateFunctionInfo *info) {
	D_ASSERT(false);
	return schema->AddFunction(context, info);
}

SchemaCatalogEntry *Catalog::GetSchema(ClientContext &context, const string &schema_name, bool if_exists) {
	D_ASSERT(!schema_name.empty());
	if (schema_name == TEMP_SCHEMA) {
		D_ASSERT(false);
	}

	auto entry = schemas->GetEntry(context, schema_name);
	if (!entry && !if_exists) {
		D_ASSERT(false);
	}

	return (SchemaCatalogEntry *)entry;
}

void Catalog::ScanSchemas(ClientContext &context, std::function<void(CatalogEntry *)> callback) {
	schemas->Scan(context, [&](CatalogEntry *entry) { callback(entry); });
}

SimilarCatalogEntry Catalog::SimilarEntryInSchemas(ClientContext &context, const string &entry_name, CatalogType type,
                                                   const vector<SchemaCatalogEntry *> &schemas) {

	vector<CatalogSet *> sets;
	std::transform(schemas.begin(), schemas.end(), std::back_inserter(sets),
	               [type](SchemaCatalogEntry *s) -> CatalogSet * { return &s->GetCatalogSet(type); });
	pair<string, idx_t> most_similar {"", (idx_t)-1};
	SchemaCatalogEntry *schema_of_most_similar = nullptr;
	for (auto schema : schemas) {
		auto entry = schema->GetCatalogSet(type).SimilarEntry(context, entry_name);
		if (!entry.first.empty() && (most_similar.first.empty() || most_similar.second > entry.second)) {
			most_similar = entry;
			schema_of_most_similar = schema;
		}
	}

	return {most_similar.first, most_similar.second, schema_of_most_similar};
}

CatalogEntryLookup Catalog::LookupEntry(ClientContext &context, CatalogType type, const string &schema_name,
                                        const string &name, bool if_exists) {
	if (!schema_name.empty()) {
		auto schema = GetSchema(context, schema_name, if_exists);

		if (!schema) {
			D_ASSERT(if_exists);
			return {nullptr, nullptr};
		}

		auto entry = schema->GetCatalogSet(type).GetEntry(context, name);

		if (!entry && !if_exists) {
			return {schema, nullptr};
		}

		return {schema, entry};
	}

	const auto paths = vector<string>();
	for (const auto &path : paths) {
		auto lookup = LookupEntry(context, type, path, name, true);
		if (lookup.Found()) {
			return lookup;
		}
	}

	if (!if_exists) {
		vector<SchemaCatalogEntry *> schemas;
		for (const auto &path : paths) {
			auto schema = GetSchema(context, path, true);
			if (schema) {
				schemas.emplace_back(schema);
			}
		}

		D_ASSERT(false);
	}

	return {nullptr, nullptr};
}

CatalogEntryLookup Catalog::LookupFuncEntry(ClientContext &context, CatalogType type, const string &schema_name,
                                        const string &name, bool if_exists) {
	auto entry = functions->GetEntry(context, name);
	if (!entry && !if_exists) {
		D_ASSERT(false);
	}
	return {nullptr, entry};
}

CatalogEntry *Catalog::GetEntry(ClientContext &context, const string &schema, const string &name) {
	D_ASSERT(false);
	vector<CatalogType> entry_types {CatalogType::TABLE_ENTRY, CatalogType::SEQUENCE_ENTRY};

	for (auto entry_type : entry_types) {
		CatalogEntry *result = GetEntry(context, entry_type, schema, name, true);
		if (result != nullptr) {
			return result;
		}
	}

	throw CatalogException("CatalogElement \"%s.%s\" does not exist!", schema, name);
}

CatalogEntry *Catalog::GetEntry(ClientContext &context, CatalogType type, const string &schema_name, const string &name,
                                bool if_exists) {
	return LookupEntry(context, type, schema_name, name, if_exists).entry;
}

CatalogEntry *Catalog::GetFuncEntry(ClientContext &context, CatalogType type, const string &schema_name, const string &name,
                                bool if_exists) {
	return LookupFuncEntry(context, type, schema_name, name, if_exists).entry;
}

CatalogEntry *Catalog::GetEntry(ClientContext &context, const string &schema_name, idx_t oid, bool if_exists) {
	if (!schema_name.empty()) {
		auto schema = GetSchema(context, schema_name, if_exists);

		if (!schema) {
			D_ASSERT(if_exists);
			return nullptr;
		}

			auto entry = schema->GetCatalogEntryFromOid(oid);

			if (!entry && !if_exists) {
				spdlog::error("[CatalogMissingOid] schema={} oid={}", schema_name, oid);
				void *frames[16];
				int frame_count = backtrace(frames, 16);
				char **symbols = backtrace_symbols(frames, frame_count);
				if (symbols) {
					for (int i = 0; i < frame_count; i++) {
						spdlog::error("[CatalogMissingOid] bt[{}] {}", i, symbols[i]);
					}
					free(symbols);
				}
				D_ASSERT(false);
			}

		return entry;
	}

	return nullptr;
}

CatalogEntry *Catalog::GetFuncEntry(ClientContext &context, const string &schema_name, idx_t oid, bool if_exists) {
	auto cat_entry = (CatalogEntry *)oid_to_catalog_entry_array.at(oid);
	return cat_entry;
}

template <>
GraphCatalogEntry *Catalog::GetEntry(ClientContext &context, const string &schema_name, const string &name,
                                     bool if_exists) {
	return (GraphCatalogEntry*) GetEntry(context, CatalogType::GRAPH_ENTRY, schema_name, name, if_exists);
}

template <>
PartitionCatalogEntry *Catalog::GetEntry(ClientContext &context, const string &schema_name, const string &name,
                                     bool if_exists) {
return (PartitionCatalogEntry*) GetEntry(context, CatalogType::PARTITION_ENTRY, schema_name, name, if_exists);
}

template <>
PropertySchemaCatalogEntry *Catalog::GetEntry(ClientContext &context, const string &schema_name, const string &name,
                                     bool if_exists) {
return (PropertySchemaCatalogEntry*) GetEntry(context, CatalogType::PROPERTY_SCHEMA_ENTRY, schema_name, name, if_exists);
}

template <>
ExtentCatalogEntry *Catalog::GetEntry(ClientContext &context, const string &schema_name, const string &name,
                                     bool if_exists) {
return (ExtentCatalogEntry*) GetEntry(context, CatalogType::EXTENT_ENTRY, schema_name, name, if_exists);
}

template <>
ChunkDefinitionCatalogEntry *Catalog::GetEntry(ClientContext &context, const string &schema_name, const string &name,
                                     bool if_exists) {
return (ChunkDefinitionCatalogEntry*) GetEntry(context, CatalogType::CHUNKDEFINITION_ENTRY, schema_name, name, if_exists);
}

void Catalog::Alter(ClientContext &context, AlterInfo *info) {
	D_ASSERT(false);
	ModifyCatalog();
	auto lookup = LookupEntry(context, info->GetCatalogType(), info->schema, info->name);
	D_ASSERT(lookup.Found());
	return lookup.schema->Alter(context, info);
}

idx_t Catalog::GetCatalogVersion() {
	return catalog_version;
}

idx_t Catalog::ModifyCatalog() {
	if (ofs && !loading_) {
		*ofs << std::to_string(catalog_version + 1) + "\n" << std::flush;
	}
	return catalog_version++;
}

// ---------------------------------------------------------------------------
// SaveCatalog — writes all graph/partition/PS/extent entries to catalog.bin
// ---------------------------------------------------------------------------

void Catalog::SaveCatalog() {
	D_ASSERT(!catalog_path_.empty());

	const string tmp_path   = catalog_path_ + "/catalog.bin.tmp";
	const string final_path = catalog_path_ + "/catalog.bin";

	// Create an internal ClientContext for entry lookups during serialization
	auto client = std::make_shared<ClientContext>(db.shared_from_this());

	CatalogSerializer ser(tmp_path);

	// Collect entries of each relevant type in OID ascending order
	using EntryPair = std::pair<idx_t, CatalogEntry *>;
	std::vector<EntryPair> all_entries;

	auto *schema_entry = GetSchema(*client, DEFAULT_SCHEMA);
	// Use the no-context Scan overload to iterate committed entries
	auto collect = [&](CatalogType t) {
		schema_entry->Scan(t, [&](CatalogEntry *e) {
			all_entries.emplace_back(e->oid, e);
		});
	};
	collect(CatalogType::GRAPH_ENTRY);
	collect(CatalogType::PARTITION_ENTRY);
	collect(CatalogType::PROPERTY_SCHEMA_ENTRY);
	collect(CatalogType::INDEX_ENTRY);
	collect(CatalogType::EXTENT_ENTRY);
	// ChunkDefinition entries are embedded inside Extent — NOT stored separately

	// Sort ascending by OID so load order is correct
	std::sort(all_entries.begin(), all_entries.end(),
	          [](const EntryPair &a, const EntryPair &b) { return a.first < b.first; });

	// Header: magic "S62C" + format_version(1) + current catalog_version(8) + count(4)
	ser.Write(static_cast<uint32_t>(0x53363243u)); // "S62C"
	ser.Write(static_cast<uint8_t>(3));            // format version (3 = temporal_id + sub_partition_oids)
	ser.Write(static_cast<uint64_t>(catalog_version.load()));
	ser.Write(static_cast<uint32_t>(all_entries.size()));

	for (auto &ep : all_entries) {
		CatalogEntry *e = ep.second;
		ser.Write(static_cast<uint8_t>(e->type));
		ser.Write(static_cast<uint64_t>(ep.first));
		ser.WriteString(e->name);
		e->Serialize(ser, *client);
	}

	ser.Commit(final_path);

	// For a fresh DB (ofs not open), also create the catalog_version sentinel file
	// so that on the next open, Initialize() detects an existing catalog
	if (!ofs) {
		std::ofstream cv(catalog_path_ + "/catalog_version");
		cv << std::to_string(catalog_version.load()) << "\n";
		cv.flush();
	}

	spdlog::info("catalog: saved {} entries to catalog.bin", all_entries.size());
}

} // namespace duckdb
