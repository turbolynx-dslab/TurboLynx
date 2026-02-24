#include "catalog/catalog_entry/list.hpp"
#include "catalog/catalog.hpp"
#include "catalog/catalog_serializer.hpp"
#include "parser/parsed_data/create_extent_info.hpp"
#include "parser/parsed_data/create_chunkdefinition_info.hpp"
#include "common/enums/graph_component_type.hpp"

#include <memory>
#include <algorithm>

namespace duckdb {

ExtentCatalogEntry::ExtentCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateExtentInfo *info)
    : StandardEntry(CatalogType::EXTENT_ENTRY, schema, catalog, info->extent) {
	this->temporary = info->temporary;
	this->extent_type = info->extent_type;
	this->eid = info->eid;
	this->local_cdf_id_version = 0;
	this->local_adjlist_cdf_id_version = std::numeric_limits<LocalChunkDefinitionID>::max();
	this->pid = info->pid;
	this->ps_oid = info->ps_oid;
	this->num_tuples_in_extent = info->num_tuples_in_extent;
}

unique_ptr<CatalogEntry> ExtentCatalogEntry::Copy(ClientContext &context) {
	D_ASSERT(false);
	//auto create_info = make_unique<CreateExtentInfo>(schema->name, name, extent_type, eid);
	//return make_unique<ExtentCatalogEntry>(catalog, schema, create_info.get());
}

void ExtentCatalogEntry::SetExtentType(ExtentType extent_type_) {
	extent_type = extent_type_;
}

LocalChunkDefinitionID ExtentCatalogEntry::GetNextChunkDefinitionID() {
	return local_cdf_id_version++;
}

void ExtentCatalogEntry::AddChunkDefinitionID(ChunkDefinitionID cdf_id) {
	chunks.push_back(cdf_id);
}

LocalChunkDefinitionID ExtentCatalogEntry::GetNextAdjListChunkDefinitionID() {
	return local_adjlist_cdf_id_version--;
}

void ExtentCatalogEntry::AddAdjListChunkDefinitionID(ChunkDefinitionID cdf_id) {
	adjlist_chunks.push_back(cdf_id);
}

// ---------------------------------------------------------------------------
// Serialization — ChunkDef entries are embedded inline (not as separate catalog entries)
// ---------------------------------------------------------------------------

void ExtentCatalogEntry::Serialize(CatalogSerializer &ser, ClientContext &ctx) const {
    ser.Write(static_cast<uint32_t>(eid));
    ser.Write(static_cast<uint8_t>(extent_type));
    ser.Write(static_cast<uint32_t>(pid));           // PartitionID (uint16_t) → uint32_t
    ser.Write(static_cast<uint64_t>(ps_oid));
    ser.Write(static_cast<uint64_t>(num_tuples_in_extent));
    ser.Write(static_cast<uint32_t>(local_cdf_id_version.load()));
    ser.Write(static_cast<uint32_t>(local_adjlist_cdf_id_version.load()));

    // Property ChunkDefs: look up from catalog and serialize data inline
    ser.Write(static_cast<uint32_t>(chunks.size()));
    for (auto cdf_oid : chunks) {
        auto *cdf = (ChunkDefinitionCatalogEntry *)catalog->GetEntry(ctx, schema->name, cdf_oid, false);
        ser.WriteString(cdf->name);
        ser.Write(static_cast<uint8_t>(cdf->data_type_id));
        ser.Write(static_cast<uint64_t>(cdf->num_entries_in_column));
    }

    // AdjList ChunkDefs: same approach
    ser.Write(static_cast<uint32_t>(adjlist_chunks.size()));
    for (auto cdf_oid : adjlist_chunks) {
        auto *cdf = (ChunkDefinitionCatalogEntry *)catalog->GetEntry(ctx, schema->name, cdf_oid, false);
        ser.WriteString(cdf->name);
        ser.Write(static_cast<uint8_t>(cdf->data_type_id));
        ser.Write(static_cast<uint64_t>(cdf->num_entries_in_column));
    }
}

void ExtentCatalogEntry::Deserialize(CatalogDeserializer &des, ClientContext &ctx) {
    eid                = des.ReadU32();
    extent_type        = static_cast<ExtentType>(des.ReadU8());
    pid                = static_cast<PartitionID>(des.ReadU32());
    ps_oid             = des.ReadU64();
    num_tuples_in_extent = des.ReadU64();
    local_cdf_id_version.store(des.ReadU32());
    local_adjlist_cdf_id_version.store(des.ReadU32());

    // Recreate property ChunkDefs
    uint32_t n = des.ReadU32();
    for (uint32_t i = 0; i < n; i++) {
        std::string cdf_name  = des.ReadString();
        LogicalTypeId type_id = static_cast<LogicalTypeId>(des.ReadU8());
        uint64_t num_entries  = des.ReadU64();

        CreateChunkDefinitionInfo ci(schema->name, cdf_name, LogicalType(type_id));
        auto *cdf = (ChunkDefinitionCatalogEntry *)catalog->CreateChunkDefinition(ctx, schema, &ci);
        cdf->SetNumEntriesInColumn(static_cast<size_t>(num_entries));
        chunks.push_back(cdf->oid);
    }

    // Recreate adjlist ChunkDefs
    n = des.ReadU32();
    for (uint32_t i = 0; i < n; i++) {
        std::string cdf_name  = des.ReadString();
        LogicalTypeId type_id = static_cast<LogicalTypeId>(des.ReadU8());
        uint64_t num_entries  = des.ReadU64();

        CreateChunkDefinitionInfo ci(schema->name, cdf_name, LogicalType(type_id));
        auto *cdf = (ChunkDefinitionCatalogEntry *)catalog->CreateChunkDefinition(ctx, schema, &ci);
        cdf->SetNumEntriesInColumn(static_cast<size_t>(num_entries));
        adjlist_chunks.push_back(cdf->oid);
    }
}

} // namespace duckdb
